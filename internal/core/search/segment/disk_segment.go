package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	segmentMagic   = "PLST"
	segmentVersion = 3 // Version 3: Binary termDict (was JSON in v1-v2)
)

// DiskSegment is an immutable segment stored on disk.
type DiskSegment struct {
	mu sync.RWMutex

	path     string
	file     *os.File
	meta     SegmentMeta
	termDict map[string]TermDictEntry // term_id -> dict entry
	bloom    *BloomFilter             // Bloom filter for fast negative lookups

	// Memory-mapped or cached for performance
	dictLoaded bool
}

// segmentHeader is written at the start of the file.
// Version 2 format: Header + Meta + BloomFilter + TermDict + Postings
type segmentHeader struct {
	Magic     [4]byte
	Version   uint32
	MetaSize  uint32
	BloomSize uint32 // NEW: Size of bloom filter section
	DictSize  uint32
	Checksum  uint32
}

// OpenDiskSegment opens an existing disk segment.
func OpenDiskSegment(path string) (*DiskSegment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	seg := &DiskSegment{
		path:     path,
		file:     file,
		termDict: make(map[string]TermDictEntry),
	}

	if err := seg.loadHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return seg, nil
}

// loadHeader reads and validates the segment header.
func (s *DiskSegment) loadHeader() error {
	var header segmentHeader
	if err := binary.Read(s.file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if string(header.Magic[:]) != segmentMagic {
		return fmt.Errorf("invalid segment magic: %s", header.Magic)
	}

	// Support versions 1-3
	if header.Version > segmentVersion {
		return fmt.Errorf("unsupported segment version: %d (max supported: %d)", header.Version, segmentVersion)
	}

	// Read metadata
	metaData := make([]byte, header.MetaSize)
	if _, err := io.ReadFull(s.file, metaData); err != nil {
		return fmt.Errorf("read meta: %w", err)
	}

	if err := json.Unmarshal(metaData, &s.meta); err != nil {
		return fmt.Errorf("unmarshal meta: %w", err)
	}

	// Read bloom filter (version 2+)
	if header.Version >= 2 && header.BloomSize > 0 {
		bloomData := make([]byte, header.BloomSize)
		if _, err := io.ReadFull(s.file, bloomData); err != nil {
			return fmt.Errorf("read bloom filter: %w", err)
		}
		bloom, err := ReadBloomFilter(bytes.NewReader(bloomData))
		if err != nil {
			return fmt.Errorf("parse bloom filter: %w", err)
		}
		s.bloom = bloom
	}

	// Read term dictionary (binary format)
	dictData := make([]byte, header.DictSize)
	if _, err := io.ReadFull(s.file, dictData); err != nil {
		return fmt.Errorf("read dict: %w", err)
	}

	dictEntries, err := decodeTermDictBinary(dictData)
	if err != nil {
		return fmt.Errorf("decode dict: %w", err)
	}

	for _, entry := range dictEntries {
		s.termDict[entry.TermID] = entry
	}

	s.dictLoaded = true
	return nil
}

// ID returns the segment identifier.
func (s *DiskSegment) ID() string {
	return s.meta.ID
}

// Meta returns segment metadata.
func (s *DiskSegment) Meta() SegmentMeta {
	return s.meta
}

// Path returns the file path.
func (s *DiskSegment) Path() string {
	return s.path
}

// Search finds all postings for a term.
func (s *DiskSegment) Search(termID string) []Hit {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Fast path: check bloom filter first (avoids dict lookup if term definitely not present)
	if s.bloom != nil && !s.bloom.MayContain(termID) {
		return nil // Definitely not in this segment
	}

	entry, ok := s.termDict[termID]
	if !ok {
		return nil
	}

	postings, err := s.readPostings(entry.PostingOffset, entry.PostingCount)
	if err != nil {
		return nil
	}

	hits := make([]Hit, len(postings))
	for i, p := range postings {
		hits[i] = Hit{
			DocID:     p.DocID,
			TermID:    termID,
			TF:        p.TF,
			Positions: p.Positions,
			SegmentID: s.meta.ID,
		}
	}

	return hits
}

// readPostings reads posting list from the file.
func (s *DiskSegment) readPostings(offset int64, count int) ([]Posting, error) {
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read posting data length
	var dataLen uint32
	if err := binary.Read(s.file, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}

	// Read posting data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(s.file, data); err != nil {
		return nil, err
	}

	// Decode binary postings
	return decodePostingsBinary(data, count)
}

// decodePostingsBinary decodes postings from binary format.
// Format per posting: docIDLen(2) + docID + tf(4) + posCount(2) + positions(4 each)
func decodePostingsBinary(data []byte, count int) ([]Posting, error) {
	postings := make([]Posting, 0, count)
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read DocID
		var docIDLen uint16
		if err := binary.Read(buf, binary.BigEndian, &docIDLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		docID := make([]byte, docIDLen)
		if _, err := io.ReadFull(buf, docID); err != nil {
			return nil, err
		}

		// Read TF
		var tf uint32
		if err := binary.Read(buf, binary.BigEndian, &tf); err != nil {
			return nil, err
		}

		// Read positions count and positions
		var posCount uint16
		if err := binary.Read(buf, binary.BigEndian, &posCount); err != nil {
			return nil, err
		}
		positions := make([]int, posCount)
		for i := uint16(0); i < posCount; i++ {
			var pos uint32
			if err := binary.Read(buf, binary.BigEndian, &pos); err != nil {
				return nil, err
			}
			positions[i] = int(pos)
		}

		postings = append(postings, Posting{
			DocID:     string(docID),
			TF:        int(tf),
			Positions: positions,
		})
	}

	return postings, nil
}

// encodeTermDictBinary encodes the term dictionary to binary format.
// Format: count(4) + entries (each: termIDLen(2) + termID + offset(8) + df(8) + postingCount(4))
func encodeTermDictBinary(entries []TermDictEntry) []byte {
	// Estimate size: 30 bytes per entry average
	buf := bytes.NewBuffer(make([]byte, 0, len(entries)*30+4))

	// Write entry count
	var temp4 [4]byte
	binary.BigEndian.PutUint32(temp4[:], uint32(len(entries)))
	buf.Write(temp4[:])

	var temp2 [2]byte
	var temp8 [8]byte

	for _, e := range entries {
		termBytes := []byte(e.TermID)

		// TermID length (2 bytes) + TermID
		binary.BigEndian.PutUint16(temp2[:], uint16(len(termBytes)))
		buf.Write(temp2[:])
		buf.Write(termBytes)

		// PostingOffset (8 bytes)
		binary.BigEndian.PutUint64(temp8[:], uint64(e.PostingOffset))
		buf.Write(temp8[:])

		// DF (8 bytes)
		binary.BigEndian.PutUint64(temp8[:], uint64(e.DF))
		buf.Write(temp8[:])

		// PostingCount (4 bytes)
		binary.BigEndian.PutUint32(temp4[:], uint32(e.PostingCount))
		buf.Write(temp4[:])
	}

	return buf.Bytes()
}

// decodeTermDictBinary decodes the term dictionary from binary format.
func decodeTermDictBinary(data []byte) ([]TermDictEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("dict data too short")
	}

	buf := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	entries := make([]TermDictEntry, 0, count)

	for i := uint32(0); i < count; i++ {
		var termIDLen uint16
		if err := binary.Read(buf, binary.BigEndian, &termIDLen); err != nil {
			return nil, err
		}

		termID := make([]byte, termIDLen)
		if _, err := io.ReadFull(buf, termID); err != nil {
			return nil, err
		}

		var offset uint64
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, err
		}

		var df uint64
		if err := binary.Read(buf, binary.BigEndian, &df); err != nil {
			return nil, err
		}

		var postingCount uint32
		if err := binary.Read(buf, binary.BigEndian, &postingCount); err != nil {
			return nil, err
		}

		entries = append(entries, TermDictEntry{
			TermID:       string(termID),
			PostingOffset: int64(offset),
			DF:           int64(df),
			PostingCount: int(postingCount),
		})
	}

	return entries, nil
}

// GetLocalDF returns the document frequency for a term in this segment.
func (s *DiskSegment) GetLocalDF(termID string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Fast path: check bloom filter first
	if s.bloom != nil && !s.bloom.MayContain(termID) {
		return 0
	}

	if entry, ok := s.termDict[termID]; ok {
		return entry.DF
	}
	return 0
}

// DocCount returns the number of documents in this segment.
func (s *DiskSegment) DocCount() int {
	return s.meta.DocCount
}

// TermCount returns the number of unique terms.
func (s *DiskSegment) TermCount() int {
	return len(s.termDict)
}

// Close closes the segment file.
func (s *DiskSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Iterator returns an iterator over all terms.
func (s *DiskSegment) Iterator() SegmentIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect and sort term IDs
	termIDs := make([]string, 0, len(s.termDict))
	for termID := range s.termDict {
		termIDs = append(termIDs, termID)
	}
	sort.Strings(termIDs)

	return &diskSegmentIterator{
		segment: s,
		termIDs: termIDs,
		pos:     -1,
	}
}

type diskSegmentIterator struct {
	segment   *DiskSegment
	termIDs   []string
	pos       int
	current   []Posting
	currentDF int64
}

func (it *diskSegmentIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.termIDs) {
		return false
	}

	termID := it.termIDs[it.pos]
	entry := it.segment.termDict[termID]
	it.currentDF = entry.DF

	postings, err := it.segment.readPostings(entry.PostingOffset, entry.PostingCount)
	if err != nil {
		it.current = nil
		return true
	}
	it.current = postings
	return true
}

func (it *diskSegmentIterator) Term() string {
	if it.pos < 0 || it.pos >= len(it.termIDs) {
		return ""
	}
	return it.termIDs[it.pos]
}

func (it *diskSegmentIterator) Postings() []Posting {
	return it.current
}

func (it *diskSegmentIterator) DF() int64 {
	return it.currentDF
}

func (it *diskSegmentIterator) Close() error {
	return nil
}

// DiskSegmentWriter writes a new segment to disk.
// File layout:
//   - Header (20 bytes): magic(4) + version(4) + metaSize(4) + dictSize(4) + checksum(4)
//   - Metadata (JSON)
//   - Term Dictionary (JSON)
//   - Postings (binary: for each term: length(4) + data)
type DiskSegmentWriter struct {
	path     string
	dir      string
	id       string
	meta     SegmentMeta
	termDict []TermDictEntry
	bloom    *BloomFilter // Bloom filter for fast negative lookups

	// Postings are written to a temp buffer, then flushed at finalize
	postingsBuf *bytes.Buffer
}

// NewDiskSegmentWriter creates a writer for a new segment.
func NewDiskSegmentWriter(dir string, id string) (*DiskSegmentWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &DiskSegmentWriter{
		path:        filepath.Join(dir, fmt.Sprintf("segment_%s.seg", id)),
		dir:         dir,
		id:          id,
		bloom:       NewBloomFilter(DefaultBloomConfig()),
		termDict:    make([]TermDictEntry, 0, 1024),
		postingsBuf: bytes.NewBuffer(make([]byte, 0, 1024*1024)),
		meta: SegmentMeta{
			ID:      id,
			Version: segmentVersion,
		},
	}, nil
}

// SetMeta sets the segment metadata.
func (w *DiskSegmentWriter) SetMeta(meta SegmentMeta) {
	w.meta = meta
	w.meta.Version = segmentVersion
	w.meta.ID = w.id
}

// AddTerm adds a term with its postings.
func (w *DiskSegmentWriter) AddTerm(termID string, postings []Posting, df int64) error {
	if len(postings) == 0 {
		return nil
	}

	// Add to bloom filter for fast negative lookups
	w.bloom.Add(termID)

	// Sort postings by doc ID
	sort.Slice(postings, func(i, j int) bool {
		return postings[i].DocID < postings[j].DocID
	})

	// 1. Guardamos la posición actual ANTES de escribir
	currentPos := int64(w.postingsBuf.Len())

	// 2. Dejamos espacio para el tamaño (4 bytes)
	// Escribimos un placeholder que luego sobreescribiremos
	sizeOffset := w.postingsBuf.Len()
	placeholder := []byte{0, 0, 0, 0}
	w.postingsBuf.Write(placeholder)

	// 3. Encode DIRECTO al buffer del writer
	startData := w.postingsBuf.Len()
	encodePostingsBinaryTo(w.postingsBuf, postings)
	endData := w.postingsBuf.Len()

	// 4. Calculamos cuánto escribimos y parcheamos el tamaño al principio
	dataLen := uint32(endData - startData)
	allBytes := w.postingsBuf.Bytes() // Acceso directo al slice interno
	binary.BigEndian.PutUint32(allBytes[sizeOffset:], dataLen)

	// Add to term dictionary (offset is relative to postings section start)
	w.termDict = append(w.termDict, TermDictEntry{
		TermID:        termID,
		PostingOffset: currentPos, // Will be adjusted in Finalize
		DF:            df,
		PostingCount:  len(postings),
	})

	return nil
}

func encodePostingsBinaryTo(buf *bytes.Buffer, postings []Posting) {
	// Usamos pequeños arrays en el stack para no alocar
	var temp4 [4]byte
	var temp2 [2]byte

	for _, p := range postings {
		// DocID length + DocID
		// Nota: []byte(string) aloca, si puedes usar casting unsafe es mejor,
		// pero por ahora esto es mucho mejor que lo de antes.
		docIDBytes := []byte(p.DocID)
		binary.BigEndian.PutUint16(temp2[:], uint16(len(docIDBytes)))
		buf.Write(temp2[:])
		buf.Write(docIDBytes)

		// TF
		binary.BigEndian.PutUint32(temp4[:], uint32(p.TF))
		buf.Write(temp4[:])

		// Positions count + positions
		binary.BigEndian.PutUint16(temp2[:], uint16(len(p.Positions)))
		buf.Write(temp2[:])
		for _, pos := range p.Positions {
			binary.BigEndian.PutUint32(temp4[:], uint32(pos))
			buf.Write(temp4[:])
		}
	}
}

// encodePostingsBinary encodes postings to binary format.
// Format per posting: docIDLen(2) + docID + tf(4) + posCount(2) + positions(4 each)
// This is ~50% smaller than JSON and 10x faster to encode/decode.
func encodePostingsBinary(postings []Posting) []byte {
	// Estimate size: avg 20 bytes per posting
	buf := make([]byte, 0, len(postings)*24)

	for _, p := range postings {
		// DocID length (2 bytes) + DocID
		docIDBytes := []byte(p.DocID)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(docIDBytes)))
		buf = append(buf, docIDBytes...)

		// TF (4 bytes)
		buf = binary.BigEndian.AppendUint32(buf, uint32(p.TF))

		// Positions count (2 bytes) + positions (4 bytes each)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(p.Positions)))
		for _, pos := range p.Positions {
			buf = binary.BigEndian.AppendUint32(buf, uint32(pos))
		}
	}

	return buf
}

func (w *DiskSegmentWriter) Finalize() (string, error) {
	w.meta.TermCount = len(w.termDict)

	// 1. Metadata JSON (es pequeño, no pasa nada)
	metaData, err := json.Marshal(w.meta)
	if err != nil {
		return "", err
	}

	// 2. Bloom Filter (ya lo tienes en un buffer)
	var bloomBuf bytes.Buffer
	w.bloom.WriteTo(&bloomBuf)
	bloomData := bloomBuf.Bytes()

	// 3. Diccionario (binary format)
	// First pass: encode with relative offsets to get size
	dictData := encodeTermDictBinary(w.termDict)

	// Calculate absolute offset for postings section (Header=24)
	postingsStart := int64(24 + len(metaData) + len(bloomData) + len(dictData))

	// Adjust offsets to absolute positions
	for i := range w.termDict {
		w.termDict[i].PostingOffset += postingsStart
	}

	// Re-encode with absolute offsets
	dictData = encodeTermDictBinary(w.termDict)

	// 4. Checksum EFICIENTE (sin appends)
	h := crc32.NewIEEE()
	h.Write(metaData)
	h.Write(bloomData)
	h.Write(dictData)
	// Nota: El checksum usualmente no incluye las postings porque son gigantes,
	// pero si lo quieres, haz h.Write(w.postingsBuf.Bytes()) aquí.
	checksum := h.Sum32()

	// 5. Preparar Header
	header := segmentHeader{
		Version:   segmentVersion,
		MetaSize:  uint32(len(metaData)),
		BloomSize: uint32(len(bloomData)),
		DictSize:  uint32(len(dictData)),
		Checksum:  checksum,
	}
	copy(header.Magic[:], segmentMagic)

	// 6. Escritura a disco con Buffer
	file, err := os.Create(w.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 128*1024) // 128KB de buffer para disco

	// Escribir en orden
	binary.Write(writer, binary.BigEndian, &header)
	writer.Write(metaData)
	writer.Write(bloomData)
	writer.Write(dictData)

	// El gran final: Las postings (ya son binarias y están en un buffer)
	// Usamos WriteTo para que el buffer se vuelque directamente al writer de archivo
	if _, err := w.postingsBuf.WriteTo(writer); err != nil {
		return "", err
	}

	if err := writer.Flush(); err != nil {
		return "", err
	}

	// Sincronización física con el disco
	file.Sync()

	return w.path, nil
}

// Abort discards the segment being written.
func (w *DiskSegmentWriter) Abort() error {
	os.Remove(w.path)
	return nil
}

// FlushMemSegment writes a memory segment to disk.
func FlushMemSegment(memSeg *MemSegment, dir string) (*DiskSegment, error) {
	writer, err := NewDiskSegmentWriter(dir, memSeg.ID())
	if err != nil {
		return nil, err
	}

	// Set metadata
	meta := memSeg.Meta()
	writer.SetMeta(meta)

	// Iterate over all terms and write them
	iter := memSeg.Iterator()
	defer iter.Close()

	for iter.Next() {
		termID := iter.Term()
		postings := iter.Postings()
		df := iter.DF()

		if err := writer.AddTerm(termID, postings, df); err != nil {
			writer.Abort()
			return nil, err
		}
	}

	// Finalize
	path, err := writer.Finalize()
	if err != nil {
		writer.Abort()
		return nil, err
	}

	// Open the written segment
	return OpenDiskSegment(path)
}
