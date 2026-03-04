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
	termDict []TermDictEntry // sorted by TermID for binary search (3-5x less RAM than map)
	bloom    *BloomFilter    // Bloom filter for fast negative lookups

	// Memory-mapped or cached for performance
	dictLoaded bool

	// Lazy-load support: termDict + bloom loaded on first access
	header   segmentHeader
	dictOnce sync.Once
	dictErr  error
}

// findTerm looks up a term in the sorted termDict via binary search. O(log N).
func (s *DiskSegment) findTerm(termID string) (TermDictEntry, bool) {
	i := sort.Search(len(s.termDict), func(i int) bool {
		return s.termDict[i].TermID >= termID
	})
	if i < len(s.termDict) && s.termDict[i].TermID == termID {
		return s.termDict[i], true
	}
	return TermDictEntry{}, false
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

// OpenDiskSegment opens an existing disk segment (eager: loads meta + dict).
// Used at startup for recovery and in tests.
func OpenDiskSegment(path string) (*DiskSegment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	seg := &DiskSegment{
		path: path,
		file: file,
	}

	if err := seg.loadMeta(); err != nil {
		file.Close()
		return nil, err
	}

	if err := seg.loadDict(); err != nil {
		file.Close()
		return nil, err
	}

	return seg, nil
}

// OpenDiskSegmentLazy opens a disk segment but defers loading the bloom filter
// and termDict until they are first needed. This avoids RAM spikes during
// flush and merge — the termDict is only loaded when a query touches the segment.
func OpenDiskSegmentLazy(path string) (*DiskSegment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	seg := &DiskSegment{
		path: path,
		file: file,
	}

	if err := seg.loadMeta(); err != nil {
		file.Close()
		return nil, err
	}

	return seg, nil
}

// loadMeta reads the 24-byte header + metadata JSON. Fast (~μs), no termDict allocation.
func (s *DiskSegment) loadMeta() error {
	if err := binary.Read(s.file, binary.BigEndian, &s.header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if string(s.header.Magic[:]) != segmentMagic {
		return fmt.Errorf("invalid segment magic: %s", s.header.Magic)
	}

	if s.header.Version > segmentVersion {
		return fmt.Errorf("unsupported segment version: %d (max supported: %d)", s.header.Version, segmentVersion)
	}

	metaData := make([]byte, s.header.MetaSize)
	if _, err := io.ReadFull(s.file, metaData); err != nil {
		return fmt.Errorf("read meta: %w", err)
	}

	if err := json.Unmarshal(metaData, &s.meta); err != nil {
		return fmt.Errorf("unmarshal meta: %w", err)
	}

	return nil
}

// loadDict loads the bloom filter + termDict from disk.
// Called eagerly by OpenDiskSegment or lazily via ensureDictLoaded.
func (s *DiskSegment) loadDict() error {
	// Position right after metadata (header is 24 bytes).
	offset := int64(24 + s.header.MetaSize)

	// Read bloom filter (version 2+)
	if s.header.Version >= 2 && s.header.BloomSize > 0 {
		bloomData := make([]byte, s.header.BloomSize)
		if _, err := s.file.ReadAt(bloomData, offset); err != nil {
			return fmt.Errorf("read bloom filter: %w", err)
		}
		offset += int64(s.header.BloomSize)
		bloom, err := ReadBloomFilter(bytes.NewReader(bloomData))
		if err != nil {
			return fmt.Errorf("parse bloom filter: %w", err)
		}
		s.bloom = bloom
	}

	// Read term dictionary (binary format)
	dictData := make([]byte, s.header.DictSize)
	if _, err := s.file.ReadAt(dictData, offset); err != nil {
		return fmt.Errorf("read dict: %w", err)
	}

	dictEntries, err := decodeTermDictBinary(dictData)
	if err != nil {
		return fmt.Errorf("decode dict: %w", err)
	}

	// Verify sorted (O(N) scan) instead of sort (O(N log N)).
	sorted := true
	for i := 1; i < len(dictEntries); i++ {
		if dictEntries[i].TermID < dictEntries[i-1].TermID {
			sorted = false
			break
		}
	}
	if !sorted {
		sort.Slice(dictEntries, func(i, j int) bool {
			return dictEntries[i].TermID < dictEntries[j].TermID
		})
	}
	s.termDict = dictEntries

	s.dictLoaded = true
	return nil
}

// ensureDictLoaded triggers lazy loading of termDict + bloom filter.
func (s *DiskSegment) ensureDictLoaded() error {
	s.dictOnce.Do(func() {
		if !s.dictLoaded {
			s.dictErr = s.loadDict()
		}
	})
	return s.dictErr
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
	if err := s.ensureDictLoaded(); err != nil {
		return nil
	}

	// Lock only for in-memory structures (bloom filter + termDict).
	s.mu.RLock()
	if s.bloom != nil && !s.bloom.MayContain(termID) {
		s.mu.RUnlock()
		return nil // Definitely not in this segment
	}

	entry, found := s.findTerm(termID)
	s.mu.RUnlock() // Release before disk I/O — pread is thread-safe.

	if !found {
		return nil
	}

	// Disk I/O without lock (pread does not use file offset).
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

// readPostings reads posting list from the file using pread (ReadAt).
// pread is atomic and does not modify the file offset, so multiple goroutines
// can read from the same file descriptor concurrently without a lock.
func (s *DiskSegment) readPostings(offset int64, count int) ([]Posting, error) {
	// Read data length (4 bytes) via pread — no seek, no lock needed.
	var lenBuf [4]byte
	if _, err := s.file.ReadAt(lenBuf[:], offset); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])

	// Read posting data via pread.
	data := make([]byte, dataLen)
	if _, err := s.file.ReadAt(data, offset+4); err != nil {
		return nil, err
	}

	// Decode binary postings
	return decodePostingsBinary(data, count)
}

// decodePostingsBinary decodes postings from binary format using direct slice indexing.
// Format per posting: docIDLen(2) + docID + tf(4) + posCount(2) + positions(4 each)
// ~3x faster than the binary.Read version (no reflection, no interface dispatch).
func decodePostingsBinary(data []byte, count int) ([]Posting, error) {
	postings := make([]Posting, 0, count)
	off := 0

	for off < len(data) {
		// DocID length (2 bytes)
		if off+2 > len(data) {
			break
		}
		docIDLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2

		// DocID
		if off+docIDLen > len(data) {
			return nil, fmt.Errorf("truncated docID at offset %d", off)
		}
		docID := string(data[off : off+docIDLen])
		off += docIDLen

		// TF (4 bytes)
		if off+4 > len(data) {
			return nil, fmt.Errorf("truncated TF at offset %d", off)
		}
		tf := int(binary.BigEndian.Uint32(data[off:]))
		off += 4

		// Position count (2 bytes)
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated posCount at offset %d", off)
		}
		posCount := int(binary.BigEndian.Uint16(data[off:]))
		off += 2

		// Positions (4 bytes each)
		if off+posCount*4 > len(data) {
			return nil, fmt.Errorf("truncated positions at offset %d", off)
		}
		positions := make([]int, posCount)
		for i := 0; i < posCount; i++ {
			positions[i] = int(binary.BigEndian.Uint32(data[off:]))
			off += 4
		}

		postings = append(postings, Posting{
			DocID:     docID,
			TF:        tf,
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
// Uses direct slice indexing (~3x faster than binary.Read which uses reflection).
func decodeTermDictBinary(data []byte) ([]TermDictEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("dict data too short")
	}

	count := int(binary.BigEndian.Uint32(data[:4]))
	entries := make([]TermDictEntry, 0, count)
	off := 4

	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated at entry %d: termID length", i)
		}
		termIDLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2

		if off+termIDLen > len(data) {
			return nil, fmt.Errorf("truncated at entry %d: termID", i)
		}
		termID := string(data[off : off+termIDLen])
		off += termIDLen

		if off+20 > len(data) {
			return nil, fmt.Errorf("truncated at entry %d: fields", i)
		}
		offset := int64(binary.BigEndian.Uint64(data[off:]))
		df := int64(binary.BigEndian.Uint64(data[off+8:]))
		postingCount := int(binary.BigEndian.Uint32(data[off+16:]))
		off += 20

		entries = append(entries, TermDictEntry{
			TermID:        termID,
			PostingOffset: offset,
			DF:            df,
			PostingCount:  postingCount,
		})
	}

	return entries, nil
}

// GetLocalDF returns the document frequency for a term in this segment.
func (s *DiskSegment) GetLocalDF(termID string) int64 {
	if err := s.ensureDictLoaded(); err != nil {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Fast path: check bloom filter first
	if s.bloom != nil && !s.bloom.MayContain(termID) {
		return 0
	}

	if entry, found := s.findTerm(termID); found {
		return entry.DF
	}
	return 0
}

// GetTermsWithPrefix returns all termIDs that start with the given prefix.
// Uses binary search to find the start position, then scans forward.
func (s *DiskSegment) GetTermsWithPrefix(prefix string) []string {
	if err := s.ensureDictLoaded(); err != nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Binary search for first entry >= prefix
	start := sort.Search(len(s.termDict), func(i int) bool {
		return s.termDict[i].TermID >= prefix
	})

	var result []string
	for i := start; i < len(s.termDict); i++ {
		tid := s.termDict[i].TermID
		if len(tid) < len(prefix) || tid[:len(prefix)] != prefix {
			break
		}
		result = append(result, tid)
	}
	return result
}

// DocCount returns the number of documents in this segment.
func (s *DiskSegment) DocCount() int {
	return s.meta.DocCount
}

// TermCount returns the number of unique terms.
// Uses meta.TermCount to avoid loading the full termDict.
func (s *DiskSegment) TermCount() int {
	return s.meta.TermCount
}

// Close closes the segment file and releases termDict memory.
func (s *DiskSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.termDict = nil // Release termDict memory eagerly
	s.bloom = nil
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Iterator returns an iterator over all terms using buffered sequential I/O.
// Opens a separate file handle with a 256KB buffer for efficient sequential reads
// during merge operations (avoids per-term syscalls).
func (s *DiskSegment) Iterator() SegmentIterator {
	if err := s.ensureDictLoaded(); err != nil {
		return &diskSegmentIterator{segment: s, pos: -1}
	}

	if len(s.termDict) == 0 {
		return &diskSegmentIterator{segment: s, pos: -1}
	}

	// Open a dedicated file handle so the iterator doesn't interfere with
	// concurrent point reads on s.file, and wrap it in a buffered reader.
	f, err := os.Open(s.path)
	if err != nil {
		// Fallback to unbuffered reads via the shared file handle.
		return &diskSegmentIterator{segment: s, pos: -1}
	}

	// Seek to the first posting offset.
	firstOffset := s.termDict[0].PostingOffset
	if _, err := f.Seek(firstOffset, io.SeekStart); err != nil {
		f.Close()
		return &diskSegmentIterator{segment: s, pos: -1}
	}

	return &diskSegmentIterator{
		segment: s,
		pos:     -1,
		file:    f,
		reader:  bufio.NewReaderSize(f, 256*1024), // 256KB read-ahead buffer
	}
}

type diskSegmentIterator struct {
	segment   *DiskSegment
	pos       int
	current   []Posting
	currentDF int64
	file      *os.File     // dedicated handle for this iterator (nil = use segment.file)
	reader    *bufio.Reader // buffered reader for sequential access
}

func (it *diskSegmentIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.segment.termDict) {
		return false
	}

	entry := it.segment.termDict[it.pos]
	it.currentDF = entry.DF

	if it.reader != nil {
		// Fast path: sequential buffered read (no seek needed — postings are
		// stored in termDict order, so we just read forward).
		postings, err := readPostingsFromReader(it.reader, entry.PostingCount)
		if err != nil {
			it.current = nil
			return true
		}
		it.current = postings
	} else {
		// Fallback: random access via shared file handle.
		postings, err := it.segment.readPostings(entry.PostingOffset, entry.PostingCount)
		if err != nil {
			it.current = nil
			return true
		}
		it.current = postings
	}
	return true
}

// readPostingsFromReader reads postings from a buffered reader (no seek).
func readPostingsFromReader(r *bufio.Reader, count int) ([]Posting, error) {
	// Read data length (4 bytes)
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return decodePostingsBinary(data, count)
}

func (it *diskSegmentIterator) Term() string {
	if it.pos < 0 || it.pos >= len(it.segment.termDict) {
		return ""
	}
	return it.segment.termDict[it.pos].TermID
}

func (it *diskSegmentIterator) Postings() []Posting {
	return it.current
}

func (it *diskSegmentIterator) DF() int64 {
	return it.currentDF
}

func (it *diskSegmentIterator) Close() error {
	if it.file != nil {
		return it.file.Close()
	}
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

// AddTermSorted is like AddTerm but assumes postings are already sorted by DocID.
// Used by mergeSegments where postings come from a k-way merge of already-sorted runs.
func (w *DiskSegmentWriter) AddTermSorted(termID string, postings []Posting, df int64) error {
	if len(postings) == 0 {
		return nil
	}

	w.bloom.Add(termID)

	currentPos := int64(w.postingsBuf.Len())

	sizeOffset := w.postingsBuf.Len()
	placeholder := []byte{0, 0, 0, 0}
	w.postingsBuf.Write(placeholder)

	startData := w.postingsBuf.Len()
	encodePostingsBinaryTo(w.postingsBuf, postings)
	endData := w.postingsBuf.Len()

	dataLen := uint32(endData - startData)
	allBytes := w.postingsBuf.Bytes()
	binary.BigEndian.PutUint32(allBytes[sizeOffset:], dataLen)

	w.termDict = append(w.termDict, TermDictEntry{
		TermID:        termID,
		PostingOffset: currentPos,
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
	// Calculate dictSize without encoding (each entry: 2+len(termID)+8+8+4)
	dictSize := 4 // count header
	for i := range w.termDict {
		dictSize += 2 + len(w.termDict[i].TermID) + 8 + 8 + 4
	}

	// Calculate absolute offset for postings section (Header=24)
	postingsStart := int64(24 + len(metaData) + len(bloomData) + dictSize)

	// Adjust offsets to absolute positions BEFORE encoding
	for i := range w.termDict {
		w.termDict[i].PostingOffset += postingsStart
	}

	// Encode once with absolute offsets
	dictData := encodeTermDictBinary(w.termDict)

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

	// fsync removed — WAL provides crash recovery, OS page cache provides
	// process-crash safety. The syncWorker's periodic WAL.Sync() limits the
	// power-failure data loss window to ~1s.

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

	// Open the written segment lazily — termDict will be loaded on first query.
	return OpenDiskSegmentLazy(path)
}
