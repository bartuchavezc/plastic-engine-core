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
	segmentVersion = 2 // Version 2: Added Bloom Filter support
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

	// Support both version 1 (no bloom) and version 2 (with bloom)
	if header.Version != segmentVersion && header.Version != 1 {
		return fmt.Errorf("unsupported segment version: %d", header.Version)
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

	// Read term dictionary
	dictData := make([]byte, header.DictSize)
	if _, err := io.ReadFull(s.file, dictData); err != nil {
		return fmt.Errorf("read dict: %w", err)
	}

	var dictEntries []TermDictEntry
	if err := json.Unmarshal(dictData, &dictEntries); err != nil {
		return fmt.Errorf("unmarshal dict: %w", err)
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

	var postings []Posting
	if err := json.Unmarshal(data, &postings); err != nil {
		return nil, err
	}

	return postings, nil
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
	segment  *DiskSegment
	termIDs  []string
	pos      int
	current  []Posting
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
	postingsBuf []byte
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
		postingsBuf: make([]byte, 0, 64*1024),
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

	// Serialize postings
	data, err := json.Marshal(postings)
	if err != nil {
		return err
	}

	// Record current position in buffer (offset will be adjusted at finalize)
	currentPos := int64(len(w.postingsBuf))

	// Write length (4 bytes) + data to buffer
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	w.postingsBuf = append(w.postingsBuf, lenBuf...)
	w.postingsBuf = append(w.postingsBuf, data...)

	// Add to term dictionary (offset is relative to postings section start)
	w.termDict = append(w.termDict, TermDictEntry{
		TermID:        termID,
		PostingOffset: currentPos, // Will be adjusted in Finalize
		DF:            df,
		PostingCount:  len(postings),
	})

	return nil
}

// Finalize completes the segment and returns the path.
func (w *DiskSegmentWriter) Finalize() (string, error) {
	// Update metadata
	w.meta.TermCount = len(w.termDict)

	// Serialize metadata
	metaData, err := json.Marshal(w.meta)
	if err != nil {
		return "", err
	}

	// Serialize bloom filter
	var bloomBuf bytes.Buffer
	if _, err := w.bloom.WriteTo(&bloomBuf); err != nil {
		return "", fmt.Errorf("serialize bloom filter: %w", err)
	}
	bloomData := bloomBuf.Bytes()

	// Header: 24 bytes (magic:4 + version:4 + metaSize:4 + bloomSize:4 + dictSize:4 + checksum:4)
	const headerSize = 24

	// We need to iterate to find the right dict size because offsets affect JSON size
	// Use a two-pass approach: first estimate, then finalize

	// Estimate dict size with current relative offsets
	tempDict, _ := json.Marshal(w.termDict)
	postingsStart := int64(headerSize) + int64(len(metaData)) + int64(len(bloomData)) + int64(len(tempDict))

	// Adjust offsets to absolute positions
	for i := range w.termDict {
		w.termDict[i].PostingOffset += postingsStart
	}

	// Serialize dict with absolute offsets
	dictData, err := json.Marshal(w.termDict)
	if err != nil {
		return "", err
	}

	// If dict size changed, we need to re-adjust
	actualPostingsStart := int64(headerSize) + int64(len(metaData)) + int64(len(bloomData)) + int64(len(dictData))
	if actualPostingsStart != postingsStart {
		// Re-adjust offsets
		diff := actualPostingsStart - postingsStart
		for i := range w.termDict {
			w.termDict[i].PostingOffset += diff
		}
		// Re-serialize
		dictData, err = json.Marshal(w.termDict)
		if err != nil {
			return "", err
		}
	}

	// Calculate checksum (includes meta, bloom, and dict)
	checksumData := append(metaData, bloomData...)
	checksumData = append(checksumData, dictData...)
	checksum := crc32.ChecksumIEEE(checksumData)

	// Build header
	header := segmentHeader{
		Version:   segmentVersion,
		MetaSize:  uint32(len(metaData)),
		BloomSize: uint32(len(bloomData)),
		DictSize:  uint32(len(dictData)),
		Checksum:  checksum,
	}
	copy(header.Magic[:], segmentMagic)

	// Create file and write everything
	file, err := os.Create(w.path)
	if err != nil {
		return "", err
	}

	writer := bufio.NewWriterSize(file, 64*1024)

	// Write header
	if err := binary.Write(writer, binary.BigEndian, &header); err != nil {
		file.Close()
		return "", err
	}

	// Write metadata
	if _, err := writer.Write(metaData); err != nil {
		file.Close()
		return "", err
	}

	// Write bloom filter
	if _, err := writer.Write(bloomData); err != nil {
		file.Close()
		return "", err
	}

	// Write dictionary
	if _, err := writer.Write(dictData); err != nil {
		file.Close()
		return "", err
	}

	// Write postings
	if _, err := writer.Write(w.postingsBuf); err != nil {
		file.Close()
		return "", err
	}

	// Flush and sync
	if err := writer.Flush(); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}

	// Get file size
	info, err := os.Stat(w.path)
	if err == nil {
		w.meta.SizeBytes = info.Size()
	}

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
