package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OpType identifies the type of WAL operation.
type OpType byte

const (
	OpIndex OpType = iota + 1
	OpDelete
	OpAlias
	OpCheckpoint
)

// Entry represents a single WAL entry.
type Entry struct {
	Type      OpType    `json:"type"`
	Timestamp time.Time `json:"ts"`
	Data      []byte    `json:"data"`
}

// IndexOp represents an index operation.
type IndexOp struct {
	TermID    string `json:"term_id"`
	DocID     string `json:"doc_id"`
	TF        int    `json:"tf"`
	Positions []int  `json:"positions,omitempty"`
	Field     string `json:"field"`
	Term      string `json:"term"`
}

// DeleteOp represents a delete operation.
type DeleteOp struct {
	DocID string `json:"doc_id"`
}

// AliasOp represents an alias creation operation.
type AliasOp struct {
	Field  string `json:"field"`
	Term   string `json:"term"`
	TermID string `json:"term_id"`
}

// CheckpointOp marks a checkpoint in the WAL.
type CheckpointOp struct {
	SegmentID string `json:"segment_id"`
	Sequence  uint64 `json:"sequence"`
}

// WAL provides write-ahead logging for durability.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	path     string
	sequence uint64
	syncMode SyncMode
}

// SyncMode determines when to sync to disk.
type SyncMode int

const (
	// SyncNone doesn't sync (fastest, least durable).
	SyncNone SyncMode = iota
	// SyncBatch syncs periodically.
	SyncBatch
	// SyncEvery syncs after every write (slowest, most durable).
	SyncEvery
)

// Options configures the WAL.
type Options struct {
	SyncMode      SyncMode
	BufferSize    int
	MaxSize       int64 // Max size before rotation
}

// DefaultOptions returns default WAL options.
func DefaultOptions() Options {
	return Options{
		SyncMode:   SyncBatch,
		BufferSize: 64 * 1024, // 64KB buffer
		MaxSize:    100 * 1024 * 1024, // 100MB
	}
}

// Open opens or creates a WAL file.
func Open(path string, opts Options) (*WAL, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	bufSize := opts.BufferSize
	if bufSize <= 0 {
		bufSize = 64 * 1024
	}

	w := &WAL{
		file:     file,
		writer:   bufio.NewWriterSize(file, bufSize),
		path:     path,
		sequence: 0,
		syncMode: opts.SyncMode,
	}

	return w, nil
}

// Append writes an entry to the WAL.
func (w *WAL) Append(entry Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sequence++
	entry.Timestamp = time.Now().UTC()

	// Encode entry
	data, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("marshal entry: %w", err)
	}

	// Write: length (4 bytes) + checksum (4 bytes) + data
	length := uint32(len(data))
	checksum := crc32.ChecksumIEEE(data)

	if err := binary.Write(w.writer, binary.BigEndian, length); err != nil {
		return 0, fmt.Errorf("write length: %w", err)
	}
	if err := binary.Write(w.writer, binary.BigEndian, checksum); err != nil {
		return 0, fmt.Errorf("write checksum: %w", err)
	}
	if _, err := w.writer.Write(data); err != nil {
		return 0, fmt.Errorf("write data: %w", err)
	}

	if w.syncMode == SyncEvery {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}

	return w.sequence, nil
}

// AppendIndex appends an index operation.
func (w *WAL) AppendIndex(op IndexOp) (uint64, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return 0, err
	}
	return w.Append(Entry{Type: OpIndex, Data: data})
}

// AppendAlias appends an alias operation.
func (w *WAL) AppendAlias(op AliasOp) (uint64, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return 0, err
	}
	return w.Append(Entry{Type: OpAlias, Data: data})
}

// AppendCheckpoint appends a checkpoint marker.
func (w *WAL) AppendCheckpoint(op CheckpointOp) (uint64, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return 0, err
	}
	return w.Append(Entry{Type: OpCheckpoint, Data: data})
}

// Sync flushes and syncs the WAL to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}
	return nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Sequence returns the current sequence number.
func (w *WAL) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}

// Reader reads WAL entries.
type Reader struct {
	file   *os.File
	reader *bufio.Reader
}

// NewReader creates a WAL reader.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

// Read reads the next entry. Returns io.EOF when done.
func (r *Reader) Read() (Entry, error) {
	var length uint32
	var checksum uint32

	if err := binary.Read(r.reader, binary.BigEndian, &length); err != nil {
		return Entry{}, err
	}
	if err := binary.Read(r.reader, binary.BigEndian, &checksum); err != nil {
		return Entry{}, err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return Entry{}, err
	}

	// Verify checksum
	if crc32.ChecksumIEEE(data) != checksum {
		return Entry{}, fmt.Errorf("checksum mismatch")
	}

	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return Entry{}, err
	}

	return entry, nil
}

// ReadAll reads all entries from the WAL.
func (r *Reader) ReadAll() ([]Entry, error) {
	var entries []Entry
	for {
		entry, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Truncate removes all entries from the WAL.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	w.writer.Reset(w.file)
	w.sequence = 0

	return nil
}

// TruncateAfterCheckpoint removes entries after the last checkpoint.
func TruncateAfterCheckpoint(path string, checkpointSeq uint64) error {
	// Read all entries
	reader, err := NewReader(path)
	if err != nil {
		return err
	}

	entries, err := reader.ReadAll()
	reader.Close()
	if err != nil && err != io.EOF {
		return err
	}

	// Find checkpoint position
	var keepEntries []Entry
	for _, e := range entries {
		keepEntries = append(keepEntries, e)
		if e.Type == OpCheckpoint {
			var cp CheckpointOp
			if err := json.Unmarshal(e.Data, &cp); err == nil {
				if cp.Sequence >= checkpointSeq {
					break
				}
			}
		}
	}

	// Rewrite WAL with only kept entries
	tempPath := path + ".tmp"
	wal, err := Open(tempPath, DefaultOptions())
	if err != nil {
		return err
	}

	for _, e := range keepEntries {
		if _, err := wal.Append(e); err != nil {
			wal.Close()
			os.Remove(tempPath)
			return err
		}
	}

	if err := wal.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	return os.Rename(tempPath, path)
}
