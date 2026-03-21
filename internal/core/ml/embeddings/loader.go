package embeddings

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

// LoadFromBinary reads a binary embedding file and batch-inserts into the store.
//
// Binary format:
//
//	[count:uint32][dim:uint32][entries...]
//	Each entry: [termLen:uint16][term:termLen bytes][vector:dim×float32 LE]
func LoadFromBinary(store *PebbleEmbeddingStore, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open embeddings file: %w", err)
	}
	defer f.Close()

	var header [8]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}

	count := binary.LittleEndian.Uint32(header[0:4])
	dim := binary.LittleEndian.Uint32(header[4:8])

	if int(dim) != store.dim {
		return 0, fmt.Errorf("file dim %d != store dim %d", dim, store.dim)
	}

	const batchSize = 1000
	batch := make(map[string][]float32, batchSize)
	loaded := 0

	vecBuf := make([]byte, int(dim)*4)
	var termLenBuf [2]byte

	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(f, termLenBuf[:]); err != nil {
			return loaded, fmt.Errorf("read term length at entry %d: %w", i, err)
		}
		termLen := binary.LittleEndian.Uint16(termLenBuf[:])

		termBuf := make([]byte, termLen)
		if _, err := io.ReadFull(f, termBuf); err != nil {
			return loaded, fmt.Errorf("read term at entry %d: %w", i, err)
		}

		if _, err := io.ReadFull(f, vecBuf); err != nil {
			return loaded, fmt.Errorf("read vector at entry %d: %w", i, err)
		}

		vec := make([]float32, dim)
		for j := 0; j < int(dim); j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(vecBuf[j*4:]))
		}

		batch[string(termBuf)] = vec

		if len(batch) >= batchSize {
			if err := store.BatchPut(batch); err != nil {
				return loaded, fmt.Errorf("batch put at entry %d: %w", i, err)
			}
			loaded += len(batch)
			batch = make(map[string][]float32, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := store.BatchPut(batch); err != nil {
			return loaded, fmt.Errorf("final batch put: %w", err)
		}
		loaded += len(batch)
	}

	return loaded, nil
}
