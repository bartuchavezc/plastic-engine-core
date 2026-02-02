package segment

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"io"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure for set membership testing.
// False positives are possible, but false negatives are not.
type BloomFilter struct {
	bits    []byte
	numBits uint64
	numHash int
}

// BloomFilterConfig configures bloom filter parameters.
type BloomFilterConfig struct {
	// ExpectedItems is the expected number of items to be added.
	ExpectedItems int
	// FalsePositiveRate is the desired false positive probability (e.g., 0.01 for 1%).
	FalsePositiveRate float64
}

// DefaultBloomConfig returns a default configuration suitable for most use cases.
func DefaultBloomConfig() BloomFilterConfig {
	return BloomFilterConfig{
		ExpectedItems:     10000,
		FalsePositiveRate: 0.01, // 1% false positive rate
	}
}

// NewBloomFilter creates a new bloom filter with the given configuration.
func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	if config.ExpectedItems <= 0 {
		config.ExpectedItems = 1000
	}
	if config.FalsePositiveRate <= 0 || config.FalsePositiveRate >= 1 {
		config.FalsePositiveRate = 0.01
	}

	// Calculate optimal size: m = -n*ln(p) / (ln(2)^2)
	n := float64(config.ExpectedItems)
	p := config.FalsePositiveRate
	m := math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2))

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	k := math.Ceil((m / n) * math.Ln2)

	// Ensure minimum values
	if m < 64 {
		m = 64
	}
	if k < 1 {
		k = 1
	}
	if k > 16 {
		k = 16 // Cap at 16 hash functions
	}

	numBits := uint64(m)
	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits:    make([]byte, numBytes),
		numBits: numBits,
		numHash: int(k),
	}
}

// NewBloomFilterFromSize creates a bloom filter with explicit size parameters.
func NewBloomFilterFromSize(numBits uint64, numHash int) *BloomFilter {
	if numBits < 64 {
		numBits = 64
	}
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 16 {
		numHash = 16
	}

	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits:    make([]byte, numBytes),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add adds an item to the bloom filter.
func (bf *BloomFilter) Add(item string) {
	h1, h2 := bf.hashes(item)

	for i := 0; i < bf.numHash; i++ {
		// Double hashing: hash_i = h1 + i*h2
		pos := (h1 + uint64(i)*h2) % bf.numBits
		bf.setBit(pos)
	}
}

// MayContain returns true if the item might be in the set.
// False means the item is definitely not in the set.
// True means the item might be in the set (with FalsePositiveRate probability of being wrong).
func (bf *BloomFilter) MayContain(item string) bool {
	h1, h2 := bf.hashes(item)

	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}

	return true
}

// hashes computes two independent hashes for double hashing.
func (bf *BloomFilter) hashes(item string) (uint64, uint64) {
	h := fnv.New128a()
	h.Write([]byte(item))
	sum := h.Sum(nil)

	// Split 128-bit hash into two 64-bit hashes
	h1 := binary.BigEndian.Uint64(sum[0:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])

	return h1, h2
}

// setBit sets the bit at position pos.
func (bf *BloomFilter) setBit(pos uint64) {
	byteIdx := pos / 8
	bitIdx := pos % 8
	bf.bits[byteIdx] |= 1 << bitIdx
}

// getBit returns true if the bit at position pos is set.
func (bf *BloomFilter) getBit(pos uint64) bool {
	byteIdx := pos / 8
	bitIdx := pos % 8
	return (bf.bits[byteIdx] & (1 << bitIdx)) != 0
}

// Size returns the size of the bloom filter in bytes.
func (bf *BloomFilter) Size() int {
	return len(bf.bits)
}

// NumBits returns the number of bits in the filter.
func (bf *BloomFilter) NumBits() uint64 {
	return bf.numBits
}

// NumHash returns the number of hash functions.
func (bf *BloomFilter) NumHash() int {
	return bf.numHash
}

// EstimateFillRatio returns the estimated fill ratio of the filter.
func (bf *BloomFilter) EstimateFillRatio() float64 {
	setBits := 0
	for _, b := range bf.bits {
		setBits += popcount(b)
	}
	return float64(setBits) / float64(bf.numBits)
}

// popcount counts the number of set bits in a byte.
func popcount(b byte) int {
	count := 0
	for b != 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}

// WriteTo writes the bloom filter to a writer.
// Format: numBits(8) + numHash(4) + bits(variable)
func (bf *BloomFilter) WriteTo(w io.Writer) (int64, error) {
	var written int64

	// Write numBits
	if err := binary.Write(w, binary.BigEndian, bf.numBits); err != nil {
		return written, err
	}
	written += 8

	// Write numHash
	if err := binary.Write(w, binary.BigEndian, uint32(bf.numHash)); err != nil {
		return written, err
	}
	written += 4

	// Write bits
	n, err := w.Write(bf.bits)
	written += int64(n)
	if err != nil {
		return written, err
	}

	return written, nil
}

// ReadBloomFilter reads a bloom filter from a reader.
func ReadBloomFilter(r io.Reader) (*BloomFilter, error) {
	var numBits uint64
	if err := binary.Read(r, binary.BigEndian, &numBits); err != nil {
		return nil, err
	}

	var numHash uint32
	if err := binary.Read(r, binary.BigEndian, &numHash); err != nil {
		return nil, err
	}

	numBytes := (numBits + 7) / 8
	bits := make([]byte, numBytes)
	if _, err := io.ReadFull(r, bits); err != nil {
		return nil, err
	}

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: int(numHash),
	}, nil
}

// SerializedSize returns the size of the serialized bloom filter.
func (bf *BloomFilter) SerializedSize() int64 {
	return 8 + 4 + int64(len(bf.bits)) // numBits + numHash + bits
}

// Merge merges another bloom filter into this one (OR operation).
// Both filters must have the same size and number of hash functions.
func (bf *BloomFilter) Merge(other *BloomFilter) bool {
	if bf.numBits != other.numBits || bf.numHash != other.numHash {
		return false
	}

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}

	return true
}

// Clear resets the bloom filter.
func (bf *BloomFilter) Clear() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// Clone creates a copy of the bloom filter.
func (bf *BloomFilter) Clone() *BloomFilter {
	bits := make([]byte, len(bf.bits))
	copy(bits, bf.bits)

	return &BloomFilter{
		bits:    bits,
		numBits: bf.numBits,
		numHash: bf.numHash,
	}
}

// Ensure hash.Hash128 interface is available (Go 1.14+)
var _ hash.Hash = fnv.New128a()
