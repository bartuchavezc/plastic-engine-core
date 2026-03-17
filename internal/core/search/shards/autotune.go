package shards

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"plastic-engine-core/internal/core/search/indexstore"
)

// NodeResourceProfile describes the hardware available to this search node.
type NodeResourceProfile struct {
	// MemoryLimitMB overrides auto-detection.
	// 0 = read PLASTIC_MEMORY_MB env var, then /proc/meminfo, then default 1GB.
	MemoryLimitMB int64

	// CPUCount overrides auto-detection.
	// 0 = read PLASTIC_CPUS env var, then runtime.NumCPU().
	CPUCount int

	// ShardCount is the expected number of shards per node.
	// 0 = default 4.
	ShardCount int
}

// NodeConfig holds all auto-tuned configuration values for a search node.
type NodeConfig struct {
	// Indexing pipeline
	WorkerMaxWorkers    int
	WorkerMaxBatchSize  int
	WorkerQueueCapacity int

	// Pebble memtable size per shard (bytes)
	PebbleMemTableSize int

	// MaxConcurrentCompactions limits Pebble compaction goroutines per shard.
	MaxConcurrentCompactions int

	// L0 write stall tuning
	MemTableStopWritesThreshold int
	L0CompactionThreshold       int
	L0StopWritesThreshold       int

	// CooccurrenceExtractWorkers caps co-occurrence goroutines per shard.
	CooccurrenceExtractWorkers int

	// BlockCacheSize is the shared Pebble block cache size in bytes.
	BlockCacheSize int64
}

// String returns a human-readable summary of the tuned configuration.
func (c NodeConfig) String() string {
	return fmt.Sprintf(
		"workers=%d batch=%d queue=%d pebbleMemTable=%dMB compactions=%d L0stop=%d L0compact=%d memStop=%d cooccWorkers=%d blockCache=%dMB",
		c.WorkerMaxWorkers, c.WorkerMaxBatchSize, c.WorkerQueueCapacity,
		c.PebbleMemTableSize/(1024*1024), c.MaxConcurrentCompactions,
		c.L0StopWritesThreshold, c.L0CompactionThreshold, c.MemTableStopWritesThreshold,
		c.CooccurrenceExtractWorkers, c.BlockCacheSize/(1024*1024),
	)
}

// SegmentCfg returns a indexstore.Config derived from this NodeConfig.
// The caller must set DataDir and TermRegistry after calling this.
func (c NodeConfig) SegmentCfg() indexstore.Config {
	cfg := indexstore.DefaultConfig()
	cfg.PostingStoreConfig.PebbleMemTableSize = c.PebbleMemTableSize
	cfg.PostingStoreConfig.MaxConcurrentCompactions = c.MaxConcurrentCompactions
	cfg.PostingStoreConfig.MemTableStopWritesThreshold = c.MemTableStopWritesThreshold
	cfg.PostingStoreConfig.L0CompactionThreshold = c.L0CompactionThreshold
	cfg.PostingStoreConfig.L0StopWritesThreshold = c.L0StopWritesThreshold
	cfg.CooccurrenceConfig.MaxExtractWorkers = c.CooccurrenceExtractWorkers
	return cfg
}

// TuneForNode computes optimal configuration for the given resource profile.
func TuneForNode(profile NodeResourceProfile) NodeConfig {
	cpus := profile.CPUCount
	if cpus <= 0 {
		cpus = DetectCPUs()
	}

	memMB := profile.MemoryLimitMB
	if memMB <= 0 {
		memMB = DetectMemoryMB()
	}

	shards := profile.ShardCount
	if shards <= 0 {
		shards = 4
	}

	// Pebble memtable size: budget = 20% of RAM, divided by (shards × memTableStopWritesThreshold).
	// This ensures worst-case total memtable usage stays within budget.
	// Example: 7600MB × 20% = 1520MB / (4 shards × 4 stopWrites) = 95MB per memtable.
	// Range: [4MB, 128MB] per individual memtable.
	memTableStopWrites := 4
	pebbleMemTableBytes := clampInt64(memMB*20/100*1024*1024/int64(shards*memTableStopWrites), 4*1024*1024, 128*1024*1024)

	// MaxBatchSize: docs to accumulate before submitting.
	maxBatch := clampInt(int(pebbleMemTableBytes/(200*1024)), 32, 2048)

	// Worker goroutines for parallel tokenization.
	maxWorkers := clampInt(max(cpus, 2), 2, 32)

	// Queue capacity
	queueCapacity := clampInt(max(maxBatch*16, 2048), 2048, 8192)

	// Compaction goroutines: scale with CPUs, cap per shard
	maxCompactions := clampInt(cpus/shards, 1, 4)

	// Co-occurrence extract workers: CPU-bound, keep small to not starve
	// Pebble compaction and indexing. Total across all shards = shards × this.
	cooccWorkers := clampInt(cpus/(shards*2), 1, 4)

	// L0 write stall tuning:
	// - L0CompactionThreshold: when to start L0→L1 compaction. Raising from 4→8
	//   batches more data per compaction (better write amplification).
	// - L0StopWritesThreshold: hard stall point. Raising from 12→24 gives compaction
	//   2× more runway before blocking writes, at the cost of higher read amp during peaks.
	l0CompactionThreshold := 8
	l0StopWritesThreshold := 24

	// Block cache: shared across all Pebble instances.
	// This is cgo memory (NOT tracked by GOMEMLIMIT), so we must budget it within
	// the gap between GOMEMLIMIT and the actual container memory limit.
	blockCacheBytes := computeBlockCacheSize(memMB)
	log.Printf("[autotune] blockCache: containerMB=%d goLimitMB=%d → cacheBytes=%dMB",
		detectContainerMemoryMB(), readGoMemLimitMB(), blockCacheBytes/(1024*1024))

	return NodeConfig{
		WorkerMaxWorkers:            maxWorkers,
		WorkerMaxBatchSize:          maxBatch,
		WorkerQueueCapacity:         queueCapacity,
		PebbleMemTableSize:          int(pebbleMemTableBytes),
		MaxConcurrentCompactions:    maxCompactions,
		MemTableStopWritesThreshold: memTableStopWrites,
		L0CompactionThreshold:       l0CompactionThreshold,
		L0StopWritesThreshold:       l0StopWritesThreshold,
		CooccurrenceExtractWorkers:  cooccWorkers,
		BlockCacheSize:              blockCacheBytes,
	}
}

// DetectResources reads available CPU and memory from the OS and environment.
func DetectResources() NodeResourceProfile {
	return NodeResourceProfile{
		MemoryLimitMB: DetectMemoryMB(),
		CPUCount:      DetectCPUs(),
	}
}

// DetectCPUs returns the effective CPU count.
func DetectCPUs() int {
	if v := os.Getenv("PLASTIC_CPUS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// DetectMemoryMB returns the effective memory limit in MB.
func DetectMemoryMB() int64 {
	if v := os.Getenv("PLASTIC_MEMORY_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	if mb := readGoMemLimitMB(); mb > 0 {
		return mb
	}
	if mb := readCgroupV2MemoryMB(); mb > 0 {
		return mb
	}
	if mb := readCgroupV1MemoryMB(); mb > 0 {
		return mb
	}
	if mb := readProcMemInfoMB(); mb > 0 {
		return mb
	}
	return 1024
}

func readGoMemLimitMB() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 && limit < math.MaxInt64 {
		return limit / 1024 / 1024
	}
	return 0
}

func readCgroupV2MemoryMB() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0
	}
	bytes, err := strconv.ParseInt(s, 10, 64)
	if err != nil || bytes <= 0 {
		return 0
	}
	return bytes / 1024 / 1024
}

func readCgroupV1MemoryMB() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0
	}
	bytes, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || bytes <= 0 {
		return 0
	}
	if bytes > (1 << 40) {
		return 0
	}
	return bytes / 1024 / 1024
}

func readProcMemInfoMB() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb / 1024
	}
	return 0
}

// computeBlockCacheSize determines a safe block cache size based on available cgo headroom.
//
// Block cache is cgo memory (calloc), invisible to Go GC and GOMEMLIMIT.
// GOMEMLIMIT is a soft limit — Go heap can temporarily exceed it during GC.
// We must leave enough buffer in the container for: Go heap spikes + block cache +
// Pebble's other cgo allocations (table cache index blocks, compaction buffers) + OS.
//
// Strategy:
//   - If both container limit and GOMEMLIMIT are known:
//     cache = 25% of (containerLimit - GOMEMLIMIT), capped at [16MB, 256MB].
//     The remaining 75% covers Go heap spikes + Pebble cgo overhead + OS.
//   - If only GOMEMLIMIT is known: 32MB (we can't safely estimate headroom).
//   - If neither: 5% of detected total memory, capped at 128MB.
func computeBlockCacheSize(goMemLimitMB int64) int64 {
	containerMB := detectContainerMemoryMB()

	goLimitMB := readGoMemLimitMB()
	if goLimitMB <= 0 {
		goLimitMB = goMemLimitMB
	}

	if containerMB > 0 && goLimitMB > 0 && containerMB > goLimitMB {
		gapMB := containerMB - goLimitMB
		cacheBytes := gapMB * 25 / 100 * 1024 * 1024
		return clampInt64(cacheBytes, 16*1024*1024, 256*1024*1024)
	}

	if goLimitMB > 0 {
		return 32 * 1024 * 1024
	}

	return clampInt64(goMemLimitMB*5/100*1024*1024, 16*1024*1024, 128*1024*1024)
}

// detectContainerMemoryMB returns the container/system memory limit in MB,
// reading from cgroup v2, then v1, then /proc/meminfo.
// This is independent of GOMEMLIMIT and gives us the actual container ceiling.
func detectContainerMemoryMB() int64 {
	if mb := readCgroupV2MemoryMB(); mb > 0 {
		return mb
	}
	if mb := readCgroupV1MemoryMB(); mb > 0 {
		return mb
	}
	if mb := readProcMemInfoMB(); mb > 0 {
		return mb
	}
	return 0
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func clampInt64(v, min, max int64) int64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
