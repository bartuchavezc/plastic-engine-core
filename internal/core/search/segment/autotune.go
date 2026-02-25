package segment

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// NodeResourceProfile describes the hardware available to this search node.
// Zero values trigger auto-detection from the OS and environment variables.
//
// The coordinator can store and push per-node overrides via cluster metadata.
// Search nodes apply them at startup by passing this to TuneForNode.
type NodeResourceProfile struct {
	// MemoryLimitMB overrides auto-detection.
	// 0 = read PLASTIC_MEMORY_MB env var, then /proc/meminfo, then default 1GB.
	MemoryLimitMB int64

	// CPUCount overrides auto-detection.
	// 0 = read PLASTIC_CPUS env var, then runtime.NumCPU().
	CPUCount int

	// ShardCount is the expected number of shards per node.
	// Used to divide the memory budget fairly per shard.
	// 0 = default 4.
	ShardCount int
}

// NodeConfig holds all auto-tuned configuration values for a search node.
// Computed once at startup via TuneForNode.
type NodeConfig struct {
	// Indexing pipeline (used to build ShardWorkerConfig)
	WorkerMaxWorkers    int
	WorkerMaxBatchSize  int
	WorkerQueueCapacity int
	FlushConcurrency    int // recommended cap on the global flush semaphore

	// Segment storage
	FlushThresholdBytes int64
	FlushThreshold      int
	MergeWorkers        int
	FlushInterval       time.Duration
	MergeInterval       time.Duration
}

// String returns a human-readable summary of the tuned configuration.
func (c NodeConfig) String() string {
	return fmt.Sprintf(
		"workers=%d batch=%d queue=%d flushConc=%d flushBytes=%dMB flushDocs=%d mergeWorkers=%d flushInterval=%s mergeInterval=%s",
		c.WorkerMaxWorkers, c.WorkerMaxBatchSize, c.WorkerQueueCapacity,
		c.FlushConcurrency, c.FlushThresholdBytes/(1024*1024),
		c.FlushThreshold, c.MergeWorkers, c.FlushInterval, c.MergeInterval,
	)
}

// SegmentCfg returns a segment.Config derived from this NodeConfig.
// The caller must set DataDir and TermRegistry after calling this.
func (c NodeConfig) SegmentCfg() Config {
	cfg := DefaultConfig()
	cfg.FlushThresholdBytes = c.FlushThresholdBytes
	cfg.FlushThreshold = c.FlushThreshold
	cfg.MergeWorkers = c.MergeWorkers
	cfg.FlushInterval = c.FlushInterval
	cfg.MergeInterval = c.MergeInterval
	return cfg
}

// TuneForNode computes optimal configuration for the given resource profile.
// Scales from 2GB/2CPU containers to 132-CPU cloud instances without any
// manual tuning — just pass the result to Apply() and use SegmentCfg().
//
// Typical usage at node startup:
//
//	profile := segment.DetectResources()
//	// Coordinator overrides (optional):
//	//   profile.MemoryLimitMB = meta.MemoryMB
//	//   profile.CPUCount = meta.CPUs
//	nodeCfg := segment.TuneForNode(profile)
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

	// Indexing memory budget: 30% of total RAM for MemSegment buffers.
	// Each shard has 2 MemSegment generations in flight (active + flushing).
	// PebbleTermRegistry is heap-light, so we can dedicate more RAM to indexing.
	// needsFlush() applies a 2x correction for Go map/GC overhead.
	//
	// 3800MB/4shards: 3800*30%/(4*2) ≈ 142MB per shard
	// 7600MB/4shards: 7600*30%/(4*2) ≈ 285MB per shard
	totalIndexingMB := memMB * 30 / 100
	flushBytes := clampInt64(totalIndexingMB*1024*1024/(int64(shards)*2), 16*1024*1024, 512*1024*1024)

	// MaxBatchSize: docs to accumulate before flushing.
	// Assumes ~200KB average tokenized doc (realistic with ngrams).
	// Range: [32, 2048]
	maxBatch := clampInt(int(flushBytes/(200*1024)), 32, 2048)

	// Worker goroutines for parallel tokenization.
	// With bulk ingestion as primary workload, use all CPUs for tokenization.
	// Tokenization goroutines yield naturally (short-lived per-doc work items),
	// so merge/flush goroutines can schedule on any CPU without a reserved core.
	// Range: [2, 32]
	maxWorkers := clampInt(max(cpus, 2), 2, 32)

	// Concurrent flush operations across all shards.
	// MUST be >= number of shards to avoid serializing flushes.
	// With more CPUs, allow more concurrent flushes.
	// Range: [4, 16]
	flushConcurrency := clampInt(max(cpus*2, 4), 4, 16)

	// Merge workers: I/O-bound, scale with CPUs but stay modest.
	// With buffered iterator I/O, merges are faster → can afford fewer workers.
	// Range: [1, 8]
	mergeWorkers := clampInt(max(cpus/2, 2), 1, 8)

	// Merge interval: how often the merge scheduler checks for work (in addition
	// to event-driven signals from flushes). During high ingestion, segments
	// accumulate fast → check frequently so merges don't fall behind.
	// 5s keeps the merge pipeline responsive without burning CPU on idle checks.
	mergeInterval := 5 * time.Second

	// Queue capacity: must absorb a full burst (e.g. 1K docs/request) without
	// backpressure. Items in queue are raw (pre-tokenization, ~1-2KB each),
	// so a large queue is cheap. Floor at 2048 for burst absorption.
	// Range: [2048, 8192]
	queueCapacity := clampInt(max(maxBatch*16, 2048), 2048, 8192)

	// FlushThreshold: scale with flush budget.
	// Let FlushThresholdBytes be the primary trigger — set doc threshold high
	// so it only kicks in as a safety net for tiny documents.
	flushThreshold := clampInt(int(flushBytes/500), 10_000, 200_000)

	return NodeConfig{
		WorkerMaxWorkers:    maxWorkers,
		WorkerMaxBatchSize:  maxBatch,
		WorkerQueueCapacity: queueCapacity,
		FlushConcurrency:    flushConcurrency,
		FlushThresholdBytes: flushBytes,
		FlushThreshold:      flushThreshold,
		MergeWorkers:        mergeWorkers,
		FlushInterval:       10 * time.Second,
		MergeInterval:       mergeInterval,
	}
}

// DetectResources reads available CPU and memory from the OS and environment.
// Environment variables take precedence:
//
//	PLASTIC_MEMORY_MB  — override detected memory (e.g. "2048")
//	PLASTIC_CPUS       — override detected CPU count (e.g. "4")
func DetectResources() NodeResourceProfile {
	return NodeResourceProfile{
		MemoryLimitMB: DetectMemoryMB(),
		CPUCount:      DetectCPUs(),
	}
}

// DetectCPUs returns the effective CPU count, respecting PLASTIC_CPUS env override.
func DetectCPUs() int {
	if v := os.Getenv("PLASTIC_CPUS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return runtime.NumCPU()
}

// DetectMemoryMB returns the effective memory limit for this process in MB.
//
// Detection order (first non-zero wins):
//  1. PLASTIC_MEMORY_MB env var — explicit operator override, works everywhere
//  2. GOMEMLIMIT env var — set by Kubernetes/Docker to match the container limit
//  3. cgroup v2 limit — /sys/fs/cgroup/memory.max (Docker/k8s modern)
//  4. cgroup v1 limit — /sys/fs/cgroup/memory/memory.limit_in_bytes (Docker legacy)
//  5. /proc/meminfo MemTotal — correct on bare metal/VMs with no container limit
//  6. 1024 MB fallback
//
// On bare metal: steps 2-4 return 0 (no limits set), step 5 returns the machine RAM. ✓
// In a Docker/k8s container: step 2, 3, or 4 returns the container limit before reaching step 5. ✓
func DetectMemoryMB() int64 {
	// 1. Explicit operator override (highest priority, works on any platform)
	if v := os.Getenv("PLASTIC_MEMORY_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}

	// 2. Go soft memory limit — set by GOMEMLIMIT env var or Kubernetes LimitRange
	if mb := readGoMemLimitMB(); mb > 0 {
		return mb
	}

	// 3. cgroup v2 memory limit (modern Docker, k8s with cgroup v2)
	if mb := readCgroupV2MemoryMB(); mb > 0 {
		return mb
	}

	// 4. cgroup v1 memory limit (older Docker, legacy k8s)
	if mb := readCgroupV1MemoryMB(); mb > 0 {
		return mb
	}

	// 5. Total system RAM from /proc/meminfo
	// Correct on bare metal/VMs. On cgroup v1 containers this would be the HOST total,
	// which is why cgroup steps above come first.
	if mb := readProcMemInfoMB(); mb > 0 {
		return mb
	}

	return 1024 // conservative fallback
}

// readGoMemLimitMB returns the Go runtime soft memory limit in MB (GOMEMLIMIT).
// Returns 0 if not set or set to unlimited.
func readGoMemLimitMB() int64 {
	limit := debug.SetMemoryLimit(-1) // -1 = query without changing
	if limit > 0 && limit < math.MaxInt64 {
		return limit / 1024 / 1024
	}
	return 0
}

// readCgroupV2MemoryMB reads the container memory limit from cgroup v2.
// Returns 0 if not in a cgroup v2 container or limit is unlimited.
func readCgroupV2MemoryMB() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0 // unlimited
	}
	bytes, err := strconv.ParseInt(s, 10, 64)
	if err != nil || bytes <= 0 {
		return 0
	}
	return bytes / 1024 / 1024
}

// readCgroupV1MemoryMB reads the container memory limit from cgroup v1.
// Returns 0 if not in a cgroup v1 container or limit is effectively unlimited.
func readCgroupV1MemoryMB() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0
	}
	bytes, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || bytes <= 0 {
		return 0
	}
	// cgroup v1 uses a very large sentinel value to mean "unlimited"
	// (typically max_int64 rounded down to page boundary ≈ 9.2×10^18)
	if bytes > (1 << 40) { // > 1 TB = unlimited sentinel
		return 0
	}
	return bytes / 1024 / 1024
}

// readProcMemInfoMB parses MemTotal from /proc/meminfo and returns MB.
// Accurate on bare metal and VMs. On cgroup v1 containers, reports the HOST total.
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
		// Format: "MemTotal:       2097152 kB"
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
