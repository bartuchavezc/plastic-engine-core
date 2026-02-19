package segment

import (
	"bufio"
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
}

// SegmentCfg returns a segment.Config derived from this NodeConfig.
// The caller must set DataDir and TermRegistry after calling this.
func (c NodeConfig) SegmentCfg() Config {
	cfg := DefaultConfig()
	cfg.FlushThresholdBytes = c.FlushThresholdBytes
	cfg.FlushThreshold = c.FlushThreshold
	cfg.MergeWorkers = c.MergeWorkers
	cfg.FlushInterval = c.FlushInterval
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

	// Indexing memory budget: 6% of total RAM, mirroring Elasticsearch's index buffer policy.
	// Each shard has 2 MemSegment generations in flight simultaneously (active + flushing).
	// GC pressure adds ~1.5× peak live heap, so we budget conservatively.
	// 2GB/4shards → 122MB total / 8 = ~15MB per shard  ✓
	// 8GB/4shards → 491MB total / 8 = ~61MB per shard  ✓
	// 128GB/8shards → capped at 256MB per shard         ✓
	totalIndexingMB := memMB * 6 / 100
	flushBytes := clampInt64(totalIndexingMB*1024*1024/(int64(shards)*2), 8*1024*1024, 256*1024*1024)

	// MaxBatchSize: docs to accumulate before flushing.
	// Assumes ~200KB average tokenized doc (realistic with ngrams).
	// Range: [32, 1024]
	maxBatch := clampInt(int(flushBytes/(200*1024)), 32, 1024)

	// Worker goroutines for parallel tokenization — half CPUs, leave rest for searches.
	// Range: [2, 32]
	maxWorkers := clampInt(cpus/2, 2, 32)

	// Concurrent flush operations across all shards.
	// MUST be >= number of shards to avoid serializing flushes: if only 1 flush
	// runs at a time, the other shards' flushLoops block on the semaphore and
	// can't service their queues → ErrBackpressure → doc drops.
	// Floor at 4 (matches typical shard count) regardless of CPU count.
	// Range: [4, 16]
	flushConcurrency := clampInt(max(cpus, 4), 4, 16)

	// Merge workers: I/O-bound, can be modest.
	// Range: [1, 8]
	mergeWorkers := clampInt(cpus/2, 1, 8)

	// Queue capacity: must absorb a full burst (e.g. 1K docs/request) without
	// backpressure. Items in queue are raw (pre-tokenization, ~1-2KB each),
	// so a large queue is cheap. Floor at 2048 for burst absorption.
	// Range: [2048, 8192]
	queueCapacity := clampInt(max(maxBatch*16, 2048), 2048, 8192)

	return NodeConfig{
		WorkerMaxWorkers:    maxWorkers,
		WorkerMaxBatchSize:  maxBatch,
		WorkerQueueCapacity: queueCapacity,
		FlushConcurrency:    flushConcurrency,
		FlushThresholdBytes: flushBytes,
		FlushThreshold:      5000,
		MergeWorkers:        mergeWorkers,
		FlushInterval:       10 * time.Second,
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
