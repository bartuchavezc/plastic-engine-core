package main

import (
	"fmt"
	"plastic-engine-core/internal/core/search/segment"
)

func main() {
	profiles := []segment.NodeResourceProfile{
		{MemoryLimitMB: 2048,   CPUCount: 2,   ShardCount: 4},
		{MemoryLimitMB: 8192,   CPUCount: 8,   ShardCount: 4},
		{MemoryLimitMB: 32768,  CPUCount: 32,  ShardCount: 8},
		{MemoryLimitMB: 131072, CPUCount: 132, ShardCount: 8},
	}
	for _, p := range profiles {
		c := segment.TuneForNode(p)
		fmt.Printf("--- %dGB / %d CPUs / %d shards ---\n", p.MemoryLimitMB/1024, p.CPUCount, p.ShardCount)
		fmt.Printf("  FlushThresholdBytes : %d MB\n", c.FlushThresholdBytes/1024/1024)
		fmt.Printf("  MaxBatchSize        : %d docs\n", c.WorkerMaxBatchSize)
		fmt.Printf("  MaxWorkers          : %d per shard\n", c.WorkerMaxWorkers)
		fmt.Printf("  FlushConcurrency    : %d concurrent\n", c.FlushConcurrency)
		fmt.Printf("  MergeWorkers        : %d\n", c.MergeWorkers)
		fmt.Printf("  QueueCapacity       : %d\n", c.WorkerQueueCapacity)
		fmt.Println()
	}
}
