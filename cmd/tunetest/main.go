package main

import (
	"fmt"
	"plastic-engine-core/internal/core/search/shards"
)

func main() {
	profiles := []shards.NodeResourceProfile{
		{MemoryLimitMB: 2048, CPUCount: 2, ShardCount: 4},
		{MemoryLimitMB: 8192, CPUCount: 8, ShardCount: 4},
		{MemoryLimitMB: 32768, CPUCount: 32, ShardCount: 8},
		{MemoryLimitMB: 131072, CPUCount: 132, ShardCount: 8},
	}
	for _, p := range profiles {
		c := shards.TuneForNode(p)
		fmt.Printf("--- %dGB / %d CPUs / %d shards ---\n", p.MemoryLimitMB/1024, p.CPUCount, p.ShardCount)
		fmt.Printf("  PebbleMemTableSize        : %d MB\n", c.PebbleMemTableSize/(1024*1024))
		fmt.Printf("  MaxBatchSize              : %d docs\n", c.WorkerMaxBatchSize)
		fmt.Printf("  MaxWorkers                : %d per shard\n", c.WorkerMaxWorkers)
		fmt.Printf("  QueueCapacity             : %d\n", c.WorkerQueueCapacity)
		fmt.Printf("  MaxConcurrentCompactions  : %d\n", c.MaxConcurrentCompactions)
		fmt.Printf("  MemTableStopWritesThreshold: %d\n", c.MemTableStopWritesThreshold)
		fmt.Printf("  L0CompactionThreshold     : %d\n", c.L0CompactionThreshold)
		fmt.Printf("  L0StopWritesThreshold     : %d\n", c.L0StopWritesThreshold)
		fmt.Printf("  CooccurrenceExtractWorkers: %d\n", c.CooccurrenceExtractWorkers)
		fmt.Println()
	}
}
