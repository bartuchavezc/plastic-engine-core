package search

import (
	"context"
)

import (
	coordinator "plastic-engine-core/internal/core/cluster/coordinator"
)

// JoinService wraps coordinator logic to expose search-specific join operations.
type JoinService struct {
	coordinator *coordinator.Coordinator
}

// NewJoinService builds a service using the provided coordinator.
func NewJoinService(coord *coordinator.Coordinator) *JoinService {
	return &JoinService{
		coordinator: coord,
	}
}

// Join delegates the join request to the coordinator.
func (s *JoinService) Join(ctx context.Context, req coordinator.JoinRequest) (coordinator.JoinResponse, error) {
	return s.coordinator.Join(ctx, req)
}

// Heartbeat proxies the heartbeat request to the coordinator.
func (s *JoinService) Heartbeat(ctx context.Context, req coordinator.HeartbeatRequest) error {
	return s.coordinator.Heartbeat(ctx, req)
}
