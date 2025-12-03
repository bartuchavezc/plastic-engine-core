package cluster

import (
	"context"
)

import (
	"plastic-engine-core/internal/core/cluster"
)

// JoinService wraps coordinator logic to expose search-specific join operations.
type JoinService struct {
	coordinator *cluster.Coordinator
}

// NewJoinService builds a service using the provided cluster.
func NewJoinService(coord *cluster.Coordinator) *JoinService {
	return &JoinService{
		coordinator: coord,
	}
}

// Join delegates the join request to the cluster.
func (s *JoinService) Join(ctx context.Context, req cluster.JoinRequest) (cluster.JoinResponse, error) {
	return s.cluster.Join(ctx, req)
}

// Heartbeat proxies the heartbeat request to the cluster.
func (s *JoinService) Heartbeat(ctx context.Context, req cluster.HeartbeatRequest) error {
	return s.cluster.Heartbeat(ctx, req)
}
