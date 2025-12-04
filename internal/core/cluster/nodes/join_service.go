package nodes

import (
	"context"
)

// JoinService wraps the nodes service for HTTP adapter compatibility.
type JoinService struct {
	service *Service
}

// NewJoinService builds a JoinService using the provided node service.
func NewJoinService(svc *Service) *JoinService {
	return &JoinService{
		service: svc,
	}
}

// Join delegates the join request to the node service.
func (s *JoinService) Join(ctx context.Context, req JoinRequest) (JoinResponse, error) {
	return s.service.Join(ctx, req)
}

// Heartbeat proxies the heartbeat request to the node service.
func (s *JoinService) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	return s.service.Heartbeat(ctx, req)
}
