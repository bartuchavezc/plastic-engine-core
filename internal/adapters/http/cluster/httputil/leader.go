package httputil

import (
	"encoding/json"
	"errors"
	"net/http"

	"plastic-engine-core/internal/core/cluster"
)

// LeaderChecker provides methods to check if the coordinator is the leader.
type LeaderChecker interface {
	IsLeader() bool
	LeaderAddr() string
}

// RequireLeader is a middleware that ensures requests are only handled by the leader.
// If the current node is not the leader, it returns a 503 Service Unavailable with
// the leader address in the response, allowing clients to redirect.
func RequireLeader(checker LeaderChecker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if checker.IsLeader() {
				next.ServeHTTP(w, r)
				return
			}

			leaderAddr := checker.LeaderAddr()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Raft-Leader", leaderAddr)
			w.WriteHeader(http.StatusServiceUnavailable)

			response := map[string]interface{}{
				"error":       "not the leader",
				"leader":      leaderAddr,
				"retry_after": 1,
			}
			_ = json.NewEncoder(w).Encode(response)
		})
	}
}

// CheckLeader verifies the coordinator is the leader and returns an appropriate
// HTTP error if not. Returns true if the check passes and the handler should continue.
func CheckLeader(w http.ResponseWriter, coord *cluster.Coordinator) bool {
	if coord.IsLeader() {
		return true
	}

	leaderAddr := coord.LeaderAddr()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Raft-Leader", leaderAddr)
	w.WriteHeader(http.StatusServiceUnavailable)

	response := map[string]interface{}{
		"error":       "not the leader",
		"leader":      leaderAddr,
		"retry_after": 1,
	}
	_ = json.NewEncoder(w).Encode(response)
	return false
}

// HandleNotLeaderError checks if the error is a NotLeaderError and writes
// an appropriate response. Returns true if the error was handled.
func HandleNotLeaderError(w http.ResponseWriter, err error) bool {
	var notLeaderErr cluster.NotLeaderError
	if errors.As(err, &notLeaderErr) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Raft-Leader", notLeaderErr.Leader)
		w.WriteHeader(http.StatusServiceUnavailable)

		response := map[string]interface{}{
			"error":       "not the leader",
			"leader":      notLeaderErr.Leader,
			"retry_after": 1,
		}
		_ = json.NewEncoder(w).Encode(response)
		return true
	}

	if errors.Is(err, cluster.ErrNotLeader) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)

		response := map[string]interface{}{
			"error":       "not the leader",
			"leader":      "",
			"retry_after": 1,
		}
		_ = json.NewEncoder(w).Encode(response)
		return true
	}

	return false
}

