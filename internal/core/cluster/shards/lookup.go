package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Info represents metadata for a shard.
type Info struct {
	ID          string
	PrimaryNode string
}

// ErrPrimaryNotFound indicates that the shard has no primary assignment.
var ErrPrimaryNotFound = errors.New("primary shard not found")

// LookupPrimaryShard resolves the shard responsible for the given index and shard key.
func LookupPrimaryShard(ctx context.Context, db *sql.DB, indexID, shardKey string) (Info, error) {
	row := db.QueryRowContext(ctx, `SELECT id, primary_node FROM shards WHERE index_id = ? AND shard_key = ?`, indexID, shardKey)

	var info Info
	if err := row.Scan(&info.ID, &info.PrimaryNode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Info{}, ErrPrimaryNotFound
		}
		return Info{}, fmt.Errorf("scan shard: %w", err)
	}

	if strings.TrimSpace(info.PrimaryNode) == "" {
		return Info{}, fmt.Errorf("shard %s has no primary assigned", info.ID)
	}

	return info, nil
}

type NodeInfo struct {
	ID            string
	AdvertiseAddr string
}

// LookupNode resolves a node by identifier and returns its advertise address.
func LookupNode(ctx context.Context, db *sql.DB, nodeID string) (NodeInfo, error) {
	if strings.TrimSpace(nodeID) == "" {
		return NodeInfo{}, fmt.Errorf("node id is required")
	}

	row := db.QueryRowContext(ctx, `SELECT id, advertise_addr FROM nodes WHERE id = ?`, nodeID)

	var (
		node          NodeInfo
		advertiseAddr sql.NullString
	)

	if err := row.Scan(&node.ID, &advertiseAddr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeInfo{}, fmt.Errorf("node %s not found", nodeID)
		}
		return NodeInfo{}, fmt.Errorf("scan node: %w", err)
	}

	if advertiseAddr.Valid {
		node.AdvertiseAddr = advertiseAddr.String
	}

	return node, nil
}
