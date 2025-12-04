# Coordinator Architecture

This document describes the coordinator component of Plastic Engine, which manages cluster metadata, shard assignments, and index definitions.

## Overview

The coordinator is the control plane of Plastic Engine. It is responsible for:

- **Index Management**: Creating, updating, and deleting index definitions
- **Shard Planning**: Determining how data should be partitioned across shards
- **Node Management**: Tracking search nodes that join and leave the cluster
- **Shard Assignment**: Assigning shards to available search nodes
- **Document Routing**: Directing incoming documents to the correct shard

## Deployment Modes

Plastic Engine offers two coordinator binaries to match your deployment needs:

### Standalone Mode (`coordinator-standalone`)

Use this for development, testing, or single-node deployments.

```bash
# Build
go build -o coordinator-standalone ./cmd/coordinator-standalone

# Run
PORT=8080 DB_PATH=./cluster.db ./coordinator-standalone
```

**Characteristics:**
- Single coordinator instance
- Writes directly to SQLite
- No consensus overhead
- Simple to deploy and debug
- Not fault-tolerant

### High Availability Mode (`coordinator`)

Use this for production deployments where fault tolerance is required.

```bash
# Build
go build -o coordinator ./cmd/coordinator

# Run a 3-node cluster
# Node 1 (bootstrap)
RAFT_BOOTSTRAP=true RAFT_NODE_ID=coord-1 ./coordinator

# Node 2
RAFT_PEERS=coord-1:7000 RAFT_NODE_ID=coord-2 ./coordinator

# Node 3
RAFT_PEERS=coord-1:7000,coord-2:7000 RAFT_NODE_ID=coord-3 ./coordinator
```

**Characteristics:**
- 3 or 5 coordinator instances (odd numbers for quorum)
- Uses Raft consensus for state replication
- Tolerates (N-1)/2 node failures
- Automatic leader election
- Consistent reads and writes

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP API port |
| `DB_PATH` | `cluster.db` | Path to SQLite metadata database |
| `RAFT_PORT` | `7000` | Raft consensus port (HA mode only) |
| `RAFT_DATA_DIR` | `./raft-data` | Directory for Raft logs and snapshots |
| `RAFT_NODE_ID` | hostname | Unique identifier for this node |
| `RAFT_ADVERTISE_ADDR` | bind address | Address advertised to other nodes |
| `RAFT_PEERS` | empty | Comma-separated list of peer addresses |
| `RAFT_BOOTSTRAP` | `false` | Set to `true` to bootstrap new cluster |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | OpenTelemetry collector endpoint |
| `ENABLE_PROMETHEUS` | `true` | Enable Prometheus metrics endpoint |
| `METRICS_PORT` | `9090` | Prometheus metrics port |

### Example Configurations

#### Development (Standalone)

```bash
export PORT=8080
export DB_PATH=/tmp/plastic-dev.db
./coordinator-standalone
```

#### Production (3-Node Cluster)

**Node 1:**
```bash
export PORT=8080
export RAFT_PORT=7000
export RAFT_NODE_ID=coord-1
export RAFT_DATA_DIR=/var/lib/plastic/raft
export RAFT_ADVERTISE_ADDR=192.168.1.10:7000
export RAFT_BOOTSTRAP=true
export DB_PATH=/var/lib/plastic/cluster.db
./coordinator
```

**Node 2:**
```bash
export PORT=8080
export RAFT_PORT=7000
export RAFT_NODE_ID=coord-2
export RAFT_DATA_DIR=/var/lib/plastic/raft
export RAFT_ADVERTISE_ADDR=192.168.1.11:7000
export RAFT_PEERS=192.168.1.10:7000
export DB_PATH=/var/lib/plastic/cluster.db
./coordinator
```

**Node 3:**
```bash
export PORT=8080
export RAFT_PORT=7000
export RAFT_NODE_ID=coord-3
export RAFT_DATA_DIR=/var/lib/plastic/raft
export RAFT_ADVERTISE_ADDR=192.168.1.12:7000
export RAFT_PEERS=192.168.1.10:7000,192.168.1.11:7000
export DB_PATH=/var/lib/plastic/cluster.db
./coordinator
```

## Architecture

### State Applier Pattern

The coordinator uses the **StateApplier** pattern to abstract how state changes are persisted:

```
                    ┌─────────────────────┐
                    │     Coordinator     │
                    │                     │
                    │  CreateIndex()      │
                    │  RegisterNode()     │
                    │  AssignShard()      │
                    └──────────┬──────────┘
                               │
                               │ applier.Apply(cmd)
                               │
            ┌──────────────────┴──────────────────┐
            │                                      │
            ▼                                      ▼
   ┌─────────────────┐                   ┌─────────────────┐
   │  LocalApplier   │                   │   RaftApplier   │
   │                 │                   │                 │
   │ SQLite.Exec()   │                   │ raft.Apply()    │
   │ (direct write)  │                   │ (consensus)     │
   └─────────────────┘                   └─────────────────┘
```

This pattern allows the same coordinator logic to work in both standalone and HA modes without code duplication.

### Raft Consensus

In HA mode, all state changes go through Raft consensus:

1. **Client Request**: HTTP request arrives at any coordinator
2. **Leader Check**: If not leader, return 503 with leader address
3. **Command Creation**: Create serializable command from request
4. **Raft Apply**: Send command to Raft for consensus
5. **Log Replication**: Leader replicates to followers
6. **Commit**: When majority acknowledges, command is committed
7. **FSM Apply**: Each node applies command to local SQLite
8. **Response**: Leader returns result to client

### Data Storage

The coordinator uses two types of storage:

**SQLite (Metadata)**
- Index definitions
- Field mappings
- Node registry
- Shard assignments
- Uses WAL mode for better concurrent read performance

**BoltDB (Raft Logs)**
- Raft log entries
- Stable store (current term, voted for)
- Only used in HA mode

## API Endpoints

### Health & Status

```
GET /health        # Always returns 200 if process is running
GET /ready         # Returns 200 only if cluster has a leader
GET /raft/status   # Returns Raft cluster status (HA mode only)
```

### Index Management

```
GET  /indexes           # List all indexes
GET  /indexes/{id}      # Get index by ID
POST /indexes           # Create new index
```

### Cluster Management

```
POST /cluster/join      # Node join request
POST /cluster/heartbeat # Node heartbeat
GET  /management/nodes  # List all nodes
GET  /management/shards # List all shards
```

### Document Ingestion

```
POST /indexes/{id}/documents  # Ingest document to index
```

## Operations

### Bootstrapping a New Cluster

1. Start the first node with `RAFT_BOOTSTRAP=true`
2. Wait for it to become leader (check `/ready` or `/raft/status`)
3. Start additional nodes with `RAFT_PEERS` pointing to the first node
4. Verify cluster formation via `/raft/status`

### Adding a Node

```bash
# On existing cluster, any node
curl http://coordinator:8080/raft/status

# Start new node with existing peers
RAFT_PEERS=existing-node:7000 ./coordinator
```

### Removing a Node

For now, simply stop the node. The cluster will continue with remaining nodes as long as quorum is maintained.

### Recovering from Failure

**Single Node Failure:**
- Cluster continues operating with remaining nodes
- Restart failed node; it will rejoin automatically

**Majority Failure:**
- Cluster becomes read-only (no quorum)
- Restore failed nodes to regain quorum
- If data loss occurred, restore from snapshot

### Snapshots

Raft automatically creates snapshots of the SQLite database to:
- Compact the Raft log
- Speed up new node synchronization

Snapshots are stored in `RAFT_DATA_DIR` (default: `./raft-data`).

## Monitoring

### Prometheus Metrics

When `ENABLE_PROMETHEUS=true`, metrics are exposed on the configured `METRICS_PORT`:

```
http://coordinator:9090/metrics
```

Key metrics:
- `http_requests_total` - Total HTTP requests by method, path, status
- `http_request_duration_seconds` - Request latency histogram
- Raft metrics (via hashicorp/raft)

### OpenTelemetry Tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to send traces to your collector:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

Spans include:
- HTTP request handling
- Raft apply operations
- SQLite transactions
- Shard assignment logic

### Raft Status Endpoint

```bash
curl http://coordinator:8080/raft/status
```

Returns:
```json
{
  "node_id": "coord-1",
  "state": "Leader",
  "is_leader": true,
  "leader_addr": "192.168.1.10:7000",
  "leader_id": "coord-1",
  "servers": [
    {"id": "coord-1", "address": "192.168.1.10:7000"},
    {"id": "coord-2", "address": "192.168.1.11:7000"},
    {"id": "coord-3", "address": "192.168.1.12:7000"}
  ],
  "stats": {
    "state": "Leader",
    "term": "5",
    "last_log_index": "1234",
    "commit_index": "1234",
    ...
  }
}
```

## Troubleshooting

### "not the leader" errors

The request was sent to a follower node. Either:
- Retry on the leader (address in `X-Raft-Leader` header)
- Use a load balancer that routes writes to the leader

### Cluster won't elect a leader

Check that:
- At least (N/2)+1 nodes are running
- Nodes can reach each other on `RAFT_PORT`
- All nodes have the same cluster configuration

### Node won't rejoin after restart

1. Check that `RAFT_DATA_DIR` is persisted across restarts
2. Verify `RAFT_NODE_ID` is the same as before
3. Check logs for Raft errors

### High latency on writes

Raft consensus adds latency (~2-5ms for local network). For high-throughput scenarios:
- Batch multiple operations
- Use async ingestion with acknowledgements
- Consider if HA is really needed

## Security Considerations

- **Network**: Use TLS between coordinator nodes (future work)
- **Authentication**: Add authentication middleware for API endpoints
- **Authorization**: Implement role-based access control
- **Data**: SQLite and BoltDB files contain sensitive metadata; protect with filesystem permissions

