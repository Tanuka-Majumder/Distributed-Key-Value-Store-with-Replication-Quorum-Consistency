# Distributed Key-Value Store with Replication and Quorum Consistency

A distributed key-value store implemented in Python, inspired by Amazon Dynamo–style quorum-based replication and consistency semantics. This system provides high availability under quorum constraints, fault tolerance, and eventual consistency through replication and quorum-based reads/writes.

## Features

- **Consistent Hashing**: Uses a consistent hash ring with virtual nodes for even data distribution
- **Replication**: Configurable replication factor (N) for data redundancy
- **Quorum Consistency**: Configurable read (R) and write (W) quorums for consistency levels
- **Eventual Consistency**: Achieved via read repair and hinted handoff to converge replicas after partial failures
- **HTTP-based API**: HTTP-based API for key-value operations
- **Async/Await**:  Built with asyncio and aiohttp for efficient concurrent request handling
- **In-Memory Storage**: In-memory storage with concurrency-safe operations under asyncio

## Architecture

The system consists of multiple components:

- **Ring**: Consistent hash ring for key distribution
- **Node**: Core logic for client operations and replication
- **Storage**: In-memory key-value storage with versioning
- **Server**: HTTP server exposing the API

### Key Concepts

- **Replication Factor (N)**: Number of replicas for each key
- **Read Quorum (R)**: Minimum responses needed for a successful read
- **Write Quorum (W)**: Minimum acknowledgments needed for a successful write
- **Consistent Hashing**: Keys are mapped to virtual nodes using SHA1-based hashing
- **Versioning**: Timestamp-based versions with Last-Write-Wins (LWW) conflict resolution

## Prerequisites

- Python 3.8+
- pip

## Installation

1. Clone or download the project files
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The server can be configured using environment variables:

- `NODE_ID`: Unique identifier for this node (default: "nodeA")
- `HOST`: Host to bind to (default: "127.0.0.1")
- `PORT`: Port to bind to (default: "8001")
- `PEERS`: Comma-separated list of peer nodes (default: "nodeA=http://127.0.0.1:8001,nodeB=http://127.0.0.1:8002,nodeC=http://127.0.0.1:8003")
- `N`: Replication factor (default: 3)
- `R`: Read quorum (default: 2)
- `W`: Write quorum (default: 2)
- `VNODE_COUNT`: Number of virtual nodes per physical node (default: 64)
- `TIMEOUT_S`: Request timeout in seconds (default: 1.0, configurable)

## Running

### Single Node (for testing)

```bash
# Set environment variables for single node
export N=1
export R=1
export W=1

python server.py
```

### Multi-Node Cluster

Start multiple instances with different configurations:

**Terminal 1 (Node A):**
```bash
export NODE_ID=nodeA
export PORT=8001
python server.py
```

**Terminal 2 (Node B):**
```bash
export NODE_ID=nodeB
export PORT=8002
python server.py
```

**Terminal 3 (Node C):**
```bash
export NODE_ID=nodeC
export PORT=8003
python server.py
```

## API Endpoints

### Health Check
- `GET /health`
  - Returns node health status

### Ring Information
- `GET /ring`
  - Returns cluster configuration and peer information

### Key-Value Operations

#### Get Value
- `GET /kv/{key}`
  - Retrieves the value for a key
  - Returns: `{"ok": true, "found": true/false, "value": "...", "version": {...}}`

#### Put Value
- `PUT /kv/{key}`
  - Body: `{"value": "..."}`
  - Stores a value for a key
  - Returns: `{"ok": true, "acks": N, "version": {...}}`

#### Delete Value
- `DELETE /kv/{key}`
  - Deletes a key-value pair
  - Returns: `{"ok": true, "acks": N, "version": {...}}`

### Internal Endpoints (for replication)
- `GET /internal/replica/{key}`
- `PUT /internal/replica/{key}`

## Testing

### Availability Test

Run the included availability test:

```bash
python availability_test.py
```

This script sends 1000 GET requests to `/kv/testkey` and reports the availability percentage.

### Manual Testing

```bash
# Put a value
curl -X PUT http://127.0.0.1:8001/kv/mykey -H "Content-Type: application/json" -d '{"value": "hello world"}'

# Get the value
curl http://127.0.0.1:8001/kv/mykey

# Delete the value
curl -X DELETE http://127.0.0.1:8001/kv/mykey

# Check health
curl http://127.0.0.1:8001/health

# Get ring info
curl http://127.0.0.1:8001/ring
```

## Consistency Levels

The system supports different consistency levels by adjusting R and W:

- **Strong Consistency**: R = N, W = N
- **Eventual Consistency**: R = 1, W = 1
- **Read-Your-Writes**: W = N, R = 1
- **Quorum**: R + W > N

## Fault Tolerance

- **Crash-stop node failures**: The system continues to operate as long as R/W quorums can be met
- **Hinted Handoff**: Failed writes are stored locally and replayed when nodes recover
- **Read Repair**: Inconsistencies are detected and repaired during reads

## Performance

- Uses asyncio for concurrent request handling
- Consistent hashing minimizes data movement during node changes
- In-memory storage provides low-latency operations

## Limitations

- In-memory storage (data lost on restart)
- No persistence layer
- No authentication/authorization
- No data partitioning beyond consistent hashing

## Future Enhancements

- Persistent storage (disk-based)
- Data partitioning
- Authentication and authorization
- Monitoring and metrics
- Auto-scaling
- Conflict resolution UI

## License

This project is for educational purposes. Feel free to modify and distribute.</content>
<parameter name="filePath">c:\Users\Tanuka\OneDrive\Documents\Distributed-Key-Value-Store-with-Replication-Quorum-Consistency\README.md