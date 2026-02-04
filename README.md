# Gossip-KV: Embedded, decentralized key-value database in Go

**Status: experimental**

Gossip-KV is an **embedded** Go library that lets you build **decentralized** clusters to exchange simple key-value data. It uses a **gossip protocol** to synchronize data between nodes without external dependencies.

### Why does this exist?

Many distributed systems need to exchange simple data such as configuration, service discovery metadata, or feature flags. Often this requires running and maintaining external services like Redis or etcd.

**Gossip-KV** solves this by providing a lightweight, self-contained, fault-tolerant mechanism right inside your Go application.

### Key features

- **Embedded**: a library, not a separate server.
- **Decentralized**: no master node, all nodes are peers.
- **Eventual consistency**: data converges over time.
- **Simple API**: `Set`/`Get` with context support.
- **Flexible types**: generic API `K ~string, V any`.
- **UDP replication**: lightweight gossip cycles.

### Technology

- **Go** (generics) for type-safe API.
- **Gossip anti-entropy**: digest + delta.
- **UDP transport** for lightweight replication.
- **LWW merge**: conflict resolution by time/version.
- **In-memory storage** with no external dependencies.
- **Codecs**: `gob` by default, fast `BytesCodec`/`StringCodec` options.

### How it works

1. Each node stores data locally in memory.
2. Periodically, a random peer is selected.
3. The node sends a digest (keys + versions).
4. The receiver computes a delta and returns missing records.
5. Records are merged using LWW.

### Configuration and modes

All parameters are configured via `Option` in `New(...)`. Below is the behavior and defaults.

**`WithNodeID(string)`**
- Sets a stable node identifier.
- If not provided, a random ID is generated.
- Used for conflict resolution and diagnostics.

**`WithBindAddr("host:port")`**
- Enables network mode (UDP gossip) and binds the socket.
- If not set, the node runs locally (no gossip).
- Required if `Seeds` are provided.

**`WithSeeds([]string)`**
- List of peer addresses (bootstrap).
- Empty list means no replication.
- You can pass all known peer addresses.

**`WithGossipInterval(time.Duration)`**
- Digest/delta exchange interval.
- Default is `2s`.
- Lower values increase load but converge faster.

**`WithDiscovery(bool)`**
- Enables mDNS discovery on the local network.
- Requires `WithBindAddr` so the node can announce itself.

**`WithCodec(codec)`**
- Serialization codec used for gossip.
- Default is `GobCodec`.
- For maximum speed use:
  - `BytesCodec` for `[]byte`
  - `StringCodec` for `string`

**`WithErrorHandler(func(error))`**
- Callback for internal errors (serialize/network).
- Best-effort, must be fast and non-blocking.

**Default behavior (no options)**
- Local in-memory database.
- No network replication.
- Random `NodeID`.

### Use cases

- Configuration propagation between service instances.
- Feature flags without external storage.
- Discovery/metadata in local clusters.
- Caches where eventual consistency is acceptable.

### Limitations

- No strong consistency (eventual consistency only).
- No persistence (in-memory only).
- No built-in auth/encryption.
- mDNS discovery is LAN-only; for cross-network use explicit seeds.

### Package layout

- `gossipkv/` — public API.
- `internal/gossip/` — gossip protocol and transport.
- `internal/storage/` — storage and merge logic.

### Quick start

1.  **Установка**

    ```bash
    go get github.com/DobryySoul/gossipkv
    ```

### Manual peer setup (no discovery)

To exchange data between nodes, provide a bind address and a list of peers.

```go
db, err := gossipkv.New[string, string](
    gossipkv.WithNodeID("node-a"),
    gossipkv.WithBindAddr("127.0.0.1:9001"),
    gossipkv.WithSeeds([]string{
        "127.0.0.1:9002",
        "127.0.0.1:9003",
    }),
)
```

2.  **Использование**

    Initialize the database and start using it.

    ```go
    package main

    import (
        "context"
        "fmt"
        "github.com/DobryySoul/gossip-kv/gossipkv"
    )

    func main() {
        // Create a new node.
        db, err := gossipkv.New[string, string]()
        if err != nil {
            // handle error
        }

        // Set a value
        err = db.Set(context.Background(), "version", "v1.0.1")
        if err != nil {
            // handle error
        }

        // Get a value
        value, err := db.Get(context.Background(), "version")
        if err != nil {
            // handle error
        }
        
        fmt.Println("value:", value)
    }
    ```