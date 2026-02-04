// Package gossipkv provides an embedded, decentralized key-value store
// with an eventual-consistency gossip protocol.
//
// # Overview
//
// gossipkv is designed for simple, low-latency data sharing between
// service instances without external dependencies. Nodes exchange updates
// using a lightweight gossip cycle (digest + delta) over UDP.
//
// # Data model
//
// Records are versioned and merged using a last-write-wins (LWW) strategy
// based on UpdatedAt, Version, and NodeID. This provides eventual
// consistency with minimal coordination overhead.
//
// # Generics
//
// The DB type is generic over key and value types. Keys must be string
// or a type with underlying string. Values can be any type.
//
// # Networking
//
// Gossip is enabled when a bind address is provided. Without a bind
// address, the database works in local in-memory mode.
//
// # Serialization
//
// Values are serialized for transport using a Codec. The default is GobCodec,
// while BytesCodec and StringCodec provide faster binary encoding for []byte
// and string values.
//
// Example
//
//	db, err := gossipkv.New[string, string](
//		gossipkv.WithBindAddr("127.0.0.1:9001"),
//		gossipkv.WithSeeds([]string{"127.0.0.1:9002"}),
//		gossipkv.WithCodec(gossipkv.StringCodec{}),
//	)
//	if err != nil {
//		// handle error
//	}
//	_ = db.Set(context.Background(), "key", "value")
//	_, _ = db.Get(context.Background(), "key")
package gossipkv
