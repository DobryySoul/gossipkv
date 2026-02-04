package gossip

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/DobryySoul/gossipkv/internal/storage"
)

func BenchmarkGossipEncodeDecode(b *testing.B) {
	records := make([]Record, 0, 100)
	for i := range 100 {
		records = append(records, Record{
			Key:       "k" + strconv.Itoa(i),
			Value:     []byte("value"),
			Version:   uint64(i + 1),
			NodeID:    "node-a",
			UpdatedAt: time.Now(),
		})
	}
	msg := Message{
		Kind:    msgDelta,
		Records: records,
	}

	for b.Loop() {
		data, err := encodeMessage(msg)
		if err != nil {
			b.Fatalf("encode failed: %v", err)
		}
		if _, err := decodeMessage(data); err != nil {
			b.Fatalf("decode failed: %v", err)
		}
	}
}

func BenchmarkGossipBuildDigest(b *testing.B) {
	store := storage.NewMemoryStore[string, string]("node-a", time.Now)
	ctx := context.Background()
	for i := range 1000 {
		_ = store.Set(ctx, "k"+strconv.Itoa(i), "value")
	}

	for b.Loop() {
		size, err := store.Len(ctx)
		if err != nil {
			b.Fatalf("len failed: %v", err)
		}
		digest := make([]DigestItem, 0, size)
		if err := store.Range(ctx, func(key string, record storage.Record[string]) bool {
			digest = append(digest, DigestItem{
				Key:       key,
				Version:   record.Version,
				NodeID:    record.NodeID,
				UpdatedAt: record.UpdatedAt,
			})
			return true
		}); err != nil {
			b.Fatalf("range failed: %v", err)
		}
		_ = digest
	}
}

func BenchmarkGossipHandleDelta(b *testing.B) {
	store := storage.NewMemoryStore[string, string]("node-a", time.Now)
	node := NewNode(
		"node-a",
		"",
		nil,
		time.Second,
		store,
		func(v string) ([]byte, error) { return []byte(v), nil },
		func(data []byte) (string, error) { return string(data), nil },
		func(error) {},
	)

	records := make([]Record, 0, 100)
	now := time.Now()
	for i := range 100 {
		records = append(records, Record{
			Key:       "k" + strconv.Itoa(i),
			Value:     []byte("value"),
			Version:   uint64(i + 1),
			NodeID:    "node-b",
			UpdatedAt: now.Add(time.Duration(i) * time.Nanosecond),
		})
	}

	for b.Loop() {
		node.handleDelta(records)
	}
}
