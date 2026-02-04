package storage

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSetGet(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := func() time.Time { return now }
	store := NewMemoryStore[string, string]("node-a", clock)

	if err := store.Set(context.Background(), "k1", "v1"); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	record, err := store.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if record.Value != "v1" {
		t.Fatalf("value mismatch: %v", record.Value)
	}
	if record.NodeID != "node-a" {
		t.Fatalf("node id mismatch: %v", record.NodeID)
	}
	if !record.UpdatedAt.Equal(now) {
		t.Fatalf("updatedAt mismatch: %v", record.UpdatedAt)
	}
	if record.Version != 1 {
		t.Fatalf("version mismatch: %v", record.Version)
	}
}

func TestMemoryStoreMerge(t *testing.T) {
	store := NewMemoryStore[string, string]("node-a", time.Now)

	older := Record[string]{
		Value:     "v1",
		Version:   1,
		NodeID:    "node-a",
		UpdatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	newer := Record[string]{
		Value:     "v2",
		Version:   2,
		NodeID:    "node-b",
		UpdatedAt: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	changed, err := store.Merge(context.Background(), "k1", older)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if !changed {
		t.Fatalf("expected change on first merge")
	}

	changed, err = store.Merge(context.Background(), "k1", newer)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if !changed {
		t.Fatalf("expected change on newer merge")
	}

	record, err := store.Get(context.Background(), "k1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if record.Value != "v2" {
		t.Fatalf("value mismatch: %v", record.Value)
	}
}

func TestMemoryStoreSnapshot(t *testing.T) {
	store := NewMemoryStore[string, string]("node-a", time.Now)
	if err := store.Set(context.Background(), "k1", "v1"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := store.Set(context.Background(), "k2", "v2"); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	snapshot, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("snapshot size mismatch: %v", len(snapshot))
	}
	if snapshot["k1"].Value != "v1" || snapshot["k2"].Value != "v2" {
		t.Fatalf("snapshot values mismatch: %#v", snapshot)
	}
}

func TestMemoryStoreRangeAndLen(t *testing.T) {
	store := NewMemoryStore[string, string]("node-a", time.Now)
	ctx := context.Background()
	_ = store.Set(ctx, "k1", "v1")
	_ = store.Set(ctx, "k2", "v2")

	size, err := store.Len(ctx)
	if err != nil {
		t.Fatalf("len failed: %v", err)
	}
	if size != 2 {
		t.Fatalf("len mismatch: %v", size)
	}

	seen := make(map[string]struct{})
	err = store.Range(ctx, func(key string, record Record[string]) bool {
		seen[key] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("range failed: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("range mismatch: %v", len(seen))
	}
}

func TestRecordNewerThanTieBreakers(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	base := Record[string]{
		Value:     "v1",
		Version:   1,
		NodeID:    "node-a",
		UpdatedAt: ts,
	}
	higherVersion := Record[string]{
		Value:     "v2",
		Version:   2,
		NodeID:    "node-a",
		UpdatedAt: ts,
	}
	lexicographicWinner := Record[string]{
		Value:     "v3",
		Version:   1,
		NodeID:    "node-b",
		UpdatedAt: ts,
	}

	if !higherVersion.NewerThan(base) {
		t.Fatalf("expected higher version to win")
	}
	if !lexicographicWinner.NewerThan(base) {
		t.Fatalf("expected node id tie-breaker to win")
	}
}

func BenchmarkMemoryStoreSet(b *testing.B) {
	store := NewMemoryStore[string, string]("node-a", time.Now)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Set(ctx, "key", "value")
	}
}

func BenchmarkMemoryStoreGet(b *testing.B) {
	store := NewMemoryStore[string, string]("node-a", time.Now)
	ctx := context.Background()
	_ = store.Set(ctx, "key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(ctx, "key")
	}
}

func BenchmarkMemoryStoreMerge(b *testing.B) {
	store := NewMemoryStore[string, string]("node-a", time.Now)
	ctx := context.Background()
	record := Record[string]{
		Value:     "value",
		Version:   1,
		NodeID:    "node-b",
		UpdatedAt: time.Now(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.Version++
		record.UpdatedAt = record.UpdatedAt.Add(time.Nanosecond)
		_, _ = store.Merge(ctx, "key", record)
	}
}
