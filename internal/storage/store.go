package storage

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("storage: key not found")

// Record is a versioned value stored in the database.
// It is used for conflict resolution and gossip merges.
type Record[V any] struct {
	Value     V
	Version   uint64
	NodeID    string
	UpdatedAt time.Time
}

// NewerThan returns true if r should replace other.
// Comparison order: UpdatedAt, Version, NodeID (lexicographic).
func (r Record[V]) NewerThan(other Record[V]) bool {
	if r.UpdatedAt.After(other.UpdatedAt) {
		return true
	}
	if r.UpdatedAt.Before(other.UpdatedAt) {
		return false
	}
	if r.Version > other.Version {
		return true
	}
	if r.Version < other.Version {
		return false
	}
	return r.NodeID > other.NodeID
}

// Store provides basic versioned KV operations.
type Store[K ~string, V any] interface {
	Set(ctx context.Context, key K, value V) error
	Get(ctx context.Context, key K) (Record[V], error)
	Merge(ctx context.Context, key K, record Record[V]) (bool, error)
	// Snapshot returns a point-in-time copy of all records.
	Snapshot(ctx context.Context) (map[K]Record[V], error)
	// Len returns the number of stored records.
	Len(ctx context.Context) (int, error)
	// Range iterates over all records until fn returns false.
	// The callback MUST NOT call mutating store methods (Set/Merge) or
	// any method that tries to take a write lock, otherwise it can deadlock.
	Range(ctx context.Context, fn func(key K, record Record[V]) bool) error
	Close() error
}
