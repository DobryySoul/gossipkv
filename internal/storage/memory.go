package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type memoryStore[K ~string, V any] struct {
	mu      sync.RWMutex
	values  map[K]Record[V]
	nodeID  string
	clock   func() time.Time
	version uint64
}

func NewMemoryStore[K ~string, V any](nodeID string, clock func() time.Time) Store[K, V] {
	if clock == nil {
		clock = time.Now
	}
	return &memoryStore[K, V]{
		values: make(map[K]Record[V]),
		nodeID: nodeID,
		clock:  clock,
	}
}

func (s *memoryStore[K, V]) Set(ctx context.Context, key K, value V) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	record := Record[V]{
		Value:     value,
		Version:   atomic.AddUint64(&s.version, 1),
		NodeID:    s.nodeID,
		UpdatedAt: s.clock(),
	}
	s.values[key] = record
	s.mu.Unlock()
	return nil
}

func (s *memoryStore[K, V]) Get(ctx context.Context, key K) (Record[V], error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return Record[V]{}, err
		}
	}
	s.mu.RLock()
	record, ok := s.values[key]
	s.mu.RUnlock()
	if !ok {
		return Record[V]{}, ErrNotFound
	}
	return record, nil
}

func (s *memoryStore[K, V]) Merge(ctx context.Context, key K, record Record[V]) (bool, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	s.mu.Lock()
	current, ok := s.values[key]
	if !ok || record.NewerThan(current) {
		s.values[key] = record
		s.mu.Unlock()
		return true, nil
	}
	s.mu.Unlock()
	return false, nil
}

func (s *memoryStore[K, V]) Snapshot(ctx context.Context) (map[K]Record[V], error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	s.mu.RLock()
	out := make(map[K]Record[V], len(s.values))
	for key, value := range s.values {
		out[key] = value
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *memoryStore[K, V]) Len(ctx context.Context) (int, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	s.mu.RLock()
	size := len(s.values)
	s.mu.RUnlock()
	return size, nil
}

// Range holds a read lock for the duration of the iteration. Do not call
// mutating methods from the callback to avoid deadlocks.
func (s *memoryStore[K, V]) Range(ctx context.Context, fn func(key K, record Record[V]) bool) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.RLock()
	for key, record := range s.values {
		if !fn(key, record) {
			break
		}
	}
	s.mu.RUnlock()
	return nil
}

func (s *memoryStore[K, V]) Close() error {
	return nil
}
