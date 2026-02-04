package gossipkv

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/DobryySoul/gossipkv/internal/discovery"
	"github.com/DobryySoul/gossipkv/internal/gossip"
	"github.com/DobryySoul/gossipkv/internal/storage"
)

// DB represents a running gossipkv node.
// It is safe for concurrent use by multiple goroutines.
// K must be a string or a type with underlying string.
type DB[K ~string, V any] struct {
	cfg       Config
	store     storage.Store[K, V]
	gossip    *gossip.Node[K, V]
	discovery *discovery.MDNS
	mu        sync.RWMutex
	closed    bool
}

// New creates a new gossipkv node with the provided options.
// The returned instance uses an in-memory store in the current version.
// K must be provided explicitly because it cannot be inferred from arguments.
func New[K ~string, V any](opts ...Option) (*DB[K, V], error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	if len(cfg.Seeds) > 0 && cfg.BindAddr == "" {
		return nil, fmt.Errorf("gossipkv: bind addr required when seeds are set")
	}

	codec := Codec[V](GobCodec[V]{})
	if cfg.codec != nil {
		typed, ok := cfg.codec.(Codec[V])
		if !ok {
			return nil, fmt.Errorf("gossipkv: codec type mismatch")
		}
		codec = typed
	}
	errorHandler := cfg.errorHandler
	if errorHandler == nil {
		errorHandler = func(error) {}
	}

	db := &DB[K, V]{
		cfg:   cfg,
		store: storage.NewMemoryStore[K, V](cfg.NodeID, nil),
	}
	if cfg.BindAddr != "" {
		node := gossip.NewNode(
			cfg.NodeID,
			cfg.BindAddr,
			cfg.Seeds,
			cfg.GossipInterval,
			db.store,
			codec.Marshal,
			codec.Unmarshal,
			errorHandler,
		)
		if err := node.Start(); err != nil {
			return nil, err
		}
		db.gossip = node
		if cfg.Discovery {
			mdns, err := discovery.NewMDNS(cfg.NodeID, cfg.BindAddr, db.gossip.AddPeers)
			if err != nil {
				_ = db.gossip.Stop()
				return nil, err
			}
			db.discovery = mdns
		}
	}
	return db, nil
}

// Set stores a value under the given key.
// The call is context-aware and returns ErrCanceled/ErrTimeout accordingly.
func (db *DB[K, V]) Set(ctx context.Context, key K, value V) error {
	if err := db.check(ctx); err != nil {
		return err
	}
	return mapStoreErr(db.store.Set(ctx, key, value))
}

// Get returns a value for the given key.
// It returns ErrNotFound if the key does not exist.
func (db *DB[K, V]) Get(ctx context.Context, key K) (V, error) {
	var zero V

	if err := db.check(ctx); err != nil {
		return zero, err
	}
	record, err := db.store.Get(ctx, key)
	if err != nil {
		return zero, mapStoreErr(err)
	}
	return record.Value, nil
}

// Close releases resources and marks the DB as closed.
// Further operations will return ErrClosed.
// The provided context allows cancellation of the close operation.
func (db *DB[K, V]) Close(ctx context.Context) error {
	if err := mapContextErr(ctx); err != nil {
		return err
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.mu.Unlock()
	if db.discovery != nil {
		db.discovery.Stop()
	}
	if db.gossip != nil {
		_ = db.gossip.Stop()
	}
	return mapStoreErr(db.store.Close())
}

func (db *DB[K, V]) check(ctx context.Context) error {
	if err := mapContextErr(ctx); err != nil {
		return err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return nil
}

func mapContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrTimeout
		}
		if errors.Is(err, context.Canceled) {
			return ErrCanceled
		}
		return err
	}
	return nil
}

func mapStoreErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrCanceled
	}
	return err
}
