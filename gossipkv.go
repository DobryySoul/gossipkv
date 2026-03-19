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
type Node[K ~string, V any] struct {
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
func New[K ~string, V any](opts ...Option) (*Node[K, V], error) {
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

	n := &Node[K, V]{
		cfg:   cfg,
		store: storage.NewMemoryStore[K, V](cfg.NodeID, nil),
	}
	if cfg.BindAddr != "" {
		node := gossip.NewNode(
			cfg.NodeID,
			cfg.BindAddr,
			cfg.Seeds,
			cfg.GossipInterval,
			n.store,
			codec.Marshal,
			codec.Unmarshal,
			errorHandler,
		)
		if err := node.Start(); err != nil {
			return nil, err
		}
		n.gossip = node
		if cfg.Discovery {
			mdns, err := discovery.NewMDNS(cfg.NodeID, cfg.BindAddr, n.gossip.AddPeers)
			if err != nil {
				_ = n.gossip.Stop()
				return nil, err
			}
			n.discovery = mdns
		}
	}
	return n, nil
}

// Set stores a value under the given key.
// The call is context-aware and returns ErrCanceled/ErrTimeout accordingly.
func (n *Node[K, V]) Set(ctx context.Context, key K, value V) error {
	if err := n.check(ctx); err != nil {
		return err
	}
	return mapStoreErr(n.store.Set(ctx, key, value))
}

// Get returns a value for the given key.
// It returns ErrNotFound if the key does not exist.
func (n *Node[K, V]) Get(ctx context.Context, key K) (V, error) {
	var zero V

	if err := n.check(ctx); err != nil {
		return zero, err
	}
	record, err := n.store.Get(ctx, key)
	if err != nil {
		return zero, mapStoreErr(err)
	}
	return record.Value, nil
}

// Close releases resources and marks the DB as closed.
// Further operations will return ErrClosed.
// The provided context allows cancellation of the close operation.
func (n *Node[K, V]) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClosed
	}
	n.closed = true
	n.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		if n.discovery != nil {
			n.discovery.Stop()
		}
		if n.gossip != nil {
			_ = n.gossip.Stop()
		}
		errCh <- n.store.Close()
	}()

	if ctx != nil {
		select {
		case <-ctx.Done():
			return mapContextErr(ctx)
		case err := <-errCh:
			return mapStoreErr(err)
		}
	} else {
		return mapStoreErr(<-errCh)
	}
}

func (n *Node[K, V]) check(ctx context.Context) error {
	if err := mapContextErr(ctx); err != nil {
		return err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
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
