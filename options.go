package gossipkv

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"time"
)

// Option configures the database on creation.
// Return an error to reject an invalid option value.
type Option func(*Config) error

// Config holds runtime configuration for a gossipkv node.
// Users typically set it via Option helpers.
type Config struct {
	NodeID         string
	BindAddr       string
	Seeds          []string
	Discovery      bool
	GossipInterval time.Duration
	codec          any
	errorHandler   func(error)
}

func defaultConfig() Config {
	return Config{
		Discovery:      true,
		GossipInterval: 2 * time.Second,
	}
}

func (c *Config) finalize() error {
	if c.NodeID == "" {
		id, err := randomNodeID()
		if err != nil {
			return err
		}
		c.NodeID = id
	}
	if c.BindAddr != "" {
		if err := validateAddr(c.BindAddr); err != nil {
			return err
		}
	}
	if c.GossipInterval <= 0 {
		return fmt.Errorf("gossipkv: gossip interval must be positive")
	}
	return nil
}

// WithNodeID sets a stable node identifier used in gossip metadata.
// If omitted, a random ID is generated.
func WithNodeID(nodeID string) Option {
	return func(c *Config) error {
		if nodeID == "" {
			return fmt.Errorf("gossipkv: node id cannot be empty")
		}
		c.NodeID = nodeID
		return nil
	}
}

// WithBindAddr sets the local bind address in host:port form.
// It is validated with net.SplitHostPort.
func WithBindAddr(addr string) Option {
	return func(c *Config) error {
		if addr == "" {
			return fmt.Errorf("gossipkv: bind addr cannot be empty")
		}
		if err := validateAddr(addr); err != nil {
			return err
		}
		c.BindAddr = addr
		return nil
	}
}

// WithSeeds sets the initial seed node addresses for bootstrapping.
func WithSeeds(seeds []string) Option {
	return func(c *Config) error {
		c.Seeds = append([]string(nil), seeds...)
		return nil
	}
}

// WithDiscovery enables or disables discovery mechanisms like mDNS.
func WithDiscovery(enabled bool) Option {
	return func(c *Config) error {
		c.Discovery = enabled
		return nil
	}
}

// WithGossipInterval sets how often the node exchanges gossip updates.
func WithGossipInterval(interval time.Duration) Option {
	return func(c *Config) error {
		if interval <= 0 {
			return fmt.Errorf("gossipkv: gossip interval must be positive")
		}
		c.GossipInterval = interval
		return nil
	}
}

// WithCodec sets the value codec used by gossip transport.
func WithCodec[V any](codec Codec[V]) Option {
	return func(c *Config) error {
		if codec == nil {
			return fmt.Errorf("gossipkv: codec cannot be nil")
		}
		c.codec = codec
		return nil
	}
}

// WithErrorHandler sets a callback for internal errors (serialization, network).
// It is best-effort and must be fast and non-blocking.
func WithErrorHandler(handler func(error)) Option {
	return func(c *Config) error {
		if handler == nil {
			return fmt.Errorf("gossipkv: error handler cannot be nil")
		}
		c.errorHandler = handler
		return nil
	}
}

func randomNodeID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("gossipkv: generate node id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func validateAddr(addr string) error {
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("gossipkv: invalid address %q: %w", addr, err)
	}
	return nil
}
