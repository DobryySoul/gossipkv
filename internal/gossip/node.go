package gossip

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/DobryySoul/gossipkv/internal/storage"
)

type Node[K ~string, V any] struct {
	id        string
	bindAddr  string
	interval  time.Duration
	store     storage.Store[K, V]
	marshal   func(V) ([]byte, error)
	unmarshal func([]byte) (V, error)
	onError   func(error)

	conn *net.UDPConn
	stop chan struct{}
	wg   sync.WaitGroup

	peersMu  sync.RWMutex
	peers    []string
	peersSet map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func NewNode[K ~string, V any](
	nodeID string,
	bindAddr string,
	peers []string,
	interval time.Duration,
	store storage.Store[K, V],
	marshal func(V) ([]byte, error),
	unmarshal func([]byte) (V, error),
	onError func(error),
) *Node[K, V] {
	filtered := filterPeers(bindAddr, peers)
	peersSet := make(map[string]struct{}, len(filtered))
	for _, peer := range filtered {
		peersSet[peer] = struct{}{}
	}
	return &Node[K, V]{
		id:        nodeID,
		bindAddr:  bindAddr,
		interval:  interval,
		store:     store,
		marshal:   marshal,
		unmarshal: unmarshal,
		onError:   onError,
		stop:      make(chan struct{}),
		peers:     filtered,
		peersSet:  peersSet,
		ctx:       context.Background(),
	}
}

func (n *Node[K, V]) Start() error {
	addr, err := net.ResolveUDPAddr("udp", n.bindAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	n.conn = conn
	n.ctx, n.cancel = context.WithCancel(context.Background())

	n.wg.Add(2)
	go n.readLoop()
	go n.gossipLoop()
	return nil
}

func (n *Node[K, V]) Stop() error {
	close(n.stop)
	if n.conn != nil {
		_ = n.conn.Close()
	}
	if n.cancel != nil {
		n.cancel()
	}
	n.wg.Wait()
	return nil
}

func (n *Node[K, V]) readLoop() {
	defer n.wg.Done()
	buf := make([]byte, 64*1024)

	for {
		n.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		nbytes, addr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-n.stop:
				return
			default:
				continue
			}
		}

		msg, err := decodeMessage(buf[:nbytes])
		if err != nil {
			n.reportErr(fmt.Errorf("gossip: decode message: %w", err))
			continue
		}
		switch msg.Kind {
		case msgDigest:
			n.handleDigest(addr, msg.Digest)
		case msgDelta:
			n.handleDelta(msg.Records)
		}
	}
}

func (n *Node[K, V]) gossipLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.sendDigest()
		}
	}
}

func (n *Node[K, V]) sendDigest() {
	peer, ok := n.randomPeer()
	if !ok {
		return
	}
	size, err := n.store.Len(n.ctx)
	if err != nil {
		return
	}
	digest := make([]DigestItem, 0, size)
	if err := n.store.Range(n.ctx, func(key K, record storage.Record[V]) bool {
		digest = append(digest, DigestItem{
			Key:       string(key),
			Version:   record.Version,
			NodeID:    record.NodeID,
			UpdatedAt: record.UpdatedAt,
		})
		return true
	}); err != nil {
		return
	}
	n.sendMessage(peer, Message{
		Kind:   msgDigest,
		Digest: digest,
	})
}

func (n *Node[K, V]) handleDigest(addr *net.UDPAddr, digest []DigestItem) {
	remote := make(map[string]storage.Record[V], len(digest))
	for _, item := range digest {
		remote[item.Key] = storage.Record[V]{
			Version:   item.Version,
			NodeID:    item.NodeID,
			UpdatedAt: item.UpdatedAt,
		}
	}

	size, err := n.store.Len(n.ctx)
	if err != nil {
		return
	}
	delta := make([]Record, 0, size)
	if err := n.store.Range(n.ctx, func(key K, record storage.Record[V]) bool {
		other, ok := remote[string(key)]
		if !ok || record.NewerThan(other) {
			value, err := n.marshal(record.Value)
			if err != nil {
				n.reportErr(fmt.Errorf("gossip: marshal: %w", err))
				return true
			}
			delta = append(delta, Record{
				Key:       string(key),
				Value:     value,
				Version:   record.Version,
				NodeID:    record.NodeID,
				UpdatedAt: record.UpdatedAt,
			})
		}
		return true
	}); err != nil {
		return
	}

	if len(delta) == 0 {
		return
	}
	n.sendMessage(addr.String(), Message{
		Kind:    msgDelta,
		Records: delta,
	})
}

func (n *Node[K, V]) handleDelta(records []Record) {
	for _, item := range records {
		value, err := n.unmarshal(item.Value)
		if err != nil {
			n.reportErr(fmt.Errorf("gossip: unmarshal: %w", err))
			continue
		}
		record := storage.Record[V]{
			Value:     value,
			Version:   item.Version,
			NodeID:    item.NodeID,
			UpdatedAt: item.UpdatedAt,
		}
		_, _ = n.store.Merge(n.ctx, K(item.Key), record)
	}
}

func (n *Node[K, V]) sendMessage(addr string, msg Message) {
	data, err := encodeMessage(msg)
	if err != nil {
		n.reportErr(fmt.Errorf("gossip: encode message: %w", err))
		return
	}
	peerAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		n.reportErr(fmt.Errorf("gossip: resolve addr: %w", err))
		return
	}
	if _, err := n.conn.WriteToUDP(data, peerAddr); err != nil {
		n.reportErr(fmt.Errorf("gossip: send: %w", err))
	}
}

func filterPeers(bindAddr string, peers []string) []string {
	seen := make(map[string]struct{}, len(peers))
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer == "" || peer == bindAddr {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		out = append(out, peer)
	}
	return out
}

func (n *Node[K, V]) AddPeers(peers []string) {
	filtered := filterPeers(n.bindAddr, peers)
	if len(filtered) == 0 {
		return
	}
	n.peersMu.Lock()
	for _, peer := range filtered {
		if _, ok := n.peersSet[peer]; ok {
			continue
		}
		n.peersSet[peer] = struct{}{}
		n.peers = append(n.peers, peer)
	}
	n.peersMu.Unlock()
}

func (n *Node[K, V]) randomPeer() (string, bool) {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()
	if len(n.peers) == 0 {
		return "", false
	}
	index, err := cryptoIntn(len(n.peers))
	if err != nil {
		return "", false
	}
	return n.peers[index], true
}

func cryptoIntn(n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("gossip: invalid bound")
	}
	limit := big.NewInt(int64(n))
	value, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return 0, err
	}
	return int(value.Int64()), nil
}

func (n *Node[K, V]) reportErr(err error) {
	if n.onError == nil || err == nil {
		return
	}
	n.onError(err)
}
