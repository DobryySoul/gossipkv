package gossip

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/DobryySoul/gossipkv/internal/storage"
)

// maxUDPPayload is a safe upper bound for a single UDP datagram.
// Messages exceeding this size are automatically chunked.
const maxUDPPayload = 65000

// Limit digest size to prevent UDP packet overflow and reduce overhead.
// 500 items easily fits in 64KB UDP packet.
const maxDigestItems = 500

// Defend against amplification by bounding the processed digest
const maxProcessItems = 1000

type Node[K ~string, V any] struct {
	id        string
	bindAddr  string
	interval  time.Duration
	store     storage.Store[K, V]
	marshal   func(V) ([]byte, error)
	unmarshal func([]byte) (V, error)
	onError   func(error)

	conn     *net.UDPConn
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

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

	n.wg.Go(func() {
		n.readLoop()
	})
	n.wg.Go(func() {
		n.gossipLoop()
	})
	return nil
}

// Stop gracefully shuts down the gossip node. It is safe to call multiple times.
func (n *Node[K, V]) Stop() error {
	n.stopOnce.Do(func() {
		close(n.stop)
		if n.conn != nil {
			_ = n.conn.Close()
		}
		if n.cancel != nil {
			n.cancel()
		}
	})
	n.wg.Wait()
	return nil
}

func (n *Node[K, V]) readLoop() {
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
		if msg.Kind == msgNeed {
			// Only accept need requests from known peers to prevent amplification reflection
			n.peersMu.RLock()
			_, knownPeer := n.peersSet[addr.String()]
			n.peersMu.RUnlock()
			if !knownPeer {
				n.reportErr(fmt.Errorf("gossip: ignoring need request from unknown peer %s", addr.String()))
				return
			}
			n.handleNeed(addr, msg.Need)
			return
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

	digest := make([]DigestItem, 0, maxDigestItems)

	_ = n.store.Range(n.ctx, func(key K, record storage.Record[V]) bool {
		digest = append(digest, DigestItem{
			Key:       string(key),
			Version:   record.Version,
			NodeID:    record.NodeID,
			UpdatedAt: record.UpdatedAt,
		})
		return len(digest) < maxDigestItems
	})

	if len(digest) == 0 {
		return
	}

	n.sendMessage(peer, Message{
		Kind:   msgDigest,
		Digest: digest,
	})
}

// handleDigest implements push-pull anti-entropy.
// It checks the received digest and requests missing/stale records,
// and proactively sends our newer records for the keys in the digest.
func (n *Node[K, V]) handleDigest(addr *net.UDPAddr, digest []DigestItem) {
	if len(digest) > maxProcessItems {
		digest = digest[:maxProcessItems]
	}

	var need []string
	var delta []Record

	for _, item := range digest {
		local, err := n.store.Get(n.ctx, K(item.Key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				need = append(need, item.Key)
			}
			continue
		}

		remoteRecord := storage.Record[V]{
			Version:   item.Version,
			NodeID:    item.NodeID,
			UpdatedAt: item.UpdatedAt,
		}

		if remoteRecord.NewerThan(local) {
			need = append(need, item.Key)
		} else if local.NewerThan(remoteRecord) {
			value, err := n.marshal(local.Value)
			if err != nil {
				n.reportErr(fmt.Errorf("gossip: marshal: %w", err))
				continue
			}
			delta = append(delta, Record{
				Key:       item.Key,
				Value:     value,
				Version:   local.Version,
				NodeID:    local.NodeID,
				UpdatedAt: local.UpdatedAt,
			})
		}
	}

	if len(need) > 0 {
		n.sendMessage(addr.String(), Message{
			Kind: msgNeed,
			Need: need,
		})
	}
	if len(delta) > 0 {
		n.sendMessage(addr.String(), Message{
			Kind:    msgDelta,
			Records: delta,
		})
	}
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

// handleNeed responds with records that the remote node has requested.
func (n *Node[K, V]) handleNeed(addr *net.UDPAddr, keys []string) {
	const maxProcessItems = 1000
	if len(keys) > maxProcessItems {
		keys = keys[:maxProcessItems]
	}

	delta := make([]Record, 0, len(keys))
	for _, key := range keys {
		record, err := n.store.Get(n.ctx, K(key))
		if err != nil {
			continue
		}
		value, err := n.marshal(record.Value)
		if err != nil {
			n.reportErr(fmt.Errorf("gossip: marshal: %w", err))
			continue
		}
		delta = append(delta, Record{
			Key:       key,
			Value:     value,
			Version:   record.Version,
			NodeID:    record.NodeID,
			UpdatedAt: record.UpdatedAt,
		})
	}
	if len(delta) > 0 {
		n.sendMessage(addr.String(), Message{
			Kind:    msgDelta,
			Records: delta,
		})
	}
}

// sendMessage encodes and sends a message via UDP. Messages that exceed
// maxUDPPayload are automatically split into smaller chunks for delta and
// need messages. Single records exceeding limits are dropped to prevent stalling.
func (n *Node[K, V]) sendMessage(addr string, msg Message) {
	data, err := encodeMessage(msg)
	if err != nil {
		n.reportErr(fmt.Errorf("gossip: encode message: %w", err))
		return
	}
	if len(data) > maxUDPPayload {
		switch msg.Kind {
		case msgDelta:
			if len(msg.Records) > 1 {
				mid := len(msg.Records) / 2
				n.sendMessage(addr, Message{Kind: msgDelta, Records: msg.Records[:mid]})
				n.sendMessage(addr, Message{Kind: msgDelta, Records: msg.Records[mid:]})
				return
			}
			// One record alone exceeds max UDP size. Drop it to prevent poison pill.
			n.reportErr(fmt.Errorf("gossip: single delta record too large: %d bytes (dropped)", len(data)))
			return
		case msgNeed:
			if len(msg.Need) > 1 {
				mid := len(msg.Need) / 2
				n.sendMessage(addr, Message{Kind: msgNeed, Need: msg.Need[:mid]})
				n.sendMessage(addr, Message{Kind: msgNeed, Need: msg.Need[mid:]})
				return
			}
		case msgDigest:
			if len(msg.Digest) > 1 {
				mid := len(msg.Digest) / 2
				n.sendMessage(addr, Message{Kind: msgDigest, Digest: msg.Digest[:mid]})
				n.sendMessage(addr, Message{Kind: msgDigest, Digest: msg.Digest[mid:]})
				return
			}
		}
		n.reportErr(fmt.Errorf("gossip: message too large: %d bytes (limit %d)", len(data), maxUDPPayload))
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
	index := rand.IntN(len(n.peers))
	return n.peers[index], true
}

func (n *Node[K, V]) reportErr(err error) {
	if n.onError == nil || err == nil {
		return
	}
	n.onError(err)
}
