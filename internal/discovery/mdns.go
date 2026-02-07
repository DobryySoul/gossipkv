package discovery

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"

	"github.com/grandcat/zeroconf"
)

const serviceName = "_gossipkv._udp"

// MDNS provides service discovery via mDNS in the local network.
type MDNS struct {
	nodeID  string
	server  *zeroconf.Server
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	entries chan *zeroconf.ServiceEntry
}

// NewMDNS announces the local node and discovers peers on the LAN.
// onPeer is called for each discovered peer address (host:port).
func NewMDNS(nodeID, bindAddr string, onPeer func([]string)) (*MDNS, error) {
	_, portStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, fmt.Errorf("discovery: invalid bind addr: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("discovery: invalid port: %w", err)
	}

	server, err := zeroconf.Register(nodeID, serviceName, "local.", port, []string{
		"node=" + nodeID,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("discovery: register: %w", err)
	}

	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		server.Shutdown()
		return nil, fmt.Errorf("discovery: resolver: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	entries := make(chan *zeroconf.ServiceEntry)
	mdns := &MDNS{
		nodeID:  nodeID,
		server:  server,
		cancel:  cancel,
		entries: entries,
	}

	mdns.wg.Add(1)
	go mdns.browseLoop(entries, onPeer)

	if err := resolver.Browse(ctx, serviceName, "local.", entries); err != nil {
		cancel()
		server.Shutdown()
		mdns.wg.Wait()
		return nil, fmt.Errorf("discovery: browse: %w", err)
	}

	return mdns, nil
}

func (m *MDNS) browseLoop(entries <-chan *zeroconf.ServiceEntry, onPeer func([]string)) {
	defer m.wg.Done()
	for entry := range entries {
		if m.isSelf(entry) {
			continue
		}
		for _, ip := range entry.AddrIPv4 {
			onPeer([]string{net.JoinHostPort(ip.String(), strconv.Itoa(entry.Port))})
		}
		for _, ip := range entry.AddrIPv6 {
			onPeer([]string{net.JoinHostPort(ip.String(), strconv.Itoa(entry.Port))})
		}
	}
}

// isSelf returns true if the discovered service entry belongs to this node.
func (m *MDNS) isSelf(entry *zeroconf.ServiceEntry) bool {
	return slices.Contains(entry.Text, "node="+m.nodeID)
}

// Stop shuts down the discovery service.
func (m *MDNS) Stop() {
	if m == nil {
		return
	}
	m.cancel()
	m.wg.Wait()
	m.server.Shutdown()
}
