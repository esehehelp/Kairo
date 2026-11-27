// Package fabric keeps a node on the fabric (issue #47): its WireGuard key,
// its interface at the address the daemon gave it, and its peers, as the
// daemon says (store.FabricView). Which endpoint reaches a peer is the
// agent's business: it tries the peer's endpoints in its order and moves
// on from one that gives no handshake.
package fabric

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"kairo/internal/store"
)

// Defaults of an agent's [fabric].
const (
	DefaultInterface  = "kairo0"
	DefaultListenPort = 51820
	// MTU leaves room for WireGuard's and VXLAN's headers under a 1500-byte
	// path.
	MTU = 1420
)

// syncEvery: how often the node says what it is and takes its peers.
const syncEvery = 30 * time.Second

// handshakeFresh: a peer with a handshake this recent is reached; one with
// none for this long after its endpoint was set is tried at its next.
const (
	handshakeFresh = 3 * time.Minute
	endpointTrial  = 30 * time.Second
)

// Manager keeps this node on the fabric.
type Manager struct {
	Interface  string
	ListenPort int
	// KeyFile holds the node's private key (base64, as wg reads it); made
	// the first time. Its address is kept beside it (KeyFile.address), and
	// the last view the daemon gave (KeyFile.view: Restore).
	KeyFile   string
	Endpoints []string
	// ProbePort is where peers time a TCP connect to this node (its
	// transfer endpoint's port; 0: none).
	ProbePort int
	// RelayFrom and RelayTo are the UDP ports this node offers to relay
	// other nodes' datagrams (0: none; relay.go).
	RelayFrom, RelayTo int
	// Routing: this node runs FRR and carries segments (an agent's:
	// segments.go). NodeID names it to FRR, Names prefixes its segments'
	// devices ("": DefaultNames) and RouterName is FRR's container.
	Routing    bool
	NodeID     string
	Names      string
	RouterName string
	// Sync says what this node is and returns its address and peers.
	Sync func(ctx context.Context, self store.FabricSelf) (store.FabricView, error)
	// Report says how this node sees its links (nil: not said).
	Report func(ctx context.Context, report store.FabricLinksReport) error
	Logger *slog.Logger
	// Now is the clock (nil: time.Now).
	Now func() time.Time

	link link // the interface (fabric_linux.go)

	mu       sync.Mutex
	public   string
	address  string
	peers    map[string]*peerState // by public key
	view     store.FabricView      // the last one applied
	received []store.FabricReceived
	reported time.Time
	relays   relays
	router   router // FRR (segments.go), made the first time it is needed
	// segMu keeps segments made and taken down one at a time (a sync and
	// launches); launched: when each segment was put here for a launch
	segMu    sync.Mutex
	launched map[int]time.Time
}

// peerState is which of a peer's endpoints is tried, since when.
type peerState struct {
	endpoint int
	since    time.Time
}

// peerSeen is a peer as the interface has it: its latest handshake (zero:
// none) and the endpoint it is reached at now ("" when unknown).
type peerSeen struct {
	handshake time.Time
	endpoint  string
}

// link is what the manager needs of the interface.
type link interface {
	// ensure makes the interface (if missing) with address/bits, the key
	// file and the port, up.
	ensure(ctx context.Context, name, address string, bits int, keyFile string, port int) error
	// peers are the interface's peers, by public key.
	peers(ctx context.Context, name string) (map[string]peerSeen, error)
	// setPeer sets a peer: its address (/32) and the endpoint tried (""
	// leaves the one it has: none, or where WireGuard last heard it from).
	setPeer(ctx context.Context, name, publicKey, address, endpoint string) error
	removePeer(ctx context.Context, name, publicKey string) error
	// segments are the VNIs of the segments made here (their devices named
	// with names); ensureSegment makes one (its VXLAN from local, on port),
	// removeSegment takes one down.
	segments(ctx context.Context, names string) ([]int, error)
	ensureSegment(ctx context.Context, names string, seg store.FabricSegment, local string, port int) error
	removeSegment(ctx context.Context, names string, vni int) error
	// router is FRR in a container named name, with hostname (FRR says it
	// to its peers: an empty one is refused), its files beside keyFile
	// where it keeps them there.
	router(name, hostname, keyFile string) router
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) logger() *slog.Logger {
	if m.Logger != nil {
		return m.Logger
	}
	return slog.Default()
}

// Address is this node's fabric address ("" before the daemon gave one).
func (m *Manager) Address() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.address
}

// OnFabric reports whether host (an IPv4 address) is in the fabric's
// prefix as this node was last told it.
func (m *Manager) OnFabric(host string) bool {
	m.mu.Lock()
	prefix := m.view.Prefix
	m.mu.Unlock()
	p, err := store.ParseFabricPrefix(prefix)
	if err != nil {
		return false
	}
	a, err := netip.ParseAddr(host)
	return err == nil && p.Contains(a)
}

// KnownAddress is the address this node was given before (kept beside its
// key), "" when none.
func KnownAddress(keyFile string) string {
	b, err := os.ReadFile(keyFile + ".address")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// key is the node's private key file's public key, the file made the first
// time (0600).
func key(keyFile string) (string, error) {
	b, err := os.ReadFile(keyFile)
	if errors.Is(err, os.ErrNotExist) {
		priv, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(keyFile), 0o700); err != nil {
			return "", err
		}
		if err := os.WriteFile(keyFile, []byte(base64.StdEncoding.EncodeToString(priv.Bytes())+"\n"), 0o600); err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(priv.PublicKey().Bytes()), nil
	}
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("fabric key %s: not a WireGuard key (32 bytes of base64)", keyFile)
	}
	priv, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(priv.PublicKey().Bytes()), nil
}

// Restore puts the last view this node was given (kept beside its key) on
// the interface, before the daemon is asked anything: a node that starts
// again (its host rebooted, its interface gone) is back on the fabric, and
// reaches the daemon there. Nothing kept: nothing done.
func (m *Manager) Restore(ctx context.Context) error {
	b, err := os.ReadFile(m.KeyFile + ".view")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var view store.FabricView
	if err := json.Unmarshal(b, &view); err != nil {
		return fmt.Errorf("fabric view %s: %w", m.KeyFile+".view", err)
	}
	if _, err := key(m.KeyFile); err != nil {
		return err
	}
	return m.apply(ctx, view)
}

// Once says what this node is, takes its address and peers and puts them on
// the interface.
func (m *Manager) Once(ctx context.Context) error {
	if m.public == "" {
		public, err := key(m.KeyFile)
		if err != nil {
			return err
		}
		m.public = public
	}
	view, err := m.Sync(ctx, store.FabricSelf{PublicKey: m.public, ListenPort: m.ListenPort, Endpoints: m.Endpoints, ProbePort: m.ProbePort,
		RelayFrom: m.RelayFrom, RelayTo: m.RelayTo, Routing: m.Routing})
	if err != nil {
		return fmt.Errorf("fabric: %w", err)
	}
	if b, err := json.Marshal(view); err == nil {
		if err := os.WriteFile(m.KeyFile+".view", b, 0o600); err != nil {
			return err
		}
	}
	return m.apply(ctx, view)
}

// apply puts view on the interface: its address, then its peers.
func (m *Manager) apply(ctx context.Context, view store.FabricView) error {
	p, err := store.ParseFabricPrefix(view.Prefix)
	if err != nil {
		return err
	}
	m.mu.Lock()
	changed := m.address != view.Address
	m.address, m.view = view.Address, view
	m.mu.Unlock()
	if changed {
		if KnownAddress(m.KeyFile) != view.Address {
			if err := os.WriteFile(m.KeyFile+".address", []byte(view.Address+"\n"), 0o600); err != nil {
				return err
			}
			m.logger().Info("fabric address given", "address", view.Address, "prefix", view.Prefix)
		}
	}
	if m.link == nil {
		m.link = newLink()
	}
	if err := m.link.ensure(ctx, m.Interface, view.Address, p.Bits(), m.KeyFile, m.ListenPort); err != nil {
		return fmt.Errorf("fabric interface %s: %w", m.Interface, err)
	}
	if err := m.setPeers(ctx, view.Peers); err != nil {
		return err
	}
	m.relays.mu.Lock()
	if m.relays.ctx == nil {
		m.relays.ctx = context.Background()
	}
	m.relays.mu.Unlock()
	m.relays.set(view.Relays, m.logger().Info)
	// the segments and FRR come after WireGuard: a failure there leaves the
	// node on the fabric, and is tried again at the next sync
	if err := m.applyRouting(ctx, view); err != nil {
		m.logger().Warn("fabric routing not applied", "error", err)
	}
	return nil
}

// endpointsOf are where peer is tried, in order: its endpoints, then the
// relay this node reaches it through, if any.
func endpointsOf(peer store.FabricNode) []string {
	out := append([]string(nil), peer.Endpoints...)
	if peer.RelayEndpoint != "" {
		out = append(out, peer.RelayEndpoint)
	}
	return out
}

// setPeers puts peers on the interface, each at the endpoint tried now, and
// takes off those no longer peers.
func (m *Manager) setPeers(ctx context.Context, peers []store.FabricNode) error {
	current, err := m.link.peers(ctx, m.Interface)
	if err != nil {
		return err
	}
	t := m.now()
	m.mu.Lock()
	if m.peers == nil {
		m.peers = map[string]*peerState{}
	}
	m.mu.Unlock()
	want := map[string]bool{}
	for _, peer := range peers {
		want[peer.PublicKey] = true
		endpoints := endpointsOf(peer)
		m.mu.Lock()
		st, ok := m.peers[peer.PublicKey]
		if !ok {
			st = &peerState{since: t}
			m.peers[peer.PublicKey] = st
		}
		seen, on := current[peer.PublicKey]
		reached := on && !seen.handshake.IsZero() && t.Sub(seen.handshake) < handshakeFresh
		// a peer reached is left where WireGuard hears it (a NAT's mapping,
		// a relay); one reached nowhere it says (none: it reaches out
		// itself, behind a NAT) is still a peer, so its handshakes are taken
		endpoint := ""
		if !reached && len(endpoints) > 0 {
			if ok && t.Sub(st.since) >= endpointTrial {
				st.endpoint, st.since = (st.endpoint+1)%len(endpoints), t
			}
			endpoint = endpoints[st.endpoint%len(endpoints)]
		}
		m.mu.Unlock()
		if err := m.link.setPeer(ctx, m.Interface, peer.PublicKey, peer.Address, endpoint); err != nil {
			return err
		}
	}
	for key := range current {
		if !want[key] {
			if err := m.link.removePeer(ctx, m.Interface, key); err != nil {
				return err
			}
			m.mu.Lock()
			delete(m.peers, key)
			m.mu.Unlock()
		}
	}
	return nil
}

// reportEvery: how often the node says how it sees its links.
const reportEvery = time.Minute

// Received hears a transfer this node received from the transfer endpoint
// addr ("host:port"): told with the next report as received from the peer
// whose fabric address host is (another's is none of the fabric's).
func (m *Manager) Received(addr string, bytes int64, took time.Duration) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.view.Peers {
		if p.Address == host {
			m.received = append(m.received, store.FabricReceived{From: p.NodeID, Bytes: bytes, Seconds: took.Seconds()})
			if len(m.received) > 256 {
				m.received = m.received[len(m.received)-256:]
			}
			return
		}
	}
}

// probe is the round trip to addr: the shortest of three TCP connects (nil
// when none was made).
func probe(ctx context.Context, addr string) *float64 {
	var best time.Duration
	for range 3 {
		start := time.Now()
		dialer := net.Dialer{Timeout: 2 * time.Second}
		c, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			continue
		}
		took := time.Since(start)
		c.Close()
		if best == 0 || took < best {
			best = took
		}
	}
	if best == 0 {
		return nil
	}
	ms := float64(best.Microseconds()) / 1000
	return &ms
}

// report says how this node sees its peers now (the endpoint each is
// reached at, its latest handshake, the round trip to it) and what it
// received from them since it last said.
func (m *Manager) report(ctx context.Context) error {
	if m.Report == nil || m.link == nil {
		return nil
	}
	current, err := m.link.peers(ctx, m.Interface)
	if err != nil {
		return err
	}
	m.mu.Lock()
	peers := append([]store.FabricNode(nil), m.view.Peers...)
	received := m.received
	m.received = nil
	m.mu.Unlock()
	report := store.FabricLinksReport{Peers: []store.FabricPeerSeen{}, Received: received}
	for _, p := range peers {
		seen := current[p.PublicKey]
		peer := store.FabricPeerSeen{Node: p.NodeID, Path: store.FabricPathDirect, Endpoint: seen.endpoint}
		if p.RelayEndpoint != "" && seen.endpoint == p.RelayEndpoint {
			peer.Path = store.FabricPathRelay
		}
		if !seen.handshake.IsZero() {
			peer.HandshakeAt = seen.handshake.UTC().Format(time.RFC3339Nano)
		}
		if p.ProbePort > 0 && !seen.handshake.IsZero() {
			addr := net.JoinHostPort(p.Address, fmt.Sprint(p.ProbePort))
			if pr, ok := m.link.(prober); ok {
				peer.RTTMillis = pr.probeFrom(ctx, addr)
			} else {
				peer.RTTMillis = probe(ctx, addr)
			}
		}
		report.Peers = append(report.Peers, peer)
	}
	if err := m.Report(ctx, report); err != nil {
		// told again with the next report
		m.mu.Lock()
		m.received = append(received, m.received...)
		m.mu.Unlock()
		return err
	}
	return nil
}

// Run keeps the node on the fabric until ctx ends: Once every syncEvery,
// its links said every reportEvery (a failure is logged and tried again).
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(syncEvery)
	defer ticker.Stop()
	m.relays.mu.Lock()
	m.relays.ctx = ctx
	m.relays.mu.Unlock()
	defer m.relays.set(nil, m.logger().Info)
	for {
		if err := m.Once(ctx); err != nil && ctx.Err() == nil {
			m.logger().Warn("fabric sync failed", "error", err)
		}
		if t := m.now(); t.Sub(m.reported) >= reportEvery {
			if err := m.report(ctx); err != nil && ctx.Err() == nil {
				m.logger().Warn("fabric links not said", "error", err)
			} else {
				m.reported = t
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
