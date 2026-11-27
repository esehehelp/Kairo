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
	"errors"
	"fmt"
	"log/slog"
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
	// the first time. Its address is kept beside it (KeyFile.address).
	KeyFile   string
	Endpoints []string
	// Sync says what this node is and returns its address and peers.
	Sync   func(ctx context.Context, self store.FabricSelf) (store.FabricView, error)
	Logger *slog.Logger
	// Now is the clock (nil: time.Now).
	Now func() time.Time

	link link // the interface (fabric_linux.go)

	mu      sync.Mutex
	public  string
	address string
	peers   map[string]*peerState // by public key
}

// peerState is which of a peer's endpoints is tried, since when.
type peerState struct {
	endpoint int
	since    time.Time
}

// link is what the manager needs of the interface.
type link interface {
	// ensure makes the interface (if missing) with address/bits, the key
	// file and the port, up.
	ensure(ctx context.Context, name, address string, bits int, keyFile string, port int) error
	// peers are the interface's peers: public key -> latest handshake
	// (zero: none).
	peers(ctx context.Context, name string) (map[string]time.Time, error)
	// setPeer sets a peer: its address (/32) and the endpoint tried.
	setPeer(ctx context.Context, name, publicKey, address, endpoint string) error
	removePeer(ctx context.Context, name, publicKey string) error
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
	view, err := m.Sync(ctx, store.FabricSelf{PublicKey: m.public, ListenPort: m.ListenPort, Endpoints: m.Endpoints})
	if err != nil {
		return fmt.Errorf("fabric: %w", err)
	}
	p, err := store.ParseFabricPrefix(view.Prefix)
	if err != nil {
		return err
	}
	m.mu.Lock()
	changed := m.address != view.Address
	m.address = view.Address
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
	return m.setPeers(ctx, view.Peers)
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
		if len(peer.Endpoints) == 0 {
			continue // it says it is reached nowhere: it reaches out itself
		}
		m.mu.Lock()
		st, ok := m.peers[peer.PublicKey]
		if !ok {
			st = &peerState{since: t}
			m.peers[peer.PublicKey] = st
		}
		hs, on := current[peer.PublicKey]
		reached := on && !hs.IsZero() && t.Sub(hs) < handshakeFresh
		if !reached && ok && t.Sub(st.since) >= endpointTrial {
			st.endpoint, st.since = (st.endpoint+1)%len(peer.Endpoints), t
		}
		endpoint := peer.Endpoints[st.endpoint%len(peer.Endpoints)]
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

// Run keeps the node on the fabric until ctx ends: Once every syncEvery
// (a failure is logged and tried again).
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(syncEvery)
	defer ticker.Stop()
	for {
		if err := m.Once(ctx); err != nil && ctx.Err() == nil {
			m.logger().Warn("fabric sync failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
