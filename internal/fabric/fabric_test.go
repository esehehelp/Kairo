package fabric

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeLink is an interface in memory.
type fakeLink struct {
	address    string
	handshakes map[string]time.Time
	endpoints  map[string]string
}

func (f *fakeLink) ensure(_ context.Context, _, address string, _ int, _ string, _ int) error {
	f.address = address
	return nil
}

func (f *fakeLink) peers(context.Context, string) (map[string]peerSeen, error) {
	out := map[string]peerSeen{}
	for k, e := range f.endpoints {
		out[k] = peerSeen{handshake: f.handshakes[k], endpoint: e}
	}
	return out, nil
}

func (f *fakeLink) setPeer(_ context.Context, _, key, _, endpoint string) error {
	if _, on := f.endpoints[key]; !on || endpoint != "" {
		f.endpoints[key] = endpoint // "" keeps where it is heard from, as WireGuard does
	}
	return nil
}

func (f *fakeLink) removePeer(_ context.Context, _, key string) error {
	delete(f.endpoints, key)
	return nil
}

// A node's key is made once and kept (its public key the same at every
// start), its address kept beside it. A peer is put at its first endpoint;
// one that gives no handshake within the trial is tried at its next, one
// that does keeps its endpoint; a peer no longer named is taken off.
func TestTheFabricKeepsPeersAtAnEndpointThatAnswers(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "fabric.key")
	now := time.Unix(1_800_000_000, 0)
	view := store.FabricView{Address: "10.209.0.1", Prefix: "10.209.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.209.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820", "203.0.113.2:51820"}},
		{NodeID: "c", Address: "10.209.0.3", PublicKey: "KC", Endpoints: []string{"192.0.2.3:51820"}},
	}}
	var said store.FabricSelf
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}}
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, Endpoints: []string{"192.0.2.1:51820"}, link: link, Now: func() time.Time { return now },
		Sync: func(_ context.Context, self store.FabricSelf) (store.FabricView, error) {
			said = self
			return view, nil
		}}
	ctx := context.Background()
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(keyFile); err != nil || runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v", fi, err)
	}
	if m.Address() != "10.209.0.1" || KnownAddress(keyFile) != "10.209.0.1" || link.address != "10.209.0.1" {
		t.Fatalf("address %q, kept %q, on the link %q", m.Address(), KnownAddress(keyFile), link.address)
	}
	if link.endpoints["KB"] != "192.0.2.2:51820" || link.endpoints["KC"] != "192.0.2.3:51820" {
		t.Fatalf("endpoints %v", link.endpoints)
	}
	public := said.PublicKey
	// c answers, b does not: after the trial b is tried at its next
	link.handshakes["KC"] = now
	now = now.Add(endpointTrial)
	link.handshakes["KC"] = now
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if link.endpoints["KB"] != "203.0.113.2:51820" || link.endpoints["KC"] != "192.0.2.3:51820" {
		t.Fatalf("after the trial: %v", link.endpoints)
	}
	// c is heard from elsewhere (a NAT's mapping): it is left there while
	// it answers
	link.endpoints["KC"] = "198.51.100.3:40000"
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if link.endpoints["KC"] != "198.51.100.3:40000" {
		t.Fatalf("an answering peer was moved: %v", link.endpoints)
	}
	// d says no endpoint (it reaches out itself): still a peer, so that its
	// handshakes are taken
	view.Peers = append(view.Peers, store.FabricNode{NodeID: "d", Address: "10.209.0.4", PublicKey: "KD"})
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if e, on := link.endpoints["KD"]; !on || e != "" {
		t.Fatalf("a peer with no endpoint: %q %v", e, on)
	}
	view.Peers = view.Peers[:2]
	// c is no longer a peer
	view.Peers = view.Peers[:1]
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if _, on := link.endpoints["KC"]; on {
		t.Fatal("c still a peer")
	}
	// a new start: the same key
	again := &Manager{KeyFile: keyFile, link: link, Sync: m.Sync, Interface: "kairo0", ListenPort: 51820}
	if err := again.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if said.PublicKey != public {
		t.Fatal("the key changed between starts")
	}
}

// A node that starts again is put back on the fabric as it was last (the
// view kept beside its key), before the daemon is asked anything.
func TestARestartedNodeIsBackOnTheFabricBeforeAskingTheDaemon(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	view := store.FabricView{Address: "10.209.0.4", Prefix: "10.209.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.209.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820"}},
	}}
	first := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}},
		Sync: func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil }}
	if err := first.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}}
	again := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link,
		Sync: func(context.Context, store.FabricSelf) (store.FabricView, error) {
			t.Fatal("the daemon asked before the node was back on the fabric")
			return store.FabricView{}, nil
		}}
	if err := again.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if link.address != "10.209.0.4" || link.endpoints["KB"] != "192.0.2.2:51820" || again.Address() != "10.209.0.4" {
		t.Fatalf("restored %q %v", link.address, link.endpoints)
	}
	// nothing kept: nothing done
	if err := (&Manager{KeyFile: filepath.Join(t.TempDir(), "none.key")}).Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A transfer received from a peer's transfer endpoint (by its fabric
// address) is said with the next report, with each peer's endpoint and
// latest handshake; one from elsewhere is none of the fabric's.
func TestLinksAreSaidWithWhatWasReceived(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	view := store.FabricView{Address: "10.209.0.1", Prefix: "10.209.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.209.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820"}},
	}}
	link := &fakeLink{handshakes: map[string]time.Time{"KB": time.Unix(1_800_000_000, 0)}, endpoints: map[string]string{}}
	var said []store.FabricLinksReport
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link,
		Sync:   func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil },
		Report: func(_ context.Context, r store.FabricLinksReport) error { said = append(said, r); return nil }}
	if err := m.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Received("10.209.0.2:7485", 64<<20, 2*time.Second)
	m.Received("192.168.1.9:7475", 64<<20, time.Second)
	if err := m.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(said) != 1 || len(said[0].Peers) != 1 || said[0].Peers[0].Node != "b" || said[0].Peers[0].Endpoint != "192.0.2.2:51820" ||
		said[0].Peers[0].HandshakeAt == "" || len(said[0].Received) != 1 || said[0].Received[0].From != "b" || said[0].Received[0].Seconds != 2 {
		t.Fatalf("said %+v", said)
	}
	// said once
	if err := m.report(context.Background()); err != nil || len(said[1].Received) != 0 {
		t.Fatalf("again: %+v %v", said, err)
	}
}
