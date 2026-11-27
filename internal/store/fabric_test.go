package store

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func fabricKey(b byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune(b)), 32)))
}

// A node is given the lowest free address of the fabric's prefix once, and
// keeps it whatever it says of itself later; it is told the other nodes
// that have a key, never itself. A malformed key or endpoint, or a prefix
// that no longer holds its address, is refused.
func TestANodesFabricAddressIsGivenOnceAndKept(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.UpsertNode(ctx, Node{ID: id, Name: id, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	const prefix = "10.209.0.0/16"
	a, err := store.SyncFabric(ctx, "a", prefix, FabricSelf{PublicKey: fabricKey('a'), ListenPort: 51820, Endpoints: []string{"192.0.2.1:51820"}})
	if err != nil || a.Address != "10.209.0.1" || a.Prefix != prefix || len(a.Peers) != 0 {
		t.Fatalf("a: %+v %v", a, err)
	}
	b, err := store.SyncFabric(ctx, "b", prefix, FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, Endpoints: []string{"192.0.2.2:51820", "203.0.113.2:51820"}})
	if err != nil || b.Address != "10.209.0.2" || len(b.Peers) != 1 || b.Peers[0].NodeID != "a" || b.Peers[0].Address != "10.209.0.1" {
		t.Fatalf("b: %+v %v", b, err)
	}
	// a says it is elsewhere now: same address, b's endpoints in its view
	a, err = store.SyncFabric(ctx, "a", prefix, FabricSelf{PublicKey: fabricKey('a'), ListenPort: 51821, Endpoints: []string{"198.51.100.1:51821"}})
	if err != nil || a.Address != "10.209.0.1" || len(a.Peers) != 1 || len(a.Peers[0].Endpoints) != 2 {
		t.Fatalf("a again: %+v %v", a, err)
	}
	nodes, err := store.FabricNodes(ctx)
	if err != nil || len(nodes) != 2 || nodes[0].ListenPort != 51821 || nodes[0].Endpoints[0] != "198.51.100.1:51821" {
		t.Fatalf("nodes: %+v %v", nodes, err)
	}
	for _, bad := range []FabricSelf{
		{PublicKey: "short", ListenPort: 51820},
		{PublicKey: fabricKey('a'), ListenPort: 0},
		{PublicKey: fabricKey('a'), ListenPort: 51820, Endpoints: []string{"no-port"}},
	} {
		if _, err := store.SyncFabric(ctx, "a", prefix, bad); !errors.Is(err, ErrFabric) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
	if _, err := store.SyncFabric(ctx, "a", "10.210.0.0/16", FabricSelf{PublicKey: fabricKey('a'), ListenPort: 51820}); !errors.Is(err, ErrFabric) {
		t.Fatalf("a prefix without a's address: %v", err)
	}
	if _, err := ParseFabricPrefix("10.209.0.1/16"); err == nil {
		t.Fatal("a prefix that is no network address")
	}
}

// A node's links are recorded as it sees them; a large transfer it received
// moves the link from the sender halfway (a small one is not counted); a
// path that changes forgets the bandwidth over the old one. The planner
// takes a transfer's speed from the link that way, before any bench.
func TestDirectedLinksAreMeasuredByTransfersAndForgottenWithTheirPath(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := store.UpsertNode(ctx, Node{ID: id, Name: id, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.SyncFabric(ctx, id, "10.209.0.0/16", FabricSelf{PublicKey: fabricKey(id[0]), ListenPort: 51820, ProbePort: 7475}); err != nil {
			t.Fatal(err)
		}
	}
	rtt := 0.6
	// a sees b directly; b received 64 MiB from a in 2 s, and 1 MiB (not counted)
	if err := store.RecordFabricLinks(ctx, "a", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "b", Path: FabricPathDirect, Endpoint: "192.0.2.2:51820", RTTMillis: &rtt}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordFabricLinks(ctx, "b", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "a", Path: FabricPathDirect, Endpoint: "192.0.2.1:51820"}},
		Received: []FabricReceived{{From: "a", Bytes: 64 << 20, Seconds: 2}, {From: "a", Bytes: 1 << 20, Seconds: 1}}}); err != nil {
		t.Fatal(err)
	}
	links, err := store.FabricLinks(ctx)
	if err != nil || len(links) != 2 || links[0].Src != "a" || links[0].RTTMillis == nil || *links[0].RTTMillis != 0.6 ||
		links[0].BytesPerSecond == nil || *links[0].BytesPerSecond != 32<<20 || links[1].BytesPerSecond != nil {
		t.Fatalf("links %+v %v", links, err)
	}
	perf, err := loadNodePerf(ctx, store.db)
	if err != nil || perf.link("a", "b") != 32<<20 || perf.link("b", "a") != defaultLinkBytesPerSecond {
		t.Fatalf("a->b %v, b->a %v, %v", perf.link("a", "b"), perf.link("b", "a"), err)
	}
	// a's path to b goes through a relay now: what was measured is forgotten
	if err := store.RecordFabricLinks(ctx, "a", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "b", Path: FabricPathRelay, Endpoint: "198.51.100.9:41000"}}}); err != nil {
		t.Fatal(err)
	}
	if links, err = store.FabricLinks(ctx); err != nil || links[0].BytesPerSecond != nil || links[0].Path != FabricPathRelay || links[0].RTTMillis == nil {
		t.Fatalf("after the path changed: %+v %v", links, err)
	}
	if err := store.RecordFabricLinks(ctx, "a", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "b", Path: "sideways"}}}); !errors.Is(err, ErrFabric) {
		t.Fatalf("an unknown path: %v", err)
	}
}

// A pair whose link has had no fresh handshake two reports running is
// given a relay: a node offering relay ports that has fresh handshakes with
// both. Each side then reaches the other at the relay's endpoint and its
// own side's port, tried after its endpoints; the relay is told the pair
// with the hosts it sees each side at. A node is also handed the endpoints
// its other peers see a peer at.
func TestAPairWithNoPathIsGivenARelay(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	const prefix = "10.209.0.0/16"
	selves := map[string]FabricSelf{
		"a": {PublicKey: fabricKey('a'), ListenPort: 51820},
		"b": {PublicKey: fabricKey('b'), ListenPort: 51820},
		"r": {PublicKey: fabricKey('r'), ListenPort: 51820, Endpoints: []string{"203.0.113.9:51820"}, RelayFrom: 41000, RelayTo: 41010},
	}
	for _, id := range []string{"a", "b", "r"} {
		if err := store.UpsertNode(ctx, Node{ID: id, Name: id, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.SyncFabric(ctx, id, prefix, selves[id]); err != nil {
			t.Fatal(err)
		}
	}
	fresh := time.Now().UTC().Format(time.RFC3339Nano)
	// r reaches both (seeing them at their NAT mappings)
	if err := store.RecordFabricLinks(ctx, "r", FabricLinksReport{Peers: []FabricPeerSeen{
		{Node: "a", Path: FabricPathDirect, Endpoint: "198.51.100.1:3001", HandshakeAt: fresh},
		{Node: "b", Path: FabricPathDirect, Endpoint: "198.51.100.2:3002", HandshakeAt: fresh},
	}}); err != nil {
		t.Fatal(err)
	}
	// a reaches b on no path, twice
	for range 2 {
		if err := store.RecordFabricLinks(ctx, "a", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "b", Path: FabricPathDirect}}}); err != nil {
			t.Fatal(err)
		}
	}
	view := func(node string) FabricView {
		t.Helper()
		v, err := store.SyncFabric(ctx, node, prefix, selves[node])
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	peer := func(v FabricView, id string) FabricNode {
		for _, p := range v.Peers {
			if p.NodeID == id {
				return p
			}
		}
		t.Fatalf("no peer %s in %+v", id, v)
		return FabricNode{}
	}
	a, b, r := view("a"), view("b"), view("r")
	if pb := peer(a, "b"); pb.RelayEndpoint != "203.0.113.9:41000" || len(pb.Endpoints) != 1 || pb.Endpoints[0] != "198.51.100.2:3002" {
		t.Fatalf("a's view of b: %+v", pb)
	}
	if pa := peer(b, "a"); pa.RelayEndpoint != "203.0.113.9:41001" {
		t.Fatalf("b's view of a: %+v", pa)
	}
	if len(r.Relays) != 1 || r.Relays[0].PortA != 41000 || r.Relays[0].PortB != 41001 || r.Relays[0].HostA != "198.51.100.1" || r.Relays[0].HostB != "198.51.100.2" {
		t.Fatalf("r's relays: %+v", r.Relays)
	}
	if pr := peer(a, "r"); pr.RelayEndpoint != "" {
		t.Fatalf("a's view of r: %+v", pr)
	}
	// b is elsewhere now: its pairs try a direct path again
	selves["b"] = FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, Endpoints: []string{"192.0.2.77:51820"}}
	view("b")
	if pb := peer(view("a"), "b"); pb.RelayEndpoint != "" {
		t.Fatalf("still relayed after b moved: %+v", pb)
	}
	if r = view("r"); len(r.Relays) != 0 {
		t.Fatalf("r still relays: %+v", r.Relays)
	}
}
