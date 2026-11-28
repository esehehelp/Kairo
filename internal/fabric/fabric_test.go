package fabric

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/internal/store"
)

// fakeLink is an interface in memory.
type fakeLink struct {
	address    string
	handshakes map[string]time.Time
	endpoints  map[string]string
	segs       map[int]string // VNI: "local:port mtu M"
	inUse      map[int]bool   // a container is on it
	frr        fakeRouter
	sampleMu   sync.Mutex
	samples    []string
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

func (f *fakeLink) segments(context.Context, string) ([]int, error) {
	var out []int
	for vni := range f.segs {
		out = append(out, vni)
	}
	return out, nil
}

func (f *fakeLink) ensureSegment(_ context.Context, _ string, seg store.FabricSegment, local string, port int) error {
	if f.segs == nil {
		f.segs = map[int]string{}
	}
	f.segs[seg.VNI] = fmt.Sprintf("%s:%d mtu %d", local, port, seg.MTU)
	return nil
}

func (f *fakeLink) removeSegment(_ context.Context, _ string, vni int) error {
	if f.inUse[vni] {
		return errSegmentInUse
	}
	delete(f.segs, vni)
	return nil
}

func (f *fakeLink) router(string, string, string) router { return &f.frr }

// sampleFrom: every sample is 32 MiB in half a second; the URLs asked are
// kept.
func (f *fakeLink) sampleFrom(_ context.Context, url string, send bool) (int64, time.Duration, error) {
	f.sampleMu.Lock()
	defer f.sampleMu.Unlock()
	if send {
		url = "send " + url
	}
	f.samples = append(f.samples, url)
	return 32 << 20, 500 * time.Millisecond, nil
}

// fakeRouter keeps the files FRR was last given.
type fakeRouter struct {
	files   map[string]string
	applied int
	removed bool
}

func (r *fakeRouter) apply(_ context.Context, files map[string]string) error {
	r.files, r.removed = files, false
	r.applied++
	return nil
}

func (r *fakeRouter) remove(context.Context) error {
	r.files, r.removed = nil, true
	return nil
}

func (r *fakeRouter) show(context.Context, string) ([]byte, error) { return nil, nil }

// A node's key is made once and kept (its public key the same at every
// start), its address kept beside it. A peer is put at its first endpoint;
// one that gives no handshake within the trial is tried at its next, one
// that does keeps its endpoint; a peer no longer named is taken off.
func TestTheFabricKeepsPeersAtAnEndpointThatAnswers(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "fabric.key")
	now := time.Unix(1_800_000_000, 0)
	view := store.FabricView{Address: "10.66.0.1", Prefix: "10.66.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.66.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820", "203.0.113.2:51820"}},
		{NodeID: "c", Address: "10.66.0.3", PublicKey: "KC", Endpoints: []string{"192.0.2.3:51820"}},
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
	if m.Address() != "10.66.0.1" || KnownAddress(keyFile) != "10.66.0.1" || link.address != "10.66.0.1" {
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
	view.Peers = append(view.Peers, store.FabricNode{NodeID: "d", Address: "10.66.0.4", PublicKey: "KD"})
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
	view := store.FabricView{Address: "10.66.0.4", Prefix: "10.66.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.66.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820"}},
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
	if link.address != "10.66.0.4" || link.endpoints["KB"] != "192.0.2.2:51820" || again.Address() != "10.66.0.4" {
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
	view := store.FabricView{Address: "10.66.0.1", Prefix: "10.66.0.0/16", Peers: []store.FabricNode{
		{NodeID: "b", Address: "10.66.0.2", PublicKey: "KB", Endpoints: []string{"192.0.2.2:51820"}},
	}}
	link := &fakeLink{handshakes: map[string]time.Time{"KB": time.Unix(1_800_000_000, 0)}, endpoints: map[string]string{}}
	var said []store.FabricLinksReport
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link,
		Sync:   func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil },
		Report: func(_ context.Context, r store.FabricLinksReport) error { said = append(said, r); return nil }}
	if err := m.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Received("10.66.0.2:7485", 64<<20, 2*time.Second)
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

// A node that routes is given its segments (each a VXLAN from its fabric
// address on the fabric's VXLAN port) and FRR's files: iBGP in the fabric's
// ASN to the reflectors over the fabric, EVPN for every local VNI; a
// reflector listens to the whole fabric. A segment no longer its is taken
// down; a node told it routes no more has FRR taken away.
func TestARoutingNodeIsGivenItsSegmentsAndFRR(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	routing := &store.FabricRouting{ASN: 4200000000, VXLANPort: 4790, Reflectors: []string{"10.66.0.1", "10.66.0.2"}}
	view := store.FabricView{Address: "10.66.0.4", Prefix: "10.66.0.0/16", Peers: []store.FabricNode{},
		Routing:  routing,
		Segments: []store.FabricSegment{{VNI: 10000, Subnet: "10.67.0.0/24", MTU: 1370}, {VNI: 10001, Subnet: "10.67.1.0/24", MTU: 1370}}}
	var said store.FabricSelf
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}}
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link, Routing: true, NodeID: "w", RouterName: "kairo-frr",
		Sync: func(_ context.Context, self store.FabricSelf) (store.FabricView, error) {
			said = self
			return view, nil
		}}
	ctx := context.Background()
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if !said.Routing {
		t.Fatal("the node did not say it routes")
	}
	if link.segs[10000] != "10.66.0.4:4790 mtu 1370" || len(link.segs) != 2 {
		t.Fatalf("segments %v", link.segs)
	}
	conf := link.frr.files["frr.conf"]
	for _, want := range []string{"router bgp 4200000000\n", " bgp router-id 10.66.0.4\n", " neighbor 10.66.0.1 peer-group fabric\n",
		" neighbor 10.66.0.2 peer-group fabric\n", " neighbor fabric update-source 10.66.0.4\n", "  advertise-all-vni\n", " neighbor fabric bfd\n"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("frr.conf lacks %q:\n%s", want, conf)
		}
	}
	if strings.Contains(conf, "listen range") || strings.Contains(conf, "route-reflector-client") {
		t.Fatalf("a client listens or reflects:\n%s", conf)
	}
	if !strings.Contains(link.frr.files["daemons"], "-l 10.66.0.4") {
		t.Fatalf("bgpd listens beyond the fabric address:\n%s", link.frr.files["daemons"])
	}
	// it becomes a reflector; segment 10001 is no longer its
	routing.Reflector, routing.Reflectors = true, []string{"10.66.0.1", "10.66.0.4"}
	view.Segments = view.Segments[:1]
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	conf = link.frr.files["frr.conf"]
	if _, on := link.segs[10001]; on || len(link.segs) != 1 {
		t.Fatalf("segments %v", link.segs)
	}
	for _, want := range []string{" bgp listen range 10.66.0.0/16 peer-group fabric\n", "  neighbor fabric route-reflector-client\n", " neighbor 10.66.0.1 peer-group fabric\n"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("a reflector's frr.conf lacks %q:\n%s", want, conf)
		}
	}
	if strings.Contains(conf, "neighbor 10.66.0.4 ") {
		t.Fatalf("a reflector names itself:\n%s", conf)
	}
	// it routes no more
	view.Routing = nil
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if !link.frr.removed {
		t.Fatal("FRR kept on a node that routes no more")
	}
}

// A gang member's segment is put on the node at its launch, before a sync
// would: a sync whose view does not have it yet leaves it a while, and none
// takes it down while a container is on it.
func TestASegmentPutUpForALaunchIsKept(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	view := store.FabricView{Address: "10.66.0.4", Prefix: "10.66.0.0/16", Peers: []store.FabricNode{},
		Routing: &store.FabricRouting{ASN: 4200000000, VXLANPort: 4790}}
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}, inUse: map[int]bool{}}
	now := time.Date(2026, 11, 28, 2, 0, 0, 0, time.UTC)
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link, NodeID: "w", RouterName: "kairo-frr", Names: "tf",
		Sync: func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil },
		Now:  func() time.Time { return now }}
	ctx := context.Background()
	if _, err := m.EnsureSegment(ctx, store.FabricSegment{VNI: 100000, Subnet: "10.67.0.0/24", MTU: 1370}); err == nil {
		t.Fatal("a segment on a node that does not route")
	}
	m.Routing = true
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	device, err := m.EnsureSegment(ctx, store.FabricSegment{VNI: 100000, Subnet: "10.67.0.0/24", MTU: 1370})
	if err != nil || device != segmentDevice("tf", 100000) || link.segs[100000] != "10.66.0.4:4790 mtu 1370" {
		t.Fatalf("ensured %q %v: %v", device, err, link.segs)
	}
	// a sync that does not have it yet
	now = now.Add(time.Minute)
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if _, on := link.segs[100000]; !on {
		t.Fatal("a launch's segment taken down by a stale sync")
	}
	// later, with a container on it
	now = now.Add(launchedSegmentKept)
	link.inUse[100000] = true
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if _, on := link.segs[100000]; !on {
		t.Fatal("a segment taken down under a container")
	}
	// the container gone
	delete(link.inUse, 100000)
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if _, on := link.segs[100000]; on {
		t.Fatal("a segment no view has, kept")
	}
}

// A node asked for the bandwidth of a link to it fetches a sample from the
// peer over the fabric and reports it as a transfer received; not again
// for the same ask.
func TestANodeAskedToMeasureALinkFetchesASample(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	view := store.FabricView{Address: "10.66.0.4", Prefix: "10.66.0.0/16",
		Peers:   []store.FabricNode{{NodeID: "b", Address: "10.66.0.2", PublicKey: "pk-b", ListenPort: 51820, ProbePort: 7485}},
		Measure: []store.FabricMeasure{{Node: "b", Address: "10.66.0.2", Port: 7485}}}
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}}
	var reports []store.FabricLinksReport
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link,
		Sync:   func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil },
		Report: func(_ context.Context, r store.FabricLinksReport) error { reports = append(reports, r); return nil }}
	ctx := context.Background()
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Once(ctx); err != nil { // asked again: not sampled again
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		m.mu.Lock()
		n := len(m.received)
		m.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := m.report(ctx); err != nil {
		t.Fatal(err)
	}
	link.sampleMu.Lock()
	samples := append([]string(nil), link.samples...)
	link.sampleMu.Unlock()
	if len(samples) != 1 || samples[0] != "https://10.66.0.2:7485/fabric/sample" {
		t.Fatalf("samples %v", samples)
	}
	if len(reports) != 1 || len(reports[0].Received) != 1 || reports[0].Received[0].From != "b" || reports[0].Received[0].Bytes != 32<<20 || reports[0].Received[0].Seconds != 0.5 {
		t.Fatalf("reports %+v", reports)
	}
}

// A node asked to time a link from it to a peer whose endpoint serves
// samples (its own does not) sends the peer one, and reports it sent.
func TestANodeSendsASampleWhenItServesNone(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "fabric.key")
	view := store.FabricView{Address: "10.66.0.4", Prefix: "10.66.0.0/16",
		Peers:   []store.FabricNode{{NodeID: "b", Address: "10.66.0.2", PublicKey: "pk-b", ListenPort: 51820, ProbePort: 7485}},
		Measure: []store.FabricMeasure{{Node: "b", Address: "10.66.0.2", Port: 7485}, {Node: "b", Address: "10.66.0.2", Port: 7485, Send: true}}}
	link := &fakeLink{handshakes: map[string]time.Time{}, endpoints: map[string]string{}}
	var reports []store.FabricLinksReport
	m := &Manager{Interface: "kairo0", ListenPort: 51820, KeyFile: keyFile, link: link,
		Sync:   func(context.Context, store.FabricSelf) (store.FabricView, error) { return view, nil },
		Report: func(_ context.Context, r store.FabricLinksReport) error { reports = append(reports, r); return nil }}
	ctx := context.Background()
	if err := m.Once(ctx); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		m.mu.Lock()
		n := len(m.sent) + len(m.received)
		m.mu.Unlock()
		if n == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := m.report(ctx); err != nil {
		t.Fatal(err)
	}
	// both ways with the one peer
	if len(reports) != 1 || len(reports[0].Sent) != 1 || reports[0].Sent[0].From != "b" || len(reports[0].Received) != 1 {
		t.Fatalf("reports %+v", reports)
	}
}

// The ingress guard drops the fabric's UDP and BGP that did not come over
// its interface, and what the host routes onto the fabric or off it but to
// what its containers may reach (the daemon's API) and its replies, in a
// table of the installation's own.
func TestTheIngressGuardKeepsTheFabricsUDPToItsInterface(t *testing.T) {
	rules := ingressRules("tf", "kairo-t0", "10.99.0.7", 4790, []netip.AddrPort{netip.MustParseAddrPort("10.99.0.1:7474"), netip.MustParseAddrPort("[fd00::1]:443")})
	for _, want := range []string{"table inet tffabric {}\ndelete table inet tffabric\n", "hook input", `ip daddr 10.99.0.7 udp dport { 4790, 3784, 3785, 4784 } iifname != "kairo-t0" drop`,
		`ip daddr 10.99.0.7 tcp dport 179 iifname != "kairo-t0" drop`, "hook forward", `iifname "kairo-t0" ct state established,related accept`,
		`oifname "kairo-t0" ip daddr 10.99.0.1 tcp dport 7474 accept`, `oifname "kairo-t0" ip6 daddr fd00::1 tcp dport 443 accept`,
		"\t\toifname \"kairo-t0\" drop\n\t\tiifname \"kairo-t0\" drop\n", "policy accept"} {
		if !strings.Contains(rules, want) {
			t.Fatalf("rules lack %q:\n%s", want, rules)
		}
	}
	// what may be reached goes before the drops
	if strings.Index(rules, "tcp dport 7474 accept") > strings.Index(rules, `oifname "kairo-t0" drop`) {
		t.Fatalf("reach after the drop:\n%s", rules)
	}
}
