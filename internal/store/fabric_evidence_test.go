package store

import (
	"context"
	"testing"
	"time"
)

// reportLink has src report its link from dst: a fresh handshake, a round
// trip, and a transfer from dst of bytes in a second.
func reportLink(t *testing.T, store *Store, src, dst string, rtt float64, bytes int64) {
	t.Helper()
	report := FabricLinksReport{Peers: []FabricPeerSeen{{Node: dst, Path: FabricPathDirect, HandshakeAt: time.Now().UTC().Format(time.RFC3339Nano), RTTMillis: &rtt}}}
	if bytes > 0 {
		report.Received = []FabricReceived{{From: dst, Bytes: bytes, Seconds: 1}}
	}
	if err := store.RecordFabricLinks(context.Background(), src, report); err != nil {
		t.Fatal(err)
	}
}

// A gang that says what its members' links must be is held while no
// measurement says so (and samples are asked for), placed once they do, and
// held when they are short of it, or a node is metered.
func TestAGangIsPlacedOnlyOnLinksMeasuredToMeetWhatItSays(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	gangProjectDeclared(t, store, "false", "", "max_rtt_ms = 5\nmin_mib_per_second = 50")
	ctx := context.Background()
	waitFor := func(reason string) {
		t.Helper()
		if r, err := store.ReserveNext(ctx, "exec-a"); err != nil || r != nil {
			t.Fatalf("placed: %+v %v", r, err)
		}
		var got string
		if err := store.db.QueryRow(`SELECT w.reason_code FROM admission_waits w JOIN gang_members m ON m.execution_id=w.execution_id WHERE m.rank=0`).Scan(&got); err != nil || got != reason {
			t.Fatalf("wait %q (%v), want %s", got, err, reason)
		}
	}
	waitFor("fabric_unmeasured")
	// each node is asked to fetch a sample from the other
	for _, x := range [][2]string{{"node-a", "node-b"}, {"node-b", "node-a"}} {
		view, err := store.SyncFabric(ctx, x[0], FabricPlan{Prefix: "10.66.0.0/16"}, FabricSelf{PublicKey: fabricKey(map[string]byte{"node-a": 'a', "node-b": 'b'}[x[0]]), ListenPort: 51820, ProbePort: 7485, Routing: true})
		if err != nil || len(view.Measure) != 1 || view.Measure[0].Node != x[1] || view.Measure[0].Port != 7485 {
			t.Fatalf("%s asked to measure %+v (%v)", x[0], view.Measure, err)
		}
	}
	// measured: 64 MiB/s each way, a round trip of 1 ms
	reportLink(t, store, "node-a", "node-b", 1, 64<<20)
	reportLink(t, store, "node-b", "node-a", 1, 64<<20)
	if view, _ := store.SyncFabric(ctx, "node-b", FabricPlan{Prefix: "10.66.0.0/16"}, FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, ProbePort: 7485, Routing: true}); len(view.Measure) != 0 {
		t.Fatalf("still asked to measure: %+v", view.Measure)
	}
	// node-b's uplink is metered: the gang does not span it
	if _, err := store.SyncFabric(ctx, "node-b", FabricPlan{Prefix: "10.66.0.0/16"}, FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, ProbePort: 7485, Routing: true, Metered: true}); err != nil {
		t.Fatal(err)
	}
	waitFor("fabric_links_short")
	if _, err := store.SyncFabric(ctx, "node-b", FabricPlan{Prefix: "10.66.0.0/16"}, FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, ProbePort: 7485, Routing: true}); err != nil {
		t.Fatal(err)
	}
	// a round trip measured longer than it says
	reportLink(t, store, "node-a", "node-b", 9, 0)
	reportLink(t, store, "node-b", "node-a", 9, 0)
	waitFor("fabric_links_short")
	reportLink(t, store, "node-a", "node-b", 1, 0)
	reportLink(t, store, "node-b", "node-a", 1, 0)
	if r, err := store.ReserveNext(ctx, "exec-a"); err != nil || r == nil || r.Gang == nil {
		t.Fatalf("not placed on links that meet it: %+v %v", r, err)
	}
}

// A node whose transfer endpoint serves no samples (a Windows node's is off
// the fabric) times both of its links itself: it fetches a sample from the
// peer, and sends the peer one, reporting it sent.
func TestANodeThatServesNoSamplesTimesBothWaysItself(t *testing.T) {
	store := openCoordinationStore(t)
	setupGangInventory(t, store)
	ctx := context.Background()
	plan := FabricPlan{Prefix: "10.66.0.0/16"}
	win := FabricSelf{PublicKey: fabricKey('a'), ListenPort: 51820, Routing: true} // no probe port
	if _, err := store.SyncFabric(ctx, "node-a", plan, win); err != nil {
		t.Fatal(err)
	}
	tx, err := store.beginWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveMeasureRequestsTx(ctx, tx, map[[2]string]bool{{"node-a", "node-b"}: true, {"node-b", "node-a"}: true}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	view, err := store.SyncFabric(ctx, "node-a", plan, win)
	if err != nil || len(view.Measure) != 2 || view.Measure[0] != (FabricMeasure{Node: "node-b", Address: view.Measure[0].Address, Port: 7485}) || !view.Measure[1].Send || view.Measure[1].Node != "node-b" {
		t.Fatalf("node-a measures %+v (%v)", view.Measure, err)
	}
	other, err := store.SyncFabric(ctx, "node-b", plan, FabricSelf{PublicKey: fabricKey('b'), ListenPort: 51820, ProbePort: 7485, Routing: true})
	if err != nil || len(other.Measure) != 0 {
		t.Fatalf("node-b measures %+v (%v)", other.Measure, err)
	}
	// what node-a sent times the link from it
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.RecordFabricLinks(ctx, "node-a", FabricLinksReport{Peers: []FabricPeerSeen{{Node: "node-b", Path: FabricPathDirect, HandshakeAt: now}},
		Sent: []FabricReceived{{From: "node-b", Bytes: 32 << 20, Seconds: 0.5}}}); err != nil {
		t.Fatal(err)
	}
	var bps float64
	if err := store.db.QueryRow(`SELECT bytes_per_second FROM fabric_links WHERE src_node='node-a' AND dst_node='node-b'`).Scan(&bps); err != nil || bps != 64<<20 {
		t.Fatalf("the link from node-a: %v %v", bps, err)
	}
}
