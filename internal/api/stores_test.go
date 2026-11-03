package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// declareStoreNode registers node "n" with executor "e", disk D: and a
// volatile store, and returns the node's token.
func declareStoreNode(t *testing.T, d *testDaemon) string {
	t.Helper()
	ctx := context.Background()
	if err := d.store.UpsertNode(ctx, store.Node{ID: "n", Name: "n", OS: "windows", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertExecutor(ctx, store.Executor{ID: "e", NodeID: "n", Kind: "windows", Attributes: json.RawMessage(`{"environment":"windows"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertProvider(ctx, "n-fs", "n", "filesystem", nil); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertResourceInstance(ctx, store.ResourceInstance{ID: "n-disk", NodeID: "n", ProviderID: "n-fs", Kind: "disk", StableIdentity: "D:", Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	spec := machine.Spec{Nodes: []machine.Node{{ID: "n"}}, Stores: []machine.Store{{ID: "scratch", Class: machine.StoreVolatile, Node: "n", Root: "D:/kairo", Filesystem: "D:"}}}
	if _, err := d.store.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	return d.token(t, "n", store.RoleNode, "n")
}

// An agent gets its node's stores. Under an observe-only daemon it gets
// none, so it makes no root and no scratch and deletes nothing; it is given
// no scratch to settle either.
func TestAnObserveOnlyDaemonGivesAgentsNoStores(t *testing.T) {
	for _, observeOnly := range []bool{false, true} {
		d := newTestDaemon(t, observeOnly)
		token := declareStoreNode(t, d)
		status, body := d.call(t, "POST", "/api/agent/stores", token, map[string]string{"node_id": "n"})
		var stores struct {
			Stores []machine.Store `json:"stores"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &stores) != nil {
			t.Fatalf("observe-only %v: %d %s", observeOnly, status, body)
		}
		if observeOnly && len(stores.Stores) != 0 || !observeOnly && (len(stores.Stores) != 1 || stores.Stores[0].ID != "scratch") {
			t.Fatalf("observe-only %v: stores %s", observeOnly, body)
		}
		status, body = d.call(t, "POST", "/api/agent/scratch-to-settle", token, map[string]string{"executor_id": "e"})
		var due struct {
			Scratch []store.ScratchSettlement `json:"scratch"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &due) != nil || len(due.Scratch) != 0 {
			t.Fatalf("observe-only %v: scratch to settle %d %s", observeOnly, status, body)
		}
	}
}
