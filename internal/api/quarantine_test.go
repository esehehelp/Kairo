package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"kairo/internal/store"
)

// "all" is an ordinary node id; the all-node quarantine has its own route.
func TestNodeQuarantineRoutesTakeNodeIDsLiterally(t *testing.T) {
	st, server := newTestServer(t)
	ctx := context.Background()
	for _, id := range []string{"node1", "all"} {
		if err := st.UpsertNode(ctx, store.Node{ID: id, Name: id, OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	quarantined := func() []string {
		t.Helper()
		response, body := callJSON(t, http.MethodGet, server.URL+"/v2/node-quarantines", nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("list: %d %s", response.StatusCode, body)
		}
		var out struct {
			Quarantines []store.NodeQuarantine `json:"quarantines"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, q := range out.Quarantines {
			ids = append(ids, q.NodeID)
		}
		return ids
	}

	if response, body := callJSON(t, http.MethodPost, server.URL+"/v2/nodes/all/quarantine", map[string]string{"reason": "x"}); response.StatusCode != http.StatusAccepted {
		t.Fatalf("quarantine node all: %d %s", response.StatusCode, body)
	}
	if ids := quarantined(); len(ids) != 1 || ids[0] != "all" {
		t.Fatalf("node 'all' quarantine = %v", ids)
	}
	if response, _ := callJSON(t, http.MethodPost, server.URL+"/v2/nodes/missing/quarantine", nil); response.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown node: %d", response.StatusCode)
	}
	if response, _ := callJSON(t, http.MethodPost, server.URL+"/v2/nodes/*/quarantine", nil); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("'*' as a node id: %d", response.StatusCode)
	}
	if response, body := callJSON(t, http.MethodPost, server.URL+"/v2/node-quarantines/all", map[string]string{"reason": "x"}); response.StatusCode != http.StatusAccepted {
		t.Fatalf("quarantine every node: %d %s", response.StatusCode, body)
	}
	if ids := quarantined(); len(ids) != 2 {
		t.Fatalf("after all-node quarantine = %v", ids)
	}
	if response, body := callJSON(t, http.MethodDelete, server.URL+"/v2/nodes/all/quarantine", nil); response.StatusCode != http.StatusOK {
		t.Fatalf("release node all: %d %s", response.StatusCode, body)
	}
	if ids := quarantined(); len(ids) != 1 || ids[0] != store.AllNodes {
		t.Fatalf("releasing node 'all' released %v", ids)
	}
	if response, body := callJSON(t, http.MethodDelete, server.URL+"/v2/node-quarantines/all", nil); response.StatusCode != http.StatusOK {
		t.Fatalf("release every node: %d %s", response.StatusCode, body)
	}
	if ids := quarantined(); len(ids) != 0 {
		t.Fatalf("after release = %v", ids)
	}
}
