package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNodeQuarantineCommandRequests(t *testing.T) {
	type call struct{ method, path, reason string }
	var calls []call
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		calls = append(calls, call{r.Method, r.URL.EscapedPath(), body["reason"]})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	for _, tc := range []struct {
		args []string
		want call
	}{
		// the README form: flags after NODE_ID
		{[]string{"quarantine", "node1", "--reason", "maint", "--api", server.URL}, call{"POST", "/v2/nodes/node1/quarantine", "maint"}},
		{[]string{"quarantine", "--api", server.URL, "--reason", "maint", "node1"}, call{"POST", "/v2/nodes/node1/quarantine", "maint"}},
		{[]string{"quarantine", "--all", "--reason", "dc", "--api", server.URL}, call{"POST", "/v2/node-quarantines/all", "dc"}},
		// a node named "all" is not every node
		{[]string{"quarantine", "all", "--api", server.URL}, call{"POST", "/v2/nodes/all/quarantine", "operator request"}},
		{[]string{"release", "node1", "--api", server.URL}, call{"DELETE", "/v2/nodes/node1/quarantine", ""}},
		{[]string{"release", "--all", "--api", server.URL}, call{"DELETE", "/v2/node-quarantines/all", ""}},
		{[]string{"quarantine", "--api", server.URL, "--", "-odd-node"}, call{"POST", "/v2/nodes/-odd-node/quarantine", "operator request"}},
	} {
		calls = nil
		if err := nodeCommand(tc.args); err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if len(calls) != 1 || calls[0] != tc.want {
			t.Fatalf("%v: calls %+v, want %+v", tc.args, calls, tc.want)
		}
	}
	for _, args := range [][]string{
		{"quarantine", "--api", server.URL},
		{"quarantine", "node1", "--all", "--api", server.URL},
		{"quarantine", "node1", "node2", "--api", server.URL},
	} {
		calls = nil
		if err := nodeCommand(args); err == nil || len(calls) != 0 {
			t.Fatalf("%v: accepted (%v, calls %+v)", args, err, calls)
		}
	}
}
