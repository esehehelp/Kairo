package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"kairo/internal/store"
)

func TestPlanApplyAPIRequiresOptimisticRevision(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := httptest.NewServer((&Server{Store: st}).Handler())
	defer server.Close()
	source := "schema_version=1\nproject=\"p\"\nqueue=\"q\"\n[[tasks]]\nkey=\"a\"\nargv=[\"run\"]\ncwd=\".\"\n"
	call := func(body any) int {
		encoded, _ := json.Marshal(body)
		response, err := http.Post(server.URL+"/v1/queues/p/q/plan", "application/json", bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	if status := call(map[string]any{"source": source, "create": true, "actor": "test", "request_id": "one"}); status != http.StatusBadRequest {
		t.Fatalf("missing expected revision: %d", status)
	}
	if status := call(map[string]any{"source": source, "expected_revision": 0, "create": true, "actor": "test", "request_id": "one"}); status != http.StatusCreated {
		t.Fatalf("create: %d", status)
	}
	changed := source + "# changed source only; normalized digest remains the same\n"
	if status := call(map[string]any{"source": changed, "expected_revision": 0, "actor": "test", "request_id": "two"}); status != http.StatusOK {
		t.Fatalf("idempotent apply: %d", status)
	}
	changed = "schema_version=1\nproject=\"p\"\nqueue=\"q\"\n[[tasks]]\nkey=\"a\"\nargv=[\"different\"]\ncwd=\".\"\n"
	if status := call(map[string]any{"source": changed, "expected_revision": 0, "actor": "test", "request_id": "three"}); status != http.StatusConflict {
		t.Fatalf("revision conflict: %d", status)
	}
}
