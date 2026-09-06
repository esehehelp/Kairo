package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestProjectPauseWaitsForDurableOperationByDefault(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/v2/scopes/scp_project/pause":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": "pop_one"})
		case "/v2/pause-operations/pop_one":
			_ = json.NewEncoder(w).Encode(map[string]any{"operation": map[string]string{"state": "quiesced"}, "targets": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := projectCommand([]string{"pause", "--api", server.URL, "--timeout", time.Second.String(), "p"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"GET /v2/scopes",
		"POST /v2/scopes/scp_project/pause",
		"GET /v2/pause-operations/pop_one",
	}
	if len(requests) != len(want) {
		t.Fatalf("request sequence = %v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request sequence = %v, want %v", requests, want)
		}
	}
}

func TestProjectResumeOnlyOpensGate(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/scopes":
			_ = json.NewEncoder(w).Encode(map[string]any{"scopes": []map[string]string{{"id": "scp_project"}}})
		case "/v2/scopes/scp_project/resume":
			_ = json.NewEncoder(w).Encode(map[string]any{"gate": map[string]any{"state": "open", "generation": 3}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := projectCommand([]string{"resume", "--api", server.URL, "p"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"GET /v2/scopes", "POST /v2/scopes/scp_project/resume"}
	if len(requests) != len(want) {
		t.Fatalf("request sequence = %v, want %v", requests, want)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request sequence = %v, want %v", requests, want)
		}
	}
}
