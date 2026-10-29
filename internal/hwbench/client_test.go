package hwbench

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// The commands a driver polls and acknowledges go over the wire as an SDK's
// do (sdk/conformance/worker_scenarios.json, "suspend checkpointed").
func TestClientPollsAndAcknowledgesCommandsAsTheSDKsDo(t *testing.T) {
	type exchange struct{ method, path, phase, ref string }
	var seen []exchange
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("authorization %q", r.Header.Get("Authorization"))
		}
		var body struct {
			Phase   string         `json:"phase"`
			Payload map[string]any `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		ref, _ := body.Payload["continuation_ref"].(string)
		seen = append(seen, exchange{r.Method, r.URL.EscapedPath(), body.Phase, ref})
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"commands":[{"id":"cmd/1","kind":"suspend","origin":"scope_pause","reason":"paused","delivery_count":1}]}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "", "tok", "bnc-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	commands, err := client.Commands(ctx)
	if err != nil || len(commands) != 1 || commands[0].ID != "cmd/1" || commands[0].Kind != "suspend" || commands[0].DeliveryCount != 1 {
		t.Fatalf("commands: %+v %v", commands, err)
	}
	for _, phase := range []string{"accepted", "checkpointing"} {
		if err = client.Ack(ctx, "cmd/1", phase, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = client.Ack(ctx, "cmd/1", "checkpointed", map[string]any{"continuation_ref": `{"run":"bnc-1","done":[]}`}); err != nil {
		t.Fatal(err)
	}
	want := []exchange{
		{"GET", "/api/worker/commands", "", ""},
		{"POST", "/api/worker/commands/cmd%2F1/acks", "accepted", ""},
		{"POST", "/api/worker/commands/cmd%2F1/acks", "checkpointing", ""},
		{"POST", "/api/worker/commands/cmd%2F1/acks", "checkpointed", `{"run":"bnc-1","done":[]}`},
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("exchanges:\n%+v\nwant\n%+v", seen, want)
	}
}
