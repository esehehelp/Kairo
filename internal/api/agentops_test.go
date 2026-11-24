package api

import (
	"encoding/json"
	"testing"

	"kairo/internal/agentops"
	"kairo/internal/store"
)

// The daemon serves every agent operation declared in agentops, each once,
// and none that is not declared there.
func TestEveryAgentOperationIsServed(t *testing.T) {
	s := &Server{}
	served := map[string]bool{}
	for _, op := range append(s.agentOps(), s.controllerOps()...) {
		if served[op.name] {
			t.Errorf("operation %s is served twice", op.name)
		}
		served[op.name] = true
	}
	declared := map[string]bool{}
	for _, op := range agentops.All {
		name := op.OpName()
		if declared[name] {
			t.Errorf("operation %s is declared twice", name)
		}
		declared[name] = true
		if !served[name] {
			t.Errorf("operation %s is declared but not served", name)
		}
	}
	for name := range served {
		if !declared[name] {
			t.Errorf("operation %s is served but not declared in agentops", name)
		}
	}
}

// The requests are the wire format between an agent and a daemon of other
// versions: an optional field is left out when unset, as agents sent it
// before the operations were declared, and an observe-only poll answers an
// empty list, not null.
func TestAgentRequestsKeepTheirWireFormat(t *testing.T) {
	for _, c := range []struct {
		value any
		want  string
	}{
		{agentops.LeaseRequest{LeaseID: "lea_1", Epoch: 3}, `{"lease_id":"lea_1","epoch":3}`},
		{agentops.RegisterRequest{Node: store.Node{ID: "n"}}, ""},
		{agentops.ForceStopAck{AttemptID: "att_1", Phase: "signalled"}, `{"attempt_id":"att_1","phase":"signalled"}`},
		{agentops.AttemptUsage{AttemptID: "att_1"}, `{"attempt_id":"att_1","ram_bytes":null,"disk_written_bytes":null}`},
		{agentops.PacksToMake.Empty, `{"packs":[]}`},
		{agentops.ReserveNext.Empty, `{"reservation":null}`},
	} {
		body, err := json.Marshal(c.value)
		if err != nil {
			t.Fatal(err)
		}
		if c.want == "" {
			var fields map[string]any
			if err := json.Unmarshal(body, &fields); err != nil {
				t.Fatal(err)
			}
			if _, ok := fields["adopt"]; ok {
				t.Errorf("an empty adopt is sent: %s", body)
			}
			if _, ok := fields["uptime_seconds"]; ok {
				t.Errorf("an unknown uptime is sent: %s", body)
			}
			continue
		}
		if string(body) != c.want {
			t.Errorf("%T = %s, want %s", c.value, body, c.want)
		}
	}
}
