package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// A node reports its labels; a [bench] table, whose programs now come with
// Kairo into the bench's container, is refused with that said.
func TestBenchTableIsRefusedAndNodesReportTheirLabels(t *testing.T) {
	agent := `server_url = "https://192.0.2.12:7474"
token = "kairo_node_x"
[node]
id = "host-a-train"
name = "host-a-train"
labels = { interconnect_addr = "10.0.0.2" }
[[executors]]
id = "ctr"
kind = "` + testKind + `"
`
	cfg, err := LoadAgent(writeConfig(t, agent))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(NodeAttributes(cfg.Node.Labels))
	if err != nil || string(body) != `{"interconnect_addr":"10.0.0.2"}` {
		t.Fatalf("attributes %s %v", body, err)
	}
	bench := "[bench]\ngpu_binary = \"/opt/kairo/kairo-bench-gpu\"\n"
	if _, err := LoadAgent(writeConfig(t, agent+bench)); err == nil || !strings.Contains(err.Error(), "[bench] is gone") {
		t.Fatalf("agent [bench]: %v", err)
	}
	if _, err := Load(writeConfig(t, daemonBase+bench)); err == nil || !strings.Contains(err.Error(), "[bench] is gone") {
		t.Fatalf("daemon [bench]: %v", err)
	}
}
