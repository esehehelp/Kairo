package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBenchProgramsAreReportedWithTheNode(t *testing.T) {
	path := writeConfig(t, `server_url = "https://192.168.1.12:7474"
token = "kairo_node_x"
[node]
id = "pve0-train"
name = "pve0-train"
labels = { interconnect_addr = "10.0.0.2" }
[bench]
gpu_binary = "/opt/kairo/kairo-bench-gpu"
executor_labels = { environment = "linux" }
[[executors]]
id = "linux"
kind = "linux"
`)
	cfg, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(cfg.Bench.Attributes(cfg.Node.Labels))
	if err != nil {
		t.Fatal(err)
	}
	self, _ := os.Executable()
	selfJSON, _ := json.Marshal(self)
	for _, want := range []string{`"interconnect_addr":"10.0.0.2"`, `"gpu":"/opt/kairo/kairo-bench-gpu"`, `"executor_labels":{"environment":"linux"}`, `"kairo":` + string(selfJSON)} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("attributes %s lack %s", body, want)
		}
	}
	if _, err = Load(writeConfig(t, daemonBase+"[bench]\nkairo_binary = \"D:/Kairo/kairo.exe\"\n")); err != nil {
		t.Fatalf("daemon [bench]: %v", err)
	}
}
