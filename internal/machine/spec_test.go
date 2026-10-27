package machine

import (
	"os"
	"strings"
	"testing"
)

const nodes = `
[[nodes]]
id = "pve0-train"
gpus = ["GPU-a"]
[[nodes]]
id = "pve0-glm"
gpus = ["GPU-a", "GPU-b"]
[[nodes]]
id = "win"
gpus = ["GPU-w"]
`

func TestHostsParseAndEffectivePolicies(t *testing.T) {
	spec, err := Parse(nodes + `
[[hosts]]
id = "pve0"
nodes = ["pve0-train", "pve0-glm"]
controller = "pve0-host"
max_slowdown = 0.05
[[hosts.devices]]
id = "GPU-b"
max_slowdown = 0.10
[[hosts.devices]]
id = "cpu"
control = false

[[hosts]]
id = "win-host"
nodes = ["win"]
`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range spec.DevicePolicies() {
		controller := "-"
		if p.Controller != nil {
			controller = *p.Controller
		}
		got = append(got, strings.Join([]string{p.HostID, p.DeviceID, p.Kind, formatFloat(p.MaxSlowdown), formatBool(p.Control), controller, formatBool(p.Controllable())}, " "))
	}
	want := []string{
		"pve0 GPU-a gpu 0.05 true pve0-host true",
		"pve0 GPU-b gpu 0.1 true pve0-host true",
		"pve0 cpu cpu 0.05 false pve0-host false",
		// no max_slowdown: 0, nothing tuned; no controller: observe only
		"win-host GPU-w gpu 0 true - false",
		"win-host cpu cpu 0 true - false",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("policies:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if ids := strings.Join(spec.NodeIDs(), ","); ids != "pve0-glm,pve0-host,pve0-train,win" {
		t.Fatalf("node ids %s", ids)
	}
}

func formatFloat(v float64) string {
	switch v {
	case 0:
		return "0"
	case 0.05:
		return "0.05"
	case 0.1:
		return "0.1"
	}
	return "?"
}

func formatBool(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func TestHostsWithoutHostsStayValid(t *testing.T) {
	spec, err := Parse(nodes)
	if err != nil || len(spec.Hosts) != 0 || len(spec.DevicePolicies()) != 0 {
		t.Fatalf("a declaration without hosts: %+v %v", spec, err)
	}
}

func TestHostValidation(t *testing.T) {
	for _, c := range []struct{ name, hosts, want string }{
		{"no id", "[[hosts]]\nnodes = [\"win\"]\n", "needs an id"},
		{"twice", "[[hosts]]\nid = \"h\"\nnodes = [\"win\"]\n[[hosts]]\nid = \"h\"\nnodes = [\"pve0-glm\"]\n", "host h is declared twice"},
		{"no nodes", "[[hosts]]\nid = \"h\"\n", "lists no nodes"},
		{"node on two hosts", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\n[[hosts]]\nid = \"b\"\nnodes = [\"win\"]\n", "node win is on hosts a and b"},
		{"node twice", "[[hosts]]\nid = \"a\"\nnodes = [\"win\", \"win\"]\n", "lists node win twice"},
		{"slowdown negative", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\nmax_slowdown = -0.1\n", "outside [0, 0.5)"},
		{"slowdown too large", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\nmax_slowdown = 0.5\n", "outside [0, 0.5)"},
		{"device slowdown", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\n[[hosts.devices]]\nid = \"cpu\"\nmax_slowdown = 0.7\n", "device cpu: max_slowdown 0.7"},
		{"gpu spans hosts", "[[hosts]]\nid = \"a\"\nnodes = [\"pve0-train\"]\n[[hosts]]\nid = \"b\"\nnodes = [\"pve0-glm\"]\n", "GPU-a is declared for nodes on hosts a and b"},
		{"device of another host", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\n[[hosts.devices]]\nid = \"GPU-a\"\n", `device "GPU-a" is neither "cpu" nor a GPU declared for one of its nodes`},
		{"undeclared device", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\n[[hosts.devices]]\nid = \"GPU-zzz\"\n", "is neither"},
		{"device twice", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\n[[hosts.devices]]\nid = \"cpu\"\n[[hosts.devices]]\nid = \"cpu\"\n", "lists device cpu twice"},
		{"controller twice", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\ncontroller = \"c\"\n[[hosts]]\nid = \"b\"\nnodes = [\"pve0-glm\", \"pve0-train\"]\ncontroller = \"c\"\n", "node c controls hosts a and b"},
		{"unknown field", "[[hosts]]\nid = \"a\"\nnodes = [\"win\"]\nmodel = \"RTX\"\n", "unknown machine field"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(nodes + c.hosts)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}
}

func TestTheExampleParses(t *testing.T) {
	body, err := os.ReadFile("../../examples/machines.toml")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := Parse(string(body))
	if err != nil || len(spec.Hosts) != 2 || len(spec.DevicePolicies()) != 4 {
		t.Fatalf("example: %+v %v", spec, err)
	}
}

func TestNormalizedSortsNodesAndDevices(t *testing.T) {
	h := Host{ID: "h", Nodes: []string{"b", "a"}, Devices: []Device{{ID: "cpu"}, {ID: "GPU-a"}}}.Normalized()
	if h.Nodes[0] != "a" || h.Devices[0].ID != "GPU-a" {
		t.Fatalf("normalized %+v", h)
	}
}
