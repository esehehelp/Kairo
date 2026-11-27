//go:build linux

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// An Incus instance is made with a gang member's NIC on its segment's
// bridge and, when the daemon's API is on the fabric, a proxy device that
// listens in the container and connects from the host.
func TestAnIncusInstanceHasTheGangNICAndTheAPIProxy(t *testing.T) {
	previous := incusCommand
	t.Cleanup(func() { incusCommand = previous })
	var body []byte
	incusCommand = func(_ context.Context, stdin []byte, args ...string) ([]byte, error) {
		switch args[0] {
		case "remote":
			return []byte(`{"kairo-docker-io":{"Addr":"https://docker.io","Protocol":"oci"}}`), nil
		case "init":
			body = stdin
			return nil, errors.New("stop here")
		}
		return nil, nil
	}
	spec := containerSpec{
		Instance: "kairo-t-att-1",
		Image:    "docker.io/library/python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b",
		Gang:     &containerGang{Device: "kfb100000", MTU: 1370, Address: "192.0.2.2/24", Members: []string{"192.0.2.1/24", "192.0.2.2/24"}, Rank: 1},
		APIProxy: "198.51.100.3:7474",
	}
	if _, err := runInstance(context.Background(), spec, t.Logf, func() {}, func() bool { return true }); err == nil {
		t.Fatal("made past init")
	}
	var made struct {
		Devices map[string]map[string]string `json:"devices"`
	}
	if err := json.Unmarshal(body, &made); err != nil {
		t.Fatalf("init body %s: %v", body, err)
	}
	nic := made.Devices["kairo-gang"]
	if nic["type"] != "nic" || nic["nictype"] != "bridged" || nic["parent"] != "kfb100000" || nic["mtu"] != "1370" || nic["name"] != gangNIC {
		t.Fatalf("gang NIC %v", nic)
	}
	proxy := made.Devices["kairo-api"]
	if proxy["type"] != "proxy" || proxy["bind"] != "instance" || proxy["listen"] != "tcp:0.0.0.0:7474" || proxy["connect"] != "tcp:198.51.100.3:7474" {
		t.Fatalf("API proxy %v", proxy)
	}
}
