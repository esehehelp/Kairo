//go:build linux

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if _, made := made.Devices["kairo-api"]; made {
		t.Fatal("the API proxy made with the instance (before its address is on lo)")
	}
}

// The API proxy is added once the init has put the daemon's address on lo,
// listening at that address only.
func TestTheAPIProxyListensAtTheDaemonsAddressOnly(t *testing.T) {
	previous := incusCommand
	t.Cleanup(func() { incusCommand = previous })
	added := make(chan []string, 1)
	incusCommand = func(_ context.Context, _ []byte, args ...string) ([]byte, error) {
		added <- args
		return nil, nil
	}
	spec := containerSpec{Instance: "kairo-t-att-1", RunDir: t.TempDir(), APIProxy: "198.51.100.3:7474"}
	go addAPIProxy(context.Background(), spec, t.Logf)
	select {
	case args := <-added:
		t.Fatalf("added before the init said its address is on lo: %v", args)
	case <-time.After(500 * time.Millisecond):
	}
	if err := os.WriteFile(filepath.Join(spec.RunDir, apiReadyFile), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case args := <-added:
		if strings.Join(args, " ") != "config device add kairo-t-att-1 kairo-api proxy bind=instance listen=tcp:198.51.100.3:7474 connect=tcp:198.51.100.3:7474" {
			t.Fatalf("added %v", args)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not added")
	}
}
