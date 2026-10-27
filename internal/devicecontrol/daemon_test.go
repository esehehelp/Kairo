package devicecontrol

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/api"
	"kairo/internal/machine"
	"kairo/internal/store"
)

// A controller against a real daemon: it registers its node, finds the
// device it controls, reports what it supports, carries out the operator's
// setting and returns the device to its defaults when it stops.
func TestControllerAgainstTheDaemon(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "kairo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := httptest.NewServer((&api.Server{Store: st}).Handler())
	defer server.Close()
	_, token, err := st.CreateAPIToken(ctx, "ctl", store.RoleNode, "ctl")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.UpsertNode(ctx, store.Node{ID: "work", Name: "work", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	cpus := &fakeActuator{}
	c := &Controller{
		Daemon:    agentclient.New(server.URL, token, nil),
		Node:      store.Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"},
		Actuators: map[string]Actuator{"cpu": cpus},
		Logger:    quiet,
	}
	c.Step(ctx) // registers the node: the declaration may name it now
	spec := machine.Spec{Hosts: []machine.Host{{ID: "h", Nodes: []string{"work"}, Controller: "ctl"}}}
	if _, err = st.ApplyMachines(ctx, spec, false); err != nil {
		t.Fatal(err)
	}
	c.Step(ctx)
	if got := cpus.take(); !slices.Equal(got, []string{"cpu defaults"}) {
		t.Fatalf("start: %v", got)
	}
	// the fake reports power_limit_w; a CPU setting must then be refused
	if _, err = st.SetDeviceSetting(ctx, "h", "cpu", store.DeviceOwner{Kind: store.OwnerManual}, store.DeviceSettings{MaxFreqMHz: new(3000)}, "alice"); err == nil {
		t.Fatal("a setting the device does not support was accepted")
	}
	cpus.capabilities = &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}}
	c.devices[deviceKey{"h", "cpu"}].capsAt = time.Time{}
	c.Step(ctx)
	result, err := st.SetDeviceSetting(ctx, "h", "cpu", store.DeviceOwner{Kind: store.OwnerManual}, store.DeviceSettings{MaxFreqMHz: new(3000)}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	c.Step(ctx)
	if got := cpus.take(); !slices.Equal(got, []string{"cpu max_freq_mhz=3000MHz"}) {
		t.Fatalf("set: %v", got)
	}
	hosts, err := st.ListHardware(ctx, "h")
	if err != nil {
		t.Fatal(err)
	}
	device := hosts[0].Devices[0]
	if device.AppliedGeneration == nil || *device.AppliedGeneration != result.Generation || device.Applied.MaxFreqMHz == nil || hosts[0].ControllerLastSeenAt == nil {
		t.Fatalf("hardware after apply: %+v", hosts[0])
	}
	c.Stop(ctx)
	if got := cpus.take(); !slices.Equal(got, []string{"cpu defaults"}) {
		t.Fatalf("stop: %v", got)
	}
	hosts, _ = st.ListHardware(ctx, "h")
	if device = hosts[0].Devices[0]; device.AppliedNote == nil || *device.AppliedNote != "controller stopped" || !device.Applied.IsDefault() {
		t.Fatalf("hardware after stop: %+v", device)
	}
}
