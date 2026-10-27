package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"kairo/internal/machine"
	"kairo/internal/store"
)

func TestRenderHardware(t *testing.T) {
	now := time.Date(2026, 10, 27, 10, 2, 0, 0, time.UTC)
	seen := now.Add(-10 * time.Second).Format(time.RFC3339Nano)
	controller, gen, low, high, def := "pve0-host", int64(3), 100.0, 350.0, 320.0
	failedAt := now.Add(-time.Minute).Format(time.RFC3339Nano)
	failure := "nvidia-smi -lmc 9501,9501: exit status 3"
	stopped := "controller stopped"
	hosts := []store.HardwareHost{{
		ID: "pve0", Nodes: []string{"pve0-glm", "pve0-train"}, Controller: &controller, MaxSlowdown: 0.05, ControllerLastSeenAt: &seen,
		Devices: []store.HardwareDevice{
			{
				DevicePolicy: machine.DevicePolicy{HostID: "pve0", DeviceID: "GPU-a", Kind: "gpu", MaxSlowdown: 0.1, Control: true, Controller: &controller},
				Generation:   3,
				Effective:    &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerBench, Ref: "lease-1"}, Settings: store.DeviceSettings{PowerLimitW: intp(250)}},
				Settings: []store.DeviceSetting{
					{Owner: store.DeviceOwner{Kind: store.OwnerBench, Ref: "lease-1"}, Settings: store.DeviceSettings{PowerLimitW: intp(250)}},
					{Owner: store.DeviceOwner{Kind: store.OwnerPolicy, Ref: "lease-1"}, Settings: store.DeviceSettings{PowerLimitW: intp(280)}},
				},
				AppliedGeneration: &gen, Applied: &store.DeviceSettings{PowerLimitW: intp(250)}, ReportedAt: &seen,
				Capabilities: &store.DeviceCapabilities{Settings: []string{store.SettingGraphicsClockMax, store.SettingPowerLimit}, GraphicsClocksMHz: []int{210, 2130}, PowerLimitMinW: &low, PowerLimitMaxW: &high, PowerLimitDefaultW: &def},
			},
			{
				DevicePolicy: machine.DevicePolicy{HostID: "pve0", DeviceID: "GPU-b", Kind: "gpu", MaxSlowdown: 0.05, Control: true, Controller: &controller},
				Generation:   2, Effective: &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{MemoryClockMHz: intp(9501)}},
				LastError: &failure, LastErrorAt: &failedAt, ReportedAt: &seen,
			},
			{
				DevicePolicy: machine.DevicePolicy{HostID: "pve0", DeviceID: "cpu", Kind: "cpu", MaxSlowdown: 0.05, Control: true, Controller: &controller},
				Applied:      &store.DeviceSettings{}, AppliedNote: &stopped, ReportedAt: &seen, Generation: 1,
			},
		},
	}, {
		ID: "win", Nodes: []string{"windows-host-1"},
		Devices: []store.HardwareDevice{{DevicePolicy: machine.DevicePolicy{HostID: "win", DeviceID: "cpu", Kind: "cpu", Control: true}}},
	}}
	var out bytes.Buffer
	if err := renderHardware(&out, hosts, now); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"pve0  nodes pve0-glm,pve0-train  controller pve0-host (seen " + clock(now.Add(-10*time.Second)) + ")  max_slowdown 0.05",
		"DEVICE  KIND  SLOWDOWN  CONTROL  DESIRED                   OWNER          APPLIED                        STATE",
		"GPU-a   gpu   0.1       yes      power_limit_w=250W        bench lease-1  power_limit_w=250W             in sync",
		"GPU-b   gpu   0.05      yes      memory_clock_mhz=9501MHz  manual alice   -                              failed",
		"cpu     cpu   0.05      yes      defaults                  -              defaults (controller stopped)  reset",
		"  GPU-a: overridden: policy lease-1 power_limit_w=280W",
		"  GPU-a: supports graphics_clock_max_mhz 210-2130MHz, power_limit_w 100-350W (default 320W)",
		"  GPU-b: error " + shortTime(now.Add(-time.Minute)) + ": " + failure,
		"win  nodes windows-host-1  controller none (observe only)  max_slowdown 0",
		"cpu     cpu   0         yes      defaults  -      -        observe only",
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, out.String())
		}
	}
}

func intp(v int) *int { return &v }

func TestHardwareSetSendsAManualSetting(t *testing.T) {
	var got map[string]any
	var path string
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.Method + " " + r.URL.EscapedPath()
		_ = json.NewDecoder(r.Body).Decode(&got)
		writeJSONResponse(w, http.StatusOK, store.DeviceSettingResult{HostID: "pve0", DeviceID: "GPU-a", Generation: 4,
			Effective: &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{PowerLimitW: intp(250), GraphicsClockMaxMHz: intp(1800)}}})
	}))
	defer server.Close()
	out, err := captureStdout(t, func() error {
		return run([]string{"hardware", "set", "--api", server.URL, "--power-limit", "250", "--graphics-clock-max", "1800", "pve0", "GPU-a"})
	})
	if err != nil {
		t.Fatal(err)
	}
	settings, _ := got["settings"].(map[string]any)
	if path != "POST /api/hardware/pve0/GPU-a/settings" || settings["power_limit_w"] != 250.0 || settings["graphics_clock_max_mhz"] != 1800.0 || len(settings) != 2 {
		t.Fatalf("request %s %+v", path, got)
	}
	if want := "pve0/GPU-a: graphics_clock_max_mhz=1800MHz power_limit_w=250W (manual alice), generation 4; its controller applies it (kairo hardware show)\n"; out != want {
		t.Fatalf("output %q", out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"hardware", "reset", "--api", server.URL, "pve0", "GPU-a"}) })
	if err != nil || path != "DELETE /api/hardware/pve0/GPU-a/settings" || !strings.HasPrefix(out, "pve0/GPU-a: no manual setting; now ") {
		t.Fatalf("reset %s: %q %v", path, out, err)
	}
}

func TestHardwareSetRefusesBadFlagsLocally(t *testing.T) {
	isolateClientEnv(t)
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"hardware", "set", "pve0", "cpu"}, "requires a setting"},
		{[]string{"hardware", "set", "pve0"}, "requires HOST and DEVICE"},
		{[]string{"hardware", "set", "--cpu-max-freq", "3000", "--cpu-max-perf", "80", "pve0", "cpu"}, "exclude each other"},
		{[]string{"hardware", "set", "--power-limit", "lots", "pve0", "GPU-a"}, "not a whole number"},
	} {
		if err := run(c.args); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%v: %v, want %q", c.args, err, c.want)
		}
	}
}

func TestFormatApplyListsHostChangesAndClearedSettings(t *testing.T) {
	from := machine.Host{ID: "pve0", Nodes: []string{"a"}, MaxSlowdown: 0}
	to := machine.Host{ID: "pve0", Nodes: []string{"a", "b"}, Controller: "pve0-host", MaxSlowdown: 0.05, Devices: []machine.Device{{ID: "cpu", Control: new(false)}}}
	result := store.MachineApplyResult{
		DryRun: true, Unchanged: 2,
		HostChanges:     []store.HostChange{{Host: "pve0", From: &from, To: &to}, {Host: "win", To: &machine.Host{ID: "win", Nodes: []string{"w"}}}, {Host: "old", From: &from}},
		ClearedSettings: []store.DeviceSetting{{HostID: "pve0", DeviceID: "cpu", Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{MaxFreqMHz: intp(3000)}}},
	}
	want := "would apply: 3 change(s), 2 unchanged\n" +
		"  ~ host pve0  nodes a -> a,b; controller - -> pve0-host; max_slowdown 0 -> 0.05; devices - -> cpu(control=false)\n" +
		"  + host win  nodes w; controller -; max_slowdown 0; devices -\n" +
		"  - host old\n" +
		"  - setting pve0/cpu  manual alice: max_freq_mhz=3000MHz (no longer controllable)\n"
	if got := formatApply("would apply", result); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if got := formatApply("applied", store.MachineApplyResult{Unchanged: 1, UnchangedHosts: 1}); got != "applied: no changes (1 GPUs, 1 hosts declared)\n" {
		t.Fatalf("no changes: %q", got)
	}
}

func TestFormatMachinesListsHostsAndDevicePolicies(t *testing.T) {
	controller := "pve0-host"
	out := formatMachines(store.Machines{
		Hosts: []machine.Host{{ID: "pve0", Nodes: []string{"a", "b"}, Controller: controller}, {ID: "win", Nodes: []string{"w"}}},
		Devices: []machine.DevicePolicy{
			{HostID: "pve0", DeviceID: "GPU-a", MaxSlowdown: 0.1, Control: true, Controller: &controller},
			{HostID: "pve0", DeviceID: "cpu", MaxSlowdown: 0.05, Control: false, Controller: &controller},
			{HostID: "win", DeviceID: "cpu", Control: true},
		},
	})
	for _, line := range []string{
		"HOST  NODES  CONTROLLER",
		"pve0  a,b    pve0-host",
		"win   w      -",
		"pve0  GPU-a   0.1           yes",
		"pve0  cpu     0.05          no",
		"win   cpu     0             no (no controller)",
	} {
		if !strings.Contains(out, line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, out)
		}
	}
}
