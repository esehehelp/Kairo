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
	controller, gen, low, high, def := "host-a-host", int64(3), 100.0, 350.0, 320.0
	failedAt := now.Add(-time.Minute).Format(time.RFC3339Nano)
	failure := "nvidia-smi -lmc 9501,9501: exit status 3"
	stopped := "controller stopped"
	hosts := []store.HardwareHost{{
		ID: "host-a", Nodes: []string{"host-a-infer", "host-a-train"}, Controller: &controller, MaxSlowdown: 0.05, ControllerLastSeenAt: &seen,
		Devices: []store.HardwareDevice{
			{
				DevicePolicy: machine.DevicePolicy{HostID: "host-a", DeviceID: "GPU-a", Kind: "gpu", MaxSlowdown: 0.1, Control: true, Controller: &controller},
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
				DevicePolicy: machine.DevicePolicy{HostID: "host-a", DeviceID: "GPU-b", Kind: "gpu", MaxSlowdown: 0.05, Control: true, Controller: &controller},
				Generation:   2, Effective: &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{MemoryClockMHz: intp(9501)}},
				LastError: &failure, LastErrorAt: &failedAt, ReportedAt: &seen,
			},
			{
				DevicePolicy: machine.DevicePolicy{HostID: "host-a", DeviceID: "cpu", Kind: "cpu", MaxSlowdown: 0.05, Control: true, Controller: &controller},
				Applied:      &store.DeviceSettings{}, AppliedNote: &stopped, ReportedAt: &seen, Generation: 1,
			},
		},
	}, {
		ID: "win", Nodes: []string{"win-node"},
		Devices: []store.HardwareDevice{{DevicePolicy: machine.DevicePolicy{HostID: "win", DeviceID: "cpu", Kind: "cpu", Control: true}}},
	}}
	var out bytes.Buffer
	if err := renderHardware(&out, hosts, now); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"host-a  nodes host-a-infer,host-a-train  controller host-a-host (seen " + clock(now.Add(-10*time.Second)) + ")  max_slowdown 0.05",
		"DEVICE  KIND  SLOWDOWN  CONTROL  DESIRED                   OWNER          APPLIED                        STATE",
		"GPU-a   gpu   0.1       yes      power_limit_w=250W        bench lease-1  power_limit_w=250W             in sync",
		"GPU-b   gpu   0.05      yes      memory_clock_mhz=9501MHz  manual alice   -                              failed",
		"cpu     cpu   0.05      yes      defaults                  -              defaults (controller stopped)  reset",
		"  GPU-a: overridden: policy lease-1 power_limit_w=280W",
		"  GPU-a: supports graphics_clock_max_mhz 210-2130MHz, power_limit_w 100-350W (default 320W)",
		"  GPU-b: error " + shortTime(now.Add(-time.Minute)) + ": " + failure,
		"win  nodes win-node  controller none (observe only)  max_slowdown 0",
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
		writeJSONResponse(w, http.StatusOK, store.DeviceSettingResult{HostID: "host-a", DeviceID: "GPU-a", Generation: 4,
			Effective: &store.DeviceSetting{Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{PowerLimitW: intp(250), GraphicsClockMaxMHz: intp(1800)}}})
	}))
	defer server.Close()
	out, err := captureStdout(t, func() error {
		return run([]string{"hardware", "set", "--api", server.URL, "--power-limit", "250", "--graphics-clock-max", "1800", "host-a", "GPU-a"})
	})
	if err != nil {
		t.Fatal(err)
	}
	settings, _ := got["settings"].(map[string]any)
	if path != "POST /api/hardware/host-a/GPU-a/settings" || settings["power_limit_w"] != 250.0 || settings["graphics_clock_max_mhz"] != 1800.0 || len(settings) != 2 {
		t.Fatalf("request %s %+v", path, got)
	}
	if want := "host-a/GPU-a: graphics_clock_max_mhz=1800MHz power_limit_w=250W (manual alice), generation 4; its controller applies it (kairo hardware show)\n"; out != want {
		t.Fatalf("output %q", out)
	}
	out, err = captureStdout(t, func() error { return run([]string{"hardware", "reset", "--api", server.URL, "host-a", "GPU-a"}) })
	if err != nil || path != "DELETE /api/hardware/host-a/GPU-a/settings" || !strings.HasPrefix(out, "host-a/GPU-a: no manual setting; now ") {
		t.Fatalf("reset %s: %q %v", path, out, err)
	}
}

func TestHardwareSetRefusesBadFlagsLocally(t *testing.T) {
	isolateClientEnv(t)
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"hardware", "set", "host-a", "cpu"}, "requires a setting"},
		{[]string{"hardware", "set", "host-a"}, "requires HOST and DEVICE"},
		{[]string{"hardware", "set", "--cpu-max-freq", "3000", "--cpu-max-perf", "80", "host-a", "cpu"}, "exclude each other"},
		{[]string{"hardware", "set", "--power-limit", "lots", "host-a", "GPU-a"}, "not a whole number"},
	} {
		if err := run(c.args); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%v: %v, want %q", c.args, err, c.want)
		}
	}
}

func TestFormatApplyListsHostChangesAndClearedSettings(t *testing.T) {
	from := machine.Host{ID: "host-a", Nodes: []string{"a"}, MaxSlowdown: 0}
	to := machine.Host{ID: "host-a", Nodes: []string{"a", "b"}, Controller: "host-a-host", MaxSlowdown: 0.05, Devices: []machine.Device{{ID: "cpu", Control: new(false)}}}
	result := store.MachineApplyResult{
		DryRun: true, Unchanged: 2,
		HostChanges:     []store.HostChange{{Host: "host-a", From: &from, To: &to}, {Host: "win", To: &machine.Host{ID: "win", Nodes: []string{"w"}}}, {Host: "old", From: &from}},
		ClearedSettings: []store.DeviceSetting{{HostID: "host-a", DeviceID: "cpu", Owner: store.DeviceOwner{Kind: store.OwnerManual, Ref: "alice"}, Settings: store.DeviceSettings{MaxFreqMHz: intp(3000)}}},
	}
	want := "would apply: 3 change(s), 2 unchanged\n" +
		"  ~ host host-a  nodes a -> a,b; controller - -> host-a-host; max_slowdown 0 -> 0.05; devices - -> cpu(control=false)\n" +
		"  + host win  nodes w; controller -; max_slowdown 0; devices -\n" +
		"  - host old\n" +
		"  - setting host-a/cpu  manual alice: max_freq_mhz=3000MHz (no longer controllable)\n"
	if got := formatApply("would apply", result); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if got := formatApply("applied", store.MachineApplyResult{Unchanged: 1, UnchangedHosts: 1}); got != "applied: no changes (1 GPUs, 1 hosts declared)\n" {
		t.Fatalf("no changes: %q", got)
	}
}

func TestFormatMachinesListsHostsAndDevicePolicies(t *testing.T) {
	controller := "host-a-host"
	out := formatMachines(store.Machines{
		Hosts: []machine.Host{{ID: "host-a", Nodes: []string{"a", "b"}, Controller: controller}, {ID: "win", Nodes: []string{"w"}}},
		Devices: []machine.DevicePolicy{
			{HostID: "host-a", DeviceID: "GPU-a", MaxSlowdown: 0.1, Control: true, Controller: &controller},
			{HostID: "host-a", DeviceID: "cpu", MaxSlowdown: 0.05, Control: false, Controller: &controller},
			{HostID: "win", DeviceID: "cpu", Control: true},
		},
	})
	for _, line := range []string{
		"HOST    NODES  CONTROLLER",
		"host-a  a,b    host-a-host",
		"win     w      -",
		"host-a  GPU-a   0.1           yes",
		"host-a  cpu     0.05          no",
		"win     cpu     0             no (no controller)",
	} {
		if !strings.Contains(out, line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, out)
		}
	}
}
