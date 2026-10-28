package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kairo/internal/machine"
)

// hostSpec is nodes "a" and "b" sharing GPU-S, on host "h" whose controller
// is node "ctl".
func hostSpec(host machine.Host) machine.Spec {
	return machine.Spec{
		Nodes: []machine.Node{{ID: "a", GPUs: []string{"GPU-S"}}, {ID: "b", GPUs: []string{"GPU-S"}}},
		Hosts: []machine.Host{host},
	}
}

var testHost = machine.Host{ID: "h", Nodes: []string{"b", "a"}, Controller: "ctl", MaxSlowdown: 0.05}

// setupHost: setupSharedGPU, the controller's node, host "h", and the
// capabilities its controller reports of GPU-S and cpu.
func setupHost(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	setupSharedGPU(t, store)
	if err := store.RegisterDeviceController(ctx, Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMachines(ctx, hostSpec(testHost), false); err != nil {
		t.Fatal(err)
	}
	reportCapabilities(t, store)
}

func reportCapabilities(t *testing.T, store *Store) {
	t.Helper()
	low, high, def := 100.0, 350.0, 320.0
	minFreq, maxFreq := 600, 5700
	reports := []DeviceReport{
		{HostID: "h", DeviceID: "GPU-S", Capabilities: &DeviceCapabilities{
			Settings:          []string{SettingGraphicsClockMax, SettingMemoryClock, SettingPowerLimit},
			GraphicsClocksMHz: []int{210, 1800, 2130}, MemoryClocksMHz: []int{405, 9501},
			PowerLimitMinW: &low, PowerLimitMaxW: &high, PowerLimitDefaultW: &def}},
		{HostID: "h", DeviceID: "cpu", Capabilities: &DeviceCapabilities{Settings: []string{SettingMaxFreq}, MaxFreqMinMHz: &minFreq, MaxFreqMaxMHz: &maxFreq}},
	}
	if skipped, err := store.ReportDevices(context.Background(), "ctl", reports); err != nil || len(skipped) != 0 {
		t.Fatalf("report capabilities: %v %v", skipped, err)
	}
}

func intp(v int) *int { return &v }

func eventCount(t *testing.T, store *Store, eventType string) int {
	t.Helper()
	var n int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM coordination_events WHERE event_type=$1`, eventType).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func polled(t *testing.T, store *Store, deviceID string) ControlledDevice {
	t.Helper()
	devices, err := store.PollDeviceSettings(context.Background(), "ctl")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range devices {
		if d.DeviceID == deviceID {
			return d
		}
	}
	t.Fatalf("device %s not polled: %+v", deviceID, devices)
	return ControlledDevice{}
}

func TestHostDeclarationIsDiffedStoredAndShown(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	if err := store.RegisterDeviceController(ctx, Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"}, "test"); err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyMachines(ctx, hostSpec(testHost), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.HostChanges) != 1 || result.HostChanges[0].From != nil || strings.Join(result.HostChanges[0].To.Nodes, ",") != "a,b" || result.Unchanged != 1 {
		t.Fatalf("dry run: %+v", result)
	}
	if machines, _ := store.ListMachines(ctx); len(machines.Hosts) != 0 {
		t.Fatalf("a dry run stored hosts: %+v", machines.Hosts)
	}
	if _, err = store.ApplyMachines(ctx, hostSpec(testHost), false); err != nil {
		t.Fatal(err)
	}
	if result, err = store.ApplyMachines(ctx, hostSpec(testHost), false); err != nil || len(result.HostChanges) != 0 || result.UnchangedHosts != 1 {
		t.Fatalf("same declaration again: %+v %v", result, err)
	}
	changed := testHost
	changed.MaxSlowdown = 0.1
	changed.Devices = []machine.Device{{ID: "cpu", Control: new(false)}}
	if result, err = store.ApplyMachines(ctx, hostSpec(changed), false); err != nil || len(result.HostChanges) != 1 || result.HostChanges[0].From.MaxSlowdown != 0.05 || result.HostChanges[0].To.MaxSlowdown != 0.1 {
		t.Fatalf("changed host: %+v %v", result, err)
	}
	machines, err := store.ListMachines(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(machines.Hosts) != 1 || len(machines.Devices) != 2 || machines.Devices[0].DeviceID != "GPU-S" || machines.Devices[1].Control || machines.Devices[0].MaxSlowdown != 0.1 {
		t.Fatalf("machines: %+v", machines)
	}
	policies, err := store.DevicePolicies(ctx)
	if err != nil || len(policies) != 2 || !policies[0].Controllable() || *policies[0].Controller != "ctl" || policies[1].Controllable() {
		t.Fatalf("policies: %+v %v", policies, err)
	}
	if eventCount(t, store, "host_declaration_changed") != 2 {
		t.Fatal("host changes were not recorded as events")
	}
	// Dropping the hosts is a change too.
	if result, err = store.ApplyMachines(ctx, machine.Spec{Nodes: hostSpec(testHost).Nodes}, false); err != nil || len(result.HostChanges) != 1 || result.HostChanges[0].To != nil {
		t.Fatalf("dropped host: %+v %v", result, err)
	}
}

func TestHostNodesAndControllerMustBeKnown(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	for _, host := range []machine.Host{
		{ID: "h", Nodes: []string{"a", "zzz"}},
		{ID: "h", Nodes: []string{"a"}, Controller: "nobody"},
	} {
		_, err := store.ApplyMachines(context.Background(), hostSpec(host), true)
		if err == nil || !strings.Contains(err.Error(), "unknown node") {
			t.Fatalf("host %+v: %v", host, err)
		}
	}
}

func TestDeviceSettingPrecedenceIsManualThenBenchThenPolicy(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	launch := startShared(t, store, "bench", "a", 10)
	lease := launch.Lease.ID
	if got := polled(t, store, "GPU-S"); got.Generation != 0 || got.Owner != nil || !got.Settings.IsDefault() {
		t.Fatalf("no setting yet: %+v", got)
	}
	result, err := store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerPolicy, Ref: lease}, DeviceSettings{PowerLimitW: intp(250)}, "policy")
	if err != nil || result.Generation != 1 || result.Effective.Owner.Kind != OwnerPolicy {
		t.Fatalf("policy: %+v %v", result, err)
	}
	if result, err = store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerBench, Ref: lease}, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, "bench"); err != nil || result.Generation != 2 || result.Effective.Owner.Kind != OwnerBench {
		t.Fatalf("bench overrides policy: %+v %v", result, err)
	}
	if result, err = store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerManual, Ref: "ignored"}, DeviceSettings{PowerLimitW: intp(200)}, "alice"); err != nil || result.Effective.Owner != (DeviceOwner{Kind: OwnerManual, Ref: "alice"}) {
		t.Fatalf("manual overrides bench: %+v %v", result, err)
	}
	got := polled(t, store, "GPU-S")
	if got.Generation != 3 || got.Owner.Kind != OwnerManual || *got.Settings.PowerLimitW != 200 || got.Kind != "gpu" {
		t.Fatalf("controller sees: %+v", got)
	}
	// A policy change under a manual setting does not change what is in effect.
	if result, err = store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerPolicy, Ref: lease}, DeviceSettings{PowerLimitW: intp(260)}, "policy"); err != nil || result.Generation != 3 {
		t.Fatalf("policy under manual: %+v %v", result, err)
	}
	if result, err = store.ClearDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerManual}, "alice"); err != nil || !result.Cleared || result.Generation != 4 || result.Effective.Owner.Kind != OwnerBench {
		t.Fatalf("cleared manual, bench again: %+v %v", result, err)
	}
	if result, err = store.ClearDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerManual}, "alice"); err != nil || result.Cleared {
		t.Fatalf("clearing nothing: %+v %v", result, err)
	}
	hosts, err := store.ListHardware(ctx, "h")
	if err != nil || len(hosts) != 1 || hosts[0].Controller == nil || hosts[0].ControllerLastSeenAt == nil {
		t.Fatalf("hardware: %+v %v", hosts, err)
	}
	gpu := hosts[0].Devices[0]
	if gpu.DeviceID != "GPU-S" || len(gpu.Settings) != 2 || gpu.Settings[1].Owner.Kind != OwnerPolicy || gpu.Capabilities == nil || gpu.MaxSlowdown != 0.05 {
		t.Fatalf("GPU: %+v", gpu)
	}
	if eventCount(t, store, "device_setting_requested") != 4 || eventCount(t, store, "device_setting_cleared") != 1 {
		t.Fatal("setting events")
	}
}

func TestReleasingALeaseClearsItsSettings(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	if _, _, err := store.SubmitExecution(ctx, sharedSpec("bench", "a", 10)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveNext(ctx, "exec-a")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	owner := DeviceOwner{Kind: OwnerBench, Ref: reservation.Lease.ID}
	if _, err = store.SetDeviceSetting(ctx, "h", "GPU-S", owner, DeviceSettings{GraphicsClockMaxMHz: intp(1800)}, "bench"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SetDeviceSetting(ctx, "h", "cpu", owner, DeviceSettings{MaxFreqMHz: intp(4000)}, "bench"); err != nil {
		t.Fatal(err)
	}
	pollShared(t, store, false, false)
	if err = store.ReleaseReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch, "bench done"); err != nil {
		t.Fatal(err)
	}
	for _, device := range []string{"GPU-S", "cpu"} {
		if got := polled(t, store, device); got.Owner != nil || got.Generation != 2 {
			t.Fatalf("%s outlived its lease: %+v", device, got)
		}
	}
	if eventCount(t, store, "device_setting_cleared") != 2 {
		t.Fatal("clearing was not recorded")
	}
	// A setting cannot be made for a released lease.
	if _, err = store.SetDeviceSetting(ctx, "h", "cpu", owner, DeviceSettings{MaxFreqMHz: intp(4000)}, "bench"); err == nil || !strings.Contains(err.Error(), "is released") {
		t.Fatalf("setting of a released lease: %v", err)
	}
	// The ordinary release: the attempt exits and quiesces.
	launch := startShared(t, store, "bench-run", "a", 10)
	if _, err = store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerPolicy, Ref: launch.Lease.ID}, DeviceSettings{PowerLimitW: intp(250)}, "policy"); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	pollShared(t, store, false, false)
	if err = store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	var left int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM device_settings`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("settings outlived the quiesced lease: %d %v", left, err)
	}
}

func TestAPollClearsTheSettingsOfAnyReleasedLease(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	launch := startShared(t, store, "bench", "a", 10)
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerBench, Ref: launch.Lease.ID}, DeviceSettings{PowerLimitW: intp(250)}, "bench"); err != nil {
		t.Fatal(err)
	}
	// released by a path that does not clear (none should exist)
	if _, err := store.testExec(`UPDATE leases SET state='released',released_at=$1 WHERE id=$2`, now(), launch.Lease.ID); err != nil {
		t.Fatal(err)
	}
	if got := polled(t, store, "GPU-S"); got.Owner != nil {
		t.Fatalf("setting of a released lease polled: %+v", got)
	}
}

func TestBenchAndPolicySettingsNeedALeaseOfTheDevice(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	launch := startShared(t, store, "bench", "a", 10)
	other := ExecutionSpec{ClientRequestID: "cpu-only", Scope: ScopePath{Project: "p", Queue: "q", Task: "t"}, Argv: []string{"run"}, CWD: "/", ExecutorSelector: json.RawMessage(`{"labels":{"site":"b"}}`)}
	if _, _, err := store.SubmitExecution(ctx, other); err != nil {
		t.Fatal(err)
	}
	cpuLease, err := store.ReserveNext(ctx, "exec-b")
	if err != nil || cpuLease == nil {
		t.Fatalf("reserve on b: %+v %v", cpuLease, err)
	}
	set := func(owner DeviceOwner, device string) error {
		_, err := store.SetDeviceSetting(ctx, "h", device, owner, DeviceSettings{MaxFreqMHz: intp(4000)}, "bench")
		if device != "cpu" {
			_, err = store.SetDeviceSetting(ctx, "h", device, owner, DeviceSettings{PowerLimitW: intp(250)}, "bench")
		}
		return err
	}
	if err = set(DeviceOwner{Kind: OwnerBench}, "cpu"); err == nil || !strings.Contains(err.Error(), "names the lease") {
		t.Fatalf("no lease: %v", err)
	}
	if err = set(DeviceOwner{Kind: OwnerBench, Ref: "lease-x"}, "cpu"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown lease: %v", err)
	}
	if err = set(DeviceOwner{Kind: OwnerBench, Ref: cpuLease.Lease.ID}, "GPU-S"); err == nil || !strings.Contains(err.Error(), "does not hold GPU-S") {
		t.Fatalf("lease without the GPU: %v", err)
	}
	if err = set(DeviceOwner{Kind: OwnerBench, Ref: cpuLease.Lease.ID}, "cpu"); err != nil {
		t.Fatalf("a lease on a node of the host may set its cpu: %v", err)
	}
	if err = set(DeviceOwner{Kind: OwnerBench, Ref: launch.Lease.ID}, "cpu"); !errors.Is(err, ErrDeviceSettingConflict) {
		t.Fatalf("another lease's bench setting: %v", err)
	}
	if _, err = store.ClearDeviceSetting(ctx, "h", "cpu", DeviceOwner{Kind: OwnerBench, Ref: launch.Lease.ID}, "bench"); !errors.Is(err, ErrDeviceSettingConflict) {
		t.Fatalf("clearing another lease's setting: %v", err)
	}
	if err = set(DeviceOwner{Kind: "anyone", Ref: launch.Lease.ID}, "cpu"); err == nil || !strings.Contains(err.Error(), "owner kind") {
		t.Fatalf("unknown owner: %v", err)
	}
	// Nothing is tuned where no slowdown is allowed; a bench run may still measure it.
	untuned := testHost
	untuned.Devices = []machine.Device{{ID: "GPU-S", MaxSlowdown: new(0.0)}}
	if _, err = store.ApplyMachines(ctx, hostSpec(untuned), false); err != nil {
		t.Fatal(err)
	}
	if err = set(DeviceOwner{Kind: OwnerPolicy, Ref: launch.Lease.ID}, "GPU-S"); err == nil || !strings.Contains(err.Error(), "max_slowdown 0") {
		t.Fatalf("policy on an untuned device: %v", err)
	}
	if err = set(DeviceOwner{Kind: OwnerBench, Ref: launch.Lease.ID}, "GPU-S"); err != nil {
		t.Fatalf("bench on an untuned device: %v", err)
	}
}

func TestSettingsAreCheckedAgainstTheReportedCapabilities(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	ctx := context.Background()
	if err := store.RegisterDeviceController(ctx, Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMachines(ctx, hostSpec(testHost), false); err != nil {
		t.Fatal(err)
	}
	manual := DeviceOwner{Kind: OwnerManual}
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", manual, DeviceSettings{PowerLimitW: intp(250)}, "alice"); err == nil || !strings.Contains(err.Error(), "has not reported") {
		t.Fatalf("before any capabilities: %v", err)
	}
	reportCapabilities(t, store)
	for _, c := range []struct {
		device   string
		settings DeviceSettings
		want     string
	}{
		{"GPU-S", DeviceSettings{PowerLimitW: intp(400)}, "power_limit_w 400W is outside the device's range 100-350W"},
		{"GPU-S", DeviceSettings{GraphicsClockMaxMHz: intp(3000)}, "outside the device's range 210-2130MHz"},
		{"GPU-S", DeviceSettings{MemoryClockMHz: intp(100)}, "outside the device's range 405-9501MHz"},
		{"GPU-S", DeviceSettings{PowerLimitW: intp(0)}, "must be positive"},
		{"GPU-S", DeviceSettings{MaxFreqMHz: intp(3000)}, "max_freq_mhz is not a setting of a gpu"},
		{"cpu", DeviceSettings{MaxPerfPct: intp(80)}, "does not support max_perf_pct (it supports: max_freq_mhz)"},
		{"cpu", DeviceSettings{MaxFreqMHz: intp(100)}, "outside the device's range 600-5700MHz"},
		{"cpu", DeviceSettings{}, "sets nothing"},
		{"GPU-X", DeviceSettings{PowerLimitW: intp(250)}, "host h has no device GPU-X"},
	} {
		if _, err := store.SetDeviceSetting(ctx, "h", c.device, manual, c.settings, "alice"); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s %s: %v, want %q", c.device, c.settings, err, c.want)
		}
	}
	if _, err := store.SetDeviceSetting(ctx, "nowhere", "cpu", manual, DeviceSettings{MaxFreqMHz: intp(3000)}, "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown host: %v", err)
	}
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", manual, DeviceSettings{GraphicsClockMaxMHz: intp(1800), MemoryClockMHz: intp(9501), PowerLimitW: intp(250)}, "alice"); err != nil {
		t.Fatal(err)
	}
}

func TestRedeclaringAHostDropsSettingsOfDevicesNoLongerControllable(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	manual := DeviceOwner{Kind: OwnerManual}
	if _, err := store.SetDeviceSetting(ctx, "h", "GPU-S", manual, DeviceSettings{PowerLimitW: intp(250)}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetDeviceSetting(ctx, "h", "cpu", manual, DeviceSettings{MaxFreqMHz: intp(4000)}, "alice"); err != nil {
		t.Fatal(err)
	}
	observeGPU := testHost
	observeGPU.Devices = []machine.Device{{ID: "GPU-S", Control: new(false)}}
	result, err := store.ApplyMachines(ctx, hostSpec(observeGPU), true)
	if err != nil || len(result.ClearedSettings) != 1 || result.ClearedSettings[0].DeviceID != "GPU-S" {
		t.Fatalf("dry run: %+v %v", result, err)
	}
	if got := polled(t, store, "GPU-S"); got.Owner == nil {
		t.Fatal("a dry run cleared a setting")
	}
	if _, err = store.ApplyMachines(ctx, hostSpec(observeGPU), false); err != nil {
		t.Fatal(err)
	}
	devices, err := store.PollDeviceSettings(ctx, "ctl")
	if err != nil || len(devices) != 1 || devices[0].DeviceID != "cpu" || devices[0].Owner == nil {
		t.Fatalf("only the cpu is controlled, and keeps its setting: %+v %v", devices, err)
	}
	if _, err = store.SetDeviceSetting(ctx, "h", "GPU-S", manual, DeviceSettings{PowerLimitW: intp(250)}, "alice"); err == nil || !strings.Contains(err.Error(), "control = false") {
		t.Fatalf("setting an uncontrolled device: %v", err)
	}
	// Without a controller nothing is controllable; without the host its devices' reports go too.
	noController := testHost
	noController.Controller = ""
	if result, err = store.ApplyMachines(ctx, hostSpec(noController), false); err != nil || len(result.ClearedSettings) != 1 || result.ClearedSettings[0].DeviceID != "cpu" {
		t.Fatalf("no controller: %+v %v", result, err)
	}
	if _, err = store.ApplyMachines(ctx, machine.Spec{Nodes: hostSpec(testHost).Nodes}, false); err != nil {
		t.Fatal(err)
	}
	var states int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM device_state`).Scan(&states); err != nil || states != 0 {
		t.Fatalf("reports of undeclared devices kept: %d %v", states, err)
	}
}

func TestControllerReportsAreRecordedAsFacts(t *testing.T) {
	store := openCoordinationStore(t)
	setupHost(t, store)
	ctx := context.Background()
	result, err := store.SetDeviceSetting(ctx, "h", "GPU-S", DeviceOwner{Kind: OwnerManual}, DeviceSettings{PowerLimitW: intp(250)}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	fail := DeviceReport{HostID: "h", DeviceID: "GPU-S", Error: "nvidia-smi -pl 250: exit status 4"}
	for range 2 { // a retry failing the same way is one event
		if _, err = store.ReportDevices(ctx, "ctl", []DeviceReport{fail}); err != nil {
			t.Fatal(err)
		}
	}
	hosts, _ := store.ListHardware(ctx, "")
	if gpu := hosts[0].Devices[0]; gpu.LastError == nil || gpu.Applied != nil || eventCount(t, store, "device_setting_failed") != 1 {
		t.Fatalf("failure: %+v", gpu)
	}
	applied := DeviceReport{HostID: "h", DeviceID: "GPU-S", Applied: &DeviceSettings{PowerLimitW: intp(250)}, AppliedGeneration: &result.Generation}
	skipped, err := store.ReportDevices(ctx, "ctl", []DeviceReport{applied, {HostID: "h", DeviceID: "GPU-elsewhere", Applied: &DeviceSettings{}}})
	if err != nil || len(skipped) != 1 || skipped[0] != "h/GPU-elsewhere" {
		t.Fatalf("report: %v %v", skipped, err)
	}
	hosts, _ = store.ListHardware(ctx, "")
	gpu := hosts[0].Devices[0]
	if gpu.LastError != nil || gpu.AppliedGeneration == nil || *gpu.AppliedGeneration != result.Generation || !gpu.Applied.Equal(DeviceSettings{PowerLimitW: intp(250)}) {
		t.Fatalf("applied: %+v", gpu)
	}
	if eventCount(t, store, "device_setting_applied") != 1 {
		t.Fatal("applied event")
	}
	// Another node may not report the host's devices.
	if skipped, err = store.ReportDevices(ctx, "a", []DeviceReport{applied}); err != nil || len(skipped) != 1 {
		t.Fatalf("report of another node: %v %v", skipped, err)
	}
	if devices, err := store.PollDeviceSettings(ctx, "a"); err != nil || len(devices) != 0 {
		t.Fatalf("another node controls nothing: %+v %v", devices, err)
	}
}

func TestRegisteringAControllerKeepsAKnownNode(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	if err := store.UpsertNode(ctx, Node{ID: "win", Name: "Windows", OS: "windows", Architecture: "amd64", Attributes: json.RawMessage(`{"interconnect_addr":"10.0.0.1"}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterDeviceController(ctx, Node{ID: "win", Name: "win", OS: "windows", Architecture: "amd64"}, "v1"); err != nil {
		t.Fatal(err)
	}
	var name, attrs string
	if err := store.db.QueryRow(`SELECT name,attributes_json FROM nodes WHERE id='win'`).Scan(&name, &attrs); err != nil || name != "Windows" || !strings.Contains(attrs, "interconnect_addr") {
		t.Fatalf("node clobbered: %s %s %v", name, attrs, err)
	}
}
