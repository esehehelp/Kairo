package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"kairo/internal/store"
)

// benchAttempt declares host "h" over node "work" (with its CPU, bench
// programs and an executor), creates a CPU bench and launches it; it returns
// the bench attempt's token, the controller's token and the run.
func benchAttempt(t *testing.T, d *testDaemon) (string, string, store.BenchRun) {
	t.Helper()
	ctx := context.Background()
	ctl := declareHost(t, d)
	if err := d.store.UpsertNode(ctx, store.Node{ID: "work", Name: "work", OS: "linux", Architecture: "amd64", Attributes: json.RawMessage(`{"bench":{"kairo":"/usr/local/bin/kairo"}}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertExecutor(ctx, store.Executor{ID: "work-exec", NodeID: "work", Kind: "local", Attributes: json.RawMessage(`{}`), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertProvider(ctx, "work-host", "work", "host", nil); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	resources := []store.ResourceInstance{
		{ID: "work-cpu", NodeID: "work", ProviderID: "work-host", Kind: "cpu", StableIdentity: "logical-cpu", Binding: json.RawMessage(`{}`)},
		{ID: "work-ram", NodeID: "work", ProviderID: "work-host", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`)},
	}
	cpu, ram := int64(8000), int64(64<<30)
	observations := []store.Observation{
		{ResourceID: "work-cpu", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &cpu, FreeBytes: &cpu},
		{ResourceID: "work-ram", ObservedAt: stamp.Format(time.RFC3339Nano), ValidUntil: stamp.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &ram, FreeBytes: &ram},
	}
	if err := d.store.ApplyObservationBatch(ctx, "work-host", resources, observations, nil); err != nil {
		t.Fatal(err)
	}
	maxFreq, minFreq := 5000, 800
	report := store.DeviceReport{HostID: "h", DeviceID: "cpu", Applied: &store.DeviceSettings{}, Capabilities: &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMinMHz: &minFreq, MaxFreqMaxMHz: &maxFreq}}
	if status, body := d.call(t, "POST", "/api/agent/device-report", ctl, map[string]any{"node_id": "ctl", "reports": []store.DeviceReport{report}}); status != http.StatusOK {
		t.Fatalf("report: %d %s", status, body)
	}
	status, body := d.call(t, "POST", "/api/hardware/h/cpu/bench", d.admin, map[string]any{"kinds": []string{"cpu-compute"}})
	var plan store.BenchPlan
	if status != http.StatusOK || json.Unmarshal(body, &plan) != nil || plan.Run.ExecutionID == "" {
		t.Fatalf("create bench: %d %s", status, body)
	}
	reservation, err := d.store.ReserveNext(ctx, "work-exec")
	if err != nil || reservation == nil {
		t.Fatalf("reserve: %+v %v", reservation, err)
	}
	if err = d.store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	launch, err := d.store.AuthorizeLaunch(ctx, reservation)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.store.ActivateLaunch(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, 100, "launcher:100"); err != nil {
		t.Fatal(err)
	}
	return launch.WorkerToken, ctl, plan.Run
}

func TestBenchAttemptReachesOnlyItsOwnRun(t *testing.T) {
	d := newTestDaemon(t, false)
	token, ctl, run := benchAttempt(t, d)
	status, body := d.call(t, "GET", "/api/worker/bench?run="+run.ID, token, nil)
	var bench store.BenchStatus
	if status != http.StatusOK || json.Unmarshal(body, &bench) != nil || bench.Run.ID != run.ID || bench.Programs.Kairo != "/usr/local/bin/kairo" || bench.Capabilities == nil {
		t.Fatalf("status: %d %s", status, body)
	}
	if status, body = d.call(t, "GET", "/api/worker/bench?run=bnc_other", token, nil); status != http.StatusForbidden {
		t.Fatalf("another run: %d %s", status, body)
	}
	// An attempt that carries out no bench reaches none.
	other := d.workerToken(t)
	if status, body = d.call(t, "GET", "/api/worker/bench", other, nil); status != http.StatusNotFound {
		t.Fatalf("a non-bench attempt: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/worker/bench/settings", other, map[string]any{"settings": map[string]int{"max_freq_mhz": 4000}}); status != http.StatusNotFound {
		t.Fatalf("a non-bench attempt's setting: %d %s", status, body)
	}
	// The bench sets its device under its own lease.
	status, body = d.call(t, "POST", "/api/worker/bench/settings?run="+run.ID, token, map[string]any{"settings": map[string]int{"max_freq_mhz": 4000}})
	var result store.DeviceSettingResult
	if status != http.StatusOK || json.Unmarshal(body, &result) != nil || result.Effective == nil || result.Effective.Owner.Kind != store.OwnerBench || result.Effective.Owner.Ref != bench.LeaseID {
		t.Fatalf("bench setting: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/worker/bench/settings", token, map[string]any{"settings": map[string]int{"max_freq_mhz": 9000}}); status != http.StatusBadRequest {
		t.Fatalf("out of range: %d %s", status, body)
	}
	// No CPU package power yet; then the controller's.
	from, to := time.Now().UTC().Add(-time.Minute), time.Now().UTC()
	energyPath := "/api/worker/bench/cpu-energy?from=" + from.Format(time.RFC3339Nano) + "&to=" + to.Format(time.RFC3339Nano)
	if status, body = d.call(t, "GET", energyPath, token, nil); status != http.StatusNotFound {
		t.Fatalf("no samples: %d %s", status, body)
	}
	var samples []store.ControllerSample
	for i := range 7 {
		at, watts := from.Add(time.Duration(i)*10*time.Second), 65.0
		samples = append(samples, store.ControllerSample{DeviceID: "cpu", ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(20 * time.Second).Format(time.RFC3339Nano), PowerW: &watts})
	}
	if status, body = d.call(t, "POST", "/api/agent/device-samples", ctl, map[string]any{"node_id": "ctl", "samples": samples}); status != http.StatusOK || !strings.Contains(string(body), `"skipped":[]`) {
		t.Fatalf("controller samples: %d %s", status, body)
	}
	var energy store.CPUEnergy
	if status, body = d.call(t, "GET", energyPath, token, nil); status != http.StatusOK || json.Unmarshal(body, &energy) != nil || energy.MeanPowerW() != 65 {
		t.Fatalf("energy: %d %s", status, body)
	}
	if status, body = d.call(t, "GET", "/api/worker/bench/cpu-energy?from=x&to=y", token, nil); status != http.StatusBadRequest {
		t.Fatalf("bad window: %d %s", status, body)
	}
	// Start, points, finish.
	if status, body = d.call(t, "POST", "/api/worker/bench/points", token, map[string]any{"points": []store.BenchPoint{{Seq: 1, Kind: "cpu-compute", Phase: store.PhaseReference}}}); status != http.StatusConflict {
		t.Fatalf("points before start: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/worker/bench/start", token, store.BenchStart{DriverBuild: "test"}); status != http.StatusOK {
		t.Fatalf("start: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/worker/bench/points", token, map[string]any{"points": []store.BenchPoint{{Seq: 1, Kind: "cpu-compute", Phase: store.PhaseReference, Valid: true}}}); status != http.StatusOK {
		t.Fatalf("points: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/worker/bench/finish", token, store.BenchFinish{Valid: true, References: map[string]store.BenchReference{"cpu-compute": {Valid: true}}}); status != http.StatusOK {
		t.Fatalf("finish: %d %s", status, body)
	}
	status, body = d.call(t, "GET", "/api/hardware/h/profile?device=cpu", d.admin, nil)
	var profile struct {
		Runs []store.BenchRun `json:"runs"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &profile) != nil || len(profile.Runs) != 1 || profile.Runs[0].State != "finished" || len(profile.Runs[0].Points) != 1 {
		t.Fatalf("profile: %d %s", status, body)
	}
	// The finish cleared the bench setting.
	if _, body = d.call(t, "GET", "/api/hardware?host=h", d.admin, nil); strings.Contains(string(body), `"kind":"bench"`) {
		t.Fatalf("bench setting left: %s", body)
	}
}

func TestBenchCreationIsAdminAndObserveOnlyPlansOnly(t *testing.T) {
	d := newTestDaemon(t, true)
	declareHost(t, d)
	read := d.token(t, "dashboard", store.RoleRead, "")
	if status, _ := d.call(t, "POST", "/api/hardware/h/cpu/bench", read, map[string]any{}); status != http.StatusForbidden {
		t.Fatalf("read token: %d", status)
	}
	if status, body := d.call(t, "POST", "/api/hardware/h/cpu/bench", d.admin, map[string]any{}); status != http.StatusLocked {
		t.Fatalf("observe only: %d %s", status, body)
	}
	if status, body := d.call(t, "GET", "/api/hardware/nowhere/profile", read, nil); status != http.StatusNotFound {
		t.Fatalf("unknown host: %d %s", status, body)
	}
}
