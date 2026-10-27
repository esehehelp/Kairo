package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func watts(w float64) *float64 { return &w }

// deviceSample has providerID report GPU uuid (by its own resource id,
// gpu-<uuid>) at at, valid for ttl, drawing power (nil: not reported).
func deviceSample(t *testing.T, store *Store, providerID, nodeID, uuid string, at time.Time, ttl time.Duration, power *float64) {
	t.Helper()
	id := "gpu-" + uuid
	total, free := int64(24<<30), int64(23<<30)
	resources := []ResourceInstance{{ID: id, NodeID: nodeID, ProviderID: providerID, Kind: "gpu", StableIdentity: uuid, Binding: json.RawMessage(`{}`)}}
	observations := []Observation{{ResourceID: id, ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(ttl).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free,
		Device: &DeviceSample{PowerW: power, DeviceProperties: DeviceProperties{MaxPowerLimitW: watts(350)}}}}
	if err := store.ApplyObservationBatch(context.Background(), providerID, resources, observations, nil); err != nil {
		t.Fatal(err)
	}
}

func attemptEnergy(t *testing.T, store *Store, attemptID string) (AttemptEnergy, bool) {
	t.Helper()
	var e AttemptEnergy
	err := store.db.QueryRow(`SELECT gpu_joules,gpu_seconds_covered,partial,updated_at FROM attempt_energy WHERE attempt_id=?`, attemptID).Scan(&e.GPUJoules, &e.GPUSecondsCovered, &e.Partial, &e.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptEnergy{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return e, true
}

// setAttemptTimes sets when an attempt started and, unless zero, exited and
// its lease was released (the samples of these tests are back-dated).
func setAttemptTimes(t *testing.T, store *Store, launch *Launch, started, ended time.Time) {
	t.Helper()
	if _, err := store.db.Exec(`UPDATE attempts SET started_at=? WHERE id=?`, started.Format(time.RFC3339Nano), launch.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	if !ended.IsZero() {
		if _, err := store.db.Exec(`UPDATE attempts SET exited_at=? WHERE id=?`, ended.Format(time.RFC3339Nano), launch.Attempt.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.Exec(`UPDATE leases SET released_at=? WHERE id=? AND released_at IS NOT NULL`, ended.Add(time.Second).Format(time.RFC3339Nano), launch.Lease.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func wantEnergy(t *testing.T, store *Store, attemptID string, joules, seconds float64, partial bool) {
	t.Helper()
	e, ok := attemptEnergy(t, store, attemptID)
	if !ok || math.Abs(e.GPUJoules-joules) > 1e-6 || math.Abs(e.GPUSecondsCovered-seconds) > 1e-6 || e.Partial != partial {
		t.Fatalf("energy %+v (recorded %v), want %v J over %v s, partial %v", e, ok, joules, seconds, partial)
	}
}

// The trapezoid of consecutive samples, cut where the attempt starts and
// ends inside an interval: power is taken as linear between two samples.
func TestAttemptEnergyIntegratesItsGPUWhileItRuns(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	if _, _, err := store.SubmitExecution(context.Background(), schedulerExecutionSpec("train")); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	at := func(seconds float64) time.Time { return base.Add(time.Duration(seconds * float64(time.Second))) }
	setAttemptTimes(t, store, launch, at(2.5), time.Time{})
	sample := func(seconds, power float64) {
		deviceSample(t, store, "gpu-provider", "node", "GPU-0", at(seconds), 15*time.Second, watts(power))
	}
	sample(0, 100)
	if _, ok := attemptEnergy(t, store, launch.Attempt.ID); ok {
		t.Fatal("energy before any interval")
	}
	sample(5, 200)  // [2.5, 5]: 150 W to 200 W over 2.5 s = 437.5 J
	sample(10, 200) // 1000 J
	wantEnergy(t, store, launch.Attempt.ID, 1437.5, 7.5, false)
	finishLaunch(t, store, launch, 0)
	setAttemptTimes(t, store, launch, at(2.5), at(12))
	sample(15, 100) // [10, 12]: 200 W to 160 W = 360 J
	sample(20, 100) // after the attempt
	wantEnergy(t, store, launch.Attempt.ID, 1797.5, 9.5, false)
	// The resource's latest reading is the last sample, with its properties.
	usage, err := store.ListResourceUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d := usage["gpu-0"].Device; d == nil || *d.PowerW != 100 || d.MaxPowerLimitW == nil || *d.MaxPowerLimitW != 350 || d.ObservedAt != sampleStamp(at(20)) {
		t.Fatalf("latest device reading %+v", d)
	}
}

// An interval the samples do not vouch for is not integrated and makes the
// attempt partial: a gap longer than a sample's validity, a sample without
// power, a run that began before the device's first sample.
func TestAttemptEnergyIsPartialAcrossWhatIsNotSampled(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	if _, _, err := store.SubmitExecution(context.Background(), schedulerExecutionSpec("train")); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	at := func(seconds float64) time.Time { return base.Add(time.Duration(seconds * float64(time.Second))) }
	setAttemptTimes(t, store, launch, at(-10), time.Time{})
	sample := func(seconds float64, power *float64) {
		deviceSample(t, store, "gpu-provider", "node", "GPU-0", at(seconds), 15*time.Second, power)
	}
	sample(0, watts(100)) // the run before it is not known
	wantEnergy(t, store, launch.Attempt.ID, 0, 0, true)
	sample(5, watts(100))
	wantEnergy(t, store, launch.Attempt.ID, 500, 5, true)
	sample(25, watts(100)) // 5 s + 15 s validity < 25 s: a gap
	sample(30, nil)        // no power
	sample(35, watts(100)) // nor from the sample without it
	sample(40, watts(300))
	wantEnergy(t, store, launch.Attempt.ID, 500+1000, 10, true)
	// A lease that went stale ends what is known of the run.
	if _, err := store.db.Exec(`UPDATE leases SET state='stale',stale_at=? WHERE id=?`, at(42).Format(time.RFC3339Nano), launch.Lease.ID); err != nil {
		t.Fatal(err)
	}
	sample(45, watts(300)) // [40, 42] at 300 W
	sample(50, watts(300))
	wantEnergy(t, store, launch.Attempt.ID, 2100, 12, true)
}

// A shared GPU is one device observed by the provider of every node it is
// declared for: their samples are one series, counted once whichever
// provider reports, in whatever order.
func TestSharedGPUEnergyIsCountedOnce(t *testing.T) {
	store := openCoordinationStore(t)
	setupSharedGPU(t, store)
	launch := startShared(t, store, "x", "a", 0)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	at := func(seconds float64) time.Time { return base.Add(time.Duration(seconds * float64(time.Second))) }
	setAttemptTimes(t, store, launch, at(0), time.Time{})
	for _, s := range []struct {
		provider, node string
		seconds        float64
	}{
		{"a-gpu", "a", 0}, {"b-gpu", "b", 2.5}, {"a-gpu", "a", 5}, {"b-gpu", "b", 5}, {"b-gpu", "b", 4}, {"b-gpu", "b", 7.5},
	} {
		deviceSample(t, store, s.provider, s.node, "GPU-S", at(s.seconds), 15*time.Second, watts(100))
	}
	wantEnergy(t, store, launch.Attempt.ID, 750, 7.5, false)
	var samples, fromB int
	if err := store.db.QueryRow(`SELECT COUNT(*),SUM(resource_id='gpu-GPU-S@b') FROM device_samples WHERE device='GPU-S' AND observed_at>=?`, sampleStamp(base)).Scan(&samples, &fromB); err != nil {
		t.Fatal(err)
	}
	if samples != 6 || fromB != 4 {
		t.Fatalf("%d samples, %d of them b's: every report is kept, under its reporter's row", samples, fromB)
	}
	usage, err := store.ListResourceUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, b := usage["gpu-GPU-S"].Device, usage["gpu-GPU-S@b"].Device
	if a == nil || b == nil || a.ObservedAt != sampleStamp(at(7.5)) || b.ObservedAt != a.ObservedAt || *a.PowerW != *b.PowerW || *b.MaxPowerLimitW != 350 {
		t.Fatalf("both rows show the device's latest reading: %+v %+v", a, b)
	}
}

func TestPruneDeviceSamplesKeepsEachDevicesLatest(t *testing.T) {
	store := openCoordinationStore(t)
	setupTwoGPUNodes(t, store)
	declareTestGPU(t, store, "GPU-A", "a")
	declareTestGPU(t, store, "GPU-B", "b")
	old := time.Now().UTC().Add(-100 * time.Hour)
	for i := range 2500 {
		deviceSample(t, store, "a-gpu", "a", "GPU-A", old.Add(time.Duration(i)*time.Second), 15*time.Second, watts(50))
	}
	for i := range 3 {
		deviceSample(t, store, "b-gpu", "b", "GPU-B", old.Add(time.Duration(i)*time.Second), 15*time.Second, watts(50))
	}
	fresh := time.Now().UTC()
	deviceSample(t, store, "a-gpu", "a", "GPU-A", fresh.Add(-time.Minute), 15*time.Second, watts(50))
	deviceSample(t, store, "a-gpu", "a", "GPU-A", fresh, 15*time.Second, watts(50))
	removed, err := store.PruneDeviceSamples(context.Background(), DeviceSampleRetention)
	if err != nil {
		t.Fatal(err)
	}
	count := func(device string) (n int, latest string) {
		if err := store.db.QueryRow(`SELECT COUNT(*),MAX(observed_at) FROM device_samples WHERE device=?`, device).Scan(&n, &latest); err != nil {
			t.Fatal(err)
		}
		return n, latest
	}
	if n, _ := count("GPU-A"); n != 2 {
		t.Fatalf("GPU-A kept %d samples, want its 2 fresh ones", n)
	}
	if n, latest := count("GPU-B"); n != 1 || latest != sampleStamp(old.Add(2*time.Second)) {
		t.Fatalf("GPU-B kept %d samples (latest %s), want only its latest", n, latest)
	}
	if removed != 2500+2 {
		t.Fatalf("removed %d", removed)
	}
}

// Sample times are fixed-width, so they order as text even where RFC 3339
// with trimmed fractions would not ("...:05Z" sorts after "...:05.5Z").
func TestSampleStampsOrderAsText(t *testing.T) {
	base := time.Date(2026, 10, 27, 10, 0, 5, 0, time.UTC)
	whole, half := sampleStamp(base), sampleStamp(base.Add(500*time.Millisecond))
	if !(whole < half) || len(whole) != len(half) {
		t.Fatalf("%s !< %s", whole, half)
	}
	if parsed, err := time.Parse(time.RFC3339Nano, half); err != nil || !parsed.Equal(base.Add(500*time.Millisecond)) {
		t.Fatalf("parse %s: %v %v", half, parsed, err)
	}
}

// task show reads the energy of each attempt with the attempt.
func TestTaskDetailShowsAttemptEnergy(t *testing.T) {
	store := openCoordinationStore(t)
	setupSchedulerInventory(t, store)
	ctx := context.Background()
	if _, err := store.ApplyProject(ctx, projectSpec(t, orchestrationManifest())); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := launchNext(t, store)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	setAttemptTimes(t, store, launch, base, time.Time{})
	detail, err := store.GetTaskDetail(ctx, "orchestrated", "prepare")
	if err != nil {
		t.Fatal(err)
	}
	if e := detail.Executions[0].Attempts[0].Energy; e != nil {
		t.Fatalf("energy before any sample: %+v", e)
	}
	deviceSample(t, store, "gpu-provider", "node", "GPU-0", base, 15*time.Second, watts(200))
	deviceSample(t, store, "gpu-provider", "node", "GPU-0", base.Add(9*time.Second), 15*time.Second, watts(200))
	if detail, err = store.GetTaskDetail(ctx, "orchestrated", "prepare"); err != nil {
		t.Fatal(err)
	}
	if e := detail.Executions[0].Attempts[0].Energy; e == nil || e.GPUJoules != 1800 || e.GPUSecondsCovered != 9 || e.Partial {
		t.Fatalf("energy %+v", e)
	}
}
