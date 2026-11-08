package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"kairo/internal/datainput"
	"kairo/internal/envspec"
	"kairo/internal/processidentity"
	"kairo/internal/provider"
	"kairo/internal/store"
)

type Local struct {
	ID     string
	Store  Coordinator
	LogDir string
	APIURL string
	// APICA is handed to attempts as KAIRO_API_CA: the daemon's CA
	// certificate (base64 DER), empty when the daemon serves plain HTTP.
	APICA               string
	Interval            time.Duration
	Logger              *slog.Logger
	NodeID              string
	Kind                string
	Attributes          map[string]string
	ObserveOnly         bool
	ObservationInterval time.Duration
	// ProviderCallTimeout bounds each provider call made on the tick (zero:
	// defaultProviderCallTimeout). StallThreshold is how long a step of the tick
	// may run before it is reported (zero: defaultStallThreshold).
	ProviderCallTimeout time.Duration
	StallThreshold      time.Duration
	Providers           map[string]provider.Provider
	lastObservation     time.Time

	// Attempt logs (logspace.go): the free space the log directory's
	// filesystem must have for a launch (zero: DefaultMinLogFreeBytes), how
	// often it is checked while attempts run (zero: defaultLogCheckInterval),
	// and how it is measured (nil: the filesystem's own figure).
	MinLogFreeBytes  int64
	LogCheckInterval time.Duration
	FreeSpace        func(dir string) (int64, error)
	// FlushLogs, when set, ships an attempt's logs before its exit is
	// recorded (a node agent's log shipper), so that they are complete at
	// the daemon once the attempt is seen to have ended.
	FlushLogs      func(ctx context.Context, attemptID string) error
	lastLogCheck   time.Time
	freeSpaceStuck bool
	// logProblems are the problems already recorded, by attempt.
	logProblems map[string]map[string]bool

	mu sync.Mutex
	// running holds the attempts running here, launched or adopted: the pid
	// of each one's shim.
	running map[string]int

	// Prepared gang ranks waiting at the launch barrier, by lease ID.
	pendingGang map[string]pendingGangRank

	// Reservations given up whose lease could not be released yet, by lease
	// ID (cleanup.go): retried at every tick, never forgotten. When the store
	// was last asked for leases this instance does not hold, and whether that
	// failed the last time.
	pendingCleanups        map[string]pendingCleanup
	lastUnlaunchedSweep    time.Time
	unlaunchedSweepFailing bool
	// Process trees of launches whose activation failed that were not seen
	// empty after the kill, by attempt, until their cleanup takes them.
	leftovers map[string]processTree
	// RetryDelay is the first wait before a launch call whose answer did not
	// arrive is made again (zero: 1 s; it doubles).
	RetryDelay time.Duration

	// Terminations of node quarantines and gangs being carried out: when the
	// graceful stop was sent, by attempt.
	terminating map[string]time.Time
	// When an exited attempt whose lease is held for what it left running
	// was last reported, and how far ending what it left has gone, by
	// attempt (quiescence.go).
	leftReported    map[string]time.Time
	exitedLeftovers map[string]leftover

	// Stores (datastore.go): the ones this executor serves, when they were
	// last asked for, whether the daemon ever answered (storesKnown) and
	// whether the last ask failed (warned once), when they were last
	// reported and whether a report runs. storeFSOverride replaces how
	// roots are reached, in tests.
	stores          []servedStore
	storesAt        time.Time
	storesKnown     bool
	storesWarned    bool
	storeReportAt   time.Time
	storeReporting  bool
	storeFSOverride storeFS

	// Scratch (scratch.go): the scratch made for attempts being launched,
	// by attempt; when scratch was last settled and whether a round runs;
	// the problems already warned of, by attempt.
	scratch map[string]string
	// scratchSized is the size, measured once, of each scratch directory of
	// an attempt not running here (datastore.go), by attempt.
	scratchSized    map[string]int64
	scratchSettleAt time.Time
	scratchSettling bool
	scratchProblems map[string]string

	// Output directories (outputs.go): the one made for each attempt being
	// launched, by attempt, until its environment is built; the run of each
	// attempt launched with one, by attempt, while it runs here; and the
	// size, measured once, of each run's directory none of whose attempts
	// runs here, by run.
	outputs     map[string]string
	outputRuns  map[string]string
	outputSized map[string]int64
	// caches: each attempt being launched's cache directory (confine.go),
	// until its environment is built.
	caches map[string]string

	// Packing (pack.go), copies to a durable store (transfer.go; PackClient
	// fetches packs from other agents, their CA trusted), deleting copied
	// sources and reading durable copies back (retire.go): each a round.
	PackClient *http.Client
	packs      round
	copies     round
	fetches    round
	deletions  round
	evictions  round
	scrubs     round

	// Force stop (forcestop.go): process tree handles of the attempts this
	// instance launched, and the progress of force stops being carried out.
	trees              map[string]*treeHandle
	forceChecked       map[string]time.Time
	forceKilled        map[string]bool
	forceOrdersFailing bool

	// Attempt usage (usage.go): the running attempts this instance samples,
	// whether a round is running and when the last one started. usageSampler
	// replaces sampleTrees in tests.
	usage        map[string]*usageState
	usageRunning bool
	usageSampled time.Time
	usageSampler func(context.Context, []usageTarget) map[string]usageSample

	// The step the tick is in and when it started; stepStalled is set once
	// watchSteps has reported it (see beginStep).
	step        string
	stepStart   time.Time
	stepStalled bool
}

type pendingGangRank struct {
	reservation *store.Reservation
	prepared    []store.ResourceInstance
}

func NewLocal(id string, st Coordinator, logDir string, logger *slog.Logger) *Local {
	return &Local{
		ID: id, Store: st, LogDir: logDir, Interval: 250 * time.Millisecond,
		Logger: logger, running: make(map[string]int),
	}
}

func (e *Local) Run(ctx context.Context) error {
	if e.Logger == nil {
		e.Logger = slog.Default()
	}
	if err := os.MkdirAll(e.LogDir, 0o755); err != nil {
		return err
	}
	go e.watchSteps(ctx)
	ticker := time.NewTicker(e.Interval)
	defer ticker.Stop()
	for {
		if err := e.tick(ctx); err != nil && ctx.Err() == nil {
			e.Logger.Error("executor tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			// Deliberately do not kill child processes. A subsequent executor
			// instance quarantines their resources until reconciliation.
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Local) tick(ctx context.Context) error {
	if e.ObservationInterval == 0 {
		e.ObservationInterval = 5 * time.Second
	}
	if time.Since(e.lastObservation) >= e.ObservationInterval {
		for providerID, p := range e.Providers {
			e.beginStep("observe:" + providerID)
			snap, err := providerCall(ctx, e, providerID, p.Observe)
			if err != nil {
				e.endStep()
				e.Logger.Warn("provider observation failed", "provider_id", providerID, "error", err)
				continue
			}
			err = e.Store.ApplyObservationBatch(ctx, providerID, snap.Resources, snap.Observations, snap.Claims)
			e.endStep()
			if err != nil {
				return err
			}
		}
		e.lastObservation = time.Now()
	}
	if e.ObserveOnly {
		return nil
	}
	e.sampleUsage(ctx)
	if err := e.inStep("quiescence", func() error { return e.reconcileQuiescence(ctx) }); err != nil {
		return err
	}
	if err := e.inStep("quarantine", func() error { return e.carryOutQuarantineTerminations(ctx) }); err != nil {
		return err
	}
	e.beginStep("force-stops")
	e.carryOutForceStops(ctx)
	e.endStep()
	e.beginStep("log-space")
	e.watchLogSpace(ctx)
	e.endStep()
	if err := e.inStep("gang-retry", func() error { return e.retryPendingGangRanks(ctx) }); err != nil {
		return err
	}
	e.beginStep("cleanup-retry")
	e.retryCleanups(ctx)
	e.sweepUnlaunched(ctx)
	e.endStep()
	e.beginStep("stores")
	e.refreshStores(ctx)
	e.reportStores()
	e.settleScratch()
	e.makePacks()
	e.makeCopies()
	e.makeFetches()
	e.makeDeletions()
	e.makeEvictions()
	e.makeScrubs()
	e.endStep()
	for {
		reservation, err := e.reserveNext(ctx)
		if err != nil || reservation == nil {
			return err
		}
		again, err := e.prepareReservation(ctx, reservation)
		if err != nil || !again {
			return err
		}
	}
}

// reserveNext returns this executor's next reservation, or nil when it has
// none (after requesting any priority preemption that it needs).
func (e *Local) reserveNext(ctx context.Context) (*store.Reservation, error) {
	e.beginStep("reserve-next")
	defer e.endStep()
	reservation, err := e.Store.ReserveNext(ctx, e.ID)
	if err != nil || reservation != nil {
		return reservation, err
	}
	commands, err := e.Store.EnsurePriorityPreemption(ctx, e.ID)
	if err != nil {
		return nil, err
	}
	if len(commands) != 0 {
		e.Logger.Info("requested cooperative priority preemption", "commands", commands)
	}
	return nil, nil
}

// prepareReservation prepares a reservation's resources, marks its lease
// prepared and launches it once the launch barrier allows. It reports whether
// the tick goes on reserving; false ends the tick.
func (e *Local) prepareReservation(ctx context.Context, reservation *store.Reservation) (bool, error) {
	e.beginStep("prepare")
	defer e.endStep()
	prepared := make([]store.ResourceInstance, 0, len(reservation.Resources))
	prepareErr := error(nil)
	refreshed := map[string]bool{}
	for _, resource := range reservation.Resources {
		p := e.Providers[resource.ProviderID]
		if p == nil {
			prepareErr = fmt.Errorf("provider %s is unavailable", resource.ProviderID)
			break
		}
		if !refreshed[resource.ProviderID] {
			snap, err := providerCall(ctx, e, resource.ProviderID, p.Observe)
			if err != nil {
				prepareErr = err
				break
			}
			if err = e.Store.ApplyObservationBatch(ctx, resource.ProviderID, snap.Resources, snap.Observations, snap.Claims); err != nil {
				prepareErr = err
				break
			}
			refreshed[resource.ProviderID] = true
		}
		// Release must be safe even when Prepare returns after a partial
		// provider-side change, so include the resource before invoking it.
		prepared = append(prepared, resource)
		if err := providerAction(ctx, e, resource.ProviderID, func(ctx context.Context) error { return p.Prepare(ctx, resource) }); err != nil {
			prepareErr = err
			break
		}
	}
	if prepareErr == nil {
		prepareErr = e.Store.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch)
	}
	if prepareErr != nil {
		e.Logger.Warn("lease prepare failed", "lease_id", reservation.Lease.ID, "error", prepareErr)
		e.abandonReservation(reservation.Lease, prepared, prepareErr.Error(), "", nil)
		return false, nil
	}
	if err := e.Store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
		e.Logger.Warn("lease not marked prepared", "lease_id", reservation.Lease.ID, "error", err)
		e.abandonReservation(reservation.Lease, prepared, err.Error(), "", nil)
		return false, nil
	}
	launch, err := e.authorize(ctx, reservation)
	if errors.Is(err, store.ErrGangNotReady) {
		// Hold the prepared rank at the barrier until every rank is prepared.
		e.holdGangRank(reservation, prepared)
		return true, nil
	}
	if err != nil {
		e.Logger.Warn("launch not authorized", "lease_id", reservation.Lease.ID, "error", err)
		e.abandonReservation(reservation.Lease, prepared, err.Error(), "", nil)
		return false, nil
	}
	e.launchAuthorized(launch)
	return true, nil
}

// defaultStallThreshold is how long a step of the tick may run before
// watchSteps reports it.
const defaultStallThreshold = 30 * time.Second

// defaultProviderCallTimeout bounds one provider call made on the tick. It is
// longer than the two nvidia-smi runs of an NVIDIA observation (each has its
// own, shorter, timeout), so a hung run fails that observation naming the
// command instead of being cut short here.
const defaultProviderCallTimeout = 45 * time.Second

// beginStep records that the tick is in step now.
func (e *Local) beginStep(step string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.step, e.stepStart, e.stepStalled = step, time.Now(), false
}

// endStep ends the current step. A step that watchSteps reported as stalled is
// reported as resumed.
func (e *Local) endStep() {
	e.mu.Lock()
	step, age, stalled := e.step, time.Since(e.stepStart), e.stepStalled
	e.step, e.stepStalled = "", false
	e.mu.Unlock()
	if stalled {
		e.Logger.Info("executor step resumed", "executor_id", e.ID, "step", step, "age_seconds", age.Seconds())
	}
}

// inStep runs fn as the named step of the tick.
func (e *Local) inStep(step string, fn func() error) error {
	e.beginStep(step)
	defer e.endStep()
	return fn()
}

// watchSteps runs beside the tick and reports a step that has run for
// StallThreshold. The tick cannot report it itself: it is blocked inside the
// step.
func (e *Local) watchSteps(ctx context.Context) {
	threshold := e.StallThreshold
	if threshold == 0 {
		threshold = defaultStallThreshold
	}
	ticker := time.NewTicker(max(min(time.Second, threshold/4), time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		e.mu.Lock()
		step, age := e.step, time.Since(e.stepStart)
		stalled := step != "" && !e.stepStalled && age >= threshold
		if stalled {
			e.stepStalled = true
		}
		e.mu.Unlock()
		if stalled {
			e.Logger.Warn("executor step stalled", "executor_id", e.ID, "step", step, "age_seconds", age.Seconds())
		}
	}
}

// abandonedMu guards abandonedCalls: per provider, the call that was given up
// on (its wait timed out) and has not returned yet. It is process-wide because
// the executors of one daemon share their providers: while such a call is
// still running (some, such as GetDiskFreeSpaceEx, cannot be cancelled and may
// never return), no executor starts another on that provider. Calls that run
// normally are not tracked and may overlap, as the executors' ticks do.
var (
	abandonedMu    sync.Mutex
	abandonedCalls = map[string]*int{}
)

// providerCall runs one call of a provider (Observe, Prepare or Release) on its
// own goroutine and waits for it at most ProviderCallTimeout. A call that has
// not returned by then is abandoned: the tick treats it as failed and goes on,
// and further calls on that provider fail at once until it returns.
func providerCall[T any](ctx context.Context, e *Local, providerID string, call func(context.Context) (T, error)) (T, error) {
	var zero T
	timeout := e.ProviderCallTimeout
	if timeout == 0 {
		timeout = defaultProviderCallTimeout
	}
	abandonedMu.Lock()
	stuck := abandonedCalls[providerID] != nil
	abandonedMu.Unlock()
	if stuck {
		return zero, fmt.Errorf("provider %s: an earlier call timed out and has not returned", providerID)
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type result struct {
		value T
		err   error
	}
	token := new(int) // this call, if it is abandoned
	done := make(chan result, 1)
	go func() {
		value, err := call(callCtx)
		// Under the lock, so that the waiter either sees the result or has
		// already marked the call abandoned (and the mark is cleared here).
		abandonedMu.Lock()
		if abandonedCalls[providerID] == token {
			delete(abandonedCalls, providerID)
		}
		done <- result{value, err}
		abandonedMu.Unlock()
	}()
	select {
	case r := <-done:
		return r.value, r.err
	case <-callCtx.Done():
		abandonedMu.Lock()
		defer abandonedMu.Unlock()
		select {
		case r := <-done: // it returned just now
			return r.value, r.err
		default:
		}
		abandonedCalls[providerID] = token
		return zero, fmt.Errorf("provider %s: call did not return: %w", providerID, callCtx.Err())
	}
}

// providerAction runs a provider's Prepare or Release through providerCall.
func providerAction(ctx context.Context, e *Local, providerID string, action func(context.Context) error) error {
	_, err := providerCall(ctx, e, providerID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, action(ctx)
	})
	return err
}

func (e *Local) launchAuthorized(launch *store.Launch) {
	if err := e.start(launch); err != nil {
		e.Logger.Error("attempt launch failed", "attempt_id", launch.Attempt.ID, "error", err)
		e.abandonReservation(launch.Lease, launch.Resources, "launch failed before activation: "+err.Error(), launch.Attempt.ID, e.takeLeftover(launch.Attempt.ID))
	}
}

func (e *Local) holdGangRank(reservation *store.Reservation, prepared []store.ResourceInstance) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pendingGang == nil {
		e.pendingGang = map[string]pendingGangRank{}
	}
	e.pendingGang[reservation.Lease.ID] = pendingGangRank{reservation: reservation, prepared: prepared}
	e.Logger.Info("gang rank prepared; waiting for the other ranks", "lease_id", reservation.Lease.ID, "gang", reservation.Gang)
}

// retryPendingGangRanks asks again for the launch of every gang rank held at
// the barrier: it launches once all ranks are prepared, and is cleaned up and
// released when the placement was aborted (or any other refusal).
func (e *Local) retryPendingGangRanks(ctx context.Context) error {
	e.mu.Lock()
	pending := make([]pendingGangRank, 0, len(e.pendingGang))
	for _, p := range e.pendingGang {
		pending = append(pending, p)
	}
	e.mu.Unlock()
	for _, p := range pending {
		launch, err := e.authorize(ctx, p.reservation)
		if errors.Is(err, store.ErrGangNotReady) {
			continue
		}
		if err != nil {
			e.Logger.Warn("gang rank not launched", "lease_id", p.reservation.Lease.ID, "error", err)
			// Into the cleanups before leaving the barrier, so that the
			// unlaunched-lease sweep never sees it held by nobody.
			e.abandonReservation(p.reservation.Lease, p.prepared, err.Error(), "", nil)
		}
		e.mu.Lock()
		delete(e.pendingGang, p.reservation.Lease.ID)
		e.mu.Unlock()
		if err == nil {
			e.launchAuthorized(launch)
		}
	}
	return nil
}

// gangEnv tells a gang rank where it stands: Kairo's names and the ones
// torch.distributed reads; the interconnect interface (this executor's or
// node's "interconnect_ifname" label) pins NCCL / gloo to the direct link.
func (e *Local) gangEnv(gang *store.GangLaunch) []string {
	if gang == nil {
		return nil
	}
	env := []string{
		"KAIRO_GANG_ID=" + gang.GangID, fmt.Sprintf("KAIRO_GANG_RANK=%d", gang.Rank), fmt.Sprintf("KAIRO_GANG_SIZE=%d", gang.Size),
		"KAIRO_GANG_MASTER_ADDR=" + gang.MasterAddr, fmt.Sprintf("KAIRO_GANG_MASTER_PORT=%d", gang.MasterPort),
		fmt.Sprintf("RANK=%d", gang.Rank), fmt.Sprintf("WORLD_SIZE=%d", gang.Size), "LOCAL_RANK=0", "LOCAL_WORLD_SIZE=1",
		"MASTER_ADDR=" + gang.MasterAddr, fmt.Sprintf("MASTER_PORT=%d", gang.MasterPort),
	}
	if ifname := e.Attributes["interconnect_ifname"]; ifname != "" {
		env = append(env, "NCCL_SOCKET_IFNAME="+ifname, "GLOO_SOCKET_IFNAME="+ifname)
	}
	return env
}

// cleanupReservation returns provider-side preparation before releasing the
// database lease. If physical cleanup or its confirming observation fails,
// the lease remains non-released and therefore cannot be allocated again.
func (e *Local) cleanupReservation(ctx context.Context, lease store.Lease, resources []store.ResourceInstance, reason string) error {
	providers := make(map[string]provider.Provider)
	for i := len(resources) - 1; i >= 0; i-- {
		resource := resources[i]
		p := e.Providers[resource.ProviderID]
		if p == nil {
			return fmt.Errorf("provider %s is unavailable during reservation cleanup", resource.ProviderID)
		}
		if err := providerAction(ctx, e, resource.ProviderID, func(ctx context.Context) error { return p.Release(ctx, resource) }); err != nil {
			return fmt.Errorf("release resource %s: %w", resource.ID, err)
		}
		providers[resource.ProviderID] = p
	}
	for providerID, p := range providers {
		snapshot, err := providerCall(ctx, e, providerID, p.Observe)
		if err != nil {
			return fmt.Errorf("observe provider %s after reservation cleanup: %w", providerID, err)
		}
		if err = e.Store.ApplyObservationBatch(ctx, providerID, snapshot.Resources, snapshot.Observations, snapshot.Claims); err != nil {
			return err
		}
	}
	return e.Store.ReleaseReservation(ctx, lease.ID, lease.CoordinationEpoch, reason)
}

// reconcileQuiescence proves process absence before returning physical
// resources and releasing the coordination lease. A process exit notification
// alone is never treated as proof that a distributed execution has stopped.
func (e *Local) reconcileQuiescence(ctx context.Context) error {
	candidates, err := e.Store.ListQuiescenceCandidates(ctx, e.ID)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		allAbsent := true
		for _, process := range candidate.Processes {
			liveness, livenessErr := processidentity.HostProcessLiveness(process.PID, process.ProcessIdentity)
			if process.Namespace == "executor" && (e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2") {
				liveness, livenessErr = processidentity.WSLProcessLiveness(ctx, e.Attributes["distro"], process.PID, process.ProcessIdentity)
			}
			if livenessErr != nil || liveness == processidentity.LivenessUnknown {
				allAbsent = false
				e.Logger.Warn("process liveness is unknown; retaining lease", "attempt_id", candidate.Attempt.ID, "pid", process.PID, "namespace", process.Namespace, "error", livenessErr)
				continue
			}
			if liveness == processidentity.LivenessAlive {
				allAbsent = false
				continue
			}
			if err := e.Store.MarkAttemptProcessExited(ctx, candidate.Attempt.ID, process.Role, process.Rank, process.ProcessIdentity); err != nil && err != store.ErrNotFound {
				return err
			}
		}
		if !allAbsent {
			continue
		}
		// The registered processes are gone; so must be what they started
		// without registering it (a DataLoader worker, a daemonised helper):
		// the job object, the process group, the processes carrying the
		// attempt's KAIRO_ATTEMPT_ID. Until then the lease stays.
		if left, err := e.leftBehind(ctx, candidate.Attempt); err != nil || left {
			e.warnLeftBehind(candidate.Attempt.ID, err)
			if err == nil {
				e.endLeftBehind(ctx, candidate.Attempt)
			}
			continue
		}
		refreshed := make(map[string]bool)
		for _, resource := range candidate.Resources {
			p := e.Providers[resource.ProviderID]
			if p == nil {
				return fmt.Errorf("provider %s is unavailable during release", resource.ProviderID)
			}
			if err := providerAction(ctx, e, resource.ProviderID, func(ctx context.Context) error { return p.Release(ctx, resource) }); err != nil {
				return fmt.Errorf("release resource %s: %w", resource.ID, err)
			}
			refreshed[resource.ProviderID] = false
		}
		for providerID := range refreshed {
			p := e.Providers[providerID]
			snapshot, observeErr := providerCall(ctx, e, providerID, p.Observe)
			if observeErr != nil {
				return fmt.Errorf("observe provider %s after release: %w", providerID, observeErr)
			}
			if applyErr := e.Store.ApplyObservationBatch(ctx, providerID, snapshot.Resources, snapshot.Observations, snapshot.Claims); applyErr != nil {
				return applyErr
			}
		}
		if err := e.Store.FinalizeQuiescence(ctx, candidate.Attempt.ID, candidate.Lease.ID, candidate.Lease.CoordinationEpoch); err != nil {
			e.Logger.Warn("quiescence not yet proven", "attempt_id", candidate.Attempt.ID, "lease_id", candidate.Lease.ID, "error", err)
			continue
		}
		e.releaseTree(candidate.Attempt.ID)
	}
	return nil
}

// terminationGrace is how long a terminated attempt (node quarantine, gang)
// gets between the graceful stop and the kill of what is left.
const terminationGrace = 10 * time.Second

// carryOutQuarantineTerminations terminates the process trees of this
// executor's attempts that a node quarantine stopped and that cannot
// checkpoint, and of the running ranks of a gang one of whose ranks stopped.
// The whole tree is reached as for a force stop (processTree): the job
// object, the launcher's process group, and the processes carrying the
// attempt's KAIRO_ATTEMPT_ID, inside WSL too. A graceful stop goes first, a
// kill after terminationGrace, and the termination is recorded only once no
// process of the tree is left. Quiescence is proven afterwards by
// reconcileQuiescence as for any exit.
func (e *Local) carryOutQuarantineTerminations(ctx context.Context) error {
	terminations, err := e.Store.ListQuarantineTerminations(ctx, e.ID)
	if err != nil {
		return err
	}
	for _, t := range terminations {
		tree := e.attemptTree(store.ForceStopOrder{AttemptID: t.AttemptID, ExecutionID: t.ExecutionID, PID: t.PID, ProcessIdentity: t.ProcessIdentity})
		alive, err := e.treeAlive(ctx, tree)
		if err != nil {
			e.Logger.Warn("termination: process liveness unknown; retrying", "attempt_id", t.AttemptID, "error", err)
			continue
		}
		if alive {
			e.mu.Lock()
			if e.terminating == nil {
				e.terminating = map[string]time.Time{}
			}
			since, signalled := e.terminating[t.AttemptID]
			if !signalled {
				e.terminating[t.AttemptID] = time.Now()
			}
			e.mu.Unlock()
			switch {
			case !signalled:
				if err := e.stopTreeGracefully(ctx, tree); err != nil {
					e.Logger.Warn("termination: graceful stop unavailable; killing the process tree", "attempt_id", t.AttemptID, "error", err)
					if err := e.killTree(ctx, tree); err != nil {
						e.Logger.Warn("termination: kill failed; retrying", "attempt_id", t.AttemptID, "error", err)
					}
				}
			case time.Since(since) >= terminationGrace:
				if err := e.killTree(ctx, tree); err != nil {
					e.Logger.Warn("termination: kill failed; retrying", "attempt_id", t.AttemptID, "error", err)
				}
			}
			continue
		}
		e.mu.Lock()
		delete(e.terminating, t.AttemptID)
		e.mu.Unlock()
		if err := e.Store.MarkQuarantineTerminated(ctx, t.AttemptID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		e.Logger.Info("terminated attempt", "attempt_id", t.AttemptID)
	}
	return nil
}

// resourceBinding is how CUDA_VISIBLE_DEVICES names a resource. A GPU is
// named by its UUID: cuda_index is nvidia-smi's PCI bus order, while CUDA
// enumerates fastest first unless CUDA_DEVICE_ORDER=PCI_BUS_ID, so on a node
// with two GPUs an index can select the card another attempt holds.
func resourceBinding(resource store.ResourceInstance) string {
	var value map[string]string
	if json.Unmarshal(resource.Binding, &value) == nil {
		if x := value["uuid"]; x != "" {
			return x
		}
		if x := value["cuda_index"]; x != "" {
			return x
		}
		if x := value["value"]; x != "" {
			return x
		}
	}
	return resource.StableIdentity
}

// launchEnv is what an attempt is told: who it is (KAIRO_ATTEMPT_TOKEN is its
// credential for the worker API), where the daemon is and which CA to trust,
// which resources it holds, its scratch (scratchEnv) and its run's output
// directory (outputEnv).
func (e *Local) launchEnv(launch *store.Launch) []string {
	bindings := make([]string, 0, len(launch.Resources))
	resourceIDs := make([]string, 0, len(launch.Resources))
	for _, resource := range launch.Resources {
		bindings = append(bindings, resourceBinding(resource))
		resourceIDs = append(resourceIDs, resource.ID)
	}
	env := []string{"KAIRO_NODE_ID=" + e.NodeID, "KAIRO_EXECUTOR_ID=" + e.ID, "KAIRO_EXECUTION_ID=" + launch.Execution.ID, "KAIRO_ATTEMPT_ID=" + launch.Attempt.ID, "KAIRO_ATTEMPT_TOKEN=" + launch.WorkerToken, "KAIRO_RESOURCE_IDS=" + strings.Join(resourceIDs, ","), "KAIRO_RESOURCE_BINDINGS=" + strings.Join(bindings, ","), "KAIRO_API_URL=" + e.APIURL}
	if e.APICA != "" {
		env = append(env, "KAIRO_API_CA="+e.APICA)
	}
	if launch.InputContinuationRef != nil {
		env = append(env, "KAIRO_CONTINUATION_REF="+*launch.InputContinuationRef)
	}
	env = append(env, e.gangEnv(launch.Gang)...)
	if len(bindings) > 0 {
		env = append(env, "CUDA_VISIBLE_DEVICES="+strings.Join(bindings, ","))
	}
	env = append(env, e.scratchEnv(launch)...)
	env = append(env, e.outputEnv(launch)...)
	env = append(env, e.cacheEnv(launch)...)
	for _, in := range launch.Inputs {
		if in.Path != "" {
			env = append(env, datainput.EnvName(in.Name)+"="+in.Path)
		}
	}
	return env
}

// checkInputs refuses a launch one of whose inputs cannot be had here (the
// daemon says why) or is not a directory there now (store/inputs.go): the
// attempt does not start, and its execution ends launch_aborted saying so.
// A WSL executor's paths are the distro's: they are not looked at from here.
func (e *Local) checkInputs(launch *store.Launch) error {
	wsl := e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2"
	for _, in := range launch.Inputs {
		if in.Problem != "" {
			return fmt.Errorf("input %s: %s", in.Name, in.Problem)
		}
		if wsl {
			continue
		}
		r, err := openDirPath(in.Path, false)
		if err != nil {
			return fmt.Errorf("input %s (%s): %w", in.Name, in.Path, err)
		}
		r.Close()
	}
	return nil
}

// envKey is how the OS compares variable names: case-insensitively on Windows
// (a declared Path replaces the parent's PATH).
func envKey(name string) string {
	if runtime.GOOS == "windows" {
		return strings.ToUpper(name)
	}
	return name
}

// envName is the name of a NAME=value entry.
func envName(kv string) string {
	name, _, _ := strings.Cut(kv, "=")
	return name
}

// command builds the attempt's launcher. Its environment is the parent's,
// then the execution's declared env, then Kairo's launch variables; a later
// layer replaces a name of an earlier one, so declared env overrides PATH and
// the like but never what Kairo sets. The parent's own KAIRO_* variables (an
// operator's KAIRO_TOKEN, say) never reach the attempt. A WSL attempt gets its
// variables through WSLENV rather than on the wsl.exe command line, which
// other tools can read and log, and runs its argv with --exec: without it
// wsl.exe joins the arguments into a line for the default shell, which expands
// $VAR and re-splits quoted words.
func (e *Local) command(launch *store.Launch) *exec.Cmd {
	launchEnv := e.launchEnv(launch)
	// own is what the attempt is given on top of the parent's environment:
	// the declared env in name order without the names Kairo sets, then
	// Kairo's; given holds its names.
	given := map[string]bool{}
	for _, kv := range launchEnv {
		given[envKey(envName(kv))] = true
	}
	own := make([]string, 0, len(launch.Env)+len(launchEnv))
	for _, name := range envspec.Names(launch.Env) {
		if !given[envKey(name)] {
			own = append(own, name+"="+launch.Env[name])
		}
	}
	for _, kv := range own {
		given[envKey(envName(kv))] = true
	}
	own = append(own, launchEnv...)
	parent := make([]string, 0, len(os.Environ()))
	wslenv := ""
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		switch {
		case strings.EqualFold(name, "WSLENV"):
			wslenv = value
		case envspec.Reserved(name), given[envKey(name)]:
		default:
			parent = append(parent, kv)
		}
	}
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		args := []string{}
		if distro := e.Attributes["distro"]; distro != "" {
			args = append(args, "-d", distro)
		}
		args = append(args, "--cd", launch.CWD, "--exec")
		args = append(args, launch.Argv...)
		cmd := exec.Command("wsl.exe", args...)
		shared := []string{}
		if wslenv != "" {
			shared = append(shared, wslenv)
		}
		for _, kv := range own {
			shared = append(shared, envName(kv)+"/u")
		}
		cmd.Env = append(append(parent, own...), "WSLENV="+strings.Join(shared, ":"))
		return cmd
	}
	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.CWD
	if wslenv != "" {
		parent = append(parent, "WSLENV="+wslenv)
	}
	cmd.Env = append(parent, own...)
	return cmd
}

// openAttemptLog opens an attempt's log for its process to write, after
// whatever the file already holds. Not with O_APPEND: on Windows that opens
// the file with append-only access (no FILE_WRITE_DATA), and MSYS/Cygwin
// programs such as Git Bash then fail every write, silently and with exit 1
// (2026-10-28).
func openAttemptLog(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (e *Local) start(launch *store.Launch) error {
	// Output written to a full filesystem goes nowhere: refuse the launch
	// rather than start a process nobody can follow.
	if err := e.checkLaunchLogSpace(); err != nil {
		return err
	}
	if err := e.checkInputs(launch); err != nil {
		return err
	}
	if err := e.checkStore(launch); err != nil {
		return err
	}
	e.makeScratch(launch)
	e.makeOutput(launch)
	e.makeCache(launch)
	launched := false
	defer func() {
		if !launched {
			// not started after all: its run no longer runs here
			e.mu.Lock()
			delete(e.outputRuns, launch.Attempt.ID)
			e.mu.Unlock()
		}
	}()
	cmd := e.command(launch)
	if cmd.Err != nil {
		return cmd.Err
	}
	e.forgetScratch(launch.Attempt.ID)
	prepareProcessTree(cmd)
	var conf *confinement
	if launch.Confine {
		c, err := e.confine(cmd)
		if err != nil {
			return err
		}
		conf = c
	}
	id := launch.Attempt.ID
	job := ""
	if runtime.GOOS == "windows" {
		job = "kairo-attempt-" + id
	}
	// The attempt's launcher is its shim (shim.go): the command starts only
	// once the shim is in its job object and the attempt is recorded, and
	// an agent restarted meanwhile adopts it.
	forgetRecord(e.LogDir, id)
	if err := os.MkdirAll(recordsDir(e.LogDir), 0o755); err != nil {
		return err
	}
	shim, goAhead, err := shimCommand(cmd, exitPath(e.LogDir, id), conf)
	if err != nil {
		return err
	}
	readEnd := shim.Stdin.(*os.File)
	outPath := filepath.Join(e.LogDir, id+".stdout.log")
	errPath := filepath.Join(e.LogDir, id+".stderr.log")
	stdout, err := openAttemptLog(outPath)
	if err != nil {
		readEnd.Close()
		goAhead.Close()
		return err
	}
	stderr, err := openAttemptLog(errPath)
	if err != nil {
		stdout.Close()
		readEnd.Close()
		goAhead.Close()
		return err
	}
	closeLogs := func() {
		stdout.Close()
		stderr.Close()
	}
	shim.Stdout, shim.Stderr = stdout, stderr
	err = shim.Start()
	readEnd.Close()
	if err != nil {
		goAhead.Close()
		closeLogs()
		return err
	}
	// The tree handle (Windows: a job object) lets a force stop reach every
	// descendant; without it the stop falls back to walking the tree.
	tree, treeErr := attachTree(shim, job)
	if treeErr != nil {
		e.Logger.Warn("attempt process tree not tracked", "attempt_id", id, "error", treeErr)
	}
	identity, identityErr := processidentity.ForPID(shim.Process.Pid)
	if identityErr != nil {
		goAhead.Close()
		e.killUnactivated(launch, shim, tree, "")
		closeLogs()
		return fmt.Errorf("read process creation identity: %w", identityErr)
	}
	// Asked again while no answer arrives: a repeat of an activation that
	// went through is answered the same (store.ActivateLaunch).
	err = e.retryUnanswered(context.Background(), func() error {
		return e.Store.ActivateLaunch(context.Background(), id, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, shim.Process.Pid, identity)
	})
	record := AttemptRecord{AttemptID: id, ExecutorID: e.ID, LeaseID: launch.Lease.ID, Epoch: launch.Lease.CoordinationEpoch,
		RunID: launch.RunID, PID: shim.Process.Pid, Identity: identity, Job: job}
	if err == nil {
		err = writeRecord(e.LogDir, record)
	}
	if err == nil {
		err = sendGoAhead(goAhead)
	}
	if err != nil {
		goAhead.Close()
		e.killUnactivated(launch, shim, tree, identity)
		forgetRecord(e.LogDir, id)
		closeLogs()
		return err
	}
	go e.recordLogPaths(launch, outPath, errPath)
	e.mu.Lock()
	e.running[id] = shim.Process.Pid
	if tree != nil {
		if e.trees == nil {
			e.trees = map[string]*treeHandle{}
		}
		e.trees[id] = tree
	}
	e.mu.Unlock()
	e.trackUsage(id, shim.Process.Pid, identity)
	go func() {
		waitErr := shim.Wait()
		exit, written := readExit(e.LogDir, id)
		if !written {
			// killed (a force stop, the OOM killer): the shim's own end
			exit = shim.ProcessState.ExitCode()
			e.Logger.Info("attempt's shim exited without writing its exit", "attempt_id", id, "exit", exit, "wait_error", waitErr)
		}
		e.finishAttempt(record, exit, closeLogs)
	}()
	launched = true
	return nil
}

// finishAttempt records the exit of an attempt launched or adopted here
// whose shim has exited, and forgets it.
func (e *Local) finishAttempt(r AttemptRecord, exit int, closeLogs func()) {
	e.untrackUsage(r.AttemptID)
	if closeLogs != nil {
		closeLogs()
	}
	e.checkLogSpaceAtExit(r.AttemptID)
	if e.FlushLogs != nil {
		// Bounded: the shipper goes on retrying in the background.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := e.FlushLogs(ctx, r.AttemptID); err != nil {
			e.Logger.Warn("attempt logs not shipped at exit; retrying in the background", "attempt_id", r.AttemptID, "error", err)
		}
		cancel()
	}
	// RecordTerminal is idempotent, so a transient failure (a remote agent
	// losing its connection to the daemon) is retried rather than leaving
	// the lease stuck in active.
	for delay := time.Second; ; delay = min(2*delay, time.Minute) {
		err := e.Store.RecordTerminal(context.Background(), r.AttemptID, r.LeaseID, r.Epoch, exit, "")
		if err == nil {
			break
		}
		e.Logger.Error("could not record attempt exit", "attempt_id", r.AttemptID, "error", err)
		if errors.Is(err, store.ErrStaleEpoch) || errors.Is(err, store.ErrNotFound) {
			break
		}
		time.Sleep(delay)
	}
	forgetRecord(e.LogDir, r.AttemptID)
	e.mu.Lock()
	delete(e.running, r.AttemptID)
	delete(e.outputRuns, r.AttemptID)
	e.mu.Unlock()
	e.forgetLogProblems(r.AttemptID)
}

// recordLogPaths records where an attempt's logs are, retrying while the
// store is unreachable: without them task show and task logs cannot find the
// logs.
func (e *Local) recordLogPaths(launch *store.Launch, outPath, errPath string) {
	deadline := time.Now().Add(30 * time.Minute)
	for delay := time.Second; ; delay = min(2*delay, time.Minute) {
		err := e.Store.SetAttemptLogPaths(context.Background(), launch.Attempt.ID, launch.Lease.CoordinationEpoch, outPath, errPath)
		if err == nil {
			return
		}
		if errors.Is(err, store.ErrStaleEpoch) || errors.Is(err, store.ErrNotFound) || time.Now().After(deadline) {
			e.Logger.Error("attempt log paths not recorded", "attempt_id", launch.Attempt.ID, "error", err)
			return
		}
		e.Logger.Warn("attempt log paths not recorded; retrying", "attempt_id", launch.Attempt.ID, "error", err)
		time.Sleep(delay)
	}
}
