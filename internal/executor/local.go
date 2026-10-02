package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	Providers           map[string]provider.Provider
	lastObservation     time.Time

	mu      sync.Mutex
	running map[string]*exec.Cmd

	// Prepared gang ranks waiting at the launch barrier, by lease ID.
	pendingGang map[string]pendingGangRank

	// Force stop (forcestop.go): process tree handles of the attempts this
	// instance launched, and the progress of force stops being carried out.
	trees              map[string]*treeHandle
	forceChecked       map[string]time.Time
	forceKilled        map[string]bool
	forceOrdersFailing bool
}

type pendingGangRank struct {
	reservation *store.Reservation
	prepared    []store.ResourceInstance
}

func NewLocal(id string, st Coordinator, logDir string, logger *slog.Logger) *Local {
	return &Local{
		ID: id, Store: st, LogDir: logDir, Interval: 250 * time.Millisecond,
		Logger: logger, running: make(map[string]*exec.Cmd),
	}
}

func (e *Local) Run(ctx context.Context) error {
	if e.Logger == nil {
		e.Logger = slog.Default()
	}
	if err := os.MkdirAll(e.LogDir, 0o755); err != nil {
		return err
	}
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
			snap, err := p.Observe(ctx)
			if err != nil {
				e.Logger.Warn("provider observation failed", "provider_id", providerID, "error", err)
				continue
			}
			if err = e.Store.ApplyObservationBatch(ctx, providerID, snap.Resources, snap.Observations, snap.Claims); err != nil {
				return err
			}
		}
		e.lastObservation = time.Now()
	}
	if e.ObserveOnly {
		return nil
	}
	if err := e.reconcileQuiescence(ctx); err != nil {
		return err
	}
	if err := e.carryOutQuarantineTerminations(ctx); err != nil {
		return err
	}
	e.carryOutForceStops(ctx)
	if err := e.retryPendingGangRanks(ctx); err != nil {
		return err
	}
	for {
		reservation, err := e.Store.ReserveNext(ctx, e.ID)
		if err != nil {
			return err
		}
		if reservation == nil {
			commands, preemptErr := e.Store.EnsurePriorityPreemption(ctx, e.ID)
			if preemptErr != nil {
				return preemptErr
			}
			if len(commands) != 0 {
				e.Logger.Info("requested cooperative priority preemption", "commands", commands)
			}
			return nil
		}
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
				snap, err := p.Observe(ctx)
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
			if err := p.Prepare(ctx, resource); err != nil {
				prepareErr = err
				break
			}
		}
		if prepareErr == nil {
			prepareErr = e.Store.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch)
		}
		if prepareErr != nil {
			if cleanupErr := e.cleanupReservation(context.Background(), reservation.Lease, prepared, prepareErr.Error()); cleanupErr != nil {
				e.Logger.Error("reservation cleanup failed", "lease_id", reservation.Lease.ID, "error", cleanupErr)
			}
			e.Logger.Warn("lease prepare failed", "lease_id", reservation.Lease.ID, "error", prepareErr)
			return nil
		}
		if err := e.Store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
			if cleanupErr := e.cleanupReservation(context.Background(), reservation.Lease, prepared, err.Error()); cleanupErr != nil {
				return fmt.Errorf("mark lease prepared: %v; cleanup: %w", err, cleanupErr)
			}
			return nil
		}
		launch, err := e.Store.AuthorizeLaunch(ctx, reservation)
		if errors.Is(err, store.ErrGangNotReady) {
			// Hold the prepared rank at the barrier until every rank is prepared.
			e.holdGangRank(reservation, prepared)
			continue
		}
		if err != nil {
			if cleanupErr := e.cleanupReservation(context.Background(), reservation.Lease, prepared, err.Error()); cleanupErr != nil {
				return fmt.Errorf("authorize launch: %v; cleanup: %w", err, cleanupErr)
			}
			return nil
		}
		e.launchAuthorized(launch)
	}
}

func (e *Local) launchAuthorized(launch *store.Launch) {
	if err := e.start(launch); err != nil {
		e.Logger.Error("attempt launch failed", "attempt_id", launch.Attempt.ID, "error", err)
		if cleanupErr := e.cleanupReservation(context.Background(), launch.Lease, launch.Resources, "launch failed before activation: "+err.Error()); cleanupErr != nil {
			e.Logger.Error("reservation cleanup failed", "lease_id", launch.Lease.ID, "error", cleanupErr)
		}
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
		launch, err := e.Store.AuthorizeLaunch(ctx, p.reservation)
		if errors.Is(err, store.ErrGangNotReady) {
			continue
		}
		e.mu.Lock()
		delete(e.pendingGang, p.reservation.Lease.ID)
		e.mu.Unlock()
		if err != nil {
			e.Logger.Warn("gang rank not launched", "lease_id", p.reservation.Lease.ID, "error", err)
			if cleanupErr := e.cleanupReservation(context.Background(), p.reservation.Lease, p.prepared, err.Error()); cleanupErr != nil {
				return fmt.Errorf("gang rank cleanup: %w", cleanupErr)
			}
			continue
		}
		e.launchAuthorized(launch)
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
		if err := p.Release(ctx, resource); err != nil {
			return fmt.Errorf("release resource %s: %w", resource.ID, err)
		}
		providers[resource.ProviderID] = p
	}
	for providerID, p := range providers {
		snapshot, err := p.Observe(ctx)
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
		refreshed := make(map[string]bool)
		for _, resource := range candidate.Resources {
			p := e.Providers[resource.ProviderID]
			if p == nil {
				return fmt.Errorf("provider %s is unavailable during release", resource.ProviderID)
			}
			if err := p.Release(ctx, resource); err != nil {
				return fmt.Errorf("release resource %s: %w", resource.ID, err)
			}
			refreshed[resource.ProviderID] = false
		}
		for providerID := range refreshed {
			p := e.Providers[providerID]
			snapshot, observeErr := p.Observe(ctx)
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

// carryOutQuarantineTerminations terminates the process trees of this
// executor's attempts that a node quarantine stopped and that cannot
// checkpoint. The launcher is found through this executor's own handle, or
// else through the recorded pid when its creation identity still matches (an
// executor restarted since the launch). Quiescence is proven afterwards by
// reconcileQuiescence as for any exit.
func (e *Local) carryOutQuarantineTerminations(ctx context.Context) error {
	terminations, err := e.Store.ListQuarantineTerminations(ctx, e.ID)
	if err != nil {
		return err
	}
	for _, t := range terminations {
		pid := 0
		e.mu.Lock()
		if cmd := e.running[t.AttemptID]; cmd != nil && cmd.Process != nil {
			pid = cmd.Process.Pid
		}
		e.mu.Unlock()
		if pid == 0 && t.PID != nil && t.ProcessIdentity != nil {
			if liveness, _ := processidentity.HostProcessLiveness(*t.PID, *t.ProcessIdentity); liveness == processidentity.LivenessAlive {
				pid = *t.PID
			}
		}
		if pid != 0 {
			if err := killProcessTree(pid); err != nil {
				// taskkill / kill also fail when the process has just exited on its own.
				gone := t.PID != nil && t.ProcessIdentity != nil
				if gone {
					liveness, _ := processidentity.HostProcessLiveness(*t.PID, *t.ProcessIdentity)
					gone = liveness == processidentity.LivenessAbsent
				}
				if !gone {
					e.Logger.Warn("quarantine termination failed; retrying", "attempt_id", t.AttemptID, "pid", pid, "error", err)
					continue
				}
			}
			e.Logger.Info("terminated attempt for node quarantine", "attempt_id", t.AttemptID, "pid", pid)
		}
		if err := e.Store.MarkQuarantineTerminated(ctx, t.AttemptID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

func resourceBinding(resource store.ResourceInstance) string {
	var value map[string]string
	if json.Unmarshal(resource.Binding, &value) == nil {
		if x := value["cuda_index"]; x != "" {
			return x
		}
		if x := value["value"]; x != "" {
			return x
		}
		if x := value["uuid"]; x != "" {
			return x
		}
	}
	return resource.StableIdentity
}

// launchEnv is what an attempt is told: who it is (KAIRO_ATTEMPT_TOKEN is its
// credential for the worker API), where the daemon is and which CA to trust,
// and which resources it holds.
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
	return env
}

// command builds the attempt's launcher. The parent's own KAIRO_* variables
// (an operator's KAIRO_TOKEN, say) never reach the attempt. A WSL attempt gets
// its variables through WSLENV rather than on the wsl.exe command line, which
// other tools can read and log.
func (e *Local) command(launch *store.Launch) *exec.Cmd {
	launchEnv := e.launchEnv(launch)
	parent := make([]string, 0, len(os.Environ()))
	wslenv := ""
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(strings.ToUpper(name), "KAIRO_"):
		case strings.EqualFold(name, "WSLENV"):
			wslenv = value
		default:
			parent = append(parent, kv)
		}
	}
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		args := []string{}
		if distro := e.Attributes["distro"]; distro != "" {
			args = append(args, "-d", distro)
		}
		args = append(args, "--cd", launch.CWD, "--")
		args = append(args, launch.Argv...)
		cmd := exec.Command("wsl.exe", args...)
		shared := []string{}
		if wslenv != "" {
			shared = append(shared, wslenv)
		}
		for _, kv := range launchEnv {
			name, _, _ := strings.Cut(kv, "=")
			shared = append(shared, name+"/u")
		}
		cmd.Env = append(append(parent, launchEnv...), "WSLENV="+strings.Join(shared, ":"))
		return cmd
	}
	cmd := exec.Command(launch.Argv[0], launch.Argv[1:]...)
	cmd.Dir = launch.CWD
	if wslenv != "" {
		parent = append(parent, "WSLENV="+wslenv)
	}
	cmd.Env = append(parent, launchEnv...)
	return cmd
}

func (e *Local) start(launch *store.Launch) error {
	cmd := e.command(launch)
	prepareProcessTree(cmd)
	outPath := filepath.Join(e.LogDir, launch.Attempt.ID+".stdout.log")
	errPath := filepath.Join(e.LogDir, launch.Attempt.ID+".stderr.log")
	stdout, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	stderr, err := os.OpenFile(errPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		stdout.Close()
		return err
	}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err = cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		return err
	}
	// The tree handle (Windows: a job object) lets a force stop reach every
	// descendant; without it the stop falls back to walking the tree.
	tree, treeErr := attachTree(cmd)
	if treeErr != nil {
		e.Logger.Warn("attempt process tree not tracked", "attempt_id", launch.Attempt.ID, "error", treeErr)
	}
	identity, identityErr := processidentity.ForPID(cmd.Process.Pid)
	if identityErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		tree.close()
		stdout.Close()
		stderr.Close()
		return fmt.Errorf("read process creation identity: %w", identityErr)
	}
	if err = e.Store.ActivateLaunch(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, cmd.Process.Pid, identity); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		tree.close()
		stdout.Close()
		stderr.Close()
		return err
	}
	_ = e.Store.SetAttemptLogPaths(context.Background(), launch.Attempt.ID, launch.Lease.CoordinationEpoch, outPath, errPath)
	e.mu.Lock()
	e.running[launch.Attempt.ID] = cmd
	if tree != nil {
		if e.trees == nil {
			e.trees = map[string]*treeHandle{}
		}
		e.trees[launch.Attempt.ID] = tree
	}
	e.mu.Unlock()
	go func() {
		waitErr := cmd.Wait()
		stdout.Close()
		stderr.Close()
		exit := cmd.ProcessState.ExitCode()
		// RecordTerminal is idempotent, so a transient failure (a remote
		// agent losing its connection to the daemon) is retried rather than
		// leaving the lease stuck in active.
		for delay := time.Second; ; delay = min(2*delay, time.Minute) {
			err := e.Store.RecordTerminal(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, exit, "")
			if err == nil {
				break
			}
			e.Logger.Error("could not record attempt exit", "attempt_id", launch.Attempt.ID, "wait_error", waitErr, "error", err)
			if errors.Is(err, store.ErrStaleEpoch) || errors.Is(err, store.ErrNotFound) {
				break
			}
			time.Sleep(delay)
		}
		e.mu.Lock()
		delete(e.running, launch.Attempt.ID)
		e.mu.Unlock()
	}()
	return nil
}
