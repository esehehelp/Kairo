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
	ID                  string
	Store               Coordinator
	LogDir              string
	APIURL              string
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

	// quarantineSignalled is when this executor, by its own clock, first saw
	// each quarantine termination signalled (tick goroutine only).
	quarantineSignalled map[string]time.Time
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
		if err != nil {
			if cleanupErr := e.cleanupReservation(context.Background(), reservation.Lease, prepared, err.Error()); cleanupErr != nil {
				return fmt.Errorf("authorize launch: %v; cleanup: %w", err, cleanupErr)
			}
			return nil
		}
		if err = e.start(launch); err != nil {
			e.Logger.Error("attempt launch failed", "attempt_id", launch.Attempt.ID, "error", err)
			if cleanupErr := e.cleanupReservation(context.Background(), launch.Lease, launch.Resources, "launch failed before activation: "+err.Error()); cleanupErr != nil {
				return cleanupErr
			}
		}
	}
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
		}
	}
	return nil
}

// killGrace is how long a quarantined process tree gets between the first
// signal and SIGKILL.
const killGrace = 10 * time.Second

// carryOutQuarantineTerminations terminates the process trees of this
// executor's attempts that a node quarantine stopped and that cannot
// checkpoint. It works from the recorded launcher identity, so an executor
// restarted since the launch carries on where the previous one stopped. The
// first signal is recorded before it is sent (a release no longer withdraws
// the termination after that), and a tree still alive killGrace later, timed
// by this executor's clock from when it signalled or, after a restart, first
// saw the termination signalled, is killed. A termination is marked done only
// once the whole tree is seen gone; until then the attempt's lease is not
// released. When liveness cannot be determined nothing is marked and it is
// retried.
func (e *Local) carryOutQuarantineTerminations(ctx context.Context) error {
	terminations, err := e.Store.ListQuarantineTerminations(ctx, e.ID)
	if err != nil {
		return err
	}
	if e.quarantineSignalled == nil {
		e.quarantineSignalled = make(map[string]time.Time)
	}
	listed := make(map[string]bool, len(terminations))
	for _, t := range terminations {
		listed[t.AttemptID] = true
	}
	for attemptID := range e.quarantineSignalled {
		if !listed[attemptID] {
			delete(e.quarantineSignalled, attemptID)
		}
	}
	for _, t := range terminations {
		host, wsl, err := e.quarantinedTreeAlive(ctx, t)
		if err != nil {
			e.Logger.Warn("quarantined attempt liveness is unknown; retrying", "attempt_id", t.AttemptID, "error", err)
			continue
		}
		if !host && !wsl {
			if err := e.Store.MarkQuarantineTerminated(ctx, t.AttemptID); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			delete(e.quarantineSignalled, t.AttemptID)
			e.Logger.Info("attempt stopped for node quarantine", "attempt_id", t.AttemptID)
			continue
		}
		force := false
		if t.SignalledAt == nil {
			if _, err := e.Store.MarkQuarantineSignalled(ctx, t.AttemptID); errors.Is(err, store.ErrNotFound) {
				continue // withdrawn by a release
			} else if err != nil {
				return err
			}
			e.quarantineSignalled[t.AttemptID] = time.Now()
		} else if first, ok := e.quarantineSignalled[t.AttemptID]; !ok {
			e.quarantineSignalled[t.AttemptID] = time.Now() // signalled before a restart
		} else if time.Since(first) < killGrace {
			continue
		} else {
			force = true
		}
		var errs []error
		if host {
			errs = append(errs, signalProcessTree(*t.PID, force))
		}
		if wsl {
			sig := "TERM"
			if force {
				sig = "KILL"
			}
			_, err := wslSignalAttempt(ctx, e.Attributes["distro"], t.AttemptID, sig)
			errs = append(errs, err)
		}
		if err := errors.Join(errs...); err != nil {
			e.Logger.Warn("quarantine termination signal failed; retrying", "attempt_id", t.AttemptID, "force", force, "error", err)
			continue
		}
		e.Logger.Info("signalled attempt for node quarantine", "attempt_id", t.AttemptID, "pid", *t.PID, "force", force)
	}
	return nil
}

// quarantinedTreeAlive reports whether the attempt's host process tree and,
// on a WSL2 executor, its processes inside the distro are still running.
func (e *Local) quarantinedTreeAlive(ctx context.Context, t store.QuarantineTermination) (host, wsl bool, err error) {
	if t.PID == nil || t.ProcessIdentity == nil {
		return false, false, errors.New("attempt has no recorded launcher")
	}
	if host, err = processTreeAlive(*t.PID, *t.ProcessIdentity); err != nil {
		return false, false, err
	}
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		if wsl, err = wslSignalAttempt(ctx, e.Attributes["distro"], t.AttemptID, "0"); err != nil {
			return false, false, err
		}
	}
	return host, wsl, nil
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

func (e *Local) start(launch *store.Launch) error {
	bindings := make([]string, 0, len(launch.Resources))
	resourceIDs := make([]string, 0, len(launch.Resources))
	for _, resource := range launch.Resources {
		bindings = append(bindings, resourceBinding(resource))
		resourceIDs = append(resourceIDs, resource.ID)
	}
	launchEnv := []string{"KAIRO_NODE_ID=" + e.NodeID, "KAIRO_EXECUTOR_ID=" + e.ID, "KAIRO_EXECUTION_ID=" + launch.Execution.ID, "KAIRO_ATTEMPT_ID=" + launch.Attempt.ID, "KAIRO_LEASE_ID=" + launch.Lease.ID, fmt.Sprintf("KAIRO_COORDINATION_EPOCH=%d", launch.Lease.CoordinationEpoch), "KAIRO_RESOURCE_IDS=" + strings.Join(resourceIDs, ","), "KAIRO_RESOURCE_BINDINGS=" + strings.Join(bindings, ","), "KAIRO_API_URL=" + e.APIURL}
	if launch.InputContinuationRef != nil {
		launchEnv = append(launchEnv, "KAIRO_CONTINUATION_REF="+*launch.InputContinuationRef)
	}
	if len(bindings) > 0 {
		launchEnv = append(launchEnv, "CUDA_VISIBLE_DEVICES="+strings.Join(bindings, ","))
	}
	var cmd *exec.Cmd
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		args := []string{}
		if distro := e.Attributes["distro"]; distro != "" {
			args = append(args, "-d", distro)
		}
		// WSL does not import arbitrary Windows environment variables unless
		// WSLENV is configured. Pass attempt fencing data explicitly through
		// /usr/bin/env so launch behavior is independent of the operator shell.
		args = append(args, "--cd", launch.CWD, "--", "env")
		args = append(args, launchEnv...)
		args = append(args, launch.Argv...)
		cmd = exec.Command("wsl.exe", args...)
	} else {
		cmd = exec.Command(launch.Argv[0], launch.Argv[1:]...)
		cmd.Dir = launch.CWD
		cmd.Env = append(os.Environ(), launchEnv...)
	}
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
	identity, identityErr := processidentity.ForPID(cmd.Process.Pid)
	if identityErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stdout.Close()
		stderr.Close()
		return fmt.Errorf("read process creation identity: %w", identityErr)
	}
	if err = e.Store.ActivateLaunch(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, cmd.Process.Pid, identity); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		stdout.Close()
		stderr.Close()
		return err
	}
	_ = e.Store.SetAttemptLogPaths(context.Background(), launch.Attempt.ID, launch.Lease.CoordinationEpoch, outPath, errPath)
	e.mu.Lock()
	e.running[launch.Attempt.ID] = cmd
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
