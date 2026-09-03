package executor

import (
	"context"
	"encoding/json"
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
	Store               *store.Store
	LogDir              string
	APIURL              string
	Interval            time.Duration
	Logger              *slog.Logger
	NodeID              string
	Kind                string
	Attributes          map[string]string
	V1                  bool
	ObserveOnly         bool
	ObservationInterval time.Duration
	Providers           map[string]provider.Provider
	lastObservation     time.Time

	mu      sync.Mutex
	running map[string]*exec.Cmd
}

func NewLocal(id string, st *store.Store, logDir string, logger *slog.Logger) *Local {
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
	if e.V1 {
		return e.tickV1(ctx)
	}
	if commandID, created, err := e.Store.EnsurePreemption(ctx); err != nil {
		return err
	} else if created {
		e.Logger.Info("requested cooperative preemption", "command_id", commandID)
	}
	for {
		launch, err := e.Store.AllocateNext(ctx, e.ID)
		if err != nil {
			return err
		}
		if launch == nil {
			return nil
		}
		if err := e.start(launch); err != nil {
			e.Logger.Error("attempt launch failed", "attempt_id", launch.Attempt.ID, "error", err)
			_ = e.Store.ReportDisposition(context.Background(), launch.Attempt.ID, "hold", nil)
			_ = e.Store.MarkAttemptExited(context.Background(), launch.Attempt.ID, -1)
		}
	}
}

func (e *Local) tickV1(ctx context.Context) error {
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
	if err := e.Store.WakeRetries(ctx); err != nil {
		return err
	}
	for {
		reservation, err := e.Store.ReserveNext(ctx, e.ID)
		if err != nil {
			return err
		}
		if reservation == nil {
			commands, preemptErr := e.Store.EnsureV1Preemption(ctx)
			if preemptErr != nil {
				return preemptErr
			}
			if len(commands) > 0 {
				e.Logger.Info("requested cooperative gang preemption", "commands", commands)
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
			if err := p.Prepare(ctx, resource); err != nil {
				prepareErr = err
				break
			}
			prepared = append(prepared, resource)
		}
		if prepareErr == nil {
			prepareErr = e.Store.ValidateReservation(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch)
		}
		if prepareErr != nil {
			for i := len(prepared) - 1; i >= 0; i-- {
				_ = e.Providers[prepared[i].ProviderID].Release(context.Background(), prepared[i])
			}
			_ = e.Store.ReleaseReservation(context.Background(), reservation.Lease.ID, reservation.Lease.CoordinationEpoch, prepareErr.Error())
			e.Logger.Warn("lease prepare failed", "lease_id", reservation.Lease.ID, "error", prepareErr)
			return nil
		}
		if err := e.Store.MarkLeasePrepared(ctx, reservation.Lease.ID, reservation.Lease.CoordinationEpoch); err != nil {
			return err
		}
		launch, err := e.Store.AuthorizeLaunch(ctx, reservation)
		if err != nil {
			return err
		}
		if err = e.startV1(launch); err != nil {
			e.Logger.Error("attempt launch failed", "attempt_id", launch.Attempt.ID, "error", err)
			_ = e.Store.TerminalV1(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, -1, "launch_error")
		}
	}
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

func (e *Local) startV1(launch *store.V1Launch) error {
	bindings := make([]string, 0, len(launch.Resources))
	resourceIDs := make([]string, 0, len(launch.Resources))
	for _, resource := range launch.Resources {
		bindings = append(bindings, resourceBinding(resource))
		resourceIDs = append(resourceIDs, resource.ID)
	}
	launchEnv := []string{"KAIRO_NODE_ID=" + e.NodeID, "KAIRO_EXECUTOR_ID=" + e.ID, "KAIRO_WORKLOAD_ID=" + launch.Task.ID, "KAIRO_ATTEMPT_ID=" + launch.Attempt.ID, "KAIRO_LEASE_ID=" + launch.Lease.ID, fmt.Sprintf("KAIRO_COORDINATION_EPOCH=%d", launch.Lease.CoordinationEpoch), "KAIRO_RESOURCE_IDS=" + strings.Join(resourceIDs, ","), "KAIRO_RESOURCE_BINDINGS=" + strings.Join(bindings, ","), "KAIRO_API_URL=" + e.APIURL}
	if launch.ContinuationRef != nil {
		launchEnv = append(launchEnv, "KAIRO_CONTINUATION_REF="+*launch.ContinuationRef)
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
		stdout.Close()
		stderr.Close()
		return fmt.Errorf("read process creation identity: %w", identityErr)
	}
	if err = e.Store.ActivateLaunch(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, launch.AuthorizationToken, cmd.Process.Pid, identity); err != nil {
		_ = cmd.Process.Kill()
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
		failure := ""
		if waitErr != nil {
			failure = "exit_nonzero"
		}
		if err := e.Store.TerminalV1(context.Background(), launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, exit, failure); err != nil {
			e.Logger.Error("could not record v1 attempt exit", "attempt_id", launch.Attempt.ID, "error", err)
		}
		e.mu.Lock()
		delete(e.running, launch.Attempt.ID)
		e.mu.Unlock()
	}()
	return nil
}

func (e *Local) start(launch *store.Launch) error {
	w := launch.Workload
	cmd := exec.Command(w.Argv[0], w.Argv[1:]...)
	cmd.Dir = w.CWD
	bindings := make([]string, 0, len(launch.Resources))
	resourceIDs := make([]string, 0, len(launch.Resources))
	for _, resource := range launch.Resources {
		bindings = append(bindings, resource.Binding)
		resourceIDs = append(resourceIDs, resource.ID)
	}
	cmd.Env = append(os.Environ(),
		"KAIRO_WORKLOAD_ID="+w.ID,
		"KAIRO_ATTEMPT_ID="+launch.Attempt.ID,
		"KAIRO_LEASE_ID="+launch.LeaseID,
		"KAIRO_RESOURCE_IDS="+strings.Join(resourceIDs, ","),
		"KAIRO_RESOURCE_BINDINGS="+strings.Join(bindings, ","),
		"KAIRO_API_URL="+e.APIURL,
	)
	if launch.ContinuationRef != nil {
		cmd.Env = append(cmd.Env, "KAIRO_CONTINUATION_REF="+*launch.ContinuationRef)
	}
	if w.ResourceKind == "gpu" {
		cmd.Env = append(cmd.Env, "CUDA_VISIBLE_DEVICES="+strings.Join(bindings, ","))
	}
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
	if err := cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		return fmt.Errorf("start %q: %w", w.Argv[0], err)
	}
	if err := e.Store.MarkAttemptRunning(context.Background(), launch.Attempt.ID, cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		stdout.Close()
		stderr.Close()
		return err
	}
	e.mu.Lock()
	e.running[launch.Attempt.ID] = cmd
	e.mu.Unlock()
	e.Logger.Info("attempt started",
		"workload_id", w.ID, "attempt_id", launch.Attempt.ID,
		"pid", cmd.Process.Pid, "resources", resourceIDs)
	go func() {
		err := cmd.Wait()
		stdout.Close()
		stderr.Close()
		exitCode := cmd.ProcessState.ExitCode()
		if err != nil {
			e.Logger.Warn("attempt exited", "attempt_id", launch.Attempt.ID, "exit_code", exitCode, "error", err)
		} else {
			e.Logger.Info("attempt exited", "attempt_id", launch.Attempt.ID, "exit_code", exitCode)
		}
		if markErr := e.Store.MarkAttemptExited(context.Background(), launch.Attempt.ID, exitCode); markErr != nil {
			e.Logger.Error("could not record attempt exit", "attempt_id", launch.Attempt.ID, "error", markErr)
		}
		e.mu.Lock()
		delete(e.running, launch.Attempt.ID)
		e.mu.Unlock()
	}()
	return nil
}
