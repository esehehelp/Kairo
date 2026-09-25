package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"

	"kairo/internal/store"
)

// The agent API lets a node agent on another host drive the same coordination
// operations the daemon's in-process executors call on the store. It is only
// mounted when a shared bearer token is configured, and carries no TLS: it is
// meant for a trusted LAN or an SSH tunnel.

type agentRegisterRequest struct {
	Node      store.Node       `json:"node"`
	Executors []store.Executor `json:"executors"`
	Providers []agentProvider  `json:"providers"`
}

type agentProvider struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

type agentObservationRequest struct {
	ProviderID   string                   `json:"provider_id"`
	Resources    []store.ResourceInstance `json:"resources"`
	Observations []store.Observation      `json:"observations"`
	Claims       []store.ExternalClaim    `json:"claims"`
}

type agentExecutorRequest struct {
	ExecutorID string `json:"executor_id"`
}

type agentLeaseRequest struct {
	LeaseID string `json:"lease_id"`
	Epoch   int64  `json:"epoch"`
	Reason  string `json:"reason,omitempty"`
}

type agentAuthorizeRequest struct {
	Reservation store.Reservation `json:"reservation"`
}

type agentActivateRequest struct {
	AttemptID       string `json:"attempt_id"`
	LeaseID         string `json:"lease_id"`
	Epoch           int64  `json:"epoch"`
	Token           string `json:"token"`
	PID             int    `json:"pid"`
	ProcessIdentity string `json:"process_identity"`
}

type agentLogPathsRequest struct {
	AttemptID  string `json:"attempt_id"`
	Epoch      int64  `json:"epoch"`
	StdoutPath string `json:"stdout_path"`
	StderrPath string `json:"stderr_path"`
}

type agentProcessExitedRequest struct {
	AttemptID string `json:"attempt_id"`
	Role      string `json:"role"`
	Rank      int    `json:"rank"`
	Identity  string `json:"identity"`
}

type agentTerminalRequest struct {
	AttemptID  string `json:"attempt_id"`
	LeaseID    string `json:"lease_id"`
	Epoch      int64  `json:"epoch"`
	ExitCode   int    `json:"exit_code"`
	ExitSignal string `json:"exit_signal"`
}

type agentAttemptLeaseRequest struct {
	AttemptID string `json:"attempt_id"`
	LeaseID   string `json:"lease_id"`
	Epoch     int64  `json:"epoch"`
}

// AgentLogAppend ships a slice of an attempt log. Offset is the byte offset of
// Data in the node-side file, which makes a retried append idempotent.
type AgentLogAppend struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

var attemptLogName = regexp.MustCompile(`^[A-Za-z0-9_-]+\.(stdout|stderr)\.log$`)

// Sentinel errors travel as stable codes so the agent client can return the
// same error values the store would have returned in-process.
var agentErrorCodes = []struct {
	code string
	err  error
}{
	{"not_found", store.ErrNotFound},
	{"stale_epoch", store.ErrStaleEpoch},
	{"gate_closed", store.ErrGateClosed},
	{"execution_started", store.ErrExecutionStarted},
	{"idempotency_conflict", store.ErrIdempotencyConflict},
}

// AgentErrorForCode maps a wire code back to the store sentinel (nil if none).
func AgentErrorForCode(code string) error {
	for _, c := range agentErrorCodes {
		if c.code == code {
			return c.err
		}
	}
	return nil
}

func writeAgentError(w http.ResponseWriter, err error) {
	code := ""
	for _, c := range agentErrorCodes {
		if errors.Is(err, c.err) {
			code = c.code
			break
		}
	}
	status := http.StatusBadRequest
	if code != "" {
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error(), "code": code})
}

func (s *Server) mountAgent(mux *http.ServeMux) {
	if s.AgentToken == "" {
		return
	}
	route := func(name string, h func(*http.Request) (any, error)) {
		mux.HandleFunc("POST /v3/agent/"+name, s.agentAuth(func(w http.ResponseWriter, r *http.Request) {
			value, err := h(r)
			if err != nil {
				writeAgentError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, value)
		}))
	}
	route("register", s.agentRegister)
	route("observations", func(r *http.Request) (any, error) {
		var b agentObservationRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.ApplyObservationBatch(r.Context(), b.ProviderID, b.Resources, b.Observations, b.Claims)
	})
	route("reserve-next", func(r *http.Request) (any, error) {
		var b agentExecutorRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		reservation, err := s.Store.ReserveNext(r.Context(), b.ExecutorID)
		return map[string]any{"reservation": reservation}, err
	})
	route("ensure-preemption", func(r *http.Request) (any, error) {
		var b agentExecutorRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		commands, err := s.Store.EnsurePriorityPreemption(r.Context(), b.ExecutorID)
		return map[string]any{"commands": commands}, err
	})
	route("validate-reservation", s.agentLease(s.Store.ValidateReservation))
	route("mark-lease-prepared", s.agentLease(s.Store.MarkLeasePrepared))
	route("release-reservation", func(r *http.Request) (any, error) {
		var b agentLeaseRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.ReleaseReservation(r.Context(), b.LeaseID, b.Epoch, b.Reason)
	})
	route("authorize-launch", func(r *http.Request) (any, error) {
		var b agentAuthorizeRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		launch, err := s.Store.AuthorizeLaunch(r.Context(), &b.Reservation)
		return map[string]any{"launch": launch}, err
	})
	route("activate-launch", func(r *http.Request) (any, error) {
		var b agentActivateRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.ActivateLaunch(r.Context(), b.AttemptID, b.LeaseID, b.Epoch, b.Token, b.PID, b.ProcessIdentity)
	})
	route("set-log-paths", func(r *http.Request) (any, error) {
		var b agentLogPathsRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		// Logs are shipped into the daemon's log directory under the same
		// file names, so the recorded paths are the daemon-side copies.
		stdout, err := s.agentLogPath(filepath.Base(filepath.ToSlash(b.StdoutPath)))
		if err != nil {
			return nil, err
		}
		stderr, err := s.agentLogPath(filepath.Base(filepath.ToSlash(b.StderrPath)))
		if err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.SetAttemptLogPaths(r.Context(), b.AttemptID, b.Epoch, stdout, stderr)
	})
	route("mark-process-exited", func(r *http.Request) (any, error) {
		var b agentProcessExitedRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.MarkAttemptProcessExited(r.Context(), b.AttemptID, b.Role, b.Rank, b.Identity)
	})
	route("record-terminal", func(r *http.Request) (any, error) {
		var b agentTerminalRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.RecordTerminal(r.Context(), b.AttemptID, b.LeaseID, b.Epoch, b.ExitCode, b.ExitSignal)
	})
	route("quiescence-candidates", func(r *http.Request) (any, error) {
		var b agentExecutorRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		candidates, err := s.Store.ListQuiescenceCandidates(r.Context(), b.ExecutorID)
		return map[string]any{"candidates": candidates}, err
	})
	route("finalize-quiescence", func(r *http.Request) (any, error) {
		var b agentAttemptLeaseRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, s.Store.FinalizeQuiescence(r.Context(), b.AttemptID, b.LeaseID, b.Epoch)
	})
	mux.HandleFunc("POST /v3/agent/logs", s.agentAuth(s.agentLogAppend))
}

func (s *Server) agentAuth(next http.HandlerFunc) http.HandlerFunc {
	want := []byte("Bearer " + s.AgentToken)
	return func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "agent token required"})
			return
		}
		next(w, r)
	}
}

func (s *Server) agentLease(op func(ctx context.Context, leaseID string, epoch int64) error) func(*http.Request) (any, error) {
	return func(r *http.Request) (any, error) {
		var b agentLeaseRequest
		if err := decode(r, &b); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, op(r.Context(), b.LeaseID, b.Epoch)
	}
}

func (s *Server) agentRegister(r *http.Request) (any, error) {
	var b agentRegisterRequest
	if err := decode(r, &b); err != nil {
		return nil, err
	}
	if b.Node.ID == "" || b.Node.Name == "" {
		return nil, errors.New("node.id and node.name are required")
	}
	ctx := r.Context()
	b.Node.Enabled = true
	if err := s.Store.UpsertNode(ctx, b.Node); err != nil {
		return nil, err
	}
	for _, p := range b.Providers {
		if p.ID == "" || p.Kind == "" {
			return nil, errors.New("provider id and kind are required")
		}
		if err := s.Store.UpsertProvider(ctx, p.ID, b.Node.ID, p.Kind, nil); err != nil {
			return nil, err
		}
	}
	stale := map[string]int{}
	for _, e := range b.Executors {
		if e.ID == "" {
			return nil, errors.New("executor id is required")
		}
		e.NodeID = b.Node.ID
		if err := s.Store.UpsertExecutor(ctx, e); err != nil {
			return nil, err
		}
		if e.Enabled {
			// A (re)starting agent cannot vouch for leases its previous
			// incarnation held; they go stale until quiescence is proven.
			n, err := s.Store.MarkExecutorUnknown(ctx, e.ID)
			if err != nil {
				return nil, err
			}
			stale[e.ID] = n
		}
	}
	return map[string]any{"stale_leases": stale}, nil
}

func (s *Server) agentLogPath(name string) (string, error) {
	if !attemptLogName.MatchString(name) {
		return "", fmt.Errorf("invalid attempt log name %q", name)
	}
	if s.LogDirectory == "" {
		return "", errors.New("daemon has no log directory")
	}
	return filepath.Join(s.LogDirectory, name), nil
}

func (s *Server) agentLogAppend(w http.ResponseWriter, r *http.Request) {
	var b AgentLogAppend
	// Log slices exceed decode()'s 1 MiB request cap once base64-encoded.
	dec := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		writeAgentError(w, err)
		return
	}
	path, err := s.agentLogPath(b.Name)
	if err != nil {
		writeAgentError(w, err)
		return
	}
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		writeAgentError(w, err)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		writeAgentError(w, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeAgentError(w, err)
		return
	}
	size := info.Size()
	if b.Offset > size {
		// A gap: the agent must resend from the daemon's current size.
		writeJSON(w, http.StatusConflict, map[string]any{"error": "log gap", "code": "log_gap", "size": size})
		return
	}
	if skip := size - b.Offset; skip < int64(len(b.Data)) {
		if _, err = f.WriteAt(b.Data[skip:], size); err != nil {
			writeAgentError(w, err)
			return
		}
		size += int64(len(b.Data)) - skip
	}
	writeJSON(w, http.StatusOK, map[string]any{"size": size})
}
