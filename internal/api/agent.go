package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"kairo/internal/store"
)

// The agent API lets a node agent on another host drive the coordination
// operations the daemon's in-process executors call on the store. A node token
// is bound to one node: every operation is checked against the node that owns
// the executor, lease, attempt, provider or log it names.

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

type agentAttemptRequest struct {
	AttemptID string `json:"attempt_id"`
}

type agentForceStopAck struct {
	AttemptID string          `json:"attempt_id"`
	Phase     string          `json:"phase"`
	Detail    json.RawMessage `json:"detail,omitempty"`
}

// AgentLogAppend ships a slice of an attempt log. Offset is the byte offset of
// Data in the node-side file, which makes a retried append idempotent.
type AgentLogAppend struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

var attemptLogName = regexp.MustCompile(`^([A-Za-z0-9_-]+)\.(stdout|stderr)\.log$`)

// How an operation behaves when the daemon is observe-only, mirroring an
// in-process executor that only observes.
const (
	observeAllowed = iota // observation and facts about processes already running
	observeEmpty          // polls answer with nothing to do
	observeRefused        // actuation is refused (423)
)

type agentOp struct {
	name    string
	handler http.HandlerFunc
}

// newAgentOp decodes the request once, checks that the caller's node owns
// the object it names (owner returns that node), applies the observe-only
// rule, and runs the operation.
func newAgentOp[T any](s *Server, name string, observe int, empty any, owner func(context.Context, T) (string, error), run func(context.Context, T) (any, error)) agentOp {
	return agentOp{name: name, handler: func(w http.ResponseWriter, r *http.Request) {
		var body T
		if err := decode(r, &body); err != nil {
			writeError(w, err)
			return
		}
		node, err := owner(r.Context(), body)
		if err != nil {
			writeError(w, err)
			return
		}
		if node != principalOf(r).NodeID {
			writeError(w, fmt.Errorf("%w: belongs to node %s", ErrForbidden, node))
			return
		}
		if s.ObserveOnly && observe == observeEmpty {
			writeJSON(w, http.StatusOK, empty)
			return
		}
		if s.ObserveOnly && observe == observeRefused {
			writeError(w, errObserveOnly)
			return
		}
		value, err := run(r.Context(), body)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	}}
}

var okBody = map[string]bool{"ok": true}

func (s *Server) agentOps() []agentOp {
	byExecutor := func(ctx context.Context, b agentExecutorRequest) (string, error) {
		return s.Store.ExecutorNode(ctx, b.ExecutorID)
	}
	byLease := func(ctx context.Context, b agentLeaseRequest) (string, error) {
		return s.Store.LeaseNode(ctx, b.LeaseID)
	}
	return []agentOp{
		newAgentOp(s, "register", observeAllowed, nil,
			func(_ context.Context, b agentRegisterRequest) (string, error) { return b.Node.ID, nil },
			s.agentRegister),
		newAgentOp(s, "observations", observeAllowed, nil,
			func(ctx context.Context, b agentObservationRequest) (string, error) {
				return s.Store.ProviderNode(ctx, b.ProviderID)
			},
			func(ctx context.Context, b agentObservationRequest) (any, error) {
				return okBody, s.Store.ApplyObservationBatch(ctx, b.ProviderID, b.Resources, b.Observations, b.Claims)
			}),
		newAgentOp(s, "reserve-next", observeEmpty, map[string]any{"reservation": nil}, byExecutor,
			func(ctx context.Context, b agentExecutorRequest) (any, error) {
				reservation, err := s.Store.ReserveNext(ctx, b.ExecutorID)
				return map[string]any{"reservation": reservation}, err
			}),
		newAgentOp(s, "ensure-preemption", observeEmpty, map[string]any{"commands": []string{}}, byExecutor,
			func(ctx context.Context, b agentExecutorRequest) (any, error) {
				commands, err := s.Store.EnsurePriorityPreemption(ctx, b.ExecutorID)
				return map[string]any{"commands": commands}, err
			}),
		newAgentOp(s, "validate-reservation", observeRefused, nil, byLease,
			func(ctx context.Context, b agentLeaseRequest) (any, error) {
				return okBody, s.Store.ValidateReservation(ctx, b.LeaseID, b.Epoch)
			}),
		newAgentOp(s, "mark-lease-prepared", observeRefused, nil, byLease,
			func(ctx context.Context, b agentLeaseRequest) (any, error) {
				return okBody, s.Store.MarkLeasePrepared(ctx, b.LeaseID, b.Epoch)
			}),
		newAgentOp(s, "release-reservation", observeRefused, nil, byLease,
			func(ctx context.Context, b agentLeaseRequest) (any, error) {
				return okBody, s.Store.ReleaseReservation(ctx, b.LeaseID, b.Epoch, b.Reason)
			}),
		newAgentOp(s, "authorize-launch", observeRefused, nil,
			func(ctx context.Context, b agentAuthorizeRequest) (string, error) {
				return s.Store.LeaseNode(ctx, b.Reservation.Lease.ID)
			},
			func(ctx context.Context, b agentAuthorizeRequest) (any, error) {
				launch, err := s.Store.AuthorizeLaunch(ctx, &b.Reservation)
				return map[string]any{"launch": launch}, err
			}),
		newAgentOp(s, "activate-launch", observeRefused, nil,
			func(ctx context.Context, b agentActivateRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentActivateRequest) (any, error) {
				return okBody, s.Store.ActivateLaunch(ctx, b.AttemptID, b.LeaseID, b.Epoch, b.Token, b.PID, b.ProcessIdentity)
			}),
		newAgentOp(s, "set-log-paths", observeAllowed, nil,
			func(ctx context.Context, b agentLogPathsRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			s.agentSetLogPaths),
		newAgentOp(s, "mark-process-exited", observeAllowed, nil,
			func(ctx context.Context, b agentProcessExitedRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentProcessExitedRequest) (any, error) {
				return okBody, s.Store.MarkAttemptProcessExited(ctx, b.AttemptID, b.Role, b.Rank, b.Identity)
			}),
		newAgentOp(s, "record-terminal", observeAllowed, nil,
			func(ctx context.Context, b agentTerminalRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentTerminalRequest) (any, error) {
				return okBody, s.Store.RecordTerminal(ctx, b.AttemptID, b.LeaseID, b.Epoch, b.ExitCode, b.ExitSignal)
			}),
		newAgentOp(s, "quiescence-candidates", observeEmpty, map[string]any{"candidates": []store.QuiescenceCandidate{}}, byExecutor,
			func(ctx context.Context, b agentExecutorRequest) (any, error) {
				candidates, err := s.Store.ListQuiescenceCandidates(ctx, b.ExecutorID)
				return map[string]any{"candidates": candidates}, err
			}),
		newAgentOp(s, "finalize-quiescence", observeRefused, nil,
			func(ctx context.Context, b agentAttemptLeaseRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentAttemptLeaseRequest) (any, error) {
				return okBody, s.Store.FinalizeQuiescence(ctx, b.AttemptID, b.LeaseID, b.Epoch)
			}),
		newAgentOp(s, "quarantine-terminations", observeEmpty, map[string]any{"terminations": []store.QuarantineTermination{}}, byExecutor,
			func(ctx context.Context, b agentExecutorRequest) (any, error) {
				terminations, err := s.Store.ListQuarantineTerminations(ctx, b.ExecutorID)
				return map[string]any{"terminations": terminations}, err
			}),
		newAgentOp(s, "quarantine-terminated", observeRefused, nil,
			func(ctx context.Context, b agentAttemptRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentAttemptRequest) (any, error) {
				return okBody, s.Store.MarkQuarantineTerminated(ctx, b.AttemptID)
			}),
		newAgentOp(s, "force-stop-orders", observeEmpty, map[string]any{"orders": []store.ForceStopOrder{}}, byExecutor,
			func(ctx context.Context, b agentExecutorRequest) (any, error) {
				orders, err := s.Store.ForceStopOrders(ctx, b.ExecutorID)
				return map[string]any{"orders": orders}, err
			}),
		newAgentOp(s, "force-stop-ack", observeRefused, nil,
			func(ctx context.Context, b agentForceStopAck) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentForceStopAck) (any, error) {
				return okBody, s.Store.AckForceStop(ctx, b.AttemptID, b.Phase, b.Detail)
			}),
	}
}

func (s *Server) agentRegister(ctx context.Context, b agentRegisterRequest) (any, error) {
	if b.Node.ID == "" || b.Node.Name == "" {
		return nil, errors.New("node.id and node.name are required")
	}
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

// agentSetLogPaths records the daemon-side copies of an attempt's logs:
// logs are shipped into the daemon's log directory under their own names,
// which must be the attempt's.
func (s *Server) agentSetLogPaths(ctx context.Context, b agentLogPathsRequest) (any, error) {
	paths := make([]string, 0, 2)
	for _, path := range []string{b.StdoutPath, b.StderrPath} {
		// The agent may be on another OS: a Windows path uses backslashes.
		name := path[strings.LastIndexAny(path, `/\`)+1:]
		if attempt, _ := logAttempt(name); attempt != b.AttemptID {
			return nil, fmt.Errorf("log %q is not named after attempt %s", name, b.AttemptID)
		}
		local, err := s.agentLogPath(name)
		if err != nil {
			return nil, err
		}
		paths = append(paths, local)
	}
	if !strings.HasSuffix(paths[0], ".stdout.log") || !strings.HasSuffix(paths[1], ".stderr.log") {
		return nil, errors.New("stdout_path and stderr_path must name the .stdout.log and .stderr.log files")
	}
	return okBody, s.Store.SetAttemptLogPaths(ctx, b.AttemptID, b.Epoch, paths[0], paths[1])
}

// logAttempt returns the attempt an attempt log file is named after.
func logAttempt(name string) (string, bool) {
	match := attemptLogName.FindStringSubmatch(name)
	if match == nil {
		return "", false
	}
	return match[1], true
}

func (s *Server) agentLogPath(name string) (string, error) {
	if _, valid := logAttempt(name); !valid {
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
		writeError(w, err)
		return
	}
	path, err := s.agentLogPath(b.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	attempt, _ := logAttempt(b.Name)
	node, err := s.Store.AttemptNode(r.Context(), attempt)
	if err != nil {
		writeError(w, err)
		return
	}
	if node != principalOf(r).NodeID {
		writeError(w, fmt.Errorf("%w: attempt %s runs on node %s", ErrForbidden, attempt, node))
		return
	}
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		writeError(w, err)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		writeError(w, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeError(w, err)
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
			writeError(w, err)
			return
		}
		size += int64(len(b.Data)) - skip
	}
	writeJSON(w, http.StatusOK, map[string]any{"size": size})
}
