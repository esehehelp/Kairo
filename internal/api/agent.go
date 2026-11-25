package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"kairo/internal/agentops"
	"kairo/internal/store"
)

// The agent API lets a node agent on another host drive the coordination
// operations the daemon's in-process executors call on the store. A node token
// is bound to one node: every operation is checked against the node that owns
// the executor, lease, attempt, provider or log it names. The operations are
// declared once, in internal/agentops, which the agents' client calls too.

// AgentLogAppend ships a slice of an attempt log. Offset is the byte offset of
// Data in the node-side file, which makes a retried append idempotent.
type AgentLogAppend struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

var attemptLogName = regexp.MustCompile(`^([A-Za-z0-9_-]+)\.(stdout|stderr)\.log$`)

type agentOp struct {
	name    string
	handler http.HandlerFunc
}

// handle serves op: it decodes the request once, checks that the caller's
// node owns the object it names (owner returns that node), applies op's
// observe-only rule, and runs the operation.
func handle[Req, Resp any](s *Server, op agentops.Op[Req, Resp], owner func(context.Context, Req) (string, error), run func(context.Context, Req) (Resp, error)) agentOp {
	return agentOp{name: op.Name, handler: func(w http.ResponseWriter, r *http.Request) {
		var body Req
		limit := op.MaxBody
		if limit == 0 {
			limit = 1 << 20
		}
		if err := decodeUpTo(r, &body, limit); err != nil {
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
		if s.ObserveOnly && op.Observe == agentops.Empty {
			writeJSON(w, http.StatusOK, op.Empty)
			return
		}
		if s.ObserveOnly && op.Observe == agentops.Refused {
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

// recorded is the answer of an operation that only records, once it has.
func recorded(err error) (agentops.OK, error) { return agentops.OK{OK: true}, err }

func (s *Server) agentOps() []agentOp {
	byExecutor := func(ctx context.Context, b agentops.ExecutorRequest) (string, error) {
		return s.Store.ExecutorNode(ctx, b.ExecutorID)
	}
	byNode := func(_ context.Context, b agentops.NodeRequest) (string, error) { return b.NodeID, nil }
	byLease := func(ctx context.Context, b agentops.LeaseRequest) (string, error) {
		return s.Store.LeaseNode(ctx, b.LeaseID)
	}
	byInput := func(ctx context.Context, b agentops.InputPathRequest) (string, error) {
		return s.Store.ExecutorNode(ctx, b.ExecutorID)
	}
	byObjectStore := func(ctx context.Context, b agentops.ObjectStoreRequest) (string, error) {
		return s.Store.ExecutorNode(ctx, b.ExecutorID)
	}
	return []agentOp{
		handle(s, agentops.Register,
			func(_ context.Context, b agentops.RegisterRequest) (string, error) { return b.Node.ID, nil },
			s.agentRegister),
		handle(s, agentops.Observations,
			func(ctx context.Context, b agentops.ObservationRequest) (string, error) {
				return s.Store.ProviderNode(ctx, b.ProviderID)
			},
			func(ctx context.Context, b agentops.ObservationRequest) (agentops.OK, error) {
				return recorded(s.Store.ApplyObservationBatch(ctx, b.ProviderID, b.Resources, b.Observations, b.Claims))
			}),
		handle(s, agentops.ReserveNext, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.ReservationAnswer, error) {
				reservation, err := s.Store.ReserveNext(ctx, b.ExecutorID)
				return agentops.ReservationAnswer{Reservation: reservation}, err
			}),
		handle(s, agentops.EnsurePreemption, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.CommandsAnswer, error) {
				commands, err := s.Store.EnsurePriorityPreemption(ctx, b.ExecutorID)
				return agentops.CommandsAnswer{Commands: commands}, err
			}),
		handle(s, agentops.Stores, byNode,
			func(ctx context.Context, b agentops.NodeRequest) (agentops.StoresAnswer, error) {
				stores, err := s.Store.NodeStores(ctx, b.NodeID)
				return agentops.StoresAnswer{Stores: stores}, err
			}),
		handle(s, agentops.StoreObservation,
			func(ctx context.Context, b agentops.StoreObservationRequest) (string, error) {
				return s.Store.StoreNode(ctx, b.Observation.StoreID)
			},
			func(ctx context.Context, b agentops.StoreObservationRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordStoreObservation(ctx, b.Observation))
			}),
		handle(s, agentops.AttemptScratch,
			func(ctx context.Context, b agentops.AttemptScratchRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.Scratch.AttemptID)
			},
			func(ctx context.Context, b agentops.AttemptScratchRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordAttemptScratch(ctx, b.Scratch))
			}),
		handle(s, agentops.ScratchToSettle, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.ScratchAnswer, error) {
				scratch, err := s.Store.ScratchToSettle(ctx, b.ExecutorID)
				return agentops.ScratchAnswer{Scratch: scratch}, err
			}),
		handle(s, agentops.SettleScratch,
			func(ctx context.Context, b agentops.SettleScratchRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.Settled.AttemptID)
			},
			func(ctx context.Context, b agentops.SettleScratchRequest) (agentops.OK, error) {
				return recorded(s.Store.SettleScratch(ctx, b.Settled))
			}),
		handle(s, agentops.RunOutput,
			func(ctx context.Context, b agentops.RunOutputRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.Output.AttemptID)
			},
			func(ctx context.Context, b agentops.RunOutputRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordRunOutput(ctx, b.Output))
			}),
		handle(s, agentops.PacksToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.PacksAnswer, error) {
				packs, err := s.Store.PacksToMake(ctx, b.ExecutorID)
				return agentops.PacksAnswer{Packs: packs}, err
			}),
		handle(s, agentops.RecordPack,
			func(ctx context.Context, b agentops.RecordPackRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Made.ExecutorID)
			},
			func(ctx context.Context, b agentops.RecordPackRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordPack(ctx, b.Made))
			}),
		handle(s, agentops.CopiesToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.CopiesAnswer, error) {
				copies, err := s.Store.CopiesToMake(ctx, b.ExecutorID)
				return agentops.CopiesAnswer{Copies: copies}, err
			}),
		handle(s, agentops.RecordDescribed, byObjectStore,
			func(ctx context.Context, b agentops.ObjectStoreRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordDescribed(ctx, b.ExecutorID, b.ObjectID, b.StoreID))
			}),
		handle(s, agentops.AbandonPack,
			func(ctx context.Context, b agentops.AbandonPackRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.ExecutorID)
			},
			func(ctx context.Context, b agentops.AbandonPackRequest) (agentops.OK, error) {
				return recorded(s.Store.AbandonPack(ctx, b.ExecutorID, b.ObjectID, b.Reason))
			}),
		handle(s, agentops.InPlaceSources, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.InPlaceSourcesAnswer, error) {
				sources, err := s.Store.InPlaceSources(ctx, b.ExecutorID)
				if sources == nil {
					sources = []store.InPlaceSource{}
				}
				return agentops.InPlaceSourcesAnswer{Sources: sources}, err
			}),
		handle(s, agentops.InputGone, byInput,
			func(ctx context.Context, b agentops.InputPathRequest) (agentops.OK, error) {
				return recorded(s.Store.InputGone(ctx, b.ExecutorID, b.ObjectID, b.Path))
			}),
		handle(s, agentops.InputIsFile, byInput,
			func(ctx context.Context, b agentops.InputPathRequest) (agentops.OK, error) {
				return recorded(s.Store.InputIsFile(ctx, b.ExecutorID, b.ObjectID, b.Path))
			}),
		handle(s, agentops.ClaimShed, byObjectStore,
			func(ctx context.Context, b agentops.ObjectStoreRequest) (agentops.OK, error) {
				return recorded(s.Store.ClaimShed(ctx, b.ExecutorID, b.ObjectID, b.StoreID))
			}),
		handle(s, agentops.RawPlaced,
			func(ctx context.Context, b agentops.RawPlacedRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.ExecutorID)
			},
			func(ctx context.Context, b agentops.RawPlacedRequest) (agentops.OK, error) {
				return recorded(s.Store.RawPlaced(ctx, b.ExecutorID, b.StoreID, b.Root, b.File))
			}),
		handle(s, agentops.KeepIdentity,
			func(ctx context.Context, b agentops.KeepIdentityRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.ExecutorID)
			},
			func(ctx context.Context, b agentops.KeepIdentityRequest) (agentops.OK, error) {
				return recorded(s.Store.KeepIdentity(ctx, b.ExecutorID, b.ObjectID, b.StoreID, b.List))
			}),
		handle(s, agentops.InventoryDurable,
			func(ctx context.Context, b agentops.InventoryRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Listing.ExecutorID)
			},
			func(ctx context.Context, b agentops.InventoryRequest) (store.DurableInventory, error) {
				return s.Store.InventoryDurable(ctx, b.Listing)
			}),
		handle(s, agentops.AdoptCopies,
			func(ctx context.Context, b agentops.AdoptCopiesRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.ExecutorID)
			},
			func(ctx context.Context, b agentops.AdoptCopiesRequest) (agentops.AdoptedAnswer, error) {
				n, err := s.Store.AdoptCopies(ctx, b.ExecutorID, b.StoreID, b.Root, b.Copies)
				return agentops.AdoptedAnswer{Adopted: n}, err
			}),
		handle(s, agentops.RecordCopy,
			func(ctx context.Context, b agentops.RecordCopyRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Made.ExecutorID)
			},
			func(ctx context.Context, b agentops.RecordCopyRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordCopy(ctx, b.Made))
			}),
		handle(s, agentops.FetchesToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.FetchesAnswer, error) {
				fetches, err := s.Store.FetchesToMake(ctx, b.ExecutorID)
				return agentops.FetchesAnswer{Fetches: fetches}, err
			}),
		handle(s, agentops.RecordFetch,
			func(ctx context.Context, b agentops.RecordFetchRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Made.ExecutorID)
			},
			func(ctx context.Context, b agentops.RecordFetchRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordFetch(ctx, b.Made))
			}),
		handle(s, agentops.DeletionsToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.DeletionsAnswer, error) {
				deletions, err := s.Store.DeletionsToMake(ctx, b.ExecutorID)
				return agentops.DeletionsAnswer{Deletions: deletions}, err
			}),
		handle(s, agentops.SettleDeletion,
			func(ctx context.Context, b agentops.SettleDeletionRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Settled.ExecutorID)
			},
			func(ctx context.Context, b agentops.SettleDeletionRequest) (agentops.OK, error) {
				return recorded(s.Store.SettleDeletion(ctx, b.Settled))
			}),
		handle(s, agentops.SourcesInUse, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.SourceIDsAnswer, error) {
				ids, err := s.Store.SourcesInUse(ctx, b.ExecutorID)
				return agentops.SourceIDsAnswer{Sources: ids}, err
			}),
		handle(s, agentops.EvictionsToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.EvictionsAnswer, error) {
				evictions, err := s.Store.EvictionsToMake(ctx, b.ExecutorID)
				return agentops.EvictionsAnswer{Evictions: evictions}, err
			}),
		handle(s, agentops.SettleEviction,
			func(ctx context.Context, b agentops.SettleEvictionRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Settled.ExecutorID)
			},
			func(ctx context.Context, b agentops.SettleEvictionRequest) (agentops.OK, error) {
				return recorded(s.Store.SettleEviction(ctx, b.Settled))
			}),
		handle(s, agentops.ScrubsToMake, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.ScrubsAnswer, error) {
				scrubs, err := s.Store.ScrubsToMake(ctx, b.ExecutorID)
				return agentops.ScrubsAnswer{Scrubs: scrubs}, err
			}),
		handle(s, agentops.RecordScrub,
			func(ctx context.Context, b agentops.RecordScrubRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.Result.ExecutorID)
			},
			func(ctx context.Context, b agentops.RecordScrubRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordScrub(ctx, b.Result))
			}),
		handle(s, agentops.PackRead,
			func(_ context.Context, b agentops.PackReadRequest) (string, error) { return b.NodeID, nil },
			func(ctx context.Context, b agentops.PackReadRequest) (agentops.PackReadAnswer, error) {
				read, err := s.Store.AuthorizePackRead(ctx, b.NodeID, b.ObjectID, b.Token)
				return agentops.PackReadAnswer{Read: read}, err
			}),
		handle(s, agentops.UnlaunchedLeases,
			func(ctx context.Context, b agentops.UnlaunchedRequest) (string, error) {
				return s.Store.ExecutorNode(ctx, b.ExecutorID)
			},
			func(ctx context.Context, b agentops.UnlaunchedRequest) (agentops.LeasesAnswer, error) {
				leases, err := s.Store.ListUnlaunchedLeases(ctx, b.ExecutorID, time.Duration(b.OlderThanSeconds*float64(time.Second)))
				return agentops.LeasesAnswer{Leases: leases}, err
			}),
		handle(s, agentops.ValidateReservation, byLease,
			func(ctx context.Context, b agentops.LeaseRequest) (agentops.OK, error) {
				return recorded(s.Store.ValidateReservation(ctx, b.LeaseID, b.Epoch))
			}),
		handle(s, agentops.MarkLeasePrepared, byLease,
			func(ctx context.Context, b agentops.LeaseRequest) (agentops.OK, error) {
				return recorded(s.Store.MarkLeasePrepared(ctx, b.LeaseID, b.Epoch))
			}),
		handle(s, agentops.ReleaseReservation, byLease,
			func(ctx context.Context, b agentops.LeaseRequest) (agentops.OK, error) {
				return recorded(s.Store.ReleaseReservation(ctx, b.LeaseID, b.Epoch, b.Reason))
			}),
		handle(s, agentops.AuthorizeLaunch,
			func(ctx context.Context, b agentops.AuthorizeRequest) (string, error) {
				return s.Store.LeaseNode(ctx, b.Reservation.Lease.ID)
			},
			func(ctx context.Context, b agentops.AuthorizeRequest) (agentops.LaunchAnswer, error) {
				launch, err := s.Store.AuthorizeLaunch(ctx, &b.Reservation)
				return agentops.LaunchAnswer{Launch: launch}, err
			}),
		handle(s, agentops.ActivateLaunch,
			func(ctx context.Context, b agentops.ActivateRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.ActivateRequest) (agentops.OK, error) {
				return recorded(s.Store.ActivateLaunch(ctx, b.AttemptID, b.LeaseID, b.Epoch, b.Token, b.PID, b.ProcessIdentity))
			}),
		handle(s, agentops.SetLogPaths,
			func(ctx context.Context, b agentops.LogPathsRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			s.agentSetLogPaths),
		handle(s, agentops.LogProblem,
			func(ctx context.Context, b agentops.LogProblemRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.LogProblemRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordAttemptLogProblem(ctx, b.AttemptID, b.Problem))
			}),
		handle(s, agentops.MarkProcessExited,
			func(ctx context.Context, b agentops.ProcessExitedRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.ProcessExitedRequest) (agentops.OK, error) {
				return recorded(s.Store.MarkAttemptProcessExited(ctx, b.AttemptID, b.Role, b.Rank, b.Identity))
			}),
		handle(s, agentops.RecordTerminal,
			func(ctx context.Context, b agentops.TerminalRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.TerminalRequest) (agentops.OK, error) {
				return recorded(s.Store.RecordTerminal(ctx, b.AttemptID, b.LeaseID, b.Epoch, b.ExitCode, b.ExitSignal))
			}),
		handle(s, agentops.QuiescenceCandidates, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.CandidatesAnswer, error) {
				candidates, err := s.Store.ListQuiescenceCandidates(ctx, b.ExecutorID)
				return agentops.CandidatesAnswer{Candidates: candidates}, err
			}),
		handle(s, agentops.FinalizeQuiescence,
			func(ctx context.Context, b agentops.AttemptLeaseRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.AttemptLeaseRequest) (agentops.OK, error) {
				return recorded(s.Store.FinalizeQuiescence(ctx, b.AttemptID, b.LeaseID, b.Epoch))
			}),
		handle(s, agentops.QuarantineTerms, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.TerminationsAnswer, error) {
				terminations, err := s.Store.ListQuarantineTerminations(ctx, b.ExecutorID)
				return agentops.TerminationsAnswer{Terminations: terminations}, err
			}),
		handle(s, agentops.QuarantineTerminated,
			func(ctx context.Context, b agentops.AttemptRequest) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.AttemptRequest) (agentops.OK, error) {
				return recorded(s.Store.MarkQuarantineTerminated(ctx, b.AttemptID))
			}),
		handle(s, agentops.ForceStopOrders, byExecutor,
			func(ctx context.Context, b agentops.ExecutorRequest) (agentops.ForceStopOrdersAnswer, error) {
				orders, err := s.Store.ForceStopOrders(ctx, b.ExecutorID)
				return agentops.ForceStopOrdersAnswer{Orders: orders}, err
			}),
		handle(s, agentops.ForceStopAcked,
			func(ctx context.Context, b agentops.ForceStopAck) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.ForceStopAck) (agentops.OK, error) {
				return recorded(s.Store.AckForceStop(ctx, b.AttemptID, b.Phase, b.Detail))
			}),
		handle(s, agentops.AttemptUsed,
			func(ctx context.Context, b agentops.AttemptUsage) (string, error) {
				return s.Store.AttemptNode(ctx, b.AttemptID)
			},
			func(ctx context.Context, b agentops.AttemptUsage) (agentops.OK, error) {
				return recorded(s.Store.RecordAttemptUsage(ctx, b.AttemptID, b.RAMBytes, b.DiskWrittenBytes))
			}),
	}
}

func (s *Server) agentRegister(ctx context.Context, b agentops.RegisterRequest) (agentops.RegisterAnswer, error) {
	var none agentops.RegisterAnswer
	if b.Node.ID == "" || b.Node.Name == "" {
		return none, errors.New("node.id and node.name are required")
	}
	if b.UptimeSeconds != nil {
		nanos := math.Ceil(*b.UptimeSeconds * float64(time.Second))
		if !(nanos > 0 && nanos < float64(math.MaxInt64)) {
			return none, errors.New("uptime_seconds must be positive and fit in a duration")
		}
		// Use the daemon's clock, which also stamps attempt activation.
		bootedAt := time.Now().UTC().Add(-time.Duration(nanos)).Format(time.RFC3339Nano)
		b.Node.BootedAt = &bootedAt
	}
	b.Node.Enabled = true
	if err := s.Store.UpsertNode(ctx, b.Node); err != nil {
		return none, err
	}
	for _, p := range b.Providers {
		if p.ID == "" || p.Kind == "" {
			return none, errors.New("provider id and kind are required")
		}
		if err := s.Store.UpsertProvider(ctx, p.ID, b.Node.ID, p.Kind, nil); err != nil {
			return none, err
		}
	}
	stale := map[string]int{}
	adopted := []string{}
	declared := make([]string, 0, len(b.Executors))
	for _, e := range b.Executors {
		if e.ID == "" {
			return none, errors.New("executor id is required")
		}
		e.NodeID = b.Node.ID
		if err := s.Store.UpsertExecutor(ctx, e); err != nil {
			return none, err
		}
		declared = append(declared, e.ID)
		if e.Enabled {
			// A (re)starting agent vouches only for the attempts it adopts;
			// every other lease its previous incarnation held goes stale
			// until quiescence is proven.
			n, kept, err := s.Store.ReregisterExecutor(ctx, e.ID, b.Adopt)
			if err != nil {
				return none, err
			}
			stale[e.ID] = n
			adopted = append(adopted, kept...)
		}
	}
	// An executor of the node the agent no longer declares is no longer run.
	if _, err := s.Store.UndeclareExecutors(ctx, b.Node.ID, declared); err != nil {
		return none, err
	}
	return agentops.RegisterAnswer{StaleLeases: stale, Adopted: adopted}, nil
}

// agentSetLogPaths records the daemon-side copies of an attempt's logs:
// logs are shipped into the daemon's log directory under their own names,
// which must be the attempt's.
func (s *Server) agentSetLogPaths(ctx context.Context, b agentops.LogPathsRequest) (agentops.OK, error) {
	paths := make([]string, 0, 2)
	for _, path := range []string{b.StdoutPath, b.StderrPath} {
		// The agent may be on another OS: a Windows path uses backslashes.
		name := path[strings.LastIndexAny(path, `/\`)+1:]
		if attempt, _ := logAttempt(name); attempt != b.AttemptID {
			return agentops.OK{}, fmt.Errorf("log %q is not named after attempt %s", name, b.AttemptID)
		}
		local, err := s.agentLogPath(name)
		if err != nil {
			return agentops.OK{}, err
		}
		paths = append(paths, local)
	}
	if !strings.HasSuffix(paths[0], ".stdout.log") || !strings.HasSuffix(paths[1], ".stderr.log") {
		return agentops.OK{}, errors.New("stdout_path and stderr_path must name the .stdout.log and .stderr.log files")
	}
	return recorded(s.Store.SetAttemptLogPaths(ctx, b.AttemptID, b.Epoch, paths[0], paths[1]))
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
		writeLogDirError(w, err)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		writeLogDirError(w, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		writeLogDirError(w, err)
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
			// A partial write is kept: the retry appends after it.
			writeLogDirError(w, err)
			return
		}
		size += int64(len(b.Data)) - skip
	}
	writeJSON(w, http.StatusOK, map[string]any{"size": size})
}

// writeLogDirError reports a failure of the daemon's own log directory (a
// full disk, say) as a daemon-side failure, which the agent retries from the
// daemon's size, rather than as a bad request.
func writeLogDirError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "daemon log directory: " + err.Error(), "code": codeInternal})
}
