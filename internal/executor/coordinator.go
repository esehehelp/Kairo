package executor

import (
	"context"

	"kairo/internal/store"
)

// Coordinator is the part of the coordination store an executor drives. The
// daemon's in-process executors use *store.Store directly; a remote node agent
// uses an HTTP client with the same semantics (internal/agentclient), so the
// launch, fencing and quiescence logic below is identical on every node.
type Coordinator interface {
	ApplyObservationBatch(ctx context.Context, providerID string, resources []store.ResourceInstance, observations []store.Observation, claims []store.ExternalClaim) error
	ReserveNext(ctx context.Context, executorID string) (*store.Reservation, error)
	EnsurePriorityPreemption(ctx context.Context, executorID string) ([]string, error)
	ValidateReservation(ctx context.Context, leaseID string, epoch int64) error
	MarkLeasePrepared(ctx context.Context, leaseID string, epoch int64) error
	AuthorizeLaunch(ctx context.Context, reservation *store.Reservation) (*store.Launch, error)
	ActivateLaunch(ctx context.Context, attemptID, leaseID string, epoch int64, token string, pid int, processIdentity string) error
	SetAttemptLogPaths(ctx context.Context, attemptID string, epoch int64, stdoutPath, stderrPath string) error
	MarkAttemptProcessExited(ctx context.Context, attemptID, role string, rank int, identity string) error
	RecordTerminal(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, exitSignal string) error
	ListQuiescenceCandidates(ctx context.Context, executorID string) ([]store.QuiescenceCandidate, error)
	FinalizeQuiescence(ctx context.Context, attemptID, leaseID string, epoch int64) error
	ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error
}

var _ Coordinator = (*store.Store)(nil)
