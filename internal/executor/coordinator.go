package executor

import (
	"context"
	"encoding/json"
	"time"

	"kairo/internal/loader"
	"kairo/internal/machine"
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
	RecordAttemptLogProblem(ctx context.Context, attemptID string, problem store.LogProblem) error
	MarkAttemptProcessExited(ctx context.Context, attemptID, role string, rank int, identity string) error
	RecordTerminal(ctx context.Context, attemptID, leaseID string, epoch int64, exitCode int, exitSignal string) error
	ListQuiescenceCandidates(ctx context.Context, executorID string) ([]store.QuiescenceCandidate, error)
	FinalizeQuiescence(ctx context.Context, attemptID, leaseID string, epoch int64) error
	ReleaseReservation(ctx context.Context, leaseID string, epoch int64, reason string) error
	ListUnlaunchedLeases(ctx context.Context, executorID string, olderThan time.Duration) ([]store.UnlaunchedLease, error)
	ListQuarantineTerminations(ctx context.Context, executorID string) ([]store.QuarantineTermination, error)
	MarkQuarantineTerminated(ctx context.Context, attemptID string) error
	ForceStopOrders(ctx context.Context, executorID string) ([]store.ForceStopOrder, error)
	AckForceStop(ctx context.Context, attemptID, phase string, detail json.RawMessage) error
	RecordAttemptUsage(ctx context.Context, attemptID string, ramBytes, diskWrittenBytes *int64) error
	// RecordAttemptStreams: what the loaders of an attempt's streams said
	// last (streamstatus.go).
	RecordAttemptStreams(ctx context.Context, attemptID string, streams []store.AttemptStream) error
	NodeStores(ctx context.Context, nodeID string) ([]machine.Store, error)
	RecordStoreObservation(ctx context.Context, obs store.StoreObservation) error
	RecordAttemptScratch(ctx context.Context, scratch store.AttemptScratch) error
	ScratchToSettle(ctx context.Context, executorID string) ([]store.ScratchSettlement, error)
	SettleScratch(ctx context.Context, settled store.ScratchSettled) error
	RecordRunOutput(ctx context.Context, out store.RunOutput) error
	PacksToMake(ctx context.Context, executorID string) ([]store.PackOrder, error)
	RecordPack(ctx context.Context, made store.PackMade) error
	CopiesToMake(ctx context.Context, executorID string) ([]store.CopyOrder, error)
	RecordCopy(ctx context.Context, made store.CopyMade) error
	// RecordDescribed: a verified copy's description is written beside it.
	RecordDescribed(ctx context.Context, executorID, objectID, storeID string) error
	// AbandonPack: the source of an import to pack is gone.
	AbandonPack(ctx context.Context, executorID, objectID, reason string) error
	// ClaimShed: a copy of a store below its reserve (or of a dropped
	// object) is about to be removed.
	ClaimShed(ctx context.Context, executorID, objectID, storeID string) error
	// KeepIdentity: the file list beside a copy of a dropped object, kept
	// before the copy is removed.
	KeepIdentity(ctx context.Context, executorID, objectID, storeID, list string) error
	// RawPlaced: a raw file fetched, checked and kept on a volatile store
	// for streams (rawcache.go), and its identity.
	RawPlaced(ctx context.Context, executorID, storeID, root string, f loader.File) error
	// RawIdentity: what a raw file was found to be the first time it was
	// read; RawRecords: how many records a kept one holds, as counted here.
	RawIdentity(ctx context.Context, executorID, url string) (loader.File, bool, error)
	RawRecords(ctx context.Context, executorID string, count store.RecordsCount) error
	// InputGone: an input's path is not there at launch.
	InputGone(ctx context.Context, executorID, objectID, path string) error
	// InputIsFile: an import read in place is a file, not a directory.
	InputIsFile(ctx context.Context, executorID, objectID, path string) error
	InPlaceSources(ctx context.Context, executorID string) ([]store.InPlaceSource, error)
	// InventoryDurable and AdoptCopies: what a durable store's directory holds.
	InventoryDurable(ctx context.Context, l store.DurableListing) (store.DurableInventory, error)
	AdoptCopies(ctx context.Context, executorID, storeID, root string, copies []store.AdoptedCopy) (int, error)
	FetchesToMake(ctx context.Context, executorID string) ([]store.FetchOrder, error)
	RecordFetch(ctx context.Context, made store.FetchMade) error
	AuthorizePackRead(ctx context.Context, nodeID, objectID, token string) (store.PackRead, error)
	// AuthorizeBenchNet: token is that of a live network bench measuring
	// nodeID's transfer endpoint (runID).
	AuthorizeBenchNet(ctx context.Context, nodeID, runID, token string) error
	DeletionsToMake(ctx context.Context, executorID string) ([]store.DeletionOrder, error)
	SettleDeletion(ctx context.Context, settled store.ObjectDeletion) error
	EvictionsToMake(ctx context.Context, executorID string) ([]store.EvictionOrder, error)
	SettleEviction(ctx context.Context, settled store.EvictionSettled) error
	ScrubsToMake(ctx context.Context, executorID string) ([]store.ScrubOrder, error)
	RecordScrub(ctx context.Context, result store.ScrubResult) error
}

var _ Coordinator = (*store.Store)(nil)
