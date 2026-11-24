// Package agentops declares the operations of the agent API once (POST
// /api/agent/<name>): the requests node agents and device controllers send
// the daemon and its answers. The daemon registers a handler for each
// (internal/api), the agents call them (internal/agentclient); adding an
// operation is its store method plus a declaration here.
//
// The JSON is the wire format between versions: an agent and its daemon are
// swapped at different times, so a field is never renamed, and one added
// is optional (omitempty, or ignored by a daemon that lacks it only when it
// does not decode strictly: the daemon refuses unknown fields).
package agentops

import (
	"encoding/json"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// Observe is what an observe-only daemon does with an operation, mirroring
// an in-process executor that only observes.
type Observe int

const (
	// Allowed: observation, and facts about processes already running.
	Allowed Observe = iota
	// Empty: a poll answered with nothing to do (the operation's Empty).
	Empty
	// Refused: actuation, refused (423).
	Refused
)

// Op is one operation: POST /api/agent/<Name> with a Req, answered with a
// Resp. Empty is the answer of an observe-only daemon to an Empty
// operation.
type Op[Req, Resp any] struct {
	Name    string
	Observe Observe
	Empty   Resp
}

// Named is any operation, by its name.
type Named interface{ OpName() string }

func (o Op[Req, Resp]) OpName() string { return o.Name }

// OK is the answer of an operation that only records.
type OK struct {
	OK bool `json:"ok"`
}

// Requests naming one executor, node, lease or attempt.
type (
	ExecutorRequest struct {
		ExecutorID string `json:"executor_id"`
	}
	NodeRequest struct {
		NodeID string `json:"node_id"`
	}
	LeaseRequest struct {
		LeaseID string `json:"lease_id"`
		Epoch   int64  `json:"epoch"`
		Reason  string `json:"reason,omitempty"`
	}
	AttemptRequest struct {
		AttemptID string `json:"attempt_id"`
	}
	AttemptLeaseRequest struct {
		AttemptID string `json:"attempt_id"`
		LeaseID   string `json:"lease_id"`
		Epoch     int64  `json:"epoch"`
	}
)

// Registration and observation.
type (
	Provider struct {
		ID   string `json:"id"`
		Kind string `json:"kind"`
	}
	RegisterRequest struct {
		Node          store.Node       `json:"node"`
		Executors     []store.Executor `json:"executors"`
		Providers     []Provider       `json:"providers"`
		UptimeSeconds *float64         `json:"uptime_seconds,omitempty"`
		// Adopt lists the attempts the agent still runs
		// (store.ReregisterExecutor); not sent when empty (a daemon before
		// adoption refuses the field).
		Adopt []store.AdoptedAttempt `json:"adopt,omitempty"`
	}
	RegisterAnswer struct {
		StaleLeases map[string]int `json:"stale_leases"`
		Adopted     []string       `json:"adopted"`
	}
	ObservationRequest struct {
		ProviderID   string                   `json:"provider_id"`
		Resources    []store.ResourceInstance `json:"resources"`
		Observations []store.Observation      `json:"observations"`
		Claims       []store.ExternalClaim    `json:"claims"`
	}
)

// Admission and launch.
type (
	ReservationAnswer struct {
		Reservation *store.Reservation `json:"reservation"`
	}
	CommandsAnswer struct {
		Commands []string `json:"commands"`
	}
	UnlaunchedRequest struct {
		ExecutorID       string  `json:"executor_id"`
		OlderThanSeconds float64 `json:"older_than_seconds"`
	}
	LeasesAnswer struct {
		Leases []store.UnlaunchedLease `json:"leases"`
	}
	AuthorizeRequest struct {
		Reservation store.Reservation `json:"reservation"`
	}
	LaunchAnswer struct {
		Launch *store.Launch `json:"launch"`
	}
	ActivateRequest struct {
		AttemptID       string `json:"attempt_id"`
		LeaseID         string `json:"lease_id"`
		Epoch           int64  `json:"epoch"`
		Token           string `json:"token"`
		PID             int    `json:"pid"`
		ProcessIdentity string `json:"process_identity"`
	}
)

// Attempts while and after they run.
type (
	LogPathsRequest struct {
		AttemptID  string `json:"attempt_id"`
		Epoch      int64  `json:"epoch"`
		StdoutPath string `json:"stdout_path"`
		StderrPath string `json:"stderr_path"`
	}
	LogProblemRequest struct {
		AttemptID string           `json:"attempt_id"`
		Problem   store.LogProblem `json:"problem"`
	}
	ProcessExitedRequest struct {
		AttemptID string `json:"attempt_id"`
		Role      string `json:"role"`
		Rank      int    `json:"rank"`
		Identity  string `json:"identity"`
	}
	TerminalRequest struct {
		AttemptID  string `json:"attempt_id"`
		LeaseID    string `json:"lease_id"`
		Epoch      int64  `json:"epoch"`
		ExitCode   int    `json:"exit_code"`
		ExitSignal string `json:"exit_signal"`
	}
	CandidatesAnswer struct {
		Candidates []store.QuiescenceCandidate `json:"candidates"`
	}
	TerminationsAnswer struct {
		Terminations []store.QuarantineTermination `json:"terminations"`
	}
	ForceStopOrdersAnswer struct {
		Orders []store.ForceStopOrder `json:"orders"`
	}
	ForceStopAck struct {
		AttemptID string          `json:"attempt_id"`
		Phase     string          `json:"phase"`
		Detail    json.RawMessage `json:"detail,omitempty"`
	}
	// AttemptUsage is what an attempt's process tree uses now; a field left
	// out is not known.
	AttemptUsage struct {
		AttemptID        string `json:"attempt_id"`
		RAMBytes         *int64 `json:"ram_bytes"`
		DiskWrittenBytes *int64 `json:"disk_written_bytes"`
	}
)

// Stores and data.
type (
	StoresAnswer struct {
		Stores []machine.Store `json:"stores"`
	}
	StoreObservationRequest struct {
		Observation store.StoreObservation `json:"observation"`
	}
	AttemptScratchRequest struct {
		Scratch store.AttemptScratch `json:"scratch"`
	}
	ScratchAnswer struct {
		Scratch []store.ScratchSettlement `json:"scratch"`
	}
	SettleScratchRequest struct {
		Settled store.ScratchSettled `json:"settled"`
	}
	RunOutputRequest struct {
		Output store.RunOutput `json:"output"`
	}
	PacksAnswer struct {
		Packs []store.PackOrder `json:"packs"`
	}
	RecordPackRequest struct {
		Made store.PackMade `json:"made"`
	}
	CopiesAnswer struct {
		Copies []store.CopyOrder `json:"copies"`
	}
	RecordCopyRequest struct {
		Made store.CopyMade `json:"made"`
	}
	// ObjectStoreRequest names an object's copy on a store, for an executor.
	ObjectStoreRequest struct {
		ExecutorID string `json:"executor_id"`
		ObjectID   string `json:"object_id"`
		StoreID    string `json:"store_id"`
	}
	AbandonPackRequest struct {
		ExecutorID string `json:"executor_id"`
		ObjectID   string `json:"object_id"`
		Reason     string `json:"reason"`
	}
	InPlaceSourcesAnswer struct {
		Sources []store.InPlaceSource `json:"sources"`
	}
	// InputPathRequest names an object read in place at a path of the
	// executor's node.
	InputPathRequest struct {
		ExecutorID string `json:"executor_id"`
		ObjectID   string `json:"object_id"`
		Path       string `json:"path"`
	}
	InventoryRequest struct {
		Listing store.DurableListing `json:"listing"`
	}
	AdoptCopiesRequest struct {
		ExecutorID string              `json:"executor_id"`
		StoreID    string              `json:"store_id"`
		Root       string              `json:"root"`
		Copies     []store.AdoptedCopy `json:"copies"`
	}
	AdoptedAnswer struct {
		Adopted int `json:"adopted"`
	}
	FetchesAnswer struct {
		Fetches []store.FetchOrder `json:"fetches"`
	}
	RecordFetchRequest struct {
		Made store.FetchMade `json:"made"`
	}
	DeletionsAnswer struct {
		Deletions []store.DeletionOrder `json:"deletions"`
	}
	SettleDeletionRequest struct {
		Settled store.ObjectDeletion `json:"settled"`
	}
	SourceIDsAnswer struct {
		Sources []string `json:"sources"`
	}
	EvictionsAnswer struct {
		Evictions []store.EvictionOrder `json:"evictions"`
	}
	SettleEvictionRequest struct {
		Settled store.EvictionSettled `json:"settled"`
	}
	ScrubsAnswer struct {
		Scrubs []store.ScrubOrder `json:"scrubs"`
	}
	RecordScrubRequest struct {
		Result store.ScrubResult `json:"result"`
	}
	PackReadRequest struct {
		NodeID   string `json:"node_id"`
		ObjectID string `json:"object_id"`
		Token    string `json:"token"`
	}
	PackReadAnswer struct {
		Read store.PackRead `json:"read"`
	}
)

// Device controllers.
type (
	ControllerRegisterRequest struct {
		Node    store.Node `json:"node"`
		Version string     `json:"version"`
	}
	ControllerRegisterAnswer struct {
		OK      bool   `json:"ok"`
		Version string `json:"version"`
	}
	DevicesAnswer struct {
		Devices []store.ControlledDevice `json:"devices"`
	}
	DeviceReportRequest struct {
		NodeID  string               `json:"node_id"`
		Reports []store.DeviceReport `json:"reports"`
	}
	SkippedAnswer struct {
		Skipped []string `json:"skipped"`
	}
	TrimsAnswer struct {
		Trims []store.TrimOrder `json:"trims"`
	}
	RecordTrimRequest struct {
		NodeID string         `json:"node_id"`
		Done   store.TrimDone `json:"done"`
	}
	DeviceSamplesRequest struct {
		NodeID  string                   `json:"node_id"`
		Samples []store.ControllerSample `json:"samples"`
	}
)

// The operations of node agents.
var (
	Register         = Op[RegisterRequest, RegisterAnswer]{Name: "register", Observe: Allowed}
	Observations     = Op[ObservationRequest, OK]{Name: "observations", Observe: Allowed}
	ReserveNext      = Op[ExecutorRequest, ReservationAnswer]{Name: "reserve-next", Observe: Empty, Empty: ReservationAnswer{}}
	EnsurePreemption = Op[ExecutorRequest, CommandsAnswer]{Name: "ensure-preemption", Observe: Empty, Empty: CommandsAnswer{Commands: []string{}}}
	// An observe-only daemon gives an agent no stores: it then neither
	// makes a root nor a scratch, nor deletes anything.
	Stores           = Op[NodeRequest, StoresAnswer]{Name: "stores", Observe: Empty, Empty: StoresAnswer{Stores: []machine.Store{}}}
	StoreObservation = Op[StoreObservationRequest, OK]{Name: "store-observation", Observe: Allowed}
	AttemptScratch   = Op[AttemptScratchRequest, OK]{Name: "attempt-scratch", Observe: Refused}
	ScratchToSettle  = Op[ExecutorRequest, ScratchAnswer]{Name: "scratch-to-settle", Observe: Empty, Empty: ScratchAnswer{Scratch: []store.ScratchSettlement{}}}
	SettleScratch    = Op[SettleScratchRequest, OK]{Name: "settle-scratch", Observe: Refused}
	RunOutput        = Op[RunOutputRequest, OK]{Name: "run-output", Observe: Refused}
	PacksToMake      = Op[ExecutorRequest, PacksAnswer]{Name: "packs-to-make", Observe: Empty, Empty: PacksAnswer{Packs: []store.PackOrder{}}}
	RecordPack       = Op[RecordPackRequest, OK]{Name: "record-pack", Observe: Refused}
	CopiesToMake     = Op[ExecutorRequest, CopiesAnswer]{Name: "copies-to-make", Observe: Empty, Empty: CopiesAnswer{Copies: []store.CopyOrder{}}}
	RecordDescribed  = Op[ObjectStoreRequest, OK]{Name: "record-described", Observe: Refused}
	AbandonPack      = Op[AbandonPackRequest, OK]{Name: "abandon-pack", Observe: Refused}
	InPlaceSources   = Op[ExecutorRequest, InPlaceSourcesAnswer]{Name: "in-place-sources", Observe: Empty, Empty: InPlaceSourcesAnswer{Sources: []store.InPlaceSource{}}}
	InputGone        = Op[InputPathRequest, OK]{Name: "input-gone", Observe: Refused}
	InputIsFile      = Op[InputPathRequest, OK]{Name: "input-is-file", Observe: Refused}
	ClaimShed        = Op[ObjectStoreRequest, OK]{Name: "claim-shed", Observe: Refused}
	InventoryDurable = Op[InventoryRequest, store.DurableInventory]{Name: "inventory-durable", Observe: Refused}
	AdoptCopies      = Op[AdoptCopiesRequest, AdoptedAnswer]{Name: "adopt-copies", Observe: Refused}
	RecordCopy       = Op[RecordCopyRequest, OK]{Name: "record-copy", Observe: Refused}
	FetchesToMake    = Op[ExecutorRequest, FetchesAnswer]{Name: "fetches-to-make", Observe: Empty, Empty: FetchesAnswer{Fetches: []store.FetchOrder{}}}
	RecordFetch      = Op[RecordFetchRequest, OK]{Name: "record-fetch", Observe: Refused}
	DeletionsToMake  = Op[ExecutorRequest, DeletionsAnswer]{Name: "deletions-to-make", Observe: Empty, Empty: DeletionsAnswer{Deletions: []store.DeletionOrder{}}}
	SettleDeletion   = Op[SettleDeletionRequest, OK]{Name: "settle-deletion", Observe: Refused}
	SourcesInUse     = Op[ExecutorRequest, SourceIDsAnswer]{Name: "sources-in-use", Observe: Allowed}
	EvictionsToMake  = Op[ExecutorRequest, EvictionsAnswer]{Name: "evictions-to-make", Observe: Empty, Empty: EvictionsAnswer{Evictions: []store.EvictionOrder{}}}
	SettleEviction   = Op[SettleEvictionRequest, OK]{Name: "settle-eviction", Observe: Refused}
	ScrubsToMake     = Op[ExecutorRequest, ScrubsAnswer]{Name: "scrubs-to-make", Observe: Empty, Empty: ScrubsAnswer{Scrubs: []store.ScrubOrder{}}}
	RecordScrub      = Op[RecordScrubRequest, OK]{Name: "record-scrub", Observe: Refused}
	// The agent of a pack's node asks before handing it over.
	PackRead             = Op[PackReadRequest, PackReadAnswer]{Name: "pack-read", Observe: Refused}
	UnlaunchedLeases     = Op[UnlaunchedRequest, LeasesAnswer]{Name: "unlaunched-leases", Observe: Empty, Empty: LeasesAnswer{Leases: []store.UnlaunchedLease{}}}
	ValidateReservation  = Op[LeaseRequest, OK]{Name: "validate-reservation", Observe: Refused}
	MarkLeasePrepared    = Op[LeaseRequest, OK]{Name: "mark-lease-prepared", Observe: Refused}
	ReleaseReservation   = Op[LeaseRequest, OK]{Name: "release-reservation", Observe: Refused}
	AuthorizeLaunch      = Op[AuthorizeRequest, LaunchAnswer]{Name: "authorize-launch", Observe: Refused}
	ActivateLaunch       = Op[ActivateRequest, OK]{Name: "activate-launch", Observe: Refused}
	SetLogPaths          = Op[LogPathsRequest, OK]{Name: "set-log-paths", Observe: Allowed}
	LogProblem           = Op[LogProblemRequest, OK]{Name: "log-problem", Observe: Allowed}
	MarkProcessExited    = Op[ProcessExitedRequest, OK]{Name: "mark-process-exited", Observe: Allowed}
	RecordTerminal       = Op[TerminalRequest, OK]{Name: "record-terminal", Observe: Allowed}
	QuiescenceCandidates = Op[ExecutorRequest, CandidatesAnswer]{Name: "quiescence-candidates", Observe: Empty, Empty: CandidatesAnswer{Candidates: []store.QuiescenceCandidate{}}}
	FinalizeQuiescence   = Op[AttemptLeaseRequest, OK]{Name: "finalize-quiescence", Observe: Refused}
	QuarantineTerms      = Op[ExecutorRequest, TerminationsAnswer]{Name: "quarantine-terminations", Observe: Empty, Empty: TerminationsAnswer{Terminations: []store.QuarantineTermination{}}}
	QuarantineTerminated = Op[AttemptRequest, OK]{Name: "quarantine-terminated", Observe: Refused}
	ForceStopOrders      = Op[ExecutorRequest, ForceStopOrdersAnswer]{Name: "force-stop-orders", Observe: Empty, Empty: ForceStopOrdersAnswer{Orders: []store.ForceStopOrder{}}}
	ForceStopAcked       = Op[ForceStopAck, OK]{Name: "force-stop-ack", Observe: Refused}
	AttemptUsed          = Op[AttemptUsage, OK]{Name: "attempt-usage", Observe: Allowed}
)

// The operations of device controllers, for the controller's own node.
var (
	ControllerRegister = Op[ControllerRegisterRequest, ControllerRegisterAnswer]{Name: "controller-register", Observe: Allowed}
	// An observe-only daemon changes nothing: the controller is told it
	// controls no device, so it only returns those it may have changed to
	// their defaults (the handler empties the answer).
	DeviceSettings = Op[NodeRequest, DevicesAnswer]{Name: "device-settings", Observe: Allowed}
	DeviceReport   = Op[DeviceReportRequest, SkippedAnswer]{Name: "device-report", Observe: Allowed}
	// An observe-only daemon deletes nothing, so asks for no trim.
	TrimsToMake = Op[NodeRequest, TrimsAnswer]{Name: "trims-to-make", Observe: Empty, Empty: TrimsAnswer{Trims: []store.TrimOrder{}}}
	RecordTrim  = Op[RecordTrimRequest, OK]{Name: "record-trim", Observe: Refused}
	// What the controller reads of its host (the CPU package's power) is
	// observation, recorded by an observe-only daemon too.
	DeviceSamples = Op[DeviceSamplesRequest, SkippedAnswer]{Name: "device-samples", Observe: Allowed}
)

// All are every operation, for the daemon to check it serves each.
var All = []Named{
	Register, Observations, ReserveNext, EnsurePreemption, Stores, StoreObservation, AttemptScratch, ScratchToSettle,
	SettleScratch, RunOutput, PacksToMake, RecordPack, CopiesToMake, RecordDescribed, AbandonPack, InPlaceSources,
	InputGone, InputIsFile, ClaimShed, InventoryDurable, AdoptCopies, RecordCopy, FetchesToMake, RecordFetch,
	DeletionsToMake, SettleDeletion, SourcesInUse, EvictionsToMake, SettleEviction, ScrubsToMake, RecordScrub,
	PackRead, UnlaunchedLeases, ValidateReservation, MarkLeasePrepared, ReleaseReservation, AuthorizeLaunch,
	ActivateLaunch, SetLogPaths, LogProblem, MarkProcessExited, RecordTerminal, QuiescenceCandidates,
	FinalizeQuiescence, QuarantineTerms, QuarantineTerminated, ForceStopOrders, ForceStopAcked, AttemptUsed,
	ControllerRegister, DeviceSettings, DeviceReport, TrimsToMake, RecordTrim, DeviceSamples,
}
