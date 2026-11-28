package store

import (
	"encoding/json"

	"kairo/internal/datainput"
	"kairo/internal/orchestration"
)

type ScopePath struct {
	Project string `json:"project"`
	Queue   string `json:"queue,omitempty"`
	Task    string `json:"task,omitempty"`
}

type Scope struct {
	ID          string  `json:"id"`
	Kind        string  `json:"kind"`
	ParentID    *string `json:"parent_id,omitempty"`
	ExternalKey string  `json:"external_key"`
	Admission   string  `json:"admission"`
	Generation  int64   `json:"generation"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

type ScopeSet struct {
	Project Scope  `json:"project"`
	Queue   *Scope `json:"queue,omitempty"`
	Task    *Scope `json:"task,omitempty"`
}

type ScopeFilter struct {
	Kind     string
	ParentID string
	Project  string
}

type ExclusiveRequest struct {
	Kind  string `json:"kind"`
	Count int    `json:"count"`
	// Device pins the request to one GPU by its UUID (a hardware bench of
	// that GPU); empty: any GPU that fits.
	Device                     string `json:"device,omitempty"`
	SameNode                   bool   `json:"same_node"`
	MinTotalMemoryBytes        int64  `json:"min_total_memory_bytes"`
	MinObservedFreeMemoryBytes int64  `json:"min_observed_free_memory_bytes"`
	OnExternalClaim            string `json:"on_external_claim"`
	OnUnattributedActivity     string `json:"on_unattributed_activity"`
	OnStaleObservation         string `json:"on_stale_observation"`
}

type DiskRequest struct {
	Filesystem        string `json:"filesystem"`
	ReserveBytes      int64  `json:"reserve_bytes"`
	MinFreeAfterBytes int64  `json:"min_free_after_bytes"`
}

type CapacityRequest struct {
	CPUMillis int64 `json:"cpu_millis"`
	// CPUMillisMin is the least of CPUMillis the execution keeps: executions
	// of no higher priority may be admitted on the rest (planner.go).
	CPUMillisMin int64         `json:"cpu_millis_min,omitempty"`
	RAMBytes     int64         `json:"ram_bytes"`
	Strength     string        `json:"strength"`
	Disks        []DiskRequest `json:"disks"`
}

type ExecutionSpec struct {
	ClientRequestID      string             `json:"client_request_id"`
	Scope                ScopePath          `json:"scope"`
	Argv                 []string           `json:"argv"`
	CWD                  string             `json:"cwd"`
	ExecutorSelector     json.RawMessage    `json:"executor_selector"`
	Priority             int                `json:"priority"`
	Checkpointable       bool               `json:"checkpointable"`
	Preemptible          bool               `json:"preemptible"`
	InputContinuationRef *string            `json:"input_continuation_ref,omitempty"`
	Exclusive            []ExclusiveRequest `json:"exclusive"`
	Capacity             CapacityRequest    `json:"capacity"`
	// Gang runs the execution as Size processes, one per node, started together
	// (see gang.go). The submitted execution is rank 0 (the leader); ranks
	// 1..Size-1 are member executions created with it.
	Gang *GangSpec `json:"gang,omitempty"`
	// Env is added to the attempt's environment (envspec.Validate); it never
	// overrides what Kairo sets. Left out of the digest when empty.
	Env map[string]string `json:"env,omitempty"`
	// ScratchTmpdir points TMPDIR, TMP and TEMP of every rank's attempt at
	// its scratch directory (when it has one). Left out of the digest when
	// false.
	ScratchTmpdir bool `json:"scratch_tmpdir,omitempty"`
	// NotBefore: not admitted, and holding nothing, before this time (RFC
	// 3339 with an offset); StartBy and FinishBy: when it should have started
	// and ended, which order it within its priority (planner.go). Left out of
	// the digest when empty.
	NotBefore string `json:"not_before,omitempty"`
	StartBy   string `json:"start_by,omitempty"`
	FinishBy  string `json:"finish_by,omitempty"`
	// Inputs are what the execution reads, handed to its attempts by path
	// (inputs.go). Left out of the digest when empty.
	Inputs []datainput.Input `json:"inputs,omitempty"`
	// Confine: its attempts may write only where Kairo hands them (the data
	// policy may confine every execution). Left out of the digest when false.
	Confine bool `json:"confine,omitempty"`
	// Store: the volatile store its attempts' scratch, output and cache
	// directories go on (the execution is planned onto its node). Left out
	// of the digest when empty.
	Store string `json:"store,omitempty"`
	// Regenerable: its run's output can be made again by rerunning its task
	// (temporal.go). Left out of the digest when false.
	Regenerable bool `json:"regenerable,omitempty"`
	// SourceSnapshot: the code snapshot (sources.go) the execution runs in,
	// cwd relative to it.
	SourceSnapshot string `json:"source_snapshot,omitempty"`
	// Image: the container image the execution runs in, an OCI reference
	// pinned by digest (registry/repository@sha256:...); only an executor
	// that runs containers takes it, and such an executor takes nothing
	// else (the executor selector's "container", set from it). Left out of
	// the digest when empty.
	Image string `json:"image,omitempty"`
	// Streams are the streams its attempts read, as its task declared them
	// (streams.go, issue #43). Left out of the digest when empty.
	Streams []orchestration.StreamDecl `json:"streams,omitempty"`
	// InputCheckpointCommand: the command whose published checkpoint a
	// continuation goes on from (its streams start where that checkpoint's
	// payload says). Left out of the digest when empty.
	InputCheckpointCommand string `json:"input_checkpoint_command,omitempty"`
	// StreamSnapshot: the code snapshot its streams' stages run in, when not
	// SourceSnapshot: a continuation run in another snapshot than the one
	// its checkpoint was made in (task rerun --from-checkpoint --source)
	// runs its stages where the checkpoint's ran, as they are part of where
	// its streams stand. Left out of the digest when empty.
	StreamSnapshot string `json:"stream_snapshot,omitempty"`
}

// GangSpec describes a multi-node execution. Every rank asks for the
// execution's resources on its own node unless it gives its own; Ranks
// overrides argv, cwd, the executor selector, exclusive or capacity of single
// ranks (a Windows rank and a Linux rank need different paths, a two-GPU
// node and a one-GPU node different requests) and merges a rank's env over the
// execution's.
type GangSpec struct {
	Size int `json:"size"`
	// Port is the rendezvous port on member 0's address on the generation's
	// segment; 0: GangDefaultPort.
	Port  int        `json:"port,omitempty"`
	Ranks []GangRank `json:"ranks,omitempty"`
	// What the members' links must be (fabric_evidence.go): a round trip of
	// at most MaxRTTMillis, at least MinMiBPerSecond each way (0: either
	// way, any), and none metered unless Metered. Absent from the JSON when
	// unset, so gang digests do not change.
	MaxRTTMillis    float64 `json:"max_rtt_ms,omitempty"`
	MinMiBPerSecond float64 `json:"min_mib_per_second,omitempty"`
	Metered         bool    `json:"metered,omitempty"`
}

type GangRank struct {
	Rank             int               `json:"rank"`
	Argv             []string          `json:"argv,omitempty"`
	CWD              string            `json:"cwd,omitempty"`
	ExecutorSelector json.RawMessage   `json:"executor_selector,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	// Exclusive and Capacity, when set, replace the execution's for this rank
	// (an empty Exclusive asks for no GPU). Absent from the JSON when unset, so
	// existing gang digests do not change.
	Exclusive *[]ExclusiveRequest `json:"exclusive,omitempty"`
	Capacity  *CapacityRequest    `json:"capacity,omitempty"`
	// InputContinuationRef and InputCheckpointCommand: of a gang's
	// continuation, the checkpoint this member goes on from (its own, of the
	// round the whole gang published: gangcheckpoint.go).
	InputContinuationRef   *string `json:"input_continuation_ref,omitempty"`
	InputCheckpointCommand string  `json:"input_checkpoint_command,omitempty"`
}

// GangLaunch is what one rank of a placed gang is told at launch: its rank and
// where rank 0 listens. It is read from the placement, never from the caller.
type GangLaunch struct {
	GangID      string `json:"gang_id"`
	PlacementID string `json:"placement_id"`
	Rank        int    `json:"rank"`
	Size        int    `json:"size"`
	MasterAddr  string `json:"master_addr"`
	MasterPort  int    `json:"master_port"`
	// The generation's segment (fabric): its VNI, subnet and MTU, this
	// member's address on it (CIDR) and every member's, in rank order.
	VNI       int      `json:"vni,omitempty"`
	Subnet    string   `json:"subnet,omitempty"`
	MTU       int      `json:"mtu,omitempty"`
	Address   string   `json:"address,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

type ExecutionRequest struct {
	ID                   string            `json:"id"`
	ProjectScopeID       string            `json:"project_scope_id"`
	QueueScopeID         *string           `json:"queue_scope_id,omitempty"`
	TaskScopeID          *string           `json:"task_scope_id,omitempty"`
	ClientRequestID      string            `json:"client_request_id"`
	SpecDigest           string            `json:"spec_digest"`
	State                string            `json:"state"`
	Argv                 []string          `json:"argv"`
	CWD                  string            `json:"cwd"`
	ExecutorSelector     json.RawMessage   `json:"executor_selector"`
	Priority             int               `json:"priority"`
	Checkpointable       bool              `json:"checkpointable"`
	Preemptible          bool              `json:"preemptible"`
	InputContinuationRef *string           `json:"input_continuation_ref,omitempty"`
	TerminalCause        *string           `json:"terminal_cause,omitempty"`
	SubmittedAt          string            `json:"submitted_at"`
	AuthorizedAt         *string           `json:"authorized_at,omitempty"`
	StartedAt            *string           `json:"started_at,omitempty"`
	TerminalAt           *string           `json:"terminal_at,omitempty"`
	Resources            []ResourceRequest `json:"resource_requests,omitempty"`
	Env                  map[string]string `json:"env,omitempty"`
	NotBefore            *string           `json:"not_before,omitempty"`
	StartBy              *string           `json:"start_by,omitempty"`
	FinishBy             *string           `json:"finish_by,omitempty"`
}

type ExecutionFilter struct {
	ProjectScopeID string
	ScopeID        string
	State          string
	Limit          int
	// After is the cursor a page ended at (ListExecutionsPage): the page
	// is the executions after it.
	After string
}

type ResourceRequest struct {
	ID          string          `json:"id"`
	ExecutionID string          `json:"execution_id"`
	RequestType string          `json:"request_type"`
	Kind        string          `json:"kind"`
	Quantity    int64           `json:"quantity"`
	Filesystem  string          `json:"filesystem,omitempty"`
	Constraints json.RawMessage `json:"constraints"`
	Policy      json.RawMessage `json:"policy"`
	Strength    string          `json:"strength"`
}

type Attempt struct {
	ID                string          `json:"id"`
	ExecutionID       string          `json:"execution_id"`
	State             string          `json:"state"`
	ExecutorID        string          `json:"executor_id"`
	CoordinationEpoch int64           `json:"coordination_epoch"`
	ProcessIdentity   *string         `json:"process_identity,omitempty"`
	PID               *int            `json:"pid,omitempty"`
	ExitCode          *int            `json:"exit_code,omitempty"`
	ExitSignal        *string         `json:"exit_signal,omitempty"`
	ContinuationRef   *string         `json:"continuation_ref,omitempty"`
	Progress          json.RawMessage `json:"progress,omitempty"`
	StartedAt         *string         `json:"started_at,omitempty"`
	LastHeartbeatAt   *string         `json:"last_heartbeat_at,omitempty"`
	CheckpointedAt    *string         `json:"checkpointed_at,omitempty"`
	ExitedAt          *string         `json:"exited_at,omitempty"`
	QuiescedAt        *string         `json:"quiesced_at,omitempty"`
}

type AttemptProcess struct {
	AttemptID       string  `json:"attempt_id"`
	Role            string  `json:"role"`
	Namespace       string  `json:"namespace"`
	Rank            int     `json:"rank"`
	PID             int     `json:"pid"`
	ProcessIdentity string  `json:"process_identity"`
	RegisteredAt    string  `json:"registered_at"`
	LastSeenAt      string  `json:"last_seen_at"`
	ExitedAt        *string `json:"exited_at,omitempty"`
}

type Lease struct {
	ID                string  `json:"id"`
	ExecutionID       string  `json:"execution_id"`
	AttemptID         *string `json:"attempt_id,omitempty"`
	ExecutorID        string  `json:"executor_id"`
	CoordinationEpoch int64   `json:"coordination_epoch"`
	State             string  `json:"state"`
	CreatedAt         string  `json:"created_at"`
	ExpiresAt         *string `json:"expires_at,omitempty"`
}

type Command struct {
	ID            string          `json:"id"`
	ExecutionID   string          `json:"execution_id"`
	AttemptID     string          `json:"attempt_id"`
	Kind          string          `json:"kind"`
	Origin        string          `json:"origin"`
	Reason        string          `json:"reason"`
	State         string          `json:"state"`
	DeliveryCount int             `json:"delivery_count"`
	CreatedAt     string          `json:"created_at"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type PauseOperation struct {
	ID              string          `json:"id"`
	ScopeID         string          `json:"scope_id"`
	ScopeGeneration int64           `json:"scope_generation"`
	RequestID       string          `json:"request_id"`
	Actor           string          `json:"actor"`
	State           string          `json:"state"`
	Detail          json.RawMessage `json:"detail"`
	CreatedAt       string          `json:"created_at"`
	UpdatedAt       string          `json:"updated_at"`
	// Force is set for `pause --force`: the captured executions are ended
	// (process trees terminated, unstarted ones withdrawn), not suspended.
	Force *ForceStopSpec `json:"force,omitempty"`
}

// ForceStopSpec is how a forced pause stops what it captured: a graceful stop
// (CTRL_BREAK / SIGTERM), then after GraceSeconds the whole tree is killed.
type ForceStopSpec struct {
	GraceSeconds int    `json:"grace_seconds"`
	Reason       string `json:"reason"`
}

// ForceStop is the record of one execution ended by a forced pause.
type ForceStop struct {
	ExecutionID  string          `json:"execution_id"`
	OperationID  string          `json:"operation_id"`
	AttemptID    *string         `json:"attempt_id,omitempty"`
	ExecutorID   *string         `json:"executor_id,omitempty"`
	GraceSeconds int             `json:"grace_seconds"`
	State        string          `json:"state"`
	Detail       json.RawMessage `json:"detail"`
	RequestedAt  string          `json:"requested_at"`
	SignalledAt  *string         `json:"signalled_at,omitempty"`
	TerminatedAt *string         `json:"terminated_at,omitempty"`
}

// ForceStopOrder is a force stop an executor has to carry out on one of its
// attempts. KillAfterSeconds is the grace left (computed by the daemon, so
// clocks of remote nodes do not matter); at 0 the tree is killed.
type ForceStopOrder struct {
	AttemptID        string  `json:"attempt_id"`
	ExecutionID      string  `json:"execution_id"`
	PID              *int    `json:"pid"`
	ProcessIdentity  *string `json:"process_identity"`
	State            string  `json:"state"`
	GraceSeconds     int     `json:"grace_seconds"`
	KillAfterSeconds float64 `json:"kill_after_seconds"`
}

type PauseTarget struct {
	OperationID   string  `json:"operation_id"`
	ExecutionID   string  `json:"execution_id"`
	AttemptID     *string `json:"attempt_id,omitempty"`
	LeaseID       *string `json:"lease_id,omitempty"`
	CommandID     *string `json:"command_id,omitempty"`
	State         string  `json:"state"`
	BlockerReason *string `json:"blocker_reason,omitempty"`
	UpdatedAt     string  `json:"updated_at"`
}

type CoordinationEvent struct {
	Sequence          int64           `json:"sequence"`
	EventType         string          `json:"event_type"`
	ProjectScopeID    *string         `json:"project_scope_id,omitempty"`
	AggregateType     string          `json:"aggregate_type"`
	AggregateID       string          `json:"aggregate_id"`
	CoordinationEpoch *int64          `json:"coordination_epoch,omitempty"`
	Payload           json.RawMessage `json:"payload"`
	CreatedAt         string          `json:"created_at"`
}

type EventFilter struct {
	ProjectScopeID string
	AfterSequence  int64
	Limit          int
}

type PauseOperationFilter struct {
	ScopeID    string
	Unfinished bool
	Limit      int
}

type PauseTargetUpdate struct {
	AttemptID     *string
	LeaseID       *string
	CommandID     *string
	State         string
	BlockerReason *string
}
