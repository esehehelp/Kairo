package store

import "encoding/json"

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
	Kind                       string `json:"kind"`
	Count                      int    `json:"count"`
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
	CPUMillis int64         `json:"cpu_millis"`
	RAMBytes  int64         `json:"ram_bytes"`
	Strength  string        `json:"strength"`
	Disks     []DiskRequest `json:"disks"`
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
}

type ExecutionFilter struct {
	ProjectScopeID string
	ScopeID        string
	State          string
	Limit          int
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
