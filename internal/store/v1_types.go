package store

import "encoding/json"

type Node struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	OS           string          `json:"os"`
	Architecture string          `json:"architecture"`
	Attributes   json.RawMessage `json:"attributes"`
	Enabled      bool            `json:"enabled"`
}
type Executor struct {
	ID         string          `json:"id"`
	NodeID     string          `json:"node_id"`
	Kind       string          `json:"kind"`
	Attributes json.RawMessage `json:"attributes"`
	Enabled    bool            `json:"enabled"`
	LastSeenAt *string         `json:"last_seen_at,omitempty"`
}
type ResourceInstance struct {
	ID                    string          `json:"id"`
	NodeID                string          `json:"node_id"`
	ProviderID            string          `json:"provider_id"`
	Kind                  string          `json:"kind"`
	StableIdentity        string          `json:"stable_identity"`
	Binding               json.RawMessage `json:"binding"`
	Attributes            json.RawMessage `json:"attributes"`
	AdminState            string          `json:"admin_state"`
	QuarantineReason      *string         `json:"quarantine_reason,omitempty"`
	DerivedState          string          `json:"derived_state"`
	ObservationAgeSeconds *float64        `json:"observation_age_seconds,omitempty"`
}
type Observation struct {
	ResourceID   string          `json:"resource_id"`
	ObservedAt   string          `json:"observed_at"`
	ValidUntil   string          `json:"valid_until"`
	TotalBytes   *int64          `json:"total_bytes,omitempty"`
	FreeBytes    *int64          `json:"free_bytes,omitempty"`
	Utilization  *float64        `json:"utilization,omitempty"`
	TemperatureC *float64        `json:"temperature_c,omitempty"`
	Evidence     json.RawMessage `json:"evidence"`
}
type ExternalClaim struct {
	ID              string          `json:"id"`
	ResourceID      string          `json:"resource_id"`
	ClaimKind       string          `json:"claim_kind"`
	ProcessIdentity *string         `json:"process_identity,omitempty"`
	Evidence        json.RawMessage `json:"evidence"`
	FirstObservedAt string          `json:"first_observed_at"`
	LastObservedAt  string          `json:"last_observed_at"`
	ClearedAt       *string         `json:"cleared_at,omitempty"`
}
type Queue struct {
	ID              string `json:"id"`
	Project         string `json:"project"`
	Name            string `json:"name"`
	CurrentRevision int    `json:"current_revision"`
	DesiredState    string `json:"desired_state"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}
type QueueRevision struct {
	QueueID    string          `json:"queue_id"`
	Revision   int             `json:"revision"`
	Digest     string          `json:"digest"`
	Normalized json.RawMessage `json:"normalized"`
	SourceText string          `json:"source_text,omitempty"`
	Actor      string          `json:"actor"`
	RequestID  string          `json:"request_id"`
	CreatedAt  string          `json:"created_at"`
}
type Task struct {
	ID               string  `json:"id"`
	QueueID          string  `json:"queue_id"`
	Key              string  `json:"key"`
	CurrentRevision  int     `json:"current_revision"`
	DesiredState     string  `json:"desired_state"`
	SchedulingState  string  `json:"scheduling_state"`
	Priority         int     `json:"priority"`
	BecameRunnableAt *string `json:"became_runnable_at,omitempty"`
	BlockReason      *string `json:"block_reason,omitempty"`
}
type ApplyResult struct {
	Queue      Queue `json:"queue"`
	Revision   int   `json:"revision"`
	Created    bool  `json:"created"`
	Idempotent bool  `json:"idempotent"`
}
type ApplyOptions struct {
	ExpectedRevision int
	Create           bool
	Actor            string
	RequestID        string
}

type Lease struct {
	ID                string  `json:"id"`
	AttemptID         *string `json:"attempt_id,omitempty"`
	TaskID            *string `json:"task_id,omitempty"`
	TaskRevision      *int    `json:"task_revision,omitempty"`
	ExecutorID        string  `json:"executor_id"`
	CoordinationEpoch int64   `json:"coordination_epoch"`
	State             string  `json:"state"`
	CreatedAt         string  `json:"created_at"`
	ExpiresAt         *string `json:"expires_at,omitempty"`
}
type V1Launch struct {
	Task               Task               `json:"task"`
	TaskRevision       int                `json:"task_revision"`
	Argv               []string           `json:"argv"`
	CWD                string             `json:"cwd"`
	Attempt            Attempt            `json:"attempt"`
	Lease              Lease              `json:"lease"`
	AuthorizationID    string             `json:"authorization_id"`
	AuthorizationToken string             `json:"authorization_token"`
	Resources          []ResourceInstance `json:"resources"`
	ContinuationRef    *string            `json:"continuation_ref,omitempty"`
}
type Reservation struct {
	Lease        Lease              `json:"lease"`
	Task         Task               `json:"task"`
	TaskRevision int                `json:"task_revision"`
	Argv         []string           `json:"argv"`
	CWD          string             `json:"cwd"`
	Resources    []ResourceInstance `json:"resources"`
}
type StatusSnapshot struct {
	Queues    []Queue            `json:"queues"`
	Tasks     []Task             `json:"tasks"`
	Resources []ResourceInstance `json:"resources"`
	Attempts  []Attempt          `json:"attempts"`
	Leases    []Lease            `json:"leases"`
	Claims    []ExternalClaim    `json:"external_claims"`
}
