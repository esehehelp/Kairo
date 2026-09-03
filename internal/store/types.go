package store

import "encoding/json"

type Resource struct {
	ID               string  `json:"id"`
	Kind             string  `json:"kind"`
	Binding          string  `json:"binding"`
	State            string  `json:"state"`
	QuarantineReason *string `json:"quarantine_reason,omitempty"`
}

type WorkloadSpec struct {
	Project            string   `json:"project"`
	ExternalKey        string   `json:"external_key"`
	Priority           int      `json:"priority"`
	Argv               []string `json:"argv"`
	CWD                string   `json:"cwd"`
	ResourceKind       string   `json:"resource_kind"`
	ResourceCount      int      `json:"resource_count"`
	CooperativeSuspend bool     `json:"cooperative_suspend"`
}

type Workload struct {
	ID                 string   `json:"id"`
	Project            string   `json:"project"`
	ExternalKey        string   `json:"external_key"`
	Priority           int      `json:"priority"`
	Argv               []string `json:"argv"`
	CWD                string   `json:"cwd"`
	ResourceKind       string   `json:"resource_kind"`
	ResourceCount      int      `json:"resource_count"`
	CooperativeSuspend bool     `json:"cooperative_suspend"`
	DesiredState       string   `json:"desired_state"`
	SchedulingState    string   `json:"scheduling_state"`
	CreatedAt          string   `json:"created_at"`
	UpdatedAt          string   `json:"updated_at"`
}

type Attempt struct {
	ID                  string          `json:"id"`
	WorkloadID          string          `json:"workload_id"`
	Ordinal             int             `json:"ordinal"`
	State               string          `json:"state"`
	ExecutorID          string          `json:"executor_id"`
	TaskID              *string         `json:"task_id,omitempty"`
	TaskRevision        *int            `json:"task_revision,omitempty"`
	CoordinationEpoch   int64           `json:"coordination_epoch"`
	ProcessIdentity     *string         `json:"process_identity,omitempty"`
	PID                 *int            `json:"pid,omitempty"`
	ExitCode            *int            `json:"exit_code,omitempty"`
	TerminalDisposition *string         `json:"terminal_disposition,omitempty"`
	ContinuationRef     *string         `json:"continuation_ref,omitempty"`
	Progress            json.RawMessage `json:"progress,omitempty"`
	StartedAt           *string         `json:"started_at,omitempty"`
	LastHeartbeatAt     *string         `json:"last_heartbeat_at,omitempty"`
	CheckpointedAt      *string         `json:"checkpointed_at,omitempty"`
	ExitedAt            *string         `json:"exited_at,omitempty"`
	QuiescedAt          *string         `json:"quiesced_at,omitempty"`
}

type Launch struct {
	Workload        Workload   `json:"workload"`
	Attempt         Attempt    `json:"attempt"`
	LeaseID         string     `json:"lease_id"`
	Resources       []Resource `json:"resources"`
	ContinuationRef *string    `json:"continuation_ref,omitempty"`
}

type Command struct {
	ID            string          `json:"id"`
	WorkloadID    string          `json:"workload_id"`
	AttemptID     string          `json:"attempt_id"`
	Kind          string          `json:"kind"`
	Reason        string          `json:"reason"`
	State         string          `json:"state"`
	DeliveryCount int             `json:"delivery_count"`
	CreatedAt     string          `json:"created_at"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type Snapshot struct {
	Resources []Resource `json:"resources"`
	Workloads []Workload `json:"workloads"`
	Attempts  []Attempt  `json:"attempts"`
}
