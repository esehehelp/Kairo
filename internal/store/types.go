package store

import "encoding/json"

// Attempt is the durable V1 execution fact for one task revision.
type Attempt struct {
	ID                string          `json:"id"`
	TaskID            *string         `json:"task_id,omitempty"`
	TaskRevision      *int            `json:"task_revision,omitempty"`
	Ordinal           int             `json:"ordinal"`
	State             string          `json:"state"`
	ExecutorID        string          `json:"executor_id"`
	CoordinationEpoch int64           `json:"coordination_epoch"`
	ProcessIdentity   *string         `json:"process_identity,omitempty"`
	PID               *int            `json:"pid,omitempty"`
	ExitCode          *int            `json:"exit_code,omitempty"`
	ContinuationRef   *string         `json:"continuation_ref,omitempty"`
	Progress          json.RawMessage `json:"progress,omitempty"`
	StartedAt         *string         `json:"started_at,omitempty"`
	LastHeartbeatAt   *string         `json:"last_heartbeat_at,omitempty"`
	CheckpointedAt    *string         `json:"checkpointed_at,omitempty"`
	ExitedAt          *string         `json:"exited_at,omitempty"`
	QuiescedAt        *string         `json:"quiesced_at,omitempty"`
}

// Command is a V1 command delivered to the worker holding the attempt lease.
type Command struct {
	ID            string          `json:"id"`
	TaskID        string          `json:"task_id"`
	AttemptID     string          `json:"attempt_id"`
	Kind          string          `json:"kind"`
	Reason        string          `json:"reason"`
	State         string          `json:"state"`
	DeliveryCount int             `json:"delivery_count"`
	CreatedAt     string          `json:"created_at"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}
