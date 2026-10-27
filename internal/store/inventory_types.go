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
	ID               string          `json:"id"`
	NodeID           string          `json:"node_id"`
	ProviderID       string          `json:"provider_id"`
	Kind             string          `json:"kind"`
	StableIdentity   string          `json:"stable_identity"`
	Binding          json.RawMessage `json:"binding"`
	Attributes       json.RawMessage `json:"attributes"`
	AdminState       string          `json:"admin_state"`
	QuarantineReason *string         `json:"quarantine_reason,omitempty"`
	// DeclaredNodeID is the node a GPU is declared for (kairo machine apply).
	DeclaredNodeID        *string  `json:"declared_node_id,omitempty"`
	DerivedState          string   `json:"derived_state"`
	ObservationAgeSeconds *float64 `json:"observation_age_seconds,omitempty"`
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
	// Device is the device's power and operating point as read with this
	// observation, when its provider reads them (ApplyObservationBatch keeps
	// it in device_samples).
	Device *DeviceSample `json:"device,omitempty"`
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
	// AttemptID and Ancestors are a provider's report of what ties the
	// process to an attempt: the KAIRO_ATTEMPT_ID it carries and the
	// identities of its living ancestors, nearest first. They decide whether
	// the claim is recorded (ApplyObservationBatch) and are not stored.
	AttemptID string   `json:"attempt_id,omitempty"`
	Ancestors []string `json:"ancestors,omitempty"`
}
