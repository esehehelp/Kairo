// Package execution parses the one-shot execution request format accepted by
// the Kairo CLI.  It deliberately contains no workflow or retry semantics.
package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
)

type Manifest struct {
	SchemaVersion        int                `toml:"schema_version" json:"schema_version"`
	ClientRequestID      string             `toml:"client_request_id" json:"client_request_id"`
	Project              string             `toml:"project" json:"project"`
	Queue                string             `toml:"queue" json:"queue,omitempty"`
	Task                 string             `toml:"task" json:"task,omitempty"`
	Argv                 []string           `toml:"argv" json:"argv"`
	CWD                  string             `toml:"cwd" json:"cwd"`
	Priority             int                `toml:"priority" json:"priority"`
	Checkpointable       bool               `toml:"checkpointable" json:"checkpointable"`
	Preemptible          bool               `toml:"preemptible" json:"preemptible"`
	InputContinuationRef *string            `toml:"input_continuation_ref" json:"input_continuation_ref,omitempty"`
	Executor             ExecutorSelector   `toml:"executor" json:"executor"`
	Exclusive            []ExclusiveRequest `toml:"exclusive" json:"exclusive"`
	Capacity             CapacityRequest    `toml:"capacity" json:"capacity"`
}

type ExecutorSelector struct {
	Labels map[string]string `toml:"labels" json:"labels"`
}

type ExclusiveRequest struct {
	Kind                       string `toml:"kind" json:"kind"`
	Count                      int    `toml:"count" json:"count"`
	SameNode                   bool   `toml:"same_node" json:"same_node"`
	MinTotalMemoryBytes        int64  `toml:"min_total_memory_bytes" json:"min_total_memory_bytes"`
	MinObservedFreeMemoryBytes int64  `toml:"min_observed_free_memory_bytes" json:"min_observed_free_memory_bytes"`
	OnExternalClaim            string `toml:"on_external_claim" json:"on_external_claim"`
	OnUnattributedActivity     string `toml:"on_unattributed_activity" json:"on_unattributed_activity"`
	OnStaleObservation         string `toml:"on_stale_observation" json:"on_stale_observation"`
}

type CapacityRequest struct {
	CPUMillis int64         `toml:"cpu_millis" json:"cpu_millis"`
	RAMBytes  int64         `toml:"ram_bytes" json:"ram_bytes"`
	Strength  string        `toml:"strength" json:"strength"`
	Disks     []DiskRequest `toml:"disks" json:"disks"`
}

type DiskRequest struct {
	Filesystem        string `toml:"filesystem" json:"filesystem"`
	ReserveBytes      int64  `toml:"reserve_bytes" json:"reserve_bytes"`
	MinFreeAfterBytes int64  `toml:"min_free_after_bytes" json:"min_free_after_bytes"`
}

type Validated struct {
	Manifest   Manifest
	Normalized []byte
	Digest     string
}

func Parse(source []byte) (Validated, error) {
	var manifest Manifest
	metadata, err := toml.Decode(string(source), &manifest)
	if err != nil {
		return Validated{}, fmt.Errorf("parse TOML: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		return Validated{}, fmt.Errorf("unknown execution field %q", undecoded[0].String())
	}
	if err := normalizeAndValidate(&manifest); err != nil {
		return Validated{}, err
	}
	normalized, err := json.Marshal(manifest)
	if err != nil {
		return Validated{}, err
	}
	sum := sha256.Sum256(normalized)
	return Validated{Manifest: manifest, Normalized: normalized, Digest: hex.EncodeToString(sum[:])}, nil
}

func normalizeAndValidate(m *Manifest) error {
	if m.SchemaVersion != 2 {
		return fmt.Errorf("schema_version must be 2, got %d", m.SchemaVersion)
	}
	if strings.TrimSpace(m.ClientRequestID) == "" {
		return errors.New("client_request_id is required")
	}
	if strings.TrimSpace(m.Project) == "" {
		return errors.New("project is required")
	}
	if m.Task != "" && strings.TrimSpace(m.Queue) == "" {
		return errors.New("task scope requires queue")
	}
	if len(m.Argv) == 0 || strings.TrimSpace(m.Argv[0]) == "" {
		return errors.New("argv is required")
	}
	if strings.TrimSpace(m.CWD) == "" {
		return errors.New("cwd is required")
	}
	if m.Preemptible && !m.Checkpointable {
		return errors.New("preemptible execution must be checkpointable")
	}
	if m.Executor.Labels == nil {
		m.Executor.Labels = map[string]string{}
	}
	if m.Exclusive == nil {
		m.Exclusive = []ExclusiveRequest{}
	}
	for i := range m.Exclusive {
		r := &m.Exclusive[i]
		if r.Kind != "gpu" || r.Count < 1 {
			return errors.New("exclusive requests must be positive GPU requests")
		}
		if r.MinTotalMemoryBytes < 0 || r.MinObservedFreeMemoryBytes < 0 {
			return errors.New("exclusive memory constraints cannot be negative")
		}
		if r.OnExternalClaim == "" {
			r.OnExternalClaim = "wait"
		}
		if r.OnUnattributedActivity == "" {
			r.OnUnattributedActivity = "wait"
		}
		if r.OnStaleObservation == "" {
			r.OnStaleObservation = "wait"
		}
		if !oneOf(r.OnExternalClaim, "wait", "fail") ||
			!oneOf(r.OnUnattributedActivity, "wait", "fail", "allow") ||
			!oneOf(r.OnStaleObservation, "wait", "fail") {
			return errors.New("invalid GPU conflict policy")
		}
	}
	if m.Capacity.Strength == "" {
		m.Capacity.Strength = "admitted"
	}
	if m.Capacity.Strength != "admitted" {
		return errors.New("only admitted capacity is supported")
	}
	if m.Capacity.CPUMillis < 0 || m.Capacity.RAMBytes < 0 {
		return errors.New("capacity cannot be negative")
	}
	for _, disk := range m.Capacity.Disks {
		if strings.TrimSpace(disk.Filesystem) == "" || disk.ReserveBytes < 0 || disk.MinFreeAfterBytes < 0 {
			return errors.New("invalid disk request")
		}
	}
	return nil
}

func oneOf(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
