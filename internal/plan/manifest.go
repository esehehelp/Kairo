package plan

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
	SchemaVersion int    `toml:"schema_version" json:"schema_version"`
	Project       string `toml:"project" json:"project"`
	Queue         string `toml:"queue" json:"queue"`
	Tasks         []Task `toml:"tasks" json:"tasks"`
}

type Task struct {
	Key            string             `toml:"key" json:"key"`
	Argv           []string           `toml:"argv" json:"argv"`
	CWD            string             `toml:"cwd" json:"cwd"`
	Priority       int                `toml:"priority" json:"priority"`
	DependsOn      []string           `toml:"depends_on" json:"depends_on"`
	Checkpointable bool               `toml:"checkpointable" json:"checkpointable"`
	Executor       ExecutorSelector   `toml:"executor" json:"executor"`
	Retry          RetryPolicy        `toml:"retry" json:"retry"`
	Exclusive      []ExclusiveRequest `toml:"exclusive" json:"exclusive"`
	Capacity       CapacityRequest    `toml:"capacity" json:"capacity"`
}

type ExecutorSelector struct {
	Labels map[string]string `toml:"labels" json:"labels"`
}

type RetryPolicy struct {
	MaxFailureAttempts    int      `toml:"max_failure_attempts" json:"max_failure_attempts"`
	RetryOn               []string `toml:"retry_on" json:"retry_on"`
	InitialBackoffSeconds int      `toml:"initial_backoff_seconds" json:"initial_backoff_seconds"`
	MaxBackoffSeconds     int      `toml:"max_backoff_seconds" json:"max_backoff_seconds"`
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
	SourceText string
	Normalized []byte
	Digest     string
	Order      []string
}

func Parse(source []byte) (Validated, error) {
	var m Manifest
	metadata, err := toml.Decode(string(source), &m)
	if err != nil {
		return Validated{}, fmt.Errorf("parse TOML: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		return Validated{}, fmt.Errorf("unknown manifest field %q", undecoded[0].String())
	}
	if err := normalizeAndValidate(&m); err != nil {
		return Validated{}, err
	}
	order, err := topologicalOrder(m.Tasks)
	if err != nil {
		return Validated{}, err
	}
	normalized, err := json.Marshal(m)
	if err != nil {
		return Validated{}, err
	}
	sum := sha256.Sum256(normalized)
	return Validated{Manifest: m, SourceText: string(source), Normalized: normalized,
		Digest: hex.EncodeToString(sum[:]), Order: order}, nil
}

func normalizeAndValidate(m *Manifest) error {
	if m.SchemaVersion != 1 {
		return fmt.Errorf("schema_version must be 1, got %d", m.SchemaVersion)
	}
	if strings.TrimSpace(m.Project) == "" || strings.TrimSpace(m.Queue) == "" {
		return errors.New("project and queue are required")
	}
	if len(m.Tasks) == 0 {
		return errors.New("at least one task is required")
	}
	seen := make(map[string]bool, len(m.Tasks))
	validRetry := map[string]bool{"launch_error": true, "exit_nonzero": true, "lost_after_reconcile": true}
	for i := range m.Tasks {
		t := &m.Tasks[i]
		if strings.TrimSpace(t.Key) == "" || seen[t.Key] {
			return fmt.Errorf("task keys must be non-empty and unique: %q", t.Key)
		}
		seen[t.Key] = true
		if len(t.Argv) == 0 || strings.TrimSpace(t.Argv[0]) == "" || strings.TrimSpace(t.CWD) == "" {
			return fmt.Errorf("task %q requires argv and cwd", t.Key)
		}
		if t.Executor.Labels == nil {
			t.Executor.Labels = map[string]string{}
		}
		if t.DependsOn == nil {
			t.DependsOn = []string{}
		}
		if t.Exclusive == nil {
			t.Exclusive = []ExclusiveRequest{}
		}
		if t.Retry.MaxFailureAttempts == 0 {
			t.Retry.MaxFailureAttempts = 1
		}
		if t.Retry.InitialBackoffSeconds == 0 {
			t.Retry.InitialBackoffSeconds = 30
		}
		if t.Retry.MaxBackoffSeconds == 0 {
			t.Retry.MaxBackoffSeconds = 600
		}
		if t.Retry.MaxFailureAttempts < 1 || t.Retry.InitialBackoffSeconds < 0 || t.Retry.MaxBackoffSeconds < t.Retry.InitialBackoffSeconds {
			return fmt.Errorf("task %q has invalid retry policy", t.Key)
		}
		for _, class := range t.Retry.RetryOn {
			if !validRetry[class] {
				return fmt.Errorf("task %q has unsupported retry class %q", t.Key, class)
			}
		}
		for j := range t.Exclusive {
			r := &t.Exclusive[j]
			if r.Kind != "gpu" || r.Count < 1 {
				return fmt.Errorf("task %q exclusive requests must be positive GPU requests", t.Key)
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
			if !oneOf(r.OnExternalClaim, "wait", "fail") || !oneOf(r.OnUnattributedActivity, "wait", "fail", "allow") || !oneOf(r.OnStaleObservation, "wait", "fail") {
				return fmt.Errorf("task %q has invalid GPU conflict policy", t.Key)
			}
		}
		if t.Capacity.Strength == "" {
			t.Capacity.Strength = "admitted"
		}
		if t.Capacity.Strength != "admitted" {
			return fmt.Errorf("task %q: v1 supports only admitted capacity", t.Key)
		}
		if t.Capacity.CPUMillis < 0 || t.Capacity.RAMBytes < 0 {
			return fmt.Errorf("task %q has negative capacity", t.Key)
		}
		for _, disk := range t.Capacity.Disks {
			if strings.TrimSpace(disk.Filesystem) == "" || disk.ReserveBytes < 0 || disk.MinFreeAfterBytes < 0 {
				return fmt.Errorf("task %q has invalid disk request", t.Key)
			}
		}
	}
	return nil
}

func topologicalOrder(tasks []Task) ([]string, error) {
	byKey := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		byKey[t.Key] = t
	}
	state := map[string]uint8{}
	order := make([]string, 0, len(tasks))
	var visit func(string) error
	visit = func(key string) error {
		if state[key] == 1 {
			return fmt.Errorf("dependency cycle includes task %q", key)
		}
		if state[key] == 2 {
			return nil
		}
		t, ok := byKey[key]
		if !ok {
			return fmt.Errorf("unknown dependency %q", key)
		}
		state[key] = 1
		for _, dep := range t.DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[key] = 2
		order = append(order, key)
		return nil
	}
	for _, t := range tasks {
		if err := visit(t.Key); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func oneOf(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
