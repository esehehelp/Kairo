package orchestration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	SchemaVersion         = 1
	PolicyRunToCompletion = "RunToCompletion@v1"
)

type Manifest struct {
	SchemaVersion int     `toml:"schema_version" json:"schema_version"`
	Project       Project `toml:"project" json:"project"`
	Tasks         []Task  `toml:"tasks" json:"tasks"`
}

type Project struct {
	Name string `toml:"name" json:"name"`
}

type Task struct {
	Name      string    `toml:"name" json:"name"`
	Queue     string    `toml:"queue" json:"queue"`
	DependsOn []string  `toml:"depends_on" json:"depends_on"`
	Policy    string    `toml:"policy" json:"policy"`
	Execution Execution `toml:"execution" json:"execution"`
}

type Execution struct {
	SchemaVersion  int                `toml:"schema_version" json:"schema_version"`
	Argv           []string           `toml:"argv" json:"argv"`
	CWD            string             `toml:"cwd" json:"cwd"`
	Priority       int                `toml:"priority" json:"priority"`
	Checkpointable bool               `toml:"checkpointable" json:"checkpointable"`
	Preemptible    bool               `toml:"preemptible" json:"preemptible"`
	Executor       ExecutorSelector   `toml:"executor" json:"executor"`
	Exclusive      []ExclusiveRequest `toml:"exclusive" json:"exclusive"`
	Capacity       CapacityRequest    `toml:"capacity" json:"capacity"`
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
	Manifest    Manifest
	Normalized  []byte
	Digest      string
	TaskDigests map[string]string
}

func Parse(source []byte) (Validated, error) {
	var manifest Manifest
	metadata, err := toml.Decode(string(source), &manifest)
	if err != nil {
		return Validated{}, fmt.Errorf("parse TOML: %w", err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		return Validated{}, fmt.Errorf("unknown project declaration field %q", undecoded[0].String())
	}
	return Normalize(manifest)
}

func Normalize(manifest Manifest) (Validated, error) {
	if manifest.SchemaVersion != SchemaVersion {
		return Validated{}, fmt.Errorf("schema_version must be %d, got %d", SchemaVersion, manifest.SchemaVersion)
	}
	manifest.Project.Name = strings.TrimSpace(manifest.Project.Name)
	if manifest.Project.Name == "" {
		return Validated{}, errors.New("project.name is required")
	}
	if len(manifest.Tasks) == 0 {
		return Validated{}, errors.New("at least one task is required")
	}

	taskNames := map[string]struct{}{}
	for i := range manifest.Tasks {
		task := &manifest.Tasks[i]
		task.Name = strings.TrimSpace(task.Name)
		task.Queue = strings.TrimSpace(task.Queue)
		task.Policy = strings.TrimSpace(task.Policy)
		if task.Name == "" || task.Queue == "" {
			return Validated{}, fmt.Errorf("task %d requires name and queue", i)
		}
		if _, exists := taskNames[task.Name]; exists {
			return Validated{}, fmt.Errorf("duplicate task name %q", task.Name)
		}
		taskNames[task.Name] = struct{}{}
		if task.Policy != PolicyRunToCompletion {
			return Validated{}, fmt.Errorf("task %q has unsupported policy %q", task.Name, task.Policy)
		}
		if err := normalizeExecution(&task.Execution); err != nil {
			return Validated{}, fmt.Errorf("task %q execution: %w", task.Name, err)
		}
		seenDependencies := map[string]struct{}{}
		for dependencyIndex := range task.DependsOn {
			dependency := strings.TrimSpace(task.DependsOn[dependencyIndex])
			if dependency == "" {
				return Validated{}, fmt.Errorf("task %q has an empty dependency", task.Name)
			}
			if _, exists := seenDependencies[dependency]; exists {
				return Validated{}, fmt.Errorf("task %q repeats dependency %q", task.Name, dependency)
			}
			seenDependencies[dependency] = struct{}{}
			task.DependsOn[dependencyIndex] = dependency
		}
		sort.Strings(task.DependsOn)
	}
	for _, task := range manifest.Tasks {
		for _, dependency := range task.DependsOn {
			if _, exists := taskNames[dependency]; !exists {
				return Validated{}, fmt.Errorf("task %q depends on unknown task %q", task.Name, dependency)
			}
			if dependency == task.Name {
				return Validated{}, fmt.Errorf("task %q cannot depend on itself", task.Name)
			}
		}
	}
	if err := validateAcyclic(manifest.Tasks); err != nil {
		return Validated{}, err
	}
	sort.Slice(manifest.Tasks, func(i, j int) bool { return manifest.Tasks[i].Name < manifest.Tasks[j].Name })

	taskDigests := make(map[string]string, len(manifest.Tasks))
	for _, task := range manifest.Tasks {
		body, err := json.Marshal(task)
		if err != nil {
			return Validated{}, err
		}
		taskDigests[task.Name] = digest(body)
	}
	normalized, err := json.Marshal(manifest)
	if err != nil {
		return Validated{}, err
	}
	return Validated{Manifest: manifest, Normalized: normalized, Digest: digest(normalized), TaskDigests: taskDigests}, nil
}

func normalizeExecution(value *Execution) error {
	if value.SchemaVersion != 2 {
		return fmt.Errorf("schema_version must be 2, got %d", value.SchemaVersion)
	}
	value.CWD = strings.TrimSpace(value.CWD)
	if len(value.Argv) == 0 || strings.TrimSpace(value.Argv[0]) == "" || value.CWD == "" {
		return errors.New("argv and cwd are required")
	}
	if value.Preemptible && !value.Checkpointable {
		return errors.New("preemptible execution must be checkpointable")
	}
	if value.Executor.Labels == nil {
		value.Executor.Labels = map[string]string{}
	}
	if value.Exclusive == nil {
		value.Exclusive = []ExclusiveRequest{}
	}
	for i := range value.Exclusive {
		request := &value.Exclusive[i]
		request.Kind = strings.TrimSpace(request.Kind)
		if request.Kind != "gpu" || request.Count < 1 {
			return fmt.Errorf("exclusive request %d must be a positive GPU request", i)
		}
		if request.MinTotalMemoryBytes < 0 || request.MinObservedFreeMemoryBytes < 0 {
			return fmt.Errorf("exclusive request %d has negative memory", i)
		}
		if request.OnExternalClaim == "" {
			request.OnExternalClaim = "wait"
		}
		if request.OnUnattributedActivity == "" {
			request.OnUnattributedActivity = "wait"
		}
		if request.OnStaleObservation == "" {
			request.OnStaleObservation = "wait"
		}
		if !oneOf(request.OnExternalClaim, "wait", "fail") ||
			!oneOf(request.OnUnattributedActivity, "wait", "fail", "allow") ||
			!oneOf(request.OnStaleObservation, "wait", "fail") {
			return fmt.Errorf("exclusive request %d has an invalid conflict policy", i)
		}
	}
	if value.Capacity.Strength == "" {
		value.Capacity.Strength = "admitted"
	}
	if value.Capacity.Strength != "admitted" {
		return errors.New("only admitted capacity is supported")
	}
	if value.Capacity.CPUMillis < 0 || value.Capacity.RAMBytes < 0 {
		return errors.New("capacity cannot be negative")
	}
	if value.Capacity.Disks == nil {
		value.Capacity.Disks = []DiskRequest{}
	}
	for i := range value.Capacity.Disks {
		disk := &value.Capacity.Disks[i]
		disk.Filesystem = strings.TrimSpace(disk.Filesystem)
		if disk.Filesystem == "" || disk.ReserveBytes < 0 || disk.MinFreeAfterBytes < 0 {
			return fmt.Errorf("disk request %d is invalid", i)
		}
	}
	return nil
}

func validateAcyclic(tasks []Task) error {
	dependencies := make(map[string][]string, len(tasks))
	for _, task := range tasks {
		dependencies[task.Name] = task.DependsOn
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("task dependency graph contains a cycle at %q", name)
		}
		if visited[name] {
			return nil
		}
		visiting[name] = true
		for _, dependency := range dependencies[name] {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		visiting[name] = false
		visited[name] = true
		return nil
	}
	for name := range dependencies {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func oneOf(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
