package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"kairo/internal/imageref"
	"kairo/internal/orchestration"
)

// Kairo keeps itself (README: Kairo keeps itself): every control snapshot
// interval the daemon declares a task of project kairo, queue
// control-snapshot, in a container (ControlImage) on the node of the first
// durable store that has a container executor: `kairo control snapshot`
// (the agent's binary, mounted in every container at /kairo/bin/kairo),
// which writes what the daemon hands it (its database dump, configuration
// and TLS: api/control.go; the code snapshots, once each, a sources task's:
// control_sources.go) into the run's output directory.
// Like any output, that is packed when the run ends and copied to every
// durable store, so the control plane can be rebuilt from a durable store
// alone.

const (
	ControlProject = "kairo"
	ControlQueue   = "control-snapshot"
	// ControlPriority: before anything, so nothing holds it back. It reserves
	// no CPU, RAM or GPU (the daemon dumps; the task only writes what it is
	// sent), so it always fits and never makes room by suspending anything.
	ControlPriority = 1_000_000
	// ControlImage is what the task's container is made of: any image runs
	// it (the binary is Kairo's, static); this one, pinned for good, is
	// small and cached on the nodes already.
	ControlImage = "docker.io/library/python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b"
)

// ControlSnapshotTask declares the control snapshot task of the slot at
// (at, truncated to every; named snapshot-<UTC time>), unless declared
// already: its name, or "" when the slot's was declared before or no node can
// run one (no durable store, or none of its nodes reports kairo).
func (s *Store) ControlSnapshotTask(ctx context.Context, at time.Time, every time.Duration) (string, error) {
	if every <= 0 {
		return "", errors.New("a control snapshot interval must be positive")
	}
	return s.declareControlTask(ctx, controlSnapshotPrefix+at.UTC().Truncate(every).Format("20060102T1504Z"))
}

// declareControlTask declares the control-snapshot task name (snapshot-* or
// sources-*: what its attempt is handed goes by its name, ControlPartOf),
// unless declared already: its name, or "" when it was declared before or no
// node can run it.
func (s *Store) declareControlTask(ctx context.Context, name string) (string, error) {
	tx, err := s.beginRead(ctx)
	if err != nil {
		return "", err
	}
	var exists bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM orchestration_tasks t JOIN coordination_scopes p ON p.id=t.project_scope_id
		WHERE p.kind='project' AND p.external_key=$1 AND t.name=$2)`, ControlProject, name).Scan(&exists)
	if err != nil || exists {
		tx.Rollback()
		return "", err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		tx.Rollback()
		return "", err
	}
	targets, err := copyTargetsTx(ctx, tx, declared)
	if err != nil || len(targets) == 0 {
		tx.Rollback()
		return "", err
	}
	// the first durable store whose node runs containers
	var node string
	for _, target := range targets {
		var containers bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM executors WHERE node_id=$1 AND enabled
			AND COALESCE((attributes_json::jsonb)->>'`+ContainerRuntimeAttribute+`','')<>'')`, target.Node).Scan(&containers); err != nil {
			tx.Rollback()
			return "", err
		}
		if containers {
			node = target.Node
			break
		}
	}
	tx.Rollback()
	if node == "" {
		return "", nil
	}
	pinned, err := imageref.Parse(ControlImage)
	if err != nil {
		return "", err
	}
	execution := orchestration.Execution{SchemaVersion: 2, Priority: ControlPriority, Image: "control",
		Argv: []string{containerKairo, "control", "snapshot"}, CWD: "/", Executor: orchestration.ExecutorSelector{Labels: map[string]string{}, Node: node}}
	declaration, err := orchestration.Normalize(orchestration.Manifest{SchemaVersion: orchestration.SchemaVersion, Project: orchestration.Project{Name: ControlProject},
		Images:    map[string]orchestration.Image{"control": {Ref: ControlImage}},
		ImagePins: map[string]string{"control": pinned.Pinned()},
		Tasks:     []orchestration.Task{{Name: name, Queue: ControlQueue, DependsOn: []string{}, Policy: orchestration.PolicyRunToCompletion, Execution: execution}}})
	if err != nil {
		return "", fmt.Errorf("control snapshot task: %w", err)
	}
	result, err := s.ApplyProject(ctx, declaration)
	if errors.Is(err, ErrOrchestrationConflict) {
		// the slot's task was declared before, by another binary or for
		// another node
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(result.Created) == 0 {
		return "", nil
	}
	return name, nil
}

// containerKairo is the agent's binary in every container (executor:
// container.go).
const containerKairo = "/kairo/bin/kairo"

// AttemptQueue is the project and queue of the execution attempt attemptID
// belongs to.
func (s *Store) AttemptQueue(ctx context.Context, attemptID string) (project, queue string, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT p.external_key,COALESCE(q.external_key,'') FROM attempts a JOIN execution_requests e ON e.id=a.execution_id
		JOIN coordination_scopes p ON p.id=e.project_scope_id LEFT JOIN coordination_scopes q ON q.id=e.queue_scope_id WHERE a.id=$1`, attemptID).Scan(&project, &queue)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return project, queue, err
}
