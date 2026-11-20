package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kairo/internal/id"
	"kairo/internal/machine"
)

// Checkpoints of running work (README: Checkpoints of running work). A run
// keeps what it writes to its output directory only once it has ended; a
// checkpoint it publishes before that is kept at once: a checkpoint whose
// reference (continuation_ref) is an absolute path on the attempt's node, a
// file or a directory outside every store, is imported into the attempt's
// project as checkpoints/<task>/<name> (ProtectCheckpoint), packed and copied
// to the durable stores like any import, the path only read. A running
// checkpointable attempt that has published none for a while is asked for
// one (RequestDueCheckpoints: a checkpoint command, origin data_protection,
// which the attempt answers at its next safe point and carries on) when its
// project has published a path checkpoint before: what a worker does for a
// checkpoint is its own (one serving a model may stop the server and name
// that by a URI), and only a path is kept.

// ProtectCheckpoint imports the checkpoint ref attemptID published: the
// object made (or the one already made of that path), "" when ref names no
// path of the node.
func (s *Store) ProtectCheckpoint(ctx context.Context, attemptID, ref string) (string, error) {
	if !nodePath(ref) {
		return "", nil
	}
	var node, project, task string
	err := s.db.QueryRowContext(ctx, `SELECT x.node_id,p.external_key,COALESCE(t.name,e.id) FROM attempts a JOIN executors x ON x.id=a.executor_id
		JOIN execution_requests e ON e.id=a.execution_id JOIN coordination_scopes p ON p.id=e.project_scope_id
		LEFT JOIN orchestration_tasks t ON t.task_scope_id=e.task_scope_id WHERE a.id=$1`, attemptID).Scan(&node, &project, &task)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return "", err
	}
	// once a path: a repeated acknowledgement imports nothing again
	var existing string
	err = s.db.QueryRowContext(ctx, `SELECT d.id FROM data_objects d JOIN executors x ON x.id=d.executor_id
		WHERE d.kind='import' AND x.node_id=$1 AND d.source_path=$2 AND d.state<>'abandoned' ORDER BY d.created_at LIMIT 1`, node, ref).Scan(&existing)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	// one inside a store (the run's output directory) is kept by the run's
	// snapshots (runsnapshots.go), ordered once it is published
	declared, err := declaredStores(ctx, s.db)
	if err != nil {
		return "", err
	}
	for _, d := range declared {
		if d.Node == node && machine.PathUnder(ref, d.Root) {
			return "", nil
		}
	}
	objects, err := s.ImportData(ctx, ImportRequest{Project: project, Node: node, Paths: []string{ref}, Name: checkpointName(task, ref)})
	if err != nil {
		return "", err
	}
	return objects[0].ObjectID, nil
}

// nodePath: ref is an absolute path of a node (C:\..., C:/..., /...), not a
// URI or a UNC path.
func nodePath(ref string) bool {
	if len(ref) >= 3 && ref[1] == ':' && (ref[2] == '\\' || ref[2] == '/') {
		c := ref[0] | 0x20
		return c >= 'a' && c <= 'z'
	}
	return strings.HasPrefix(ref, "/") && !strings.HasPrefix(ref, "//")
}

// checkpointName is checkpoints/<task>/<the path's last component>, the task
// left out when that is too long a name.
func checkpointName(task, ref string) string {
	base := ref[strings.LastIndexAny(strings.TrimRight(ref, `/\`), `/\`)+1:]
	base = strings.TrimRight(base, `/\`)
	name := "checkpoints/" + task + "/" + base
	if len(name) > 200 {
		name = "checkpoints/" + base
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

// RequestDueCheckpoints asks every running checkpointable attempt that has
// run for every and has had no checkpoint asked of it for every (nor has one
// in flight, nor a suspend), of a project that has published a checkpoint
// that is a path before, for a checkpoint: the commands made.
func (s *Store) RequestDueCheckpoints(ctx context.Context, every time.Duration) ([]string, error) {
	if every <= 0 {
		return nil, nil
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cutoff := time.Now().UTC().Add(-every).Format(time.RFC3339Nano)
	type target struct {
		executionID, attemptID, leaseID, projectID string
		epoch                                      int64
	}
	var targets []target
	err = eachRow(ctx, tx, `SELECT e.id,a.id,l.id,e.project_scope_id,a.coordination_epoch FROM execution_requests e
		JOIN attempts a ON a.execution_id=e.id AND a.state='running'
		JOIN leases l ON l.attempt_id=a.id AND l.state='active'
		WHERE e.state='started' AND e.checkpointable AND a.started_at IS NOT NULL AND a.started_at::timestamptz<$1::text::timestamptz
		AND NOT EXISTS (SELECT 1 FROM commands c WHERE c.attempt_id=a.id AND (c.state IN('pending','accepted','checkpointing') OR (c.kind='checkpoint' AND c.created_at::timestamptz>=$1::text::timestamptz)))
		AND EXISTS (SELECT 1 FROM commands pc JOIN execution_requests pe ON pe.id=pc.execution_id
			WHERE pe.project_scope_id=e.project_scope_id AND pc.state='checkpointed'
			AND pc.payload_json ~ '"continuation_ref" *: *"([A-Za-z]:(/|\\\\)|/[^/])')
		ORDER BY a.started_at,a.id`, []any{cutoff}, func(rows *sql.Rows) error {
		var t target
		if err := rows.Scan(&t.executionID, &t.attemptID, &t.leaseID, &t.projectID, &t.epoch); err != nil {
			return err
		}
		targets = append(targets, t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var commands []string
	for _, t := range targets {
		commandID, stamp := id.New("cmd"), now()
		reason := fmt.Sprintf("Kairo keeps running work: no checkpoint for %s", every)
		if _, err = tx.ExecContext(ctx, `INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'checkpoint','data_protection',$6,'pending',$7,$8)`,
			commandID, t.executionID, t.attemptID, t.leaseID, t.epoch, reason, stamp, stamp); err != nil {
			return nil, err
		}
		if err = appendCoordinationEventTx(ctx, tx, "checkpoint_requested", &t.projectID, "command", commandID, &t.epoch, map[string]any{"execution_id": t.executionID, "attempt_id": t.attemptID, "lease_id": t.leaseID, "actor": "kairo", "origin": "data_protection"}); err != nil {
			return nil, err
		}
		commands = append(commands, commandID)
	}
	if len(commands) == 0 {
		return nil, nil
	}
	return commands, tx.Commit()
}
