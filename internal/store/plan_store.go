package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kairo/internal/id"
	"kairo/internal/plan"
)

func (s *Store) ApplyPlan(ctx context.Context, validated plan.Validated, options ApplyOptions) (ApplyResult, error) {
	if strings.TrimSpace(options.Actor) == "" {
		options.Actor = "unknown"
	}
	if strings.TrimSpace(options.RequestID) == "" {
		return ApplyResult{}, errors.New("request ID is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, err
	}
	defer tx.Rollback()
	m := validated.Manifest
	queue, err := getQueueByNameTx(ctx, tx, m.Project, m.Queue)
	if errors.Is(err, sql.ErrNoRows) {
		if !options.Create {
			return ApplyResult{}, fmt.Errorf("%w: queue does not exist; use create", ErrRevisionConflict)
		}
		if options.ExpectedRevision != 0 {
			return ApplyResult{}, fmt.Errorf("%w: expected 0 for a new queue", ErrRevisionConflict)
		}
		t := now()
		queue = Queue{ID: id.New("que"), Project: m.Project, Name: m.Queue, DesiredState: "active", CreatedAt: t, UpdatedAt: t}
		if _, err = tx.ExecContext(ctx, `INSERT INTO queues(id,project,name,created_at,updated_at) VALUES(?,?,?,?,?)`, queue.ID, queue.Project, queue.Name, t, t); err != nil {
			return ApplyResult{}, err
		}
	} else if err != nil {
		return ApplyResult{}, err
	} else if options.Create {
		return ApplyResult{}, fmt.Errorf("%w: queue already exists", ErrRevisionConflict)
	}

	var requestRevision int
	var requestDigest string
	if err := tx.QueryRowContext(ctx, `SELECT revision,digest FROM queue_revisions WHERE queue_id=? AND request_id=?`, queue.ID, options.RequestID).Scan(&requestRevision, &requestDigest); err == nil {
		if requestDigest != validated.Digest {
			return ApplyResult{}, fmt.Errorf("%w: request ID was already used for another manifest", ErrRevisionConflict)
		}
		return ApplyResult{Queue: queue, Revision: requestRevision, Idempotent: true}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ApplyResult{}, err
	}
	var currentDigest string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM queue_revisions WHERE queue_id=? AND revision=?`, queue.ID, queue.CurrentRevision).Scan(&currentDigest)
	if err == nil && currentDigest == validated.Digest {
		return ApplyResult{Queue: queue, Revision: queue.CurrentRevision, Idempotent: true}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ApplyResult{}, err
	}
	if queue.CurrentRevision != options.ExpectedRevision {
		return ApplyResult{}, fmt.Errorf("%w: expected %d, current %d", ErrRevisionConflict, options.ExpectedRevision, queue.CurrentRevision)
	}
	newQueueRevision := queue.CurrentRevision + 1
	t := now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO queue_revisions(queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		queue.ID, newQueueRevision, validated.Digest, string(validated.Normalized), validated.SourceText, options.Actor, options.RequestID, t); err != nil {
		return ApplyResult{}, err
	}

	byKey := make(map[string]plan.Task, len(m.Tasks))
	for _, task := range m.Tasks {
		byKey[task.Key] = task
	}
	type revisionRef struct {
		id       string
		revision int
		changed  bool
	}
	refs := make(map[string]revisionRef, len(m.Tasks))
	for _, key := range validated.Order {
		spec := byKey[key]
		var task Task
		var retryJSON string
		err := tx.QueryRowContext(ctx, `SELECT id,current_revision,desired_state,scheduling_state,effective_priority,effective_retry_json,became_runnable_at FROM tasks WHERE queue_id=? AND task_key=?`, queue.ID, key).
			Scan(&task.ID, &task.CurrentRevision, &task.DesiredState, &task.SchedulingState, &task.Priority, &retryJSON, &task.BecameRunnableAt)
		created := errors.Is(err, sql.ErrNoRows)
		if err != nil && !created {
			return ApplyResult{}, err
		}
		if created {
			task = Task{ID: id.New("tsk"), QueueID: queue.ID, Key: key, DesiredState: "active", SchedulingState: "pending", Priority: spec.Priority}
			retry, _ := json.Marshal(spec.Retry)
			if _, err = tx.ExecContext(ctx, `INSERT INTO tasks(id,queue_id,task_key,desired_state,scheduling_state,effective_priority,effective_retry_json,created_at,updated_at) VALUES(?,?,?,'active','pending',?,?,?,?)`, task.ID, queue.ID, key, spec.Priority, string(retry), t, t); err != nil {
				return ApplyResult{}, err
			}
		}
		structural := structuralDigest(spec)
		needRevision := created
		if !created {
			var previousDigest string
			if err := tx.QueryRowContext(ctx, `SELECT spec_digest FROM task_revisions WHERE task_id=? AND revision=?`, task.ID, task.CurrentRevision).Scan(&previousDigest); err != nil {
				return ApplyResult{}, err
			}
			needRevision = previousDigest != structural
		}
		for _, dep := range spec.DependsOn {
			if refs[dep].changed {
				needRevision = true
			}
		}
		retry, _ := json.Marshal(spec.Retry)
		if _, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='active',effective_priority=?,effective_retry_json=?,updated_at=? WHERE id=?`, spec.Priority, string(retry), t, task.ID); err != nil {
			return ApplyResult{}, err
		}
		// Any accepted plan revision is an explicit admission retry boundary,
		// including a priority-only change.
		if _, err = tx.ExecContext(ctx, `UPDATE admission_blocks SET cleared_at=? WHERE task_id=? AND cleared_at IS NULL`, t, task.ID); err != nil {
			return ApplyResult{}, err
		}
		if task.SchedulingState == "failed_admission" {
			task.SchedulingState = "pending"
			if _, err = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state='pending',became_runnable_at=NULL WHERE id=?`, task.ID); err != nil {
				return ApplyResult{}, err
			}
			if task.CurrentRevision > 0 {
				if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET scheduling_state='pending',became_runnable_at=NULL WHERE task_id=? AND revision=?`, task.ID, task.CurrentRevision); err != nil {
					return ApplyResult{}, err
				}
			}
		}
		if !needRevision {
			if task.DesiredState != "active" {
				if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='active',scheduling_state=CASE WHEN scheduling_state IN('paused','cancelled') THEN 'pending' ELSE scheduling_state END,became_runnable_at=CASE WHEN scheduling_state IN('paused','cancelled') THEN NULL ELSE became_runnable_at END WHERE task_id=? AND revision=?`, task.ID, task.CurrentRevision); err != nil {
					return ApplyResult{}, err
				}
				if task.SchedulingState == "paused" || task.SchedulingState == "cancelled" {
					if _, err = tx.ExecContext(ctx, `UPDATE tasks SET scheduling_state='pending',became_runnable_at=NULL WHERE id=?`, task.ID); err != nil {
						return ApplyResult{}, err
					}
				}
			}
			refs[key] = revisionRef{task.ID, task.CurrentRevision, false}
			continue
		}
		newTaskRevision := task.CurrentRevision + 1
		argv, _ := json.Marshal(spec.Argv)
		selector, _ := json.Marshal(spec.Executor)
		runnable := any(nil)
		if len(spec.DependsOn) == 0 {
			runnable = t
		}
		if task.CurrentRevision > 0 {
			if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET superseded=1 WHERE task_id=? AND revision=?`, task.ID, task.CurrentRevision); err != nil {
				return ApplyResult{}, err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO task_revisions(task_id,revision,queue_revision,spec_digest,argv_json,cwd,executor_selector_json,priority,checkpointable,retry_json,became_runnable_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			task.ID, newTaskRevision, newQueueRevision, structural, string(argv), spec.CWD, string(selector), spec.Priority, spec.Checkpointable, string(retry), runnable, t); err != nil {
			return ApplyResult{}, err
		}
		for _, depKey := range spec.DependsOn {
			dep := refs[depKey]
			if _, err = tx.ExecContext(ctx, `INSERT INTO task_dependencies(task_id,task_revision,dependency_task_id,dependency_revision) VALUES(?,?,?,?)`, task.ID, newTaskRevision, dep.id, dep.revision); err != nil {
				return ApplyResult{}, err
			}
		}
		if err := insertRequests(ctx, tx, task.ID, newTaskRevision, spec); err != nil {
			return ApplyResult{}, err
		}
		summaryState := "pending"
		if task.SchedulingState == "running" || task.SchedulingState == "preempting" {
			summaryState = task.SchedulingState
		}
		if _, err = tx.ExecContext(ctx, `UPDATE tasks SET current_revision=?,desired_state='active',scheduling_state=?,became_runnable_at=?,updated_at=? WHERE id=?`, newTaskRevision, summaryState, runnable, t, task.ID); err != nil {
			return ApplyResult{}, err
		}
		refs[key] = revisionRef{task.ID, newTaskRevision, true}
	}
	// Removed tasks never receive a new attempt. Running revisions may finish.
	rows, err := tx.QueryContext(ctx, `SELECT id,task_key,current_revision,scheduling_state FROM tasks WHERE queue_id=?`, queue.ID)
	if err != nil {
		return ApplyResult{}, err
	}
	type removedTask struct {
		id, key string
		rev     int
		state   string
	}
	var removed []removedTask
	for rows.Next() {
		var x removedTask
		if err := rows.Scan(&x.id, &x.key, &x.rev, &x.state); err != nil {
			rows.Close()
			return ApplyResult{}, err
		}
		if _, ok := byKey[x.key]; !ok {
			removed = append(removed, x)
		}
	}
	rows.Close()
	for _, x := range removed {
		state := "cancelled"
		if x.state == "running" || x.state == "preempting" {
			state = x.state
		}
		if _, err = tx.ExecContext(ctx, `UPDATE tasks SET desired_state='cancelled',scheduling_state=?,updated_at=? WHERE id=?`, state, t, x.id); err != nil {
			return ApplyResult{}, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE task_revisions SET desired_state='cancelled',scheduling_state=CASE WHEN scheduling_state IN('running','preempting') THEN scheduling_state ELSE 'cancelled' END WHERE task_id=? AND revision=?`, x.id, x.rev); err != nil {
			return ApplyResult{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE queues SET current_revision=?,updated_at=? WHERE id=?`, newQueueRevision, t, queue.ID); err != nil {
		return ApplyResult{}, err
	}
	if err = appendEvent(ctx, tx, "plan_applied", "queue", queue.ID, nil, map[string]any{"revision": newQueueRevision, "digest": validated.Digest}); err != nil {
		return ApplyResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return ApplyResult{}, err
	}
	queue.CurrentRevision = newQueueRevision
	queue.UpdatedAt = t
	return ApplyResult{Queue: queue, Revision: newQueueRevision, Created: options.ExpectedRevision == 0}, nil
}

func structuralDigest(t plan.Task) string {
	value := struct {
		Argv           []string
		CWD            string
		Depends        []string
		Checkpointable bool
		Executor       plan.ExecutorSelector
		Exclusive      []plan.ExclusiveRequest
		Capacity       plan.CapacityRequest
	}{t.Argv, t.CWD, t.DependsOn, t.Checkpointable, t.Executor, t.Exclusive, t.Capacity}
	b, _ := json.Marshal(value)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func insertRequests(ctx context.Context, tx *sql.Tx, taskID string, revision int, t plan.Task) error {
	for _, r := range t.Exclusive {
		constraints, _ := json.Marshal(map[string]any{"same_node": r.SameNode, "min_total_memory_bytes": r.MinTotalMemoryBytes, "min_observed_free_memory_bytes": r.MinObservedFreeMemoryBytes})
		policy, _ := json.Marshal(map[string]string{"on_external_claim": r.OnExternalClaim, "on_unattributed_activity": r.OnUnattributedActivity, "on_stale_observation": r.OnStaleObservation})
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,task_id,task_revision,request_type,kind,quantity,constraints_json,policy_json) VALUES(?,?,?,'exclusive',?,?,?,?)`, id.New("req"), taskID, revision, r.Kind, r.Count, string(constraints), string(policy)); err != nil {
			return err
		}
	}
	if t.Capacity.CPUMillis > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,task_id,task_revision,request_type,kind,quantity,strength) VALUES(?,?,?,'capacity','cpu',?,'admitted')`, id.New("req"), taskID, revision, t.Capacity.CPUMillis); err != nil {
			return err
		}
	}
	if t.Capacity.RAMBytes > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,task_id,task_revision,request_type,kind,quantity,strength) VALUES(?,?,?,'capacity','ram',?,'admitted')`, id.New("req"), taskID, revision, t.Capacity.RAMBytes); err != nil {
			return err
		}
	}
	for _, d := range t.Capacity.Disks {
		c, _ := json.Marshal(map[string]int64{"min_free_after_bytes": d.MinFreeAfterBytes})
		if _, err := tx.ExecContext(ctx, `INSERT INTO resource_requests(id,task_id,task_revision,request_type,kind,quantity,filesystem,constraints_json,strength) VALUES(?,?,?,'capacity','disk',?,?,?,'admitted')`, id.New("req"), taskID, revision, d.ReserveBytes, d.Filesystem, string(c)); err != nil {
			return err
		}
	}
	return nil
}

func getQueueByNameTx(ctx context.Context, tx *sql.Tx, project, name string) (Queue, error) {
	var q Queue
	err := tx.QueryRowContext(ctx, `SELECT id,project,name,current_revision,desired_state,created_at,updated_at FROM queues WHERE project=? AND name=?`, project, name).Scan(&q.ID, &q.Project, &q.Name, &q.CurrentRevision, &q.DesiredState, &q.CreatedAt, &q.UpdatedAt)
	return q, err
}
func (s *Store) GetQueue(ctx context.Context, project, name string) (Queue, error) {
	var q Queue
	err := s.db.QueryRowContext(ctx, `SELECT id,project,name,current_revision,desired_state,created_at,updated_at FROM queues WHERE project=? AND name=?`, project, name).Scan(&q.ID, &q.Project, &q.Name, &q.CurrentRevision, &q.DesiredState, &q.CreatedAt, &q.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return q, err
}
func (s *Store) QueueHistory(ctx context.Context, queueID string) ([]QueueRevision, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT queue_id,revision,digest,normalized_json,source_text,actor,request_id,created_at FROM queue_revisions WHERE queue_id=? ORDER BY revision DESC`, queueID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueRevision
	for rows.Next() {
		var r QueueRevision
		var n string
		if err := rows.Scan(&r.QueueID, &r.Revision, &r.Digest, &n, &r.SourceText, &r.Actor, &r.RequestID, &r.CreatedAt); err != nil {
			return nil, err
		}
		r.Normalized = json.RawMessage(n)
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) ListQueues(ctx context.Context) ([]Queue, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,project,name,current_revision,desired_state,created_at,updated_at FROM queues ORDER BY project,name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.ID, &q.Project, &q.Name, &q.CurrentRevision, &q.DesiredState, &q.CreatedAt, &q.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}
func appendEvent(ctx context.Context, tx *sql.Tx, eventType, aggregateType, aggregateID string, epoch *int64, payload any) error {
	body, _ := json.Marshal(payload)
	_, err := tx.ExecContext(ctx, `INSERT INTO coordination_events(event_type,aggregate_type,aggregate_id,coordination_epoch,payload_json,created_at) VALUES(?,?,?,?,?,?)`, eventType, aggregateType, aggregateID, epoch, string(body), now())
	return err
}
func randomToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token := hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(sum[:]), nil
}
