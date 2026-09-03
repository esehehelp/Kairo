package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kairo/internal/id"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

//go:embed migration_v2.sql
var migrationV2 string

var (
	ErrNotFound         = errors.New("not found")
	ErrSpecConflict     = errors.New("workload idempotency key already has a different spec")
	ErrRevisionConflict = errors.New("queue revision conflict")
	ErrStaleEpoch       = errors.New("stale coordination epoch")
	ErrLegacySchema     = errors.New("legacy Kairo database detected; back up the database and recreate it")
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	var migrated int
	err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&migrated)
	if err != nil {
		db.Close()
		return nil, err
	}
	if migrated == 0 {
		var userTables int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&userTables); err != nil {
			db.Close()
			return nil, err
		}
		if userTables != 0 {
			db.Close()
			return nil, ErrLegacySchema
		}
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	} else {
		var version int
		if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
			db.Close()
			return nil, err
		}
		if version == 1 {
			if _, err := db.Exec(migrationV2); err != nil {
				db.Close()
				return nil, fmt.Errorf("migrate schema to version 2: %w", err)
			}
			version = 2
		}
		if version != 2 {
			db.Close()
			return nil, fmt.Errorf("unsupported schema version %d", version)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) AddResource(ctx context.Context, resource Resource) error {
	if resource.ID == "" || resource.Kind == "" || resource.Binding == "" {
		return errors.New("resource id, kind, and binding are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t := now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO nodes(id,name,os,architecture,created_at,updated_at)
		VALUES('compat-node','compat-node','unknown','unknown',?,?) ON CONFLICT(id) DO NOTHING`, t, t); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO resource_providers(id,node_id,kind,created_at,updated_at)
		VALUES('compat-provider','compat-node','compat',?,?) ON CONFLICT(id) DO NOTHING`, t, t); err != nil {
		return err
	}
	bindingJSON, _ := json.Marshal(map[string]string{"value": resource.Binding})
	if _, err = tx.ExecContext(ctx, `INSERT INTO resource_instances(
		id,node_id,provider_id,kind,stable_identity,binding_json,created_at,updated_at)
		VALUES(?,'compat-node','compat-provider',?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,binding_json=excluded.binding_json,updated_at=excluded.updated_at`,
		resource.ID, resource.Kind, resource.ID, string(bindingJSON), t, t); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO resources(id, kind, binding, state, created_at)
		VALUES(?, ?, ?, 'ready', ?)
		ON CONFLICT(id) DO UPDATE SET kind=excluded.kind, binding=excluded.binding`,
		resource.ID, resource.Kind, resource.Binding, t)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func validateSpec(spec WorkloadSpec) error {
	if strings.TrimSpace(spec.Project) == "" || strings.TrimSpace(spec.ExternalKey) == "" {
		return errors.New("project and external_key are required")
	}
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return errors.New("argv must contain an executable")
	}
	if strings.TrimSpace(spec.CWD) == "" {
		return errors.New("cwd is required")
	}
	if strings.TrimSpace(spec.ResourceKind) == "" || spec.ResourceCount < 1 {
		return errors.New("a positive resource request is required")
	}
	return nil
}

func (s *Store) SubmitWorkload(ctx context.Context, spec WorkloadSpec) (Workload, bool, error) {
	if err := validateSpec(spec); err != nil {
		return Workload{}, false, err
	}
	argv, _ := json.Marshal(spec.Argv)
	t := now()
	w := Workload{
		ID: id.New("wrk"), Project: spec.Project, ExternalKey: spec.ExternalKey,
		Priority: spec.Priority, Argv: spec.Argv, CWD: spec.CWD,
		ResourceKind: spec.ResourceKind, ResourceCount: spec.ResourceCount,
		CooperativeSuspend: spec.CooperativeSuspend,
		DesiredState:       "active", SchedulingState: "queued", CreatedAt: t, UpdatedAt: t,
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workloads(
			id, project, external_key, priority, argv_json, cwd, resource_kind,
			resource_count, cooperative_suspend, desired_state, scheduling_state,
			created_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 'queued', ?, ?)`,
		w.ID, w.Project, w.ExternalKey, w.Priority, string(argv), w.CWD,
		w.ResourceKind, w.ResourceCount, w.CooperativeSuspend, t, t)
	if err == nil {
		return w, true, nil
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		return Workload{}, false, err
	}
	existing, getErr := s.GetWorkloadByExternalKey(ctx, spec.Project, spec.ExternalKey)
	if getErr != nil {
		return Workload{}, false, getErr
	}
	if !workloadMatchesSpec(existing, spec) {
		return Workload{}, false, ErrSpecConflict
	}
	return existing, false, nil
}

func workloadMatchesSpec(workload Workload, spec WorkloadSpec) bool {
	if workload.Project != spec.Project || workload.ExternalKey != spec.ExternalKey ||
		workload.Priority != spec.Priority || workload.CWD != spec.CWD ||
		workload.ResourceKind != spec.ResourceKind ||
		workload.ResourceCount != spec.ResourceCount ||
		workload.CooperativeSuspend != spec.CooperativeSuspend ||
		len(workload.Argv) != len(spec.Argv) {
		return false
	}
	for i := range workload.Argv {
		if workload.Argv[i] != spec.Argv[i] {
			return false
		}
	}
	return true
}

func scanWorkload(scanner interface{ Scan(...any) error }) (Workload, error) {
	var w Workload
	var argvJSON string
	var cooperative int
	err := scanner.Scan(
		&w.ID, &w.Project, &w.ExternalKey, &w.Priority, &argvJSON, &w.CWD,
		&w.ResourceKind, &w.ResourceCount, &cooperative, &w.DesiredState,
		&w.SchedulingState, &w.CreatedAt, &w.UpdatedAt,
	)
	if err != nil {
		return Workload{}, err
	}
	w.CooperativeSuspend = cooperative != 0
	if err := json.Unmarshal([]byte(argvJSON), &w.Argv); err != nil {
		return Workload{}, err
	}
	return w, nil
}

const workloadColumns = `id, project, external_key, priority, argv_json, cwd,
	resource_kind, resource_count, cooperative_suspend, desired_state,
	scheduling_state, created_at, updated_at`

func (s *Store) GetWorkload(ctx context.Context, workloadID string) (Workload, error) {
	w, err := scanWorkload(s.db.QueryRowContext(ctx,
		`SELECT `+workloadColumns+` FROM workloads WHERE id=?`, workloadID))
	if errors.Is(err, sql.ErrNoRows) {
		return Workload{}, ErrNotFound
	}
	return w, err
}

func (s *Store) GetWorkloadByExternalKey(ctx context.Context, project, key string) (Workload, error) {
	w, err := scanWorkload(s.db.QueryRowContext(ctx,
		`SELECT `+workloadColumns+` FROM workloads WHERE project=? AND external_key=?`, project, key))
	if errors.Is(err, sql.ErrNoRows) {
		return Workload{}, ErrNotFound
	}
	return w, err
}

func scanAttempt(scanner interface{ Scan(...any) error }) (Attempt, error) {
	var a Attempt
	var progress sql.NullString
	err := scanner.Scan(
		&a.ID, &a.WorkloadID, &a.Ordinal, &a.State, &a.ExecutorID, &a.PID,
		&a.ExitCode, &a.TerminalDisposition, &a.ContinuationRef, &progress,
		&a.StartedAt, &a.LastHeartbeatAt, &a.CheckpointedAt, &a.ExitedAt, &a.QuiescedAt,
	)
	if progress.Valid {
		a.Progress = json.RawMessage(progress.String)
	}
	return a, err
}

const attemptColumns = `id, workload_id, ordinal, state, executor_id, pid,
	exit_code, terminal_disposition, continuation_ref, progress_json, started_at,
	last_heartbeat_at, checkpointed_at, exited_at, quiesced_at`

func (s *Store) GetAttempt(ctx context.Context, attemptID string) (Attempt, error) {
	a, err := scanAttempt(s.db.QueryRowContext(ctx,
		`SELECT `+attemptColumns+` FROM attempts WHERE id=?`, attemptID))
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, ErrNotFound
	}
	return a, err
}

func (s *Store) AllocateNext(ctx context.Context, executorID string) (*Launch, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	w, err := scanWorkload(tx.QueryRowContext(ctx, `
		SELECT `+workloadColumns+` FROM workloads
		WHERE desired_state='active' AND scheduling_state='queued'
		ORDER BY priority DESC, created_at ASC LIMIT 1`))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT r.id, r.kind, r.binding, r.state, r.quarantine_reason
		FROM resources r
		WHERE r.kind=? AND r.state='ready' AND NOT EXISTS (
			SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
			WHERE li.resource_id=r.id AND l.state IN ('active', 'revocation_requested')
		)
		ORDER BY r.id LIMIT ?`, w.ResourceKind, w.ResourceCount)
	if err != nil {
		return nil, err
	}
	var resources []Resource
	for rows.Next() {
		var r Resource
		if err := rows.Scan(&r.ID, &r.Kind, &r.Binding, &r.State, &r.QuarantineReason); err != nil {
			rows.Close()
			return nil, err
		}
		resources = append(resources, r)
	}
	rows.Close()
	if len(resources) != w.ResourceCount {
		return nil, nil
	}
	var ordinal int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(ordinal), 0)+1 FROM attempts WHERE workload_id=?`, w.ID).Scan(&ordinal); err != nil {
		return nil, err
	}
	var continuationRef sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT continuation_ref FROM attempts
		WHERE workload_id=? AND continuation_ref IS NOT NULL
		ORDER BY ordinal DESC LIMIT 1`, w.ID).Scan(&continuationRef)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	attemptID, leaseID, t := id.New("att"), id.New("lea"), now()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO attempts(id, workload_id, ordinal, state, executor_id)
		VALUES(?, ?, ?, 'authorized', ?)`, attemptID, w.ID, ordinal, executorID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO leases(id, attempt_id, state, created_at) VALUES(?, ?, 'active', ?)`,
		leaseID, attemptID, t); err != nil {
		return nil, err
	}
	for _, r := range resources {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO lease_items(lease_id, resource_id) VALUES(?, ?)`, leaseID, r.ID); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workloads SET scheduling_state='running', updated_at=? WHERE id=?`, t, w.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	a := Attempt{ID: attemptID, WorkloadID: w.ID, Ordinal: ordinal, State: "authorized", ExecutorID: executorID}
	launch := &Launch{Workload: w, Attempt: a, LeaseID: leaseID, Resources: resources}
	if continuationRef.Valid {
		launch.ContinuationRef = &continuationRef.String
	}
	return launch, nil
}

func (s *Store) MarkAttemptRunning(ctx context.Context, attemptID string, pid int) error {
	t := now()
	res, err := s.db.ExecContext(ctx, `
		UPDATE attempts
		SET state=CASE WHEN state='authorized' THEN 'running' ELSE state END,
		    pid=?, started_at=?, last_heartbeat_at=?
		WHERE id=? AND state IN ('authorized', 'suspend_requested')`, pid, t, t, attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("attempt %s is not launchable", attemptID)
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, attemptID string, progress json.RawMessage) error {
	if len(progress) == 0 {
		progress = json.RawMessage(`{}`)
	}
	if !json.Valid(progress) {
		return errors.New("progress must be valid JSON")
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE attempts SET last_heartbeat_at=?, progress_json=? WHERE id=?`,
		now(), string(progress), attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ensureSuspendCommandTx(
	ctx context.Context, tx *sql.Tx, workloadID, attemptID, reason string,
) (string, bool, error) {
	var existing string
	err := tx.QueryRowContext(ctx, `
		SELECT id FROM commands
		WHERE attempt_id=? AND kind='suspend'
		  AND state NOT IN ('completed', 'rejected')
		ORDER BY created_at LIMIT 1`, attemptID).Scan(&existing)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	commandID, t := id.New("cmd"), now()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO commands(
			id, workload_id, attempt_id, kind, reason, state,
			delivery_count, created_at, updated_at
		) VALUES(?, ?, ?, 'suspend', ?, 'pending', 0, ?, ?)`,
		commandID, workloadID, attemptID, reason, t, t); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempts SET state='suspend_requested'
		WHERE id=? AND state IN ('authorized', 'running')`, attemptID); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE leases SET state='revocation_requested', revocation_requested_at=?
		WHERE attempt_id=? AND state='active'`, t, attemptID); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE workloads SET scheduling_state='preempting', updated_at=? WHERE id=?`,
		t, workloadID); err != nil {
		return "", false, err
	}
	return commandID, true, nil
}

// EnsurePreemption requests cooperative suspension when a queued workload has
// a strictly higher priority than a running, preemptible workload. It never
// releases a lease; only executor-observed quiescence may do that.
func (s *Store) EnsurePreemption(ctx context.Context) (string, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	var queuedPriority int
	err = tx.QueryRowContext(ctx, `
		SELECT priority FROM workloads
		WHERE desired_state='active' AND scheduling_state='queued'
		ORDER BY priority DESC, created_at LIMIT 1`).Scan(&queuedPriority)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var workloadID, attemptID string
	var runningPriority int
	err = tx.QueryRowContext(ctx, `
		SELECT w.id, a.id, w.priority
		FROM workloads w
		JOIN attempts a ON a.workload_id=w.id
		JOIN leases l ON l.attempt_id=a.id
		WHERE w.scheduling_state='running'
		  AND w.cooperative_suspend=1
		  AND a.state='running'
		  AND l.state='active'
		ORDER BY w.priority ASC, a.ordinal DESC LIMIT 1`).Scan(
		&workloadID, &attemptID, &runningPriority)
	if errors.Is(err, sql.ErrNoRows) || queuedPriority <= runningPriority {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	commandID, created, err := s.ensureSuspendCommandTx(
		ctx, tx, workloadID, attemptID, "higher_priority_workload")
	if err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return commandID, created, nil
}

func (s *Store) PauseWorkload(ctx context.Context, workloadID string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var state string
	if err := tx.QueryRowContext(ctx,
		`SELECT scheduling_state FROM workloads WHERE id=?`, workloadID).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	t := now()
	if _, err := tx.ExecContext(ctx,
		`UPDATE workloads SET desired_state='paused', updated_at=? WHERE id=?`, t, workloadID); err != nil {
		return "", err
	}
	if state == "queued" || state == "held" {
		_, err = tx.ExecContext(ctx,
			`UPDATE workloads SET scheduling_state='paused', updated_at=? WHERE id=?`, t, workloadID)
		if err != nil {
			return "", err
		}
		return "", tx.Commit()
	}
	if state != "running" && state != "preempting" {
		return "", tx.Commit()
	}
	var attemptID string
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM attempts WHERE workload_id=?
		  AND state IN ('authorized', 'running', 'suspend_requested')
		ORDER BY ordinal DESC LIMIT 1`, workloadID).Scan(&attemptID); err != nil {
		return "", err
	}
	commandID, _, err := s.ensureSuspendCommandTx(ctx, tx, workloadID, attemptID, "manual_pause")
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return commandID, nil
}

func (s *Store) ResumeWorkload(ctx context.Context, workloadID string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE workloads
		SET desired_state='active', scheduling_state='queued', updated_at=?
		WHERE id=? AND scheduling_state IN ('paused', 'held')`, now(), workloadID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		if _, err := s.GetWorkload(ctx, workloadID); err != nil {
			return err
		}
		return fmt.Errorf("workload %s is not paused or held", workloadID)
	}
	return nil
}

// PollCommands implements at-least-once delivery. Every poll returns every
// non-terminal command and increments its delivery count. Acknowledgements do
// not suppress replay until the executor has observed quiescence.
func (s *Store) PollCommands(ctx context.Context, attemptID string) ([]Command, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, workload_id, attempt_id, kind, reason, state,
		       delivery_count, created_at
		FROM commands
		WHERE attempt_id=? AND state NOT IN ('completed', 'rejected')
		ORDER BY created_at`, attemptID)
	if err != nil {
		return nil, err
	}
	var commands []Command
	for rows.Next() {
		var c Command
		if err := rows.Scan(
			&c.ID, &c.WorkloadID, &c.AttemptID, &c.Kind, &c.Reason,
			&c.State, &c.DeliveryCount, &c.CreatedAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		commands = append(commands, c)
	}
	rows.Close()
	for i := range commands {
		commands[i].DeliveryCount++
		if _, err := tx.ExecContext(ctx, `
			UPDATE commands SET delivery_count=delivery_count+1, updated_at=? WHERE id=?`,
			now(), commands[i].ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return commands, nil
}

func ackState(phase string) (string, bool) {
	switch phase {
	case "accepted", "checkpointing", "checkpointed", "rejected":
		return phase, true
	default:
		return "", false
	}
}

func commandStateRank(state string) int {
	switch state {
	case "pending":
		return 0
	case "accepted":
		return 1
	case "checkpointing":
		return 2
	case "checkpointed":
		return 3
	case "completed", "rejected":
		return 4
	default:
		return -1
	}
}

func (s *Store) AckCommand(
	ctx context.Context, attemptID, commandID, phase string, payload json.RawMessage,
) error {
	state, ok := ackState(phase)
	if !ok {
		return fmt.Errorf("unsupported acknowledgement phase %q", phase)
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return errors.New("ack payload must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var commandAttempt, currentState string
	if err := tx.QueryRowContext(ctx,
		`SELECT attempt_id, state FROM commands WHERE id=?`, commandID).
		Scan(&commandAttempt, &currentState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if commandAttempt != attemptID {
		return errors.New("command does not belong to attempt")
	}
	t := now()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO command_acks(command_id, attempt_id, phase, payload_json, created_at)
		VALUES(?, ?, ?, ?, ?)`, commandID, attemptID, phase, string(payload), t); err != nil {
		return err
	}
	shouldAdvance := commandStateRank(state) >= commandStateRank(currentState)
	if state == "rejected" && commandStateRank(currentState) > commandStateRank("accepted") {
		shouldAdvance = false
	}
	if currentState == "completed" || currentState == "rejected" {
		shouldAdvance = false
	}
	if shouldAdvance {
		if _, err := tx.ExecContext(ctx,
			`UPDATE commands SET state=?, updated_at=? WHERE id=?`, state, t, commandID); err != nil {
			return err
		}
	}
	if phase == "rejected" && shouldAdvance {
		if _, err := tx.ExecContext(ctx, `
			UPDATE attempts
			SET state=CASE WHEN pid IS NULL THEN 'authorized' ELSE 'running' END
			WHERE id=? AND state='suspend_requested'`, attemptID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE leases
			SET state='active', revocation_requested_at=NULL
			WHERE attempt_id=? AND state='revocation_requested'`, attemptID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE workloads SET scheduling_state='running', updated_at=?
			WHERE id=(SELECT workload_id FROM attempts WHERE id=?)
			  AND scheduling_state='preempting'`, t, attemptID); err != nil {
			return err
		}
	}
	if phase == "checkpointed" && currentState != "rejected" {
		var body struct {
			ContinuationRef string `json:"continuation_ref"`
		}
		_ = json.Unmarshal(payload, &body)
		if _, err := tx.ExecContext(ctx, `
			UPDATE attempts SET checkpointed_at=?, continuation_ref=? WHERE id=?`,
			t, nullable(body.ContinuationRef), attemptID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *Store) ReportDisposition(
	ctx context.Context, attemptID, disposition string, payload json.RawMessage,
) error {
	if disposition != "close" && disposition != "requeue" && disposition != "hold" {
		return errors.New("disposition must be close, requeue, or hold")
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return errors.New("terminal payload must be valid JSON")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE attempts SET terminal_disposition=?, terminal_payload_json=? WHERE id=?`,
		disposition, string(payload), attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrNotFound
	}
	return nil
}

// MarkAttemptExited records process exit and executor-observed quiescence in a
// single transaction. checkpointed is deliberately not required for lease
// release: resource safety and continuation safety are independent facts.
func (s *Store) MarkAttemptExited(ctx context.Context, attemptID string, exitCode int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var workloadID, attemptState string
	var disposition sql.NullString
	var checkpointedAt sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT workload_id, state, terminal_disposition, checkpointed_at
		FROM attempts WHERE id=?`, attemptID).
		Scan(&workloadID, &attemptState, &disposition, &checkpointedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if attemptState == "quiesced" {
		return nil
	}
	if attemptState == "lost" {
		return fmt.Errorf("attempt %s is lost and requires reconciliation", attemptID)
	}
	t := now()
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempts
		SET state='quiesced', exit_code=?, exited_at=?, quiesced_at=?
		WHERE id=?`, exitCode, t, t, attemptID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE leases SET state='released', released_at=?
		WHERE attempt_id=? AND state IN ('active', 'revocation_requested')`,
		t, attemptID); err != nil {
		return err
	}
	var commandID sql.NullString
	_ = tx.QueryRowContext(ctx, `
		SELECT id FROM commands WHERE attempt_id=? AND kind='suspend'
		  AND state!='rejected'
		ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&commandID)
	if commandID.Valid {
		payload := `{"source":"executor","condition":"process_exited"}`
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO command_acks(command_id, attempt_id, phase, payload_json, created_at)
			VALUES(?, ?, 'quiesced', ?, ?)`, commandID.String, attemptID, payload, t); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE commands SET state='completed', updated_at=?, completed_at=? WHERE id=?`,
			t, t, commandID.String); err != nil {
			return err
		}
	}
	var desired string
	if err := tx.QueryRowContext(ctx,
		`SELECT desired_state FROM workloads WHERE id=?`, workloadID).Scan(&desired); err != nil {
		return err
	}
	next := "held"
	if commandID.Valid {
		if desired == "active" && checkpointedAt.Valid {
			next = "queued"
		} else if desired == "paused" {
			next = "paused"
		}
	} else if disposition.Valid {
		switch disposition.String {
		case "close":
			next = "closed"
		case "requeue":
			next = "queued"
		case "hold":
			next = "held"
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workloads SET scheduling_state=?, updated_at=? WHERE id=?`, next, t, workloadID); err != nil {
		return err
	}
	return tx.Commit()
}

// QuarantineUnreconciled is called on daemon startup. Lease expiry or daemon
// restart is not evidence that a physical resource is free, so resources from
// unfinished attempts are quarantined instead of reallocated.
func (s *Store) QuarantineUnreconciled(ctx context.Context, executorID string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	reason := "executor restarted before attempt could be reconciled"
	res, err := tx.ExecContext(ctx, `
		UPDATE resources SET state='quarantined', quarantine_reason=?
		WHERE id IN (
			SELECT li.resource_id FROM lease_items li
			JOIN leases l ON l.id=li.lease_id
			JOIN attempts a ON a.id=l.attempt_id
			WHERE a.executor_id=? AND l.state IN ('active', 'revocation_requested')
		)`, reason, executorID)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE leases SET state='stale'
		WHERE attempt_id IN (
			SELECT id FROM attempts WHERE executor_id=?
			  AND state IN ('authorized', 'running', 'suspend_requested')
		) AND state IN ('active', 'revocation_requested')`, executorID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempts SET state='lost'
		WHERE executor_id=? AND state IN ('authorized', 'running', 'suspend_requested')`,
		executorID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE workloads SET scheduling_state='held', updated_at=?
		WHERE id IN (SELECT workload_id FROM attempts WHERE executor_id=? AND state='lost')`,
		now(), executorID); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) SetResourceReady(ctx context.Context, resourceID string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE resources SET state='ready', quarantine_reason=NULL WHERE id=?`, resourceID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, kind, binding, state, quarantine_reason FROM resources ORDER BY id`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var r Resource
		if err := rows.Scan(&r.ID, &r.Kind, &r.Binding, &r.State, &r.QuarantineReason); err != nil {
			rows.Close()
			return out, err
		}
		out.Resources = append(out.Resources, r)
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT `+workloadColumns+` FROM workloads ORDER BY created_at`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		w, err := scanWorkload(rows)
		if err != nil {
			rows.Close()
			return out, err
		}
		out.Workloads = append(out.Workloads, w)
	}
	rows.Close()
	rows, err = s.db.QueryContext(ctx, `SELECT `+attemptColumns+` FROM attempts ORDER BY rowid`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			rows.Close()
			return out, err
		}
		out.Attempts = append(out.Attempts, a)
	}
	rows.Close()
	return out, rows.Err()
}
