package store

// Gang executions: one logical execution run as Size processes on distinct
// nodes (multi-node training, a pipeline-parallel inference server).
//
// A gang is Size ordinary execution requests, one per rank, created together
// by SubmitExecution; rank 0 (the leader) is the one orchestration tracks. The
// coordination added on top of the one-execution machinery is:
//
//   - placement: when an executor considers the leader, every rank is placed on
//     a distinct node in one transaction (all leases or none). A rank's lease
//     for another executor waits in gang_placement_leases until that executor
//     asks ReserveNext, which hands it over before reserving anything else.
//   - launch barrier: AuthorizeLaunch issues no rank's launch until every
//     rank's lease is prepared (ErrGangNotReady); the executor keeps its
//     prepared reservation meanwhile. A placement that is not prepared in time,
//     or loses a lease before launch, is aborted (ErrGangAborted) and every rank
//     goes back to waiting.
//   - rendezvous: each rank is launched with its rank, the gang size and rank
//     0's interconnect address and port (GangLaunch).
//   - one fate: when a launched rank stops other than by a clean exit or a
//     checkpoint, the ranks still running are terminated; a suspend command to
//     one rank is sent to every rank (ReconcileGangs).
//   - outcome: orchestration reads the gang as a whole (gangAggregateTx).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"kairo/internal/id"
)

// GangPrepareTimeout is how long a placement may wait for every rank to be
// prepared before it is aborted.
const GangPrepareTimeout = 2 * time.Minute

const (
	gangPortFirst = 29500
	gangPortLast  = 29999
)

type gangExecutor struct {
	id, nodeID, attributes string
	nodeAttributes         string
}

func (g gangExecutor) attribute(key string) string {
	for _, raw := range []string{g.attributes, g.nodeAttributes} {
		var values map[string]any
		if json.Unmarshal([]byte(raw), &values) == nil {
			if v, ok := values[key]; ok && fmt.Sprint(v) != "" {
				return fmt.Sprint(v)
			}
		}
	}
	return ""
}

type gangMember struct {
	rank      int
	candidate executionCandidate
}

// gangOfExecutionTx returns the gang an execution belongs to ("" if none), its
// rank and the gang's leader.
func gangOfExecutionTx(ctx context.Context, q queryer, executionID string) (gangID string, rank int, leaderID string, err error) {
	err = q.QueryRowContext(ctx, `SELECT m.gang_id,m.rank,g.leader_execution_id FROM gang_members m JOIN execution_gangs g ON g.id=m.gang_id WHERE m.execution_id=?`, executionID).Scan(&gangID, &rank, &leaderID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, "", nil
	}
	return gangID, rank, leaderID, err
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func loadCandidateTx(ctx context.Context, tx *sql.Tx, executionID string) (executionCandidate, error) {
	var c executionCandidate
	err := tx.QueryRowContext(ctx, `SELECT e.id,e.project_scope_id,e.queue_scope_id,e.task_scope_id,e.client_request_id,e.spec_digest,e.state,e.argv_json,e.cwd,e.executor_selector_json,e.priority,e.checkpointable,e.preemptible,e.input_continuation_ref,e.terminal_cause,e.submitted_at,e.authorized_at,e.started_at,e.terminal_at
		FROM execution_requests e WHERE e.id=?`, executionID).Scan(&c.execution.ID, &c.execution.ProjectScopeID, &c.execution.QueueScopeID, &c.execution.TaskScopeID, &c.execution.ClientRequestID, &c.execution.SpecDigest, &c.execution.State, &c.argvJSON, &c.execution.CWD, &c.selectorJSON, &c.execution.Priority, &c.execution.Checkpointable, &c.execution.Preemptible, &c.execution.InputContinuationRef, &c.execution.TerminalCause, &c.execution.SubmittedAt, &c.execution.AuthorizedAt, &c.execution.StartedAt, &c.execution.TerminalAt)
	return c, err
}

func gangMembersTx(ctx context.Context, tx *sql.Tx, gangID string) ([]gangMember, error) {
	rows, err := tx.QueryContext(ctx, `SELECT rank,execution_id FROM gang_members WHERE gang_id=? ORDER BY rank`, gangID)
	if err != nil {
		return nil, err
	}
	type ref struct {
		rank int
		id   string
	}
	var refs []ref
	for rows.Next() {
		var r ref
		if err = rows.Scan(&r.rank, &r.id); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, r)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	members := make([]gangMember, 0, len(refs))
	for _, r := range refs {
		c, err := loadCandidateTx(ctx, tx, r.id)
		if err != nil {
			return nil, err
		}
		members = append(members, gangMember{rank: r.rank, candidate: c})
	}
	return members, nil
}

// gangMatchesExecutor reports whether any rank of the gang led by leaderID may
// run on an executor with these attributes.
func gangMatchesExecutor(ctx context.Context, tx *sql.Tx, gangID, executorAttributes string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.executor_selector_json FROM gang_members m JOIN execution_requests e ON e.id=m.execution_id WHERE m.gang_id=?`, gangID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var selector string
		if err = rows.Scan(&selector); err != nil {
			return false, err
		}
		if selectorMatches(selector, executorAttributes) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// placeGangTx places every rank of the gang on a distinct node, preferring the
// calling executor for the first rank it can take. All leases are created or
// none (a savepoint is rolled back when a rank finds no node). It reports
// whether the gang was placed.
func placeGangTx(ctx context.Context, tx *sql.Tx, gangID, callerExecutorID string) (bool, error) {
	var size, port int
	if err := tx.QueryRowContext(ctx, `SELECT size,port FROM execution_gangs WHERE id=?`, gangID).Scan(&size, &port); err != nil {
		return false, err
	}
	members, err := gangMembersTx(ctx, tx, gangID)
	if err != nil {
		return false, err
	}
	if len(members) != size {
		return false, fmt.Errorf("gang %s has %d of %d ranks", gangID, len(members), size)
	}
	for _, m := range members {
		var live int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE execution_id=? AND state!='released'`, m.candidate.execution.ID).Scan(&live); err != nil {
			return false, err
		}
		if m.candidate.execution.State != "waiting" || live != 0 {
			return false, nil
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT x.id,x.node_id,x.attributes_json,n.attributes_json FROM executors x JOIN nodes n ON n.id=x.node_id
		WHERE x.enabled=1 AND n.enabled=1 AND NOT EXISTS(SELECT 1 FROM node_quarantines q WHERE q.node_id IN(x.node_id,'*'))
		ORDER BY x.id`)
	if err != nil {
		return false, err
	}
	var executors []gangExecutor
	for rows.Next() {
		var e gangExecutor
		if err = rows.Scan(&e.id, &e.nodeID, &e.attributes, &e.nodeAttributes); err != nil {
			rows.Close()
			return false, err
		}
		executors = append(executors, e)
	}
	if err = rows.Close(); err != nil {
		return false, err
	}
	sort.SliceStable(executors, func(i, j int) bool { return executors[i].id == callerExecutorID && executors[j].id != callerExecutorID })

	if _, err = tx.ExecContext(ctx, `SAVEPOINT gang_place`); err != nil {
		return false, err
	}
	abandon := func() (bool, error) {
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO gang_place`); err != nil {
			return false, err
		}
		_, err := tx.ExecContext(ctx, `RELEASE gang_place`)
		return false, err
	}
	type assignment struct {
		rank                 int
		leaseID, executionID string
		executor             gangExecutor
	}
	var placed []assignment
	usedNodes := map[string]bool{}
	for _, m := range members {
		found := false
		for _, e := range executors {
			if usedNodes[e.nodeID] || !selectorMatches(m.candidate.selectorJSON, e.attributes) {
				continue
			}
			if m.rank == 0 && e.attribute("interconnect_addr") == "" {
				continue // rank 0 must be reachable by the others
			}
			reservation, terminalized, err := reserveExecutionTx(ctx, tx, e.id, e.nodeID, m.candidate)
			if err != nil {
				_, _ = abandon()
				return false, err
			}
			if terminalized {
				// A rank's conflict policy asked to fail; the gang waits instead
				// of failing one rank (the terminalization is rolled back).
				return abandon()
			}
			if reservation == nil {
				continue
			}
			placed = append(placed, assignment{rank: m.rank, leaseID: reservation.Lease.ID, executionID: m.candidate.execution.ID, executor: e})
			usedNodes[e.nodeID] = true
			found = true
			break
		}
		if !found {
			return abandon()
		}
	}
	master := placed[0].executor
	if port == 0 {
		port, err = freeGangPortTx(ctx, tx, master.nodeID)
		if err != nil {
			_, _ = abandon()
			return false, err
		}
		if port == 0 {
			return abandon()
		}
	}
	placementID, stamp := id.New("gpl"), now()
	if _, err = tx.ExecContext(ctx, `INSERT INTO gang_placements(id,gang_id,state,master_addr,master_port,master_node_id,created_at,updated_at) VALUES(?,?,'placed',?,?,?,?,?)`, placementID, gangID, master.attribute("interconnect_addr"), port, master.nodeID, stamp, stamp); err != nil {
		_, _ = abandon()
		return false, err
	}
	for _, a := range placed {
		if _, err = tx.ExecContext(ctx, `INSERT INTO gang_placement_leases(placement_id,rank,lease_id,execution_id,executor_id,node_id) VALUES(?,?,?,?,?,?)`, placementID, a.rank, a.leaseID, a.executionID, a.executor.id, a.executor.nodeID); err != nil {
			_, _ = abandon()
			return false, err
		}
	}
	leader := members[0].candidate.execution
	if err = appendCoordinationEventTx(ctx, tx, "gang_placed", &leader.ProjectScopeID, "gang", gangID, nil, map[string]any{"placement_id": placementID, "master_addr": master.attribute("interconnect_addr"), "master_port": port}); err != nil {
		_, _ = abandon()
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `RELEASE gang_place`); err != nil {
		return false, err
	}
	return true, nil
}

// freeGangPortTx picks the lowest rendezvous port no live placement on the
// node uses (0 if all are taken).
func freeGangPortTx(ctx context.Context, tx *sql.Tx, nodeID string) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT gp.master_port FROM gang_placements gp WHERE gp.master_node_id=? AND gp.state IN('placed','launched')
		AND EXISTS(SELECT 1 FROM gang_placement_leases gpl JOIN leases l ON l.id=gpl.lease_id WHERE gpl.placement_id=gp.id AND l.state!='released')`, nodeID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	used := map[int]bool{}
	for rows.Next() {
		var p int
		if err = rows.Scan(&p); err != nil {
			return 0, err
		}
		used[p] = true
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for p := gangPortFirst; p <= gangPortLast; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, nil
}

// gangLaunchTx is the gang information of a placed lease (nil for a lease that
// is not a gang rank's).
func gangLaunchTx(ctx context.Context, q queryer, leaseID string) (*GangLaunch, string, error) {
	var g GangLaunch
	var state string
	err := q.QueryRowContext(ctx, `SELECT gp.gang_id,gp.id,gpl.rank,eg.size,gp.master_addr,gp.master_port,gp.state
		FROM gang_placement_leases gpl JOIN gang_placements gp ON gp.id=gpl.placement_id JOIN execution_gangs eg ON eg.id=gp.gang_id
		WHERE gpl.lease_id=?`, leaseID).Scan(&g.GangID, &g.PlacementID, &g.Rank, &g.Size, &g.MasterAddr, &g.MasterPort, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return &g, state, nil
}

// deliverGangLease hands executorID the oldest gang rank lease placed for it
// that it has not received yet, as an ordinary reservation.
func (s *Store) deliverGangLease(ctx context.Context, executorID string) (*Reservation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var leaseID, executionID string
	err = tx.QueryRowContext(ctx, `SELECT gpl.lease_id,gpl.execution_id FROM gang_placement_leases gpl
		JOIN gang_placements gp ON gp.id=gpl.placement_id JOIN leases l ON l.id=gpl.lease_id
		WHERE gpl.executor_id=? AND gpl.delivered_at IS NULL AND gp.state='placed' AND l.state='reserved'
		ORDER BY gp.created_at,gpl.rank LIMIT 1`, executorID).Scan(&leaseID, &executionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	candidate, err := loadCandidateTx(ctx, tx, executionID)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal([]byte(candidate.argvJSON), &candidate.execution.Argv); err != nil {
		return nil, err
	}
	candidate.execution.ExecutorSelector = json.RawMessage(candidate.selectorJSON)
	var lease Lease
	var expires sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT id,execution_id,executor_id,coordination_epoch,state,created_at,expires_at FROM leases WHERE id=?`, leaseID).Scan(&lease.ID, &lease.ExecutionID, &lease.ExecutorID, &lease.CoordinationEpoch, &lease.State, &lease.CreatedAt, &expires); err != nil {
		return nil, err
	}
	if expires.Valid {
		lease.ExpiresAt = &expires.String
	}
	resources, err := leaseResourcesTx(ctx, tx, leaseID)
	if err != nil {
		return nil, err
	}
	gates := map[string]int64{}
	rows, err := tx.QueryContext(ctx, `SELECT scope_id,generation FROM lease_gate_snapshots WHERE lease_id=?`, leaseID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var scope string
		var generation int64
		if err = rows.Scan(&scope, &generation); err != nil {
			rows.Close()
			return nil, err
		}
		gates[scope] = generation
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	gang, _, err := gangLaunchTx(ctx, tx, leaseID)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE gang_placement_leases SET delivered_at=? WHERE lease_id=?`, now(), leaseID); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Reservation{Lease: lease, Execution: candidate.execution, Argv: candidate.execution.Argv, CWD: candidate.execution.CWD, Resources: resources, GateGenerations: gates, Gang: gang}, nil
}

func leaseResourcesTx(ctx context.Context, q queryer, leaseID string) ([]ResourceInstance, error) {
	rows, err := q.QueryContext(ctx, `SELECT r.id,r.node_id,r.provider_id,r.kind,r.stable_identity,r.binding_json,r.attributes_json,r.admin_state,r.quarantine_reason
		FROM lease_items li JOIN resource_instances r ON r.id=li.resource_id WHERE li.lease_id=? AND li.kind='gpu' ORDER BY r.id`, leaseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResourceInstance
	for rows.Next() {
		var resource ResourceInstance
		var binding, attributes string
		if err = rows.Scan(&resource.ID, &resource.NodeID, &resource.ProviderID, &resource.Kind, &resource.StableIdentity, &binding, &attributes, &resource.AdminState, &resource.QuarantineReason); err != nil {
			return nil, err
		}
		resource.Binding = json.RawMessage(binding)
		resource.Attributes = json.RawMessage(attributes)
		out = append(out, resource)
	}
	return out, rows.Err()
}

// gangLaunchGateTx is the launch barrier: a gang rank is authorized only once
// every rank of its placement is prepared. It returns the rank's GangLaunch
// (nil for a non-gang lease) and marks the placement launched.
func gangLaunchGateTx(ctx context.Context, tx *sql.Tx, leaseID string) (*GangLaunch, error) {
	gang, state, err := gangLaunchTx(ctx, tx, leaseID)
	if err != nil || gang == nil {
		return nil, err
	}
	if state == "aborted" {
		return nil, ErrGangAborted
	}
	var total, prepared, lost int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(l.state IN('prepared','active','releasing')),0),
		COALESCE(SUM(l.state IN('released','stale','revocation_requested')),0)
		FROM gang_placement_leases gpl JOIN leases l ON l.id=gpl.lease_id WHERE gpl.placement_id=?`, gang.PlacementID).Scan(&total, &prepared, &lost); err != nil {
		return nil, err
	}
	if lost != 0 && state == "placed" {
		return nil, ErrGangAborted
	}
	if state == "placed" && prepared != total {
		return nil, ErrGangNotReady
	}
	if state == "placed" {
		if _, err = tx.ExecContext(ctx, `UPDATE gang_placements SET state='launched',updated_at=? WHERE id=? AND state='placed'`, now(), gang.PlacementID); err != nil {
			return nil, err
		}
	}
	return gang, nil
}

// ReconcileGangs keeps each gang to one fate. Before launch it aborts a
// placement that lost a lease or was not prepared within GangPrepareTimeout;
// after launch it terminates the running ranks of a gang one of whose ranks
// stopped abnormally, and sends a suspend command given to one rank to all.
func (s *Store) ReconcileGangs(ctx context.Context) error {
	if err := s.abortStalePlacements(ctx); err != nil {
		return err
	}
	return s.reconcileLaunchedGangs(ctx)
}

func (s *Store) abortStalePlacements(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT gp.id,gp.created_at,
		(SELECT COUNT(*) FROM gang_placement_leases gpl JOIN leases l ON l.id=gpl.lease_id WHERE gpl.placement_id=gp.id AND l.state IN('released','stale','revocation_requested'))
		FROM gang_placements gp WHERE gp.state='placed'`)
	if err != nil {
		return err
	}
	type pending struct {
		id, reason string
	}
	var aborts []pending
	for rows.Next() {
		var placementID, created string
		var lost int
		if err = rows.Scan(&placementID, &created, &lost); err != nil {
			rows.Close()
			return err
		}
		createdAt, _ := time.Parse(time.RFC3339Nano, created)
		switch {
		case lost != 0:
			aborts = append(aborts, pending{placementID, "a rank lost its lease before launch"})
		case time.Since(createdAt) > GangPrepareTimeout:
			aborts = append(aborts, pending{placementID, fmt.Sprintf("not every rank was prepared within %s", GangPrepareTimeout)})
		}
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, a := range aborts {
		if err := s.abortPlacement(ctx, a.id, a.reason); err != nil {
			return err
		}
	}
	return nil
}

// abortPlacement gives a placement up. Leases no executor has received yet are
// released here (nothing was prepared for them); a received lease is released
// by its executor, which learns of the abort from AuthorizeLaunch.
func (s *Store) abortPlacement(ctx context.Context, placementID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := now()
	res, err := tx.ExecContext(ctx, `UPDATE gang_placements SET state='aborted',abort_reason=?,updated_at=? WHERE id=? AND state='placed'`, reason, stamp, placementID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE leases SET state='released',released_at=? WHERE state='reserved' AND id IN(
		SELECT lease_id FROM gang_placement_leases WHERE placement_id=? AND delivered_at IS NULL)`, stamp, placementID); err != nil {
		return err
	}
	var gangID, projectID string
	if err = tx.QueryRowContext(ctx, `SELECT gp.gang_id,e.project_scope_id FROM gang_placements gp JOIN execution_gangs g ON g.id=gp.gang_id JOIN execution_requests e ON e.id=g.leader_execution_id WHERE gp.id=?`, placementID).Scan(&gangID, &projectID); err != nil {
		return err
	}
	if err = appendCoordinationEventTx(ctx, tx, "gang_placement_aborted", &projectID, "gang", gangID, nil, map[string]any{"placement_id": placementID, "reason": reason}); err != nil {
		return err
	}
	return tx.Commit()
}

type gangRankState struct {
	rank                    int
	executionID, executorID string
	leaseState              string
	attemptID               sql.NullString
	attemptState            sql.NullString
	exitCode                sql.NullInt64
	checkpointable          bool
	command                 sql.NullString // origin of the rank's live or checkpointed suspend
	commandReason           sql.NullString
	terminated              bool
}

func (s *Store) reconcileLaunchedGangs(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT gp.id,gp.gang_id FROM gang_placements gp WHERE gp.state='launched'
		AND EXISTS(SELECT 1 FROM gang_placement_leases gpl JOIN leases l ON l.id=gpl.lease_id WHERE gpl.placement_id=gp.id AND l.state!='released')`)
	if err != nil {
		return err
	}
	type placement struct{ id, gangID string }
	var live []placement
	for rows.Next() {
		var p placement
		if err = rows.Scan(&p.id, &p.gangID); err != nil {
			rows.Close()
			return err
		}
		live = append(live, p)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, p := range live {
		ranks, err := s.gangRankStates(ctx, p.id)
		if err != nil {
			return err
		}
		stopped := ""
		var origin, reason string
		for _, r := range ranks {
			if r.command.Valid && origin == "" {
				origin, reason = r.command.String, fmt.Sprintf("gang rank %d: %s", r.rank, r.commandReason.String)
			}
			if stopped != "" {
				continue
			}
			switch {
			case !r.attemptID.Valid:
				// between the barrier and its own launch, unless its lease is gone
				if r.leaseState == "released" || r.leaseState == "stale" || r.leaseState == "revocation_requested" {
					stopped = fmt.Sprintf("rank %d lost its lease before launch", r.rank)
				}
			case r.attemptState.String == "lost":
				stopped = fmt.Sprintf("rank %d lost its executor", r.rank)
			case (r.attemptState.String == "exited" || r.attemptState.String == "quiesced") && !r.command.Valid && (!r.exitCode.Valid || r.exitCode.Int64 != 0):
				stopped = fmt.Sprintf("rank %d stopped (exit %v)", r.rank, nullableInt(r.exitCode))
			}
		}
		for _, r := range ranks {
			running := r.attemptID.Valid && (r.attemptState.String == "running" || r.attemptState.String == "quiescing")
			if !running {
				continue
			}
			switch {
			case stopped != "" && !r.terminated && !(r.command.Valid && r.attemptState.String == "quiescing"):
				if err := s.requestGangTermination(ctx, r, p.gangID, stopped); err != nil {
					return err
				}
			case stopped == "" && origin != "" && !r.command.Valid && r.checkpointable && r.attemptState.String == "running":
				if _, err := s.EnqueueSuspend(ctx, r.attemptID.String, origin, reason); err != nil && !errors.Is(err, ErrNotFound) {
					return fmt.Errorf("suspend gang rank %d: %w", r.rank, err)
				}
			}
		}
	}
	return nil
}

func nullableInt(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}

func (s *Store) gangRankStates(ctx context.Context, placementID string) ([]gangRankState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT gpl.rank,gpl.execution_id,gpl.executor_id,l.state,a.id,a.state,a.exit_code,e.checkpointable,
		(SELECT c.origin FROM commands c WHERE c.attempt_id=a.id AND c.state!='rejected' ORDER BY c.created_at DESC LIMIT 1),
		(SELECT c.reason FROM commands c WHERE c.attempt_id=a.id AND c.state!='rejected' ORDER BY c.created_at DESC LIMIT 1),
		EXISTS(SELECT 1 FROM gang_terminations t WHERE t.attempt_id=a.id) OR EXISTS(SELECT 1 FROM quarantine_terminations t WHERE t.attempt_id=a.id)
		FROM gang_placement_leases gpl JOIN leases l ON l.id=gpl.lease_id JOIN execution_requests e ON e.id=gpl.execution_id
		LEFT JOIN attempts a ON a.id=l.attempt_id
		WHERE gpl.placement_id=? ORDER BY gpl.rank`, placementID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []gangRankState
	for rows.Next() {
		var r gangRankState
		if err = rows.Scan(&r.rank, &r.executionID, &r.executorID, &r.leaseState, &r.attemptID, &r.attemptState, &r.exitCode, &r.checkpointable, &r.command, &r.commandReason, &r.terminated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) requestGangTermination(ctx context.Context, r gangRankState, gangID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO gang_terminations(attempt_id,execution_id,executor_id,gang_id,reason,state,requested_at) VALUES(?,?,?,?,?,'pending',?) ON CONFLICT(attempt_id) DO NOTHING`, r.attemptID.String, r.executionID, r.executorID, gangID, reason, now())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		projectID, err := executionProjectIDTx(ctx, tx, r.executionID)
		if err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "gang_termination_requested", &projectID, "attempt", r.attemptID.String, nil, map[string]any{"execution_id": r.executionID, "gang_id": gangID, "rank": r.rank, "reason": reason}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// gangOutcome is what orchestration needs to know about a gang beyond its
// aggregated execution state.
type gangOutcome struct {
	gang bool
	// a rank was terminated by a node quarantine: run the task again
	quarantined bool
	// a rank failed to launch after others had started: run the gang again
	launchAborted bool
}

// gangAggregateTx folds the ranks of the gang led by current's execution into
// current, so the orchestration policy reads the gang like one execution:
// terminal when every rank is terminal, quiesced and released when every rank
// is, exit 0 only when every rank exited 0, lost when any rank was lost. The
// leader's attempt keeps deciding checkpoint continuation.
func gangAggregateTx(ctx context.Context, tx *sql.Tx, current *currentTaskExecution) (gangOutcome, error) {
	gangID, _, leaderID, err := gangOfExecutionTx(ctx, tx, current.ExecutionID)
	if err != nil || gangID == "" || leaderID != current.ExecutionID {
		return gangOutcome{}, err
	}
	out := gangOutcome{gang: true}
	rows, err := tx.QueryContext(ctx, `SELECT e.state,e.terminal_cause,a.id,a.state,a.exit_code,a.exit_signal,
		(SELECT l.state FROM leases l WHERE l.execution_id=e.id ORDER BY l.created_at DESC LIMIT 1),
		EXISTS(SELECT 1 FROM quarantine_terminations t WHERE t.attempt_id=a.id AND t.state='terminated')
		FROM gang_members m JOIN execution_requests e ON e.id=m.execution_id LEFT JOIN attempts a ON a.execution_id=e.id
		WHERE m.gang_id=? ORDER BY m.rank`, gangID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	allTerminal, allQuiesced, allReleased, allZero, anyLost, anyNoAttempt := true, true, true, true, false, false
	var firstNonZero *int
	for rows.Next() {
		var state string
		var cause, attemptID, attemptState, signal, leaseState sql.NullString
		var exit sql.NullInt64
		var quarantined bool
		if err = rows.Scan(&state, &cause, &attemptID, &attemptState, &exit, &signal, &leaseState, &quarantined); err != nil {
			return out, err
		}
		allTerminal = allTerminal && state == "terminal"
		if !attemptID.Valid {
			anyNoAttempt = true
			allQuiesced = false
		} else {
			allQuiesced = allQuiesced && attemptState.String == "quiesced"
			anyLost = anyLost || attemptState.String == "lost"
		}
		allReleased = allReleased && leaseState.Valid && leaseState.String == "released"
		if !exit.Valid || exit.Int64 != 0 || (signal.Valid && signal.String != "") {
			allZero = false
			if exit.Valid && exit.Int64 != 0 && firstNonZero == nil {
				v := int(exit.Int64)
				firstNonZero = &v
			}
		}
		out.quarantined = out.quarantined || quarantined
		out.launchAborted = out.launchAborted || (cause.Valid && len(cause.String) >= len("launch_aborted") && cause.String[:len("launch_aborted")] == "launch_aborted")
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if !allTerminal {
		current.ExecutionState = "started"
		return out, nil
	}
	if anyNoAttempt && current.AttemptID != nil && !out.launchAborted {
		current.AttemptID = nil
	}
	lost := "lost"
	quiesced := "quiesced"
	released := "released"
	switch {
	case anyLost:
		current.AttemptState = &lost
	case allQuiesced:
		current.AttemptState = &quiesced
	default:
		other := "exited"
		current.AttemptState = &other
	}
	if allReleased {
		current.LeaseState = &released
	} else {
		other := "releasing"
		current.LeaseState = &other
	}
	if allZero {
		zero := 0
		current.ExitCode, current.ExitSignal = &zero, nil
	} else if firstNonZero != nil {
		current.ExitCode = firstNonZero
	}
	return out, nil
}
