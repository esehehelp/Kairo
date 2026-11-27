package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"kairo/internal/id"
	"kairo/internal/orchestration"
)

// Gang-wide checkpoints (issue #47, phase 4). A gang's members are separate
// attempts, but one model: a checkpoint is the gang's only when every member
// made its part at the same step. The SDK (DistributedAdapter) makes that
// so: each member's first process polls its own attempt, and the gang acts
// on a suspend, or a checkpoint, only once every member holds one; each
// member then publishes its own part, saying in the payload which round it
// belongs to ({"gang": {"round": R, "rank": r, "size": n}}).
//
// The daemon gives every member the command one of them got (a suspend:
// ReconcileGangs; a checkpoint: propagateGangCheckpoints), and goes on from
// a round only when every member of the generation published its part of
// it: after a suspend, from the suspend's round; after a loss (with
// continue_after_loss), from the latest complete round, which the
// generation may have started from. Each member of the continuation goes
// on from its own part, on the node holding its run's output directory.

// gangPart is a member's published part of a round.
type gangPart struct {
	rank      int
	ref       string
	commandID string
	kind      string
	at        time.Time
}

// gangRound is a round of a generation: its members' parts.
type gangRound struct {
	id      string
	parts   map[int]gangPart
	at      time.Time // the latest part's
	suspend bool      // every part is a suspend's
}

// gangGeneration is the generation led by an execution: its members (by
// rank) and the rounds they published.
type gangGeneration struct {
	size    int
	members []gangGenerationMember
	rounds  map[string]*gangRound
}

type gangGenerationMember struct {
	rank                        int
	executionID, attemptID      string
	attemptState, node, runID   string
	terminalCause               string
	inputRef, inputCommand      string
	suspendState, suspendOrigin string // its latest suspend command's, if any
}

// gangGenerationTx reads the generation led by leaderID.
func gangGenerationTx(ctx context.Context, tx *sql.Tx, leaderID string) (*gangGeneration, error) {
	g := &gangGeneration{rounds: map[string]*gangRound{}}
	err := eachRow(ctx, tx, `SELECT m.rank,m.execution_id,COALESCE(a.id,''),COALESCE(a.state,''),COALESCE(x.node_id,''),COALESCE(e.terminal_cause,''),
		COALESCE(e.input_continuation_ref,''),COALESCE(e.input_checkpoint_command_id,''),
		COALESCE((SELECT c.state FROM commands c WHERE c.attempt_id=a.id AND c.kind='suspend' ORDER BY c.created_at DESC LIMIT 1),''),
		COALESCE((SELECT c.origin FROM commands c WHERE c.attempt_id=a.id AND c.kind='suspend' ORDER BY c.created_at DESC LIMIT 1),'')
		FROM execution_gangs eg JOIN gang_members m ON m.gang_id=eg.id JOIN execution_requests e ON e.id=m.execution_id
		LEFT JOIN LATERAL (SELECT * FROM attempts a WHERE a.execution_id=e.id ORDER BY a.authorized_at DESC NULLS LAST LIMIT 1) a ON true
		LEFT JOIN executors x ON x.id=a.executor_id
		WHERE eg.leader_execution_id=$1 ORDER BY m.rank`, []any{leaderID}, func(rows *sql.Rows) error {
		var m gangGenerationMember
		if err := rows.Scan(&m.rank, &m.executionID, &m.attemptID, &m.attemptState, &m.node, &m.terminalCause, &m.inputRef, &m.inputCommand, &m.suspendState, &m.suspendOrigin); err != nil {
			return err
		}
		g.members = append(g.members, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	g.size = len(g.members)
	for i := range g.members {
		m := &g.members[i]
		if m.runID, err = runIDTx(ctx, tx, m.executionID); err != nil {
			return nil, err
		}
		if m.attemptID == "" {
			continue
		}
		err = eachRow(ctx, tx, `SELECT id,kind,payload_json,updated_at FROM commands WHERE attempt_id=$1 AND state='checkpointed'`, []any{m.attemptID}, func(rows *sql.Rows) error {
			var p gangPart
			var payload, at string
			if err := rows.Scan(&p.commandID, &p.kind, &payload, &at); err != nil {
				return err
			}
			p.at, _ = time.Parse(time.RFC3339Nano, at)
			var body struct {
				ContinuationRef string `json:"continuation_ref"`
				Gang            struct {
					Round string `json:"round"`
					Rank  *int   `json:"rank"`
				} `json:"gang"`
			}
			if json.Unmarshal([]byte(payload), &body) != nil || body.ContinuationRef == "" || body.Gang.Round == "" || (body.Gang.Rank != nil && *body.Gang.Rank != m.rank) {
				return nil // not a part of a gang's round
			}
			p.rank, p.ref = m.rank, body.ContinuationRef
			r := g.rounds[body.Gang.Round]
			if r == nil {
				r = &gangRound{id: body.Gang.Round, parts: map[int]gangPart{}, suspend: true}
				g.rounds[body.Gang.Round] = r
			}
			if old, ok := r.parts[m.rank]; !ok || old.at.Before(p.at) {
				r.parts[m.rank] = p
			}
			if p.at.After(r.at) {
				r.at = p.at
			}
			r.suspend = r.suspend && p.kind == "suspend"
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

// latestComplete is the latest round every member published its part of
// (nil: none).
func (g *gangGeneration) latestComplete() *gangRound {
	var rounds []*gangRound
	for _, r := range g.rounds {
		if len(r.parts) == g.size {
			rounds = append(rounds, r)
		}
	}
	sort.Slice(rounds, func(i, j int) bool { return rounds[i].at.After(rounds[j].at) })
	if len(rounds) == 0 {
		return nil
	}
	return rounds[0]
}

// inherited is the round the generation went on from, as each member's
// execution has its part (nil: it went on from none).
func (g *gangGeneration) inherited() *gangRound {
	r := &gangRound{id: "inherited", parts: map[int]gangPart{}}
	for _, m := range g.members {
		if m.inputRef == "" {
			return nil
		}
		r.parts[m.rank] = gangPart{rank: m.rank, ref: m.inputRef, commandID: m.inputCommand}
	}
	return r
}

// gangContinuationTx decides a terminal checkpointable gang whose members
// published (or should have published) checkpoints: it plans the
// continuation from a complete round, or blocks or fails the task, and
// reports whether it did; false leaves the task to the ordinary rules (no
// member suspended, nor was lost).
func gangContinuationTx(ctx context.Context, tx *sql.Tx, task orchestrationTaskRow, current currentTaskExecution, template orchestration.Execution) (bool, error) {
	g, err := gangGenerationTx(ctx, tx, current.ExecutionID)
	if err != nil || g.size == 0 {
		return false, err
	}
	lost, suspended := false, false
	for _, m := range g.members {
		// lost: its attempt was (and stays so in its execution once its
		// lease is reconciled)
		lost = lost || m.attemptState == "lost" || m.terminalCause == "executor_identity_lost"
		suspended = suspended || m.suspendState == "checkpointed"
	}
	if !lost && !suspended {
		return false, nil
	}
	released := current.LeaseState != nil && *current.LeaseState == "released"
	if lost {
		if !template.ContinueAfterLoss {
			return false, nil // blocked as a lost attempt
		}
		if !released {
			return true, updateTaskState(ctx, tx, task.taskScopeID, "running", map[string]any{"reason": "a gang member lost: the gang goes on from its latest gang-wide checkpoint once every lease is released", "execution_id": current.ExecutionID})
		}
		round := g.latestComplete()
		if round == nil {
			round = g.inherited()
		}
		if round == nil {
			return true, updateTaskState(ctx, tx, task.taskScopeID, "blocked", map[string]any{"reason": "a gang member lost; no checkpoint every member published", "execution_id": current.ExecutionID})
		}
		return true, planGangContinuation(ctx, tx, task, current, g, round, "loss_continuation", "attempt_lost")
	}
	// a suspend: once every member is quiesced and released
	if current.AttemptState == nil || *current.AttemptState != "quiesced" || !released {
		return true, nil
	}
	origin := ""
	for _, m := range g.members {
		if m.suspendState == "checkpointed" {
			origin = m.suspendOrigin
			break
		}
	}
	if round := g.latestComplete(); round != nil && round.suspend {
		return true, planGangContinuation(ctx, tx, task, current, g, round, "continuation", origin)
	}
	// not every member published its part of the suspend: from the latest
	// round they all did, if any (what came after is lost)
	if round := g.latestComplete(); round != nil {
		return true, planGangContinuation(ctx, tx, task, current, g, round, "continuation", origin)
	}
	if round := g.inherited(); round != nil {
		return true, planGangContinuation(ctx, tx, task, current, g, round, "continuation", origin)
	}
	return true, updateTaskState(ctx, tx, task.taskScopeID, "failed", map[string]any{"reason": "the gang's suspend did not publish a checkpoint of every member", "execution_id": current.ExecutionID})
}

// planGangContinuation plans the gang's next generation from round: each
// member from its own part, on the node of its run's output directory.
func planGangContinuation(ctx context.Context, tx *sql.Tx, task orchestrationTaskRow, current currentTaskExecution, g *gangGeneration, round *gangRound, reason, origin string) error {
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	var members []map[string]any
	for _, m := range g.members {
		p := round.parts[m.rank]
		node := m.node
		if home, ok, err := runOutputHomeTx(ctx, tx, declared, m.runID); err != nil {
			return err
		} else if ok {
			node = home.nodeID
		}
		members = append(members, map[string]any{"rank": m.rank, "continuation_ref": p.ref, "command_id": p.commandID, "node": node})
	}
	leader := round.parts[0]
	trigger := map[string]any{"predecessor_execution_id": current.ExecutionID, "origin": origin, "command_id": leader.commandID,
		"continuation_ref": leader.ref, "gang_round": round.id, "gang_members": members}
	if current.AttemptID != nil {
		trigger["attempt_id"] = *current.AttemptID
	}
	ref := leader.ref
	return planTaskExecution(ctx, tx, task, current.Ordinal+1, reason, trigger, &ref)
}

// applyGangContinuation puts on spec's members what a gang continuation's
// trigger says of each: its part of the round, its node.
func applyGangContinuation(spec *ExecutionSpec, trigger map[string]any) error {
	raw, ok := trigger["gang_members"]
	if !ok || spec.Gang == nil {
		return nil
	}
	body, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	var members []struct {
		Rank            int    `json:"rank"`
		ContinuationRef string `json:"continuation_ref"`
		CommandID       string `json:"command_id"`
		Node            string `json:"node"`
	}
	if err := json.Unmarshal(body, &members); err != nil {
		return err
	}
	for _, m := range members {
		if m.Rank < 0 || m.Rank >= spec.Gang.Size {
			return fmt.Errorf("gang continuation member %d of %d", m.Rank, spec.Gang.Size)
		}
		var r *GangRank
		for i := range spec.Gang.Ranks {
			if spec.Gang.Ranks[i].Rank == m.Rank {
				r = &spec.Gang.Ranks[i]
			}
		}
		if r == nil {
			spec.Gang.Ranks = append(spec.Gang.Ranks, GangRank{Rank: m.Rank})
			r = &spec.Gang.Ranks[len(spec.Gang.Ranks)-1]
		}
		ref := m.ContinuationRef
		r.InputContinuationRef, r.InputCheckpointCommand = &ref, m.CommandID
		if m.Node != "" {
			selector := r.ExecutorSelector
			if len(selector) == 0 {
				selector = spec.ExecutorSelector
			}
			pinned := ExecutionSpec{ExecutorSelector: selector}
			if err := pinNodeSpec(&pinned, m.Node); err != nil {
				return err
			}
			r.ExecutorSelector = pinned.ExecutorSelector
		}
	}
	sort.Slice(spec.Gang.Ranks, func(i, j int) bool { return spec.Gang.Ranks[i].Rank < spec.Gang.Ranks[j].Rank })
	return nil
}

// propagateGangCheckpoints gives every running member of a launched gang
// without one a checkpoint command when another member has one in flight
// (an operator's, or a data protection one): the gang checkpoints only once
// every member holds one.
func (s *Store) propagateGangCheckpoints(ctx context.Context) error {
	const live = `SELECT gpl.placement_id,c.origin,c.reason,gpl.rank FROM gang_placements gp JOIN gang_placement_leases gpl ON gpl.placement_id=gp.id
		JOIN leases l ON l.id=gpl.lease_id JOIN commands c ON c.attempt_id=l.attempt_id
		WHERE gp.state='launched' AND c.kind='checkpoint' AND c.state IN('pending','accepted','checkpointing')`
	var found bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(`+live+`)`).Scan(&found); err != nil || !found {
		return err
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type source struct {
		origin, reason string
		rank           int
	}
	sources := map[string]source{}
	if err := eachRow(ctx, tx, live+` ORDER BY c.created_at`, nil, func(rows *sql.Rows) error {
		var placement string
		var s source
		if err := rows.Scan(&placement, &s.origin, &s.reason, &s.rank); err != nil {
			return err
		}
		if _, ok := sources[placement]; !ok {
			sources[placement] = s
		}
		return nil
	}); err != nil {
		return err
	}
	for placement, src := range sources {
		type target struct {
			executionID, attemptID, leaseID string
			epoch                           int64
			rank                            int
		}
		var targets []target
		if err := eachRow(ctx, tx, `SELECT gpl.execution_id,a.id,l.id,l.coordination_epoch,gpl.rank FROM gang_placement_leases gpl
			JOIN leases l ON l.id=gpl.lease_id AND l.state='active' JOIN attempts a ON a.id=l.attempt_id AND a.state='running'
			JOIN execution_requests e ON e.id=gpl.execution_id AND e.checkpointable
			WHERE gpl.placement_id=$1 AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.attempt_id=a.id AND
				((c.kind='checkpoint' AND c.state IN('pending','accepted','checkpointing')) OR (c.kind='suspend' AND c.state!='rejected')))`,
			[]any{placement}, func(rows *sql.Rows) error {
				var t target
				if err := rows.Scan(&t.executionID, &t.attemptID, &t.leaseID, &t.epoch, &t.rank); err != nil {
					return err
				}
				targets = append(targets, t)
				return nil
			}); err != nil {
			return err
		}
		for _, t := range targets {
			commandID, stamp := id.New("cmd"), now()
			reason := fmt.Sprintf("gang rank %d: %s", src.rank, src.reason)
			if _, err := tx.ExecContext(ctx, `INSERT INTO commands(id,execution_id,attempt_id,lease_id,coordination_epoch,kind,origin,reason,state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'checkpoint',$6,$7,'pending',$8,$9)`,
				commandID, t.executionID, t.attemptID, t.leaseID, t.epoch, src.origin, reason, stamp, stamp); err != nil {
				return err
			}
			projectID, err := executionProjectIDTx(ctx, tx, t.executionID)
			if err != nil {
				return err
			}
			if err := appendCoordinationEventTx(ctx, tx, "checkpoint_requested", &projectID, "command", commandID, &t.epoch, map[string]any{"execution_id": t.executionID, "attempt_id": t.attemptID, "lease_id": t.leaseID, "origin": src.origin, "gang_rank": t.rank}); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
