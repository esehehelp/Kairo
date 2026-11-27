package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kairo/internal/machine"
)

// Where an execution goes when its run's output directory is on one node
// (README: Admission, a run's output directory). The directory holds what
// the run wrote, its checkpoint among it (/kairo/output in the container):
// an execution of the run runs where the directory is, or has it brought
// to where it goes first (outputlandings.go).
//
// A continuation after a suspend may go to any node its declaration allows.
// The pass estimates, for each node it could run on, when it would end
// there: when it could start (now if it fits; else when the leases in its
// way are expected to end, startAt), after the directory is brought there
// (transferTime: its size over the measured path between the two nodes'
// agents, nodePerf), and the run time it has left (estimate.go) at that
// node's speed against the speed of the node holding the directory (GPU
// work: the GPUs' gemm throughput; CPU work: the CPU's). It goes to another
// node only when it is expected to end there sooner by at least moveMinGain
// (and a tenth of the time it would take where the directory is): the
// directory is then ordered moved (orderOutputMoveTx), and it waits for it
// (output_moving), holding what it needs there from when the directory is
// expected to arrive (from the node it comes from). One move per continuation, one at a time per run; a
// move needs the agent holding the directory (its executor live). Every
// other execution of a run with a directory runs where the directory is
// (output_elsewhere while no executor there matches it); a rerun from a
// checkpoint waits for the output holding it (output_moving, or
// output_unavailable once given up).

// moveMinGain is the least a continuation must be expected to gain by
// moving to another node.
const moveMinGain = 10 * time.Minute

// candidateOutput is what the pass knows of a candidate's run's output
// directory.
type candidateOutput struct {
	runID  string
	reason string // the execution's orchestration reason ("" when it has none)
	home   *outputHome
	// landing is this execution's landing; runBusy: another landing of the
	// run is not done yet (a move whose directory left behind is still to
	// be removed), so no move is ordered now.
	landing *landingView
	runBusy bool
}

// landingView is a landing as the pass sees it.
type landingView struct {
	reason, state, fromRun, node, problem string
	bytes                                 int64
	created                               time.Time
}

// planMove is a move the pass orders for a continuation.
type planMove struct {
	to       string
	home     outputHome
	estimate moveEstimate
}

// moveEstimate is why a continuation moved: what was expected of each node
// it could run on (kept with its landing, output_landings.estimate_json).
type moveEstimate struct {
	RunID            string         `json:"run_id"`
	Bytes            int64          `json:"bytes"`
	From             string         `json:"from"`
	To               string         `json:"to"`
	RemainingSeconds float64        `json:"remaining_seconds,omitempty"`
	Nodes            []nodeEstimate `json:"nodes"`
}

type nodeEstimate struct {
	Node            string  `json:"node"`
	Start           string  `json:"start,omitempty"`
	TransferSeconds float64 `json:"transfer_seconds"`
	// SpeedRatio: its speed over that of the node holding the directory
	// (absent when either was not measured: taken as equal).
	SpeedRatio float64 `json:"speed_ratio,omitempty"`
	Finish     string  `json:"finish,omitempty"`
	finish     time.Time
	known      bool
}

// loadCandidateOutputs notes on each candidate where its run's output
// directory is, and its landing: one query each for the runs, their
// directories and the landings, whatever the number of candidates.
func loadCandidateOutputs(ctx context.Context, tx *sql.Tx, m *planModel, candidates []*planCandidate, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	outputs := map[string]*candidateOutput{}
	var runs []string
	err := eachRow(ctx, tx, `SELECT e.id,`+executionRunID+`,COALESCE(o.reason,'') FROM execution_requests e`+executionRunJoins+` WHERE e.id=ANY($1::text[])`, []any{ids}, func(rows *sql.Rows) error {
		o := &candidateOutput{}
		var id string
		if err := rows.Scan(&id, &o.runID, &o.reason); err != nil {
			return err
		}
		outputs[id] = o
		runs = append(runs, o.runID)
		return nil
	})
	if err != nil {
		return err
	}
	homes := map[string]*outputHome{}
	err = eachRow(ctx, tx, `SELECT DISTINCT ON (o.run_id) o.run_id,o.store_id,o.executor_id,x.node_id,o.root,o.path,COALESCE(o.bytes,0)
		FROM run_outputs o JOIN executors x ON x.id=o.executor_id WHERE o.run_id=ANY($1::text[]) AND o.state='active' ORDER BY o.run_id,o.updated_at DESC`, []any{runs}, func(rows *sql.Rows) error {
		h := &outputHome{}
		if err := rows.Scan(&h.runID, &h.storeID, &h.executorID, &h.nodeID, &h.root, &h.path, &h.bytes); err != nil {
			return err
		}
		homes[h.runID] = h
		return nil
	})
	if err != nil {
		return err
	}
	landings := map[string]*landingView{}
	busy := map[string]string{} // run -> an execution whose landing is not done
	err = eachRow(ctx, tx, `SELECT execution_id,run_id,reason,state,from_run_id,node_id,COALESCE(problem,''),COALESCE(bytes,0),created_at FROM output_landings
		WHERE execution_id=ANY($1::text[]) OR run_id=ANY($2::text[]) AND state IN('waiting','packing','bringing','placed')`, []any{ids, runs}, func(rows *sql.Rows) error {
		var execution, run, created string
		l := &landingView{}
		if err := rows.Scan(&execution, &run, &l.reason, &l.state, &l.fromRun, &l.node, &l.problem, &l.bytes, &created); err != nil {
			return err
		}
		l.created, _ = time.Parse(time.RFC3339Nano, created)
		landings[execution] = l
		if l.state != landingDone && l.state != landingFailed {
			busy[run] = execution
		}
		return nil
	})
	if err != nil {
		return err
	}
	var declared []machine.Store
	if len(homes) > 0 {
		if declared, err = declaredStores(ctx, tx); err != nil {
			return err
		}
	}
	m.landable = map[string]bool{}
	for _, d := range declared {
		if d.Class != machine.StoreVolatile {
			continue
		}
		for _, e := range m.executors {
			if e.nodeID == d.Node && (d.Executor == "" || d.Executor == e.id) {
				m.landable[d.Node] = true
			}
		}
	}
	for _, c := range candidates {
		o := outputs[c.execution.ID]
		if o == nil {
			continue
		}
		if h := homes[o.runID]; h != nil && outputOwnership(declared, h.nodeID, h.storeID, h.root, h.path, h.runID) == "" {
			o.home = h
		}
		o.landing = landings[c.execution.ID]
		if other, ok := busy[o.runID]; ok && other != c.execution.ID {
			o.runBusy = true
		}
		if o.home != nil || o.landing != nil {
			c.output = o
		}
	}
	return nil
}

// transferTime is how long bringing bytes from node a to node b is expected
// to take: over the path between their agents, plus a quarter for packing
// and unpacking, plus half a minute.
func (m *planModel) transferTime(bytes int64, a, b string) time.Duration {
	seconds := float64(max(bytes, 0))/m.perf.link(a, b)*1.25 + 30
	return time.Duration(seconds * float64(time.Second))
}

// speed is how fast node runs demand: its GPUs' gemm throughput (their
// mean) for GPU work, its CPU's for the rest; 0 when not measured.
func (m *planModel) speed(node string, demand planDemand) float64 {
	if m.perf == nil {
		return 0
	}
	gpus := false
	for _, g := range demand.gpus {
		gpus = gpus || g.count > 0
	}
	if !gpus {
		return m.perf.cpu[node]
	}
	var sum float64
	n := 0
	for _, g := range m.gpus[node] {
		if v, ok := m.perf.gpu[g.device]; ok {
			sum += v
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

// planOutput decides for a candidate with a run output directory (or a
// landing): a wait, a move, or the nodes it may go to (options narrowed to
// the directory's); done is false when the pass goes on with it.
func (p *planner) planOutput(c *planCandidate, ranks []planRank, options [][]planExecutor) (planPlacement, bool) {
	o := c.output
	if o == nil {
		return planPlacement{}, false
	}
	if l := o.landing; l != nil {
		switch l.state {
		case landingWaiting, landingPacking, landingBringing:
			from := l.node
			if o.home != nil {
				from = o.home.nodeID
			}
			p.holdForLanding(c, ranks, l, from)
			return planPlacement{wait: planWait{reason: "output_moving", node: l.node, message: landingMessage(o, l)}}, true
		case landingFailed:
			if l.reason == landingCheckpoint {
				return planPlacement{wait: planWait{reason: "output_unavailable", node: l.node,
					message: "the output of run " + l.fromRun + ", which holds its checkpoint, was not brought to " + l.node + ": " + l.problem}}, true
			}
		}
	}
	h := o.home
	if h == nil {
		return planPlacement{}, false
	}
	// (a gang's members each go where their own output is: no move)
	if _, live := p.m.executor(h.executorID); live && c.gangID == "" && o.reason == "continuation" && o.landing == nil && !o.runBusy {
		if est, ok := p.chooseNode(c, ranks, options, *h); ok {
			move := &planMove{to: est.To, home: *h, estimate: est}
			p.holdForLanding(c, ranks, &landingView{node: est.To, bytes: h.bytes, created: p.m.stamp, fromRun: h.runID}, h.nodeID)
			return planPlacement{move: move, wait: planWait{reason: "output_moving", node: est.To,
				message: fmt.Sprintf("its run's output directory (%s) is to be brought from %s to %s, where it is expected to end sooner", formatBytes(h.bytes), h.nodeID, est.To)}}, true
		}
	}
	// on its node, by the executor that made it when that one may run it
	// (it serves the store the directory is on)
	var here []planExecutor
	for _, e := range options[0] {
		if e.nodeID == h.nodeID {
			here = append(here, e)
		}
	}
	for _, e := range here {
		if e.id == h.executorID {
			here = []planExecutor{e}
			break
		}
	}
	if len(here) == 0 {
		return planPlacement{wait: planWait{reason: "output_elsewhere", node: h.nodeID,
			message: "its run's output directory (its checkpoint) is on " + h.nodeID + ", where no live executor matches its executor labels"}}, true
	}
	options[0] = here
	return planPlacement{}, false
}

// landingMessage says what a candidate waits for while its landing is made.
func landingMessage(o *candidateOutput, l *landingView) string {
	switch {
	case l.state == landingWaiting:
		return "the output of run " + l.fromRun + ", which holds its checkpoint, is to be brought to " + l.node + " once packed"
	case l.reason == landingCheckpoint:
		return fmt.Sprintf("the output of run %s, which holds its checkpoint (%s), is being brought to %s", l.fromRun, formatBytes(l.bytes), l.node)
	case l.state == landingPacking:
		return fmt.Sprintf("its run's output directory (%s) is being packed to be brought to %s, where it is expected to end sooner", formatBytes(l.bytes), l.node)
	}
	return fmt.Sprintf("its run's output directory (%s) is being brought to %s, where it is expected to end sooner", formatBytes(l.bytes), l.node)
}

// holdForLanding holds, on the node a landing goes to, what the candidate
// needs there from when the directory is expected to be there: work
// expected to end by then may use it meanwhile.
func (p *planner) holdForLanding(c *planCandidate, ranks []planRank, l *landingView, fromNode string) {
	if l.node == "" {
		return
	}
	from := l.created.Add(p.m.transferTime(l.bytes, fromNode, l.node))
	if from.Before(p.m.stamp) {
		from = p.m.stamp
	}
	f := p.m.fit(l.node, ranks[0].demand, p.overlayFor(nil), noneFreed)
	p.holds = append(p.holds, planHold{from: from, fit: f, by: c.execution.ID})
}

// expectedStart is when the candidate could start on e's node: now when it
// fits there now, else when the leases in its way are expected to end
// (known false when the estimates cannot tell); possible is false when it
// fits there at no time.
func (p *planner) expectedStart(c *planCandidate, ranks []planRank, e planExecutor) (start time.Time, known, possible bool) {
	if p.m.fit(e.nodeID, ranks[0].demand, p.overlayFor(c), noneFreed).ok {
		return p.m.stamp, true, true
	}
	f := p.m.fit(e.nodeID, ranks[0].demand, newOverlay(), allFreed)
	if !f.ok {
		return time.Time{}, false, false
	}
	start, known = p.startAt(ranks, []planSlot{{executor: e, rank: ranks[0].rank, fit: f}}, p.overlayFor(nil))
	return start, known, true
}

// chooseNode estimates when the candidate would end on each node it may go
// to (see above) and returns the node to move to, if one is enough sooner.
func (p *planner) chooseNode(c *planCandidate, ranks []planRank, options [][]planExecutor, h outputHome) (moveEstimate, bool) {
	est := moveEstimate{RunID: h.runID, Bytes: h.bytes, From: h.nodeID, RemainingSeconds: c.endsIn.Seconds()}
	homeSpeed := p.m.speed(h.nodeID, ranks[0].demand)
	seen := map[string]bool{}
	for _, e := range options[0] {
		if seen[e.nodeID] || e.nodeID != h.nodeID && !p.m.landable[e.nodeID] {
			continue
		}
		seen[e.nodeID] = true
		start, known, possible := p.expectedStart(c, ranks, e)
		if !possible {
			continue
		}
		n := nodeEstimate{Node: e.nodeID}
		var transfer time.Duration
		if e.nodeID != h.nodeID {
			transfer = p.m.transferTime(h.bytes, h.nodeID, e.nodeID)
		}
		n.TransferSeconds = transfer.Seconds()
		if known {
			if s := p.m.speed(e.nodeID, ranks[0].demand); s > 0 && homeSpeed > 0 {
				n.SpeedRatio = s / homeSpeed
			}
			n.finish, n.known = expectedEnd(p.m.stamp, start, transfer, c.endsIn, n.SpeedRatio), true
			n.Start, n.Finish = instantString(start), instantString(n.finish)
		}
		est.Nodes = append(est.Nodes, n)
	}
	to, ok := bestMove(est.Nodes, h.nodeID, p.m.stamp)
	est.To = to
	return est, ok
}

// expectedEnd is when work that has run left at the speed of the node
// holding its directory ends on a node it could start on at start, once its
// directory is brought there (transfer from now), running ratio times as
// fast (0: as fast).
func expectedEnd(now, start time.Time, transfer, run time.Duration, ratio float64) time.Time {
	begin := start
	if arrive := now.Add(transfer); arrive.After(begin) {
		begin = arrive
	}
	if ratio > 0 {
		run = time.Duration(float64(run) / ratio)
	}
	return begin.Add(run)
}

// bestMove is the node to move to of those estimated (see above): the one
// expected to end soonest, when that is not home and ends sooner than home
// by moveMinGain and a tenth of the time home would take (home not known:
// any node known will do).
func bestMove(nodes []nodeEstimate, home string, now time.Time) (string, bool) {
	var at, best *nodeEstimate
	for i := range nodes {
		n := &nodes[i]
		if n.Node == home {
			at = n
		}
		if n.known && (best == nil || n.finish.Before(best.finish)) {
			best = n
		}
	}
	if best == nil || best.Node == home {
		return "", false
	}
	if at != nil && at.known {
		margin := max(moveMinGain, at.finish.Sub(now)/10)
		if !best.finish.Add(margin).Before(at.finish) {
			return "", false
		}
	}
	return best.Node, true
}

// instantString is t as the store writes instants.
func instantString(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
