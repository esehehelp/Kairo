package store

// Admission planning. Every reserve and preemption call of every executor runs
// one deterministic pass over the whole installation: all waiting executions,
// all nodes with a live executor, every lease and claim. The pass, not the
// executor that happens to poll first, decides where each execution goes:
//
//   - order: priority first (higher first); within a priority, an execution
//     with a start_by or finish_by goes first, the one that must start
//     soonest first (a finish_by less its estimated run time); then the
//     project with the smallest dominant share of GPUs, CPU and RAM held,
//     then the earlier submission. A wait that misses a start_by or
//     finish_by says so; neither makes anything stop.
//   - not_before: an execution is not admitted, and holds nothing, before
//     its not_before; from notBeforeHold before it, it holds what it needs
//     where it is going from its not_before, as backfill holds are (work
//     expected to end by then may use it meanwhile).
//   - placement: an execution goes to the node it fits on best, the one left
//     with the fewest free GPUs (CPU work stays off GPU nodes, GPU work packs),
//     then the most free CPU. It is reserved when that node's executor polls.
//   - backfill: an execution that fits nowhere holds what it can use on its
//     closest node, and only when what stops it is other leases (they end),
//     from when it is expected to start there (estimate.go: when enough of
//     those leases are expected to have ended; now when no estimate tells).
//     Later executions run on whatever nobody holds, and on what it holds
//     if they are expected to end by then. An execution blocked by
//     something that does not end by itself (a foreign GPU process, a stale
//     observation, a node too small) holds nothing.
//   - preemption: an execution that fits nowhere may suspend cooperative
//     executions of lower priority (checkpointable and preemptible) whose
//     GPUs or RAM are what stops it, the fewest needed. CPU is never a reason
//     to suspend anything: too little of it slows work down, it breaks
//     nothing.
//   - accounting: a lease's capacity is counted once. CPU is total minus
//     reservations, where an execution that declared cpu_millis_min lends
//     the rest of its reservation to executions of the same or higher
//     priority (it runs slower instead of being stopped); RAM is what is
//     observed free minus the part of each reservation its attempt does not
//     use yet (attempt_usage), a reservation counted only up to what runs
//     like it were seen to take (estimate.go), when enough of them were;
//     disk is what is observed free minus every reservation, since bytes
//     written overstate what stays on disk.
//   - a node with a filesystem under the free-space watermark admits nothing.
//
// Every waiting execution's reason is kept in admission_waits.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"kairo/internal/instant"
)

// notBeforeHold is how long before its not_before an execution starts to hold
// what it needs (planner.notYet).
const notBeforeHold = 2 * time.Hour

// SchedulingPolicy holds the installation-wide admission settings.
type SchedulingPolicy struct {
	// DiskMinFreeBytes is the free space every registered filesystem keeps,
	// capped at 5% of its size: a node with a filesystem under it admits
	// nothing, and no admission may take a filesystem under it. 0 disables it.
	DiskMinFreeBytes int64
}

// DefaultDiskMinFreeBytes is the default filesystem watermark (5 GiB).
const DefaultDiskMinFreeBytes = 5 << 30

// SetSchedulingPolicy replaces the admission settings.
func (s *Store) SetSchedulingPolicy(p SchedulingPolicy) { s.policy = p }

const (
	// executorLiveWindow: an executor not seen for this long gets no
	// placements (one never seen yet is assumed live).
	executorLiveWindow = time.Minute
	// usageFreshness: an older usage sample counts as no sample.
	usageFreshness = 2 * time.Minute
)

// liveLeaseStates are the lease states that hold their resources.
const liveLeaseStates = `('reserved','prepared','active','releasing','stale','revocation_requested')`

type planExecutor struct {
	id, nodeID, attributes, nodeAttributes string
}

func (e planExecutor) gang() gangExecutor {
	return gangExecutor{id: e.id, nodeID: e.nodeID, attributes: e.attributes, nodeAttributes: e.nodeAttributes}
}

type planGPU struct {
	id, nodeID, device string
	fresh              bool
	total, free        sql.NullInt64
}

type planDevice struct {
	lease                  string // the live lease on any row of the device
	external, unattributed bool
	evidence               string // a foreign process on it, for messages
}

type capKey struct{ node, kind, filesystem string }

type planCapacity struct {
	resourceID  string
	total, free sql.NullInt64
}

type planLease struct {
	id, executionID, attemptID, executorID, nodeID, projectID, label string
	epoch                                                            int64
	priority                                                         int
	gang                                                             bool
	startedAt                                                        string
	// victim: a running cooperative attempt that priority preemption may
	// suspend; suspending: it already has a live suspend command.
	victim, suspending, allowsUnattributed bool
	preemptionCommand                      string
	devices                                []string
	capacity                               map[capKey]int64
	used                                   map[capKey]int64 // taken up, of capacity
	cpuLendable                            int64            // reserved CPU above cpu_millis_min
	ramExpected                            int64            // RAM runs like it took at most, with headroom (0: not known)
	// For its run-time estimate (estimate.go): the execution's queue, Task
	// and argv, and when its attempt started and whether it has exited.
	queueID, taskID *string
	argvJSON        string
	attemptStarted  string
	attemptExited   bool
}

type gpuDemand struct {
	count       int64
	constraints gpuConstraints
	policy      conflictPolicy
}

type capDemand struct {
	kind, filesystem string
	quantity         int64
	minFreeAfter     int64
}

type planDemand struct {
	priority int // borrows lendable CPU of leases of no higher priority
	gpus     []gpuDemand
	capacity []capDemand
}

type planRank struct {
	rank      int
	candidate executionCandidate
	demand    planDemand
}

type planCandidate struct {
	executionCandidate
	label  string
	demand planDemand
	gangID string
	ranks  []planRank
	// inputsWait: why it is not admitted yet for its inputs (inputs.go);
	// empty when it has none, or each is where it is planned.
	inputsWait string
	// endsIn is how long it is expected to run once admitted (0: no
	// estimate, it may use nothing held for others).
	endsIn time.Duration
}

type planModel struct {
	stamp     time.Time
	policy    SchedulingPolicy
	executors []planExecutor
	gpus      map[string][]planGPU // by node, ordered by id
	devices   map[string]*planDevice
	capacity  map[capKey]planCapacity
	leases    map[string]*planLease
	leaseIDs  []string
	lowDisk   map[string][]string // node -> messages
	labels    map[string]string   // scope id -> name
	totals    map[string]float64  // kind -> installation total, for shares
	// leaseEnds is when each live lease is expected to give back what it
	// holds (estimate.go); a lease without one is not expected to.
	leaseEnds map[string]time.Time
}

// planOverlay is what the pass has handed out so far: devices placed on or
// held for an execution, and capacity placed or held per node. The pass
// keeps placements (and room made by preemption) in one, and the holds of
// waiting executions apart (planner.holds, overlayFor).
type planOverlay struct {
	devices  map[string]string
	capacity map[capKey]int64
}

func newOverlay() planOverlay {
	return planOverlay{devices: map[string]string{}, capacity: map[capKey]int64{}}
}

func (o planOverlay) clone() planOverlay {
	c := newOverlay()
	for k, v := range o.devices {
		c.devices[k] = v
	}
	for k, v := range o.capacity {
		c.capacity[k] = v
	}
	return c
}

type planCapacityChoice struct {
	resourceID string
	key        capKey
	quantity   int64
}

// planFit is one execution's fit on one node: what it gets (all of it when
// ok, what is free of it otherwise) or why not.
type planFit struct {
	ok       bool
	node     string
	gpus     []string // resource ids
	devices  []string
	capacity []planCapacityChoice
	reason   string
	message  string
	terminal bool
}

func (f *planFit) fail(reason, message string, terminal bool) {
	f.ok = false
	if f.reason == "" {
		f.reason, f.message, f.terminal = reason, message, terminal
	}
}

// assignedRank holds, in an overlay, what assign gave a rank of the placement
// it is building.
const assignedRank = "(another rank of this placement)"

func (o planOverlay) take(f planFit, executionID string) {
	for _, d := range f.devices {
		o.devices[d] = executionID
	}
	for _, c := range f.capacity {
		o.capacity[c.key] += c.quantity
	}
}

func loadPlanModel(ctx context.Context, tx *sql.Tx, policy SchedulingPolicy, callerID string) (*planModel, error) {
	m := &planModel{
		stamp: time.Now().UTC(), policy: policy, gpus: map[string][]planGPU{}, devices: map[string]*planDevice{},
		capacity: map[capKey]planCapacity{}, leases: map[string]*planLease{}, lowDisk: map[string][]string{},
		labels: map[string]string{}, totals: map[string]float64{},
	}
	if err := m.loadExecutors(ctx, tx, callerID); err != nil {
		return nil, err
	}
	if err := m.loadResources(ctx, tx); err != nil {
		return nil, err
	}
	if err := m.loadLeases(ctx, tx); err != nil {
		return nil, err
	}
	var scopes []string
	for _, l := range m.leases {
		scopes = append(scopes, l.projectID, l.label)
	}
	if err := m.loadLabels(ctx, tx, scopes); err != nil {
		return nil, err
	}
	for _, l := range m.leases {
		l.label = m.labels[l.label]
	}
	m.findLowDisk()
	return m, nil
}

// loadLabels adds the names of scopes not named yet: only those of leases and
// candidates are read, not every scope there is (a pass runs several times a
// second per executor, and scopes only grow).
func (m *planModel) loadLabels(ctx context.Context, tx *sql.Tx, scopeIDs []string) error {
	var missing []string
	for _, scopeID := range scopeIDs {
		if _, ok := m.labels[scopeID]; !ok && scopeID != "" {
			m.labels[scopeID] = ""
			missing = append(missing, scopeID)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return eachRow(ctx, tx, `SELECT id,external_key FROM coordination_scopes WHERE id=ANY($1::text[])`, []any{missing}, func(rows *sql.Rows) error {
		var scopeID, name string
		if err := rows.Scan(&scopeID, &name); err != nil {
			return err
		}
		m.labels[scopeID] = name
		return nil
	})
}

func (m *planModel) loadExecutors(ctx context.Context, tx *sql.Tx, callerID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT x.id,x.node_id,x.attributes_json,n.attributes_json,x.last_seen_at FROM executors x JOIN nodes n ON n.id=x.node_id
		WHERE x.enabled AND n.enabled AND NOT EXISTS(SELECT 1 FROM node_quarantines q WHERE q.node_id IN(x.node_id,'*'))
		ORDER BY x.id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var e planExecutor
		var seen sql.NullString
		if err = rows.Scan(&e.id, &e.nodeID, &e.attributes, &e.nodeAttributes, &seen); err != nil {
			return err
		}
		if e.id != callerID && seen.Valid {
			if at, parseErr := time.Parse(time.RFC3339Nano, seen.String); parseErr == nil && m.stamp.Sub(at) > executorLiveWindow {
				continue
			}
		}
		m.executors = append(m.executors, e)
	}
	return rows.Err()
}

func (m *planModel) loadResources(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT ri.id,ri.node_id,ri.kind,ri.stable_identity,o.valid_until,o.total_bytes,o.free_bytes,
		ri.kind<>'gpu' OR `+declaredGPUFilter+`
		FROM resource_instances ri
		LEFT JOIN resource_observations o ON o.id=(SELECT id FROM resource_observations WHERE resource_id=ri.id ORDER BY id DESC LIMIT 1)
		WHERE ri.admin_state='enabled' AND ri.kind IN('gpu','cpu','ram','disk') ORDER BY ri.id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var resourceID, nodeID, kind, identity string
		var valid sql.NullString
		var total, free sql.NullInt64
		var declared bool
		if err = rows.Scan(&resourceID, &nodeID, &kind, &identity, &valid, &total, &free, &declared); err != nil {
			return err
		}
		fresh := false
		if deadline, parseErr := time.Parse(time.RFC3339Nano, valid.String); valid.Valid && parseErr == nil && deadline.After(m.stamp) {
			fresh = true
		}
		if kind == "gpu" {
			if !declared {
				continue
			}
			m.gpus[nodeID] = append(m.gpus[nodeID], planGPU{id: resourceID, nodeID: nodeID, device: identity, fresh: fresh, total: total, free: free})
			if m.devices[identity] == nil {
				m.devices[identity] = &planDevice{}
				m.totals["gpu"]++
			}
			continue
		}
		filesystem := ""
		if kind == "disk" {
			filesystem = identity
		}
		key := capKey{nodeID, kind, filesystem}
		if _, seen := m.capacity[key]; seen || !fresh {
			continue
		}
		m.capacity[key] = planCapacity{resourceID: resourceID, total: total, free: free}
		if kind != "disk" && total.Valid {
			m.totals[kind] += float64(total.Int64)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	claims, err := tx.QueryContext(ctx, `SELECT ri.stable_identity,c.claim_kind,c.evidence_json FROM external_claims c JOIN resource_instances ri ON ri.id=c.resource_id
		WHERE c.cleared_at IS NULL AND ri.kind='gpu' ORDER BY c.first_observed_at`)
	if err != nil {
		return err
	}
	defer claims.Close()
	for claims.Next() {
		var identity, claimKind, evidence string
		if err = claims.Scan(&identity, &claimKind, &evidence); err != nil {
			return err
		}
		device := m.devices[identity]
		if device == nil {
			continue
		}
		if claimKind == "external_process" {
			if !device.external {
				var e map[string]string
				_ = json.Unmarshal([]byte(evidence), &e)
				device.evidence = strings.TrimSpace(fmt.Sprintf("pid %s %s", e["pid"], e["process_name"]))
			}
			device.external = true
		} else {
			device.unattributed = true
		}
	}
	return claims.Err()
}

func (m *planModel) loadLeases(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT l.id,l.execution_id,COALESCE(l.attempt_id,''),l.executor_id,x.node_id,x.enabled,l.state,l.coordination_epoch,COALESCE(a.state,''),
		e.priority,e.checkpointable,e.preemptible,e.state,e.project_scope_id,COALESCE(e.task_scope_id,e.queue_scope_id,e.project_scope_id),COALESCE(e.started_at,''),
		EXISTS(SELECT 1 FROM gang_members gm WHERE gm.execution_id=e.id),
		COALESCE((SELECT MIN(CASE WHEN COALESCE(rr.policy_json::jsonb->>'on_unattributed_activity','wait')='allow' THEN 1 ELSE 0 END)
			FROM resource_requests rr WHERE rr.execution_id=e.id AND rr.request_type='exclusive' AND rr.kind='gpu'),0),
		u.ram_bytes,u.disk_written_bytes,u.observed_at,
		COALESCE((SELECT rr.quantity-(rr.constraints_json::jsonb->>'min_quantity')::bigint FROM resource_requests rr WHERE rr.execution_id=e.id AND rr.request_type='capacity' AND rr.kind='cpu' AND rr.constraints_json::jsonb->>'min_quantity' IS NOT NULL),0),
		e.queue_scope_id,e.task_scope_id,e.argv_json,COALESCE(a.started_at,''),a.exited_at IS NOT NULL
		FROM leases l JOIN executors x ON x.id=l.executor_id JOIN execution_requests e ON e.id=l.execution_id
		LEFT JOIN attempts a ON a.id=l.attempt_id LEFT JOIN attempt_usage u ON u.attempt_id=l.attempt_id
		WHERE l.state IN `+liveLeaseStates+` ORDER BY l.id`)
	if err != nil {
		return err
	}
	type usage struct {
		ram, disk sql.NullInt64
		at        sql.NullString
	}
	usages := map[string]usage{}
	running := map[string]bool{}
	for rows.Next() {
		l := &planLease{capacity: map[capKey]int64{}, used: map[capKey]int64{}}
		var executorEnabled, checkpointable, preemptible bool
		var leaseState, attemptState, executionState string
		var u usage
		if err = rows.Scan(&l.id, &l.executionID, &l.attemptID, &l.executorID, &l.nodeID, &executorEnabled, &leaseState, &l.epoch, &attemptState,
			&l.priority, &checkpointable, &preemptible, &executionState, &l.projectID, &l.label, &l.startedAt, &l.gang, &l.allowsUnattributed,
			&u.ram, &u.disk, &u.at, &l.cpuLendable, &l.queueID, &l.taskID, &l.argvJSON, &l.attemptStarted, &l.attemptExited); err != nil {
			rows.Close()
			return err
		}
		l.victim = l.attemptID != "" && leaseState == "active" && attemptState == "running" && executionState == "started" && checkpointable && preemptible && executorEnabled
		running[l.id] = attemptState == "running" || attemptState == "quiescing"
		usages[l.id] = u
		m.leases[l.id] = l
		m.leaseIDs = append(m.leaseIDs, l.id)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	items, err := tx.QueryContext(ctx, `SELECT li.lease_id,li.kind,li.quantity,li.filesystem,COALESCE(ri.stable_identity,''),COALESCE(ri.node_id,''),COALESCE(ri.admin_state,'')
		FROM lease_items li JOIN leases l ON l.id=li.lease_id LEFT JOIN resource_instances ri ON ri.id=li.resource_id
		WHERE l.state IN `+liveLeaseStates+` ORDER BY li.lease_id,li.kind,li.filesystem`)
	if err != nil {
		return err
	}
	defer items.Close()
	for items.Next() {
		var leaseID, kind, filesystem, identity, resourceNode, adminState string
		var quantity int64
		if err = items.Scan(&leaseID, &kind, &quantity, &filesystem, &identity, &resourceNode, &adminState); err != nil {
			return err
		}
		l := m.leases[leaseID]
		if l == nil {
			continue
		}
		if kind == "gpu" {
			l.devices = append(l.devices, identity)
			if device := m.devices[identity]; device != nil {
				device.lease = leaseID
			}
			continue
		}
		l.capacity[capKey{l.nodeID, kind, filesystem}] += quantity
		_ = resourceNode
		_ = adminState
	}
	if err = items.Err(); err != nil {
		return err
	}
	for _, l := range m.leases {
		// A victim's GPUs must be safe to hand over once it suspends: on its
		// executor's node, observed, and without foreign activity.
		for _, identity := range l.devices {
			device := m.devices[identity]
			if device == nil || device.external || device.unattributed && !l.allowsUnattributed || !m.deviceObserved(identity, l.nodeID) {
				l.victim = false
			}
		}
		u := usages[l.id]
		at, parseErr := time.Parse(time.RFC3339Nano, u.at.String)
		if !running[l.id] || !u.at.Valid || parseErr != nil || m.stamp.Sub(at) > usageFreshness {
			continue
		}
		// Bytes written are not taken as disk used: a rewritten checkpoint
		// or a deleted temporary file counts in them, so they could only
		// shrink a reservation too far.
		for _, key := range sortedCapKeys(l.capacity) {
			if key.kind == "ram" && u.ram.Valid {
				l.used[key] = min(u.ram.Int64, l.capacity[key])
			}
		}
	}
	commands, err := tx.QueryContext(ctx, `SELECT c.attempt_id,c.origin,c.id,c.state FROM commands c JOIN leases l ON l.attempt_id=c.attempt_id
		WHERE c.kind='suspend' AND l.state IN `+liveLeaseStates+` ORDER BY c.created_at`)
	if err != nil {
		return err
	}
	defer commands.Close()
	byAttempt := map[string]*planLease{}
	for _, l := range m.leases {
		if l.attemptID != "" {
			byAttempt[l.attemptID] = l
		}
	}
	refused := map[*planLease]bool{}
	for commands.Next() {
		var attemptID, origin, commandID, state string
		if err = commands.Scan(&attemptID, &origin, &commandID, &state); err != nil {
			return err
		}
		l := byAttempt[attemptID]
		if l == nil {
			continue
		}
		if state == "rejected" {
			refused[l] = true
			continue
		}
		if !l.suspending {
			l.suspending = true
			if origin == "priority_preemption" {
				l.preemptionCommand = commandID
			}
		}
	}
	if err = commands.Err(); err != nil {
		return err
	}
	// An attempt that rejected a suspend is not suspended again by
	// preemption (that rejection is the project's answer, and asking again
	// would loop): it is no victim, so the planner looks for room elsewhere
	// instead of choosing it every pass.
	for l := range refused {
		if !l.suspending {
			l.victim = false
		}
	}
	return nil
}

func (m *planModel) executor(executorID string) (planExecutor, bool) {
	for _, e := range m.executors {
		if e.id == executorID {
			return e, true
		}
	}
	return planExecutor{}, false
}

// onNode is the executor a slot runs on: the caller when it is on the slot's
// node and matches the selector (executors of one node share its resources),
// else the one the pass chose.
func (m *planModel) onNode(slot planSlot, caller planExecutor, selector string) planExecutor {
	if caller.id != "" && caller.nodeID == slot.executor.nodeID && selectorMatches(selector, caller.nodeID, caller.attributes) &&
		(slot.rank != 0 || slot.executor.gang().attribute("interconnect_addr") == "" || caller.gang().attribute("interconnect_addr") != "") {
		return caller
	}
	return slot.executor
}

// deviceObserved reports whether the device has a fresh row on node.
func (m *planModel) deviceObserved(identity, node string) bool {
	for _, g := range m.gpus[node] {
		if g.device == identity {
			return g.fresh
		}
	}
	return false
}

func sortedCapKeys(capacity map[capKey]int64) []capKey {
	keys := make([]capKey, 0, len(capacity))
	for k := range capacity {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].filesystem < keys[j].filesystem
	})
	return keys
}

// watermark is the free space a filesystem keeps.
func (m *planModel) watermark(c planCapacity) int64 {
	if m.policy.DiskMinFreeBytes <= 0 || !c.total.Valid {
		return 0
	}
	return min(m.policy.DiskMinFreeBytes, c.total.Int64/20)
}

func (m *planModel) findLowDisk() {
	for _, key := range sortedPlanKeys(m.capacity) {
		c := m.capacity[key]
		if key.kind != "disk" || !c.free.Valid {
			continue
		}
		if mark := m.watermark(c); c.free.Int64 < mark {
			m.lowDisk[key.node] = append(m.lowDisk[key.node], fmt.Sprintf("%s has %s free, under the %s watermark", key.filesystem, formatBytes(c.free.Int64), formatBytes(mark)))
		}
	}
}

func sortedPlanKeys(capacity map[capKey]planCapacity) []capKey {
	keys := make([]capKey, 0, len(capacity))
	for k := range capacity {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.node != b.node {
			return a.node < b.node
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		return a.filesystem < b.filesystem
	})
	return keys
}

// available is what capacity key offers to an execution of priority before
// this pass hands any out, with the leases freed() names counted as gone.
func (m *planModel) available(key capKey, c planCapacity, freed func(string) bool, priority int) (int64, bool) {
	var reserved, outstanding, reclaimed, lent int64
	for _, leaseID := range m.leaseIDs {
		l := m.leases[leaseID]
		quantity, ok := l.capacity[key]
		if !ok {
			continue
		}
		used, measured := l.used[key]
		if freed(leaseID) {
			// A suspended attempt gives back the RAM it uses; what it wrote
			// stays on disk.
			if key.kind == "ram" {
				if measured {
					reclaimed += used
				} else {
					reclaimed += quantity
				}
			}
			continue
		}
		// A RAM reservation counts up to what runs like it were seen to
		// take (with headroom), never less than its attempt uses now; one
		// whose use is not known now counts in full, and so does one holding
		// a GPU: the host memory CUDA pins is not in the samples (RSS, PSS,
		// working set), and a host without swap hangs when it runs out.
		counted := quantity
		if key.kind == "ram" && l.ramExpected > 0 && measured && len(l.devices) == 0 {
			counted = min(quantity, max(used, l.ramExpected))
		}
		reserved += counted
		outstanding += counted - used
		if key.kind == "cpu" && l.priority <= priority {
			lent += l.cpuLendable
		}
	}
	switch key.kind {
	case "cpu":
		return c.total.Int64 - reserved + lent, c.total.Valid
	case "ram":
		if !c.total.Valid || !c.free.Valid {
			return 0, false
		}
		return min(c.total.Int64-reserved, min(c.total.Int64, c.free.Int64+reclaimed)-outstanding), true
	default:
		return c.free.Int64 - outstanding, c.free.Valid
	}
}

func noneFreed(string) bool { return false }
func allFreed(string) bool  { return true }

// fit places demand on node against what ov has handed out.
func (m *planModel) fit(node string, demand planDemand, ov planOverlay, freed func(string) bool) planFit {
	f := planFit{ok: true, node: node}
	if low := m.lowDisk[node]; len(low) != 0 {
		f.fail("disk_low_watermark", node+": "+strings.Join(low, "; "), false)
		return f
	}
	taken := map[string]bool{}
	for _, d := range demand.gpus {
		var chosen int64
		var external, unattributed, stale, leased, held, lowMemory bool
		var holders, evidence []string
		for _, g := range m.gpus[node] {
			if chosen == d.count {
				break
			}
			if taken[g.device] || d.constraints.Device != "" && g.device != d.constraints.Device {
				continue
			}
			device := m.devices[g.device]
			if by := ov.devices[g.device]; by != "" {
				held = true
				continue
			}
			released := false
			if device.lease != "" {
				if !freed(device.lease) {
					leased = true
					holders = append(holders, m.leaseLabel(device.lease))
					continue
				}
				released = true
			}
			if device.external {
				external = true
				evidence = append(evidence, g.device+" ("+device.evidence+")")
				continue
			}
			if device.unattributed && d.policy.Unattributed != "allow" && !(released && m.leases[device.lease].allowsUnattributed) {
				unattributed = true
				continue
			}
			if !g.fresh {
				stale = true
				continue
			}
			if !g.total.Valid || g.total.Int64 < d.constraints.MinTotal {
				continue
			}
			// A suspending victim's memory is given back with the device.
			if !released && (!g.free.Valid || g.free.Int64 < d.constraints.MinFree) {
				lowMemory = true
				continue
			}
			taken[g.device] = true
			f.gpus = append(f.gpus, g.id)
			f.devices = append(f.devices, g.device)
			chosen++
		}
		if chosen == d.count {
			continue
		}
		need := fmt.Sprintf("%s: %d GPU(s) needed, %d usable", node, d.count, chosen)
		if d.constraints.MinTotal > 0 {
			need = fmt.Sprintf("%s: %d GPU(s) of >= %s needed, %d usable", node, d.count, formatBytes(d.constraints.MinTotal), chosen)
		}
		if d.constraints.Device != "" {
			need = fmt.Sprintf("%s: GPU %s needed, not usable", node, d.constraints.Device)
		}
		switch {
		case external:
			f.fail("external_claim", need+"; used by a process Kairo did not start: "+strings.Join(evidence, ", "), d.policy.External == "fail")
		case unattributed:
			f.fail("unattributed_activity", need+"; memory in use by no process Kairo can see", d.policy.Unattributed == "fail")
		case stale:
			f.fail("stale_observation", need+"; the GPU observation is out of date", d.policy.Stale == "fail")
		case held:
			message := need + "; held for an execution ahead of this one"
			if leased {
				message += ", the rest leased by " + strings.Join(unique(holders), ", ")
			}
			f.fail("held_for_higher_priority", message, false)
		case leased:
			f.fail("gpu_leased", need+"; leased by "+strings.Join(unique(holders), ", "), false)
		case lowMemory:
			f.fail("gpu_memory_low", need+fmt.Sprintf("; less than %s of GPU memory free", formatBytes(d.constraints.MinFree)), false)
		default:
			f.fail("insufficient_gpu", need, false)
		}
	}
	for _, c := range demand.capacity {
		key := capKey{node, c.kind, c.filesystem}
		row, registered := m.capacity[key]
		name := c.kind
		if c.kind == "disk" {
			name = "disk " + c.filesystem
		}
		if !registered {
			f.fail("insufficient_"+c.kind, fmt.Sprintf("%s: no observed %s", node, name), false)
			continue
		}
		available, known := m.available(key, row, freed, demand.priority)
		if !known {
			f.fail("insufficient_"+c.kind, fmt.Sprintf("%s: %s is not observed", node, name), false)
			continue
		}
		available -= ov.capacity[key]
		floor := int64(0)
		if c.kind == "disk" {
			floor = max(c.minFreeAfter, m.watermark(row))
		}
		if available-c.quantity >= floor {
			f.capacity = append(f.capacity, planCapacityChoice{resourceID: row.resourceID, key: key, quantity: c.quantity})
			continue
		}
		if part := min(c.quantity, available-floor); part > 0 {
			f.capacity = append(f.capacity, planCapacityChoice{resourceID: row.resourceID, key: key, quantity: part})
		}
		have := max(0, available-floor)
		if c.kind == "cpu" {
			f.fail("insufficient_cpu", fmt.Sprintf("%s: %s CPU needed, %s free", node, formatMillis(c.quantity), formatMillis(have)), false)
		} else {
			f.fail("insufficient_"+c.kind, fmt.Sprintf("%s: %s %s needed, %s free", node, formatBytes(c.quantity), name, formatBytes(have)), false)
		}
	}
	return f
}

func (m *planModel) leaseLabel(leaseID string) string {
	l := m.leases[leaseID]
	label := m.labels[l.projectID] + "/" + l.label
	if l.suspending {
		label += " (suspending)"
	}
	return fmt.Sprintf("%s (priority %d)", label, l.priority)
}

// freeGPUs counts node's GPUs nobody has or holds, apart from taken.
func (m *planModel) freeGPUs(node string, ov planOverlay, taken []string) int {
	n := 0
	for _, g := range m.gpus[node] {
		device := m.devices[g.device]
		if device.lease == "" && ov.devices[g.device] == "" && !device.external && g.fresh && !slices.Contains(taken, g.device) {
			n++
		}
	}
	return n
}

// better reports whether fit a leaves its node in a better state than b.
func (m *planModel) better(a, b planFit, ov planOverlay) bool {
	if fa, fb := m.freeGPUs(a.node, ov, a.devices), m.freeGPUs(b.node, ov, b.devices); fa != fb {
		return fa < fb
	}
	if ca, cb := m.cpuLeft(a, ov), m.cpuLeft(b, ov); ca != cb {
		return ca > cb
	}
	return a.node < b.node
}

// cpuLeft is the fraction of its node's CPU left after fit.
func (m *planModel) cpuLeft(f planFit, ov planOverlay) float64 {
	key := capKey{f.node, "cpu", ""}
	row, ok := m.capacity[key]
	if !ok || !row.total.Valid || row.total.Int64 <= 0 {
		return 0
	}
	available, _ := m.available(key, row, noneFreed, math.MinInt)
	available -= ov.capacity[key]
	for _, c := range f.capacity {
		if c.key == key {
			available -= c.quantity
		}
	}
	return float64(available) / float64(row.total.Int64)
}

type planWait struct {
	reason, node, message string
}

type planSlot struct {
	executor planExecutor
	rank     int
	fit      planFit
}

// planPlacement is the pass's decision for one candidate: slots to reserve
// (every rank of a gang), a terminal reason, or a wait.
type planPlacement struct {
	slots    []planSlot
	terminal string
	wait     planWait
	victims  []*planLease
}

type planner struct {
	m       *planModel
	ov      planOverlay
	preempt bool
	// victims are leases already chosen to make room in this pass.
	victims map[string]bool
	usage   map[string]map[string]float64 // project -> kind -> held
	// holds are what waiting executions hold (place), apart from ov.
	holds []planHold
}

func newPlanner(m *planModel, preempt bool) *planner {
	p := &planner{m: m, ov: newOverlay(), preempt: preempt, victims: map[string]bool{}, usage: map[string]map[string]float64{}}
	for _, l := range m.leases {
		p.account(l.projectID, "gpu", float64(len(l.devices)))
		for key, quantity := range l.capacity {
			p.account(l.projectID, key.kind, float64(quantity))
		}
	}
	return p
}

func (p *planner) account(project, kind string, quantity float64) {
	if p.usage[project] == nil {
		p.usage[project] = map[string]float64{}
	}
	p.usage[project][kind] += quantity
}

// share is a project's dominant share of GPUs, CPU and RAM.
func (p *planner) share(project string) float64 {
	share := 0.0
	for _, kind := range []string{"gpu", "cpu", "ram"} {
		if total := p.m.totals[kind]; total > 0 {
			share = math.Max(share, p.usage[project][kind]/total)
		}
	}
	return share
}

// order yields candidates by priority, then the latest they may start to
// keep their start_by and finish_by (those with one first), then fair share,
// then submission.
func (p *planner) order(candidates []*planCandidate, visit func(*planCandidate) (bool, error)) error {
	remaining := append([]*planCandidate(nil), candidates...)
	latest := make(map[*planCandidate]time.Time, len(candidates))
	for _, c := range candidates {
		if at, ok := c.mustStartBy(p.m.stamp); ok {
			latest[c] = at
		}
	}
	for len(remaining) != 0 {
		best := 0
		for i := 1; i < len(remaining); i++ {
			a, b := remaining[i].execution, remaining[best].execution
			if a.Priority != b.Priority {
				if a.Priority > b.Priority {
					best = i
				}
				continue
			}
			la, hasA := latest[remaining[i]]
			lb, hasB := latest[remaining[best]]
			if hasA != hasB {
				if hasA {
					best = i
				}
				continue
			}
			if hasA && !la.Equal(lb) {
				if la.Before(lb) {
					best = i
				}
				continue
			}
			if sa, sb := p.share(a.ProjectScopeID), p.share(b.ProjectScopeID); sa != sb {
				if sa < sb {
					best = i
				}
				continue
			}
			if a.SubmittedAt != b.SubmittedAt {
				if a.SubmittedAt < b.SubmittedAt {
					best = i
				}
				continue
			}
			if a.ID < b.ID {
				best = i
			}
		}
		c := remaining[best]
		remaining = append(remaining[:best], remaining[best+1:]...)
		stop, err := visit(c)
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// slotsFor lists, per node, the executor a demand would run on there.
func (p *planner) slotsFor(selector string, leader bool) []planExecutor {
	var slots []planExecutor
	seen := map[string]bool{}
	for _, e := range p.m.executors {
		if seen[e.nodeID] || !selectorMatches(selector, e.nodeID, e.attributes) {
			continue
		}
		if leader && e.gang().attribute("interconnect_addr") == "" {
			continue
		}
		seen[e.nodeID] = true
		slots = append(slots, e)
	}
	return slots
}

// place decides one candidate and records the decision in the overlay; a
// wait says when it misses the candidate's start_by or finish_by.
func (p *planner) place(c *planCandidate) planPlacement {
	placement, start := p.decide(c)
	if len(placement.slots) == 0 && placement.terminal == "" {
		placement.wait.message += p.deadlineNote(c, start)
	}
	return placement
}

// decide is place without the deadline note; start is when the candidate is
// expected to start when it waits (zero: not known).
func (p *planner) decide(c *planCandidate) (planPlacement, time.Time) {
	ranks := c.ranks
	if c.gangID == "" {
		ranks = []planRank{{rank: 0, candidate: c.executionCandidate, demand: c.demand}}
	}
	options := make([][]planExecutor, len(ranks))
	for i, r := range ranks {
		options[i] = p.slotsFor(r.candidate.selectorJSON, c.gangID != "" && r.rank == 0)
		if len(options[i]) == 0 {
			return planPlacement{wait: planWait{reason: "no_executor", message: rankPrefix(c, r) + "no live executor matches the executor labels"}}, time.Time{}
		}
	}
	if c.inputsWait != "" {
		return planPlacement{wait: planWait{reason: "inputs_not_ready", message: c.inputsWait}}, time.Time{}
	}
	if notBefore, ok := instant.Parse(deref(c.execution.NotBefore)); ok && notBefore.After(p.m.stamp) {
		return p.notYet(c, ranks, options, notBefore)
	}
	// What the pass has handed out, as c sees it: what is held for an
	// execution expected to start only after c would end does not stop it.
	ov := p.overlayFor(c)
	if slots, ok := p.assign(ranks, options, ov, noneFreed); ok {
		p.commit(c, slots)
		return planPlacement{slots: slots}, time.Time{}
	}
	// Nowhere to go now. A single execution whose every node refuses it by
	// its own policy ends here.
	if c.gangID == "" {
		terminal, reason := true, ""
		for _, e := range options[0] {
			f := p.m.fit(e.nodeID, ranks[0].demand, ov, noneFreed)
			terminal = terminal && f.terminal
			reason = f.reason
		}
		if terminal {
			return planPlacement{terminal: reason}, time.Time{}
		}
	}
	// Where it would go once the leases in its way end, if anywhere.
	target, transient := p.assign(ranks, options, newOverlay(), allFreed)
	if !transient {
		f := p.m.fit(options[0][0].nodeID, ranks[0].demand, ov, noneFreed)
		if f.ok || f.reason == "held_for_higher_priority" || f.reason == "gpu_leased" {
			f = p.m.fit(options[0][0].nodeID, ranks[0].demand, newOverlay(), allFreed)
		}
		message := f.message
		if f.ok || f.reason == "" {
			message = "no node can hold every rank at once"
		}
		return planPlacement{wait: planWait{reason: f.reason, node: f.node, message: rankPrefix(c, ranks[0]) + message}}, time.Time{}
	}
	// Nothing is suspended for room that only an execution waiting for its
	// not_before holds: it is free until then, and suspending work now would
	// leave it idle (the execution waits, holding as below, instead).
	// Nor for room it would not make: what would still be short is then the
	// reason it waits.
	var stuck planWait
	if !p.onlyNotBeforeInTheWay(c, ranks, options) {
		stuck = p.stillShort(c, ranks, target, ov)
	}
	if p.preempt && stuck.reason == "" && !p.onlyNotBeforeInTheWay(c, ranks, options) {
		if victims, slots, ok := p.makeRoom(c, ranks, target, ov); ok {
			for _, v := range victims {
				p.victims[v.id] = true
			}
			for _, s := range slots {
				p.ov.take(s.fit, c.execution.ID)
			}
			labels := make([]string, len(victims))
			for i, v := range victims {
				labels[i] = p.m.leaseLabel(v.id)
			}
			return planPlacement{victims: victims, wait: planWait{reason: "preempting", node: slots[0].fit.node, message: "suspending " + strings.Join(labels, ", ") + " to make room"}}, time.Time{}
		}
	}
	// Hold what it can use where it is going, from when it is expected to
	// start there: work expected to end by then may use it meanwhile.
	all := p.overlayFor(nil)
	start, known := p.startAt(ranks, target, all)
	if !known {
		start = p.m.stamp
	}
	var wait planWait
	for i, slot := range target {
		f := p.m.fit(slot.fit.node, ranks[i].demand, all, noneFreed)
		p.holds = append(p.holds, planHold{from: start, fit: f, by: c.execution.ID})
		if wait.reason == "" && !f.ok {
			wait = planWait{reason: f.reason, node: f.node, message: rankPrefix(c, ranks[i]) + f.message}
		}
	}
	// What it may suspend would not make room: the reason is what would
	// still be short (RAM, say, where a GPU it could have is leased).
	if stuck.reason != "" {
		message := stuck.message + ", so none is suspended for it"
		if wait.reason != stuck.reason && wait.message != "" {
			message += "; and " + wait.message
		}
		wait = planWait{reason: stuck.reason, node: stuck.node, message: message}
	}
	if start.After(p.m.stamp) {
		wait.message += "; expected to start in about " + formatAbout(start.Sub(p.m.stamp)) +
			" by estimated run times, and work expected to end before then may use what it holds"
	}
	if !known {
		start = time.Time{}
	}
	return planPlacement{wait: wait}, start
}

// notYet decides a candidate before its not_before: nothing is placed and
// nothing suspended for it. Within notBeforeHold of it, what it needs is held
// where it is going (as for a blocked execution, place) from its not_before,
// or from when what runs there is expected to end if that is later: only
// work expected to end by then may use it meanwhile.
func (p *planner) notYet(c *planCandidate, ranks []planRank, options [][]planExecutor, notBefore time.Time) (planPlacement, time.Time) {
	zone := notBefore.Location()
	wait := planWait{reason: "not_before", message: "not admitted before its not_before " + formatInstant(notBefore, zone)}
	if notBefore.Sub(p.m.stamp) > notBeforeHold {
		wait.message += "; nothing is held for it before " + formatInstant(notBefore.Add(-notBeforeHold), zone)
		return planPlacement{wait: wait}, notBefore
	}
	// Where it fits by its not_before: beside what the pass has handed out,
	// once the leases expected to end by then have; else where it would go
	// once every lease in its way ends.
	all := p.overlayFor(nil)
	endsBy := func(id string) bool {
		end, known := p.m.leaseEnds[id]
		return known && !end.After(notBefore)
	}
	target, ok := p.assign(ranks, options, all, endsBy)
	if !ok {
		if target, ok = p.assign(ranks, options, newOverlay(), allFreed); !ok {
			wait.message += "; no node can hold it, so nothing is held for it"
			return planPlacement{wait: wait}, time.Time{}
		}
	}
	start, known := p.startAt(ranks, target, all)
	if !known || start.Before(notBefore) {
		start = notBefore
	}
	whole := true
	for i, slot := range target {
		f := p.m.fit(slot.fit.node, ranks[i].demand, all, noneFreed)
		whole = whole && f.ok
		p.holds = append(p.holds, planHold{from: start, fit: f, by: c.execution.ID, notBefore: true})
	}
	wait.node = target[0].fit.node
	held := "what it needs on " + wait.node + " is held for it"
	if !whole {
		held = "it is to run on " + wait.node + ", where what of it is free now is held for it"
	}
	wait.message += "; " + held + ": only work expected to end by " + formatInstant(start, zone) + " may use it"
	if !known {
		return planPlacement{wait: wait}, time.Time{}
	}
	return planPlacement{wait: wait}, start
}

// staleDeadline: a start_by or finish_by further past than this (a rerun
// days later of a task that declared one) neither orders nor is reported.
const staleDeadline = 12 * time.Hour

// deadline is a start_by or finish_by of c, unless it is stale.
func (c *planCandidate) deadline(stamp time.Time, value *string) (time.Time, bool) {
	t, ok := instant.Parse(deref(value))
	if !ok || stamp.Sub(t) > staleDeadline {
		return time.Time{}, false
	}
	return t, true
}

// mustStartBy is the latest c may start to keep its start_by and its
// finish_by (less its estimated run time, when there is one); ok is false
// when it has neither (or only stale ones).
func (c *planCandidate) mustStartBy(stamp time.Time) (time.Time, bool) {
	at, ok := c.deadline(stamp, c.execution.StartBy)
	if finishBy, has := c.deadline(stamp, c.execution.FinishBy); has {
		finishBy = finishBy.Add(-c.endsIn)
		if !ok || finishBy.Before(at) {
			at, ok = finishBy, true
		}
	}
	return at, ok
}

// deadlineNote is what a wait of c says of its start_by and finish_by: that
// one has passed, or that it is expected to be missed (start: when c is
// expected to start, zero when not known).
func (p *planner) deadlineNote(c *planCandidate, start time.Time) string {
	var notes []string
	if startBy, ok := c.deadline(p.m.stamp, c.execution.StartBy); ok {
		switch {
		case p.m.stamp.After(startBy):
			notes = append(notes, "its start_by "+formatInstant(startBy, startBy.Location())+" has passed")
		case !start.IsZero() && start.After(startBy):
			notes = append(notes, "expected to start after its start_by "+formatInstant(startBy, startBy.Location()))
		}
	}
	if finishBy, ok := c.deadline(p.m.stamp, c.execution.FinishBy); ok {
		begin := start
		if begin.IsZero() {
			begin = p.m.stamp
		}
		switch {
		case p.m.stamp.After(finishBy):
			notes = append(notes, "its finish_by "+formatInstant(finishBy, finishBy.Location())+" has passed")
		case c.endsIn > 0 && begin.Add(c.endsIn).After(finishBy):
			notes = append(notes, "expected to end after its finish_by "+formatInstant(finishBy, finishBy.Location())+" by estimated run times")
		}
	}
	if len(notes) == 0 {
		return ""
	}
	return "; " + strings.Join(notes, "; ")
}

// formatInstant is a time as a wait shows it, in zone (the one it was
// declared with).
func formatInstant(t time.Time, zone *time.Location) string {
	return t.In(zone).Format("2006-01-02 15:04 -07:00")
}

// deref is *s, or "" for nil.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// planHold is what a waiting execution holds where it is going.
type planHold struct {
	// from is when it is expected to start there (the pass's stamp when no
	// estimate says): an execution expected to end by then may use the hold.
	from time.Time
	fit  planFit
	by   string
	// notBefore: held for an execution waiting for its not_before (notYet).
	notBefore bool
}

// overlayFor is what the pass has handed out as c sees it: every placement,
// and every hold unless c is expected to end by the time the hold starts
// (c nil, or without an estimate: every hold).
func (p *planner) overlayFor(c *planCandidate) planOverlay {
	return p.overlayHolding(c, func(planHold) bool { return true })
}

// overlayHolding is overlayFor(c) with only the holds keep accepts.
func (p *planner) overlayHolding(c *planCandidate, keep func(planHold) bool) planOverlay {
	if len(p.holds) == 0 {
		return p.ov
	}
	var end time.Time
	if c != nil && c.endsIn > 0 {
		end = p.m.stamp.Add(c.endsIn)
	}
	ov := p.ov.clone()
	for _, h := range p.holds {
		if keep(h) && (end.IsZero() || end.After(h.from)) {
			ov.take(h.fit, h.by)
		}
	}
	return ov
}

// onlyNotBeforeInTheWay reports whether c would fit now but for what is held
// for executions waiting for their not_before.
func (p *planner) onlyNotBeforeInTheWay(c *planCandidate, ranks []planRank, options [][]planExecutor) bool {
	held := false
	for _, h := range p.holds {
		held = held || h.notBefore
	}
	if !held {
		return false
	}
	ov := p.overlayHolding(c, func(h planHold) bool { return !h.notBefore })
	_, ok := p.assign(ranks, options, ov, noneFreed)
	return ok
}

// startAt is when ranks could start on target if what runs there ends as
// estimated: per rank, when enough of the leases in its way will have ended
// for it to fit beside what ov hands out, and the latest of those. known is
// false when the estimates cannot tell (a lease in the way has none, or all
// the leases with one ending is not enough).
func (p *planner) startAt(ranks []planRank, target []planSlot, ov planOverlay) (time.Time, bool) {
	start := p.m.stamp
	for i, slot := range target {
		node := map[string]bool{slot.fit.node: true}
		var inTheWay []string
		for _, id := range p.m.leaseIDs {
			if p.reaches(p.m.leases[id], node) {
				inTheWay = append(inTheWay, id)
			}
		}
		sort.SliceStable(inTheWay, func(a, b int) bool {
			ea, ka := p.m.leaseEnds[inTheWay[a]]
			eb, kb := p.m.leaseEnds[inTheWay[b]]
			if ka != kb {
				return ka
			}
			return ea.Before(eb)
		})
		gone := map[string]bool{}
		freed := func(id string) bool { return gone[id] }
		at, fits := p.m.stamp, p.m.fit(slot.fit.node, ranks[i].demand, ov, freed).ok
		for _, id := range inTheWay {
			if fits {
				break
			}
			end, known := p.m.leaseEnds[id]
			if !known {
				break
			}
			gone[id] = true
			at = end
			fits = p.m.fit(slot.fit.node, ranks[i].demand, ov, freed).ok
		}
		if !fits {
			return time.Time{}, false
		}
		if at.After(start) {
			start = at
		}
	}
	return start, true
}

// formatAbout is a rough duration: minutes under an hour, else hours and
// minutes.
func formatAbout(d time.Duration) string {
	minutes := int(d.Round(time.Minute) / time.Minute)
	if minutes < 60 {
		return fmt.Sprintf("%d min", max(1, minutes))
	}
	return fmt.Sprintf("%dh%02dm", minutes/60, minutes%60)
}

func rankPrefix(c *planCandidate, r planRank) string {
	if c.gangID == "" {
		return ""
	}
	return fmt.Sprintf("rank %d: ", r.rank)
}

// assign puts every rank on a distinct node (all or nothing), each on the
// best node left for it, trying the most constrained rank first.
func (p *planner) assign(ranks []planRank, options [][]planExecutor, ov planOverlay, freed func(string) bool) ([]planSlot, bool) {
	order := make([]int, len(ranks))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return len(options[order[a]]) < len(options[order[b]]) })
	slots := make([]planSlot, len(ranks))
	used := map[string]bool{}
	var walk func(int, planOverlay) bool
	walk = func(depth int, ov planOverlay) bool {
		if depth == len(order) {
			return true
		}
		i := order[depth]
		var fits []planSlot
		for _, e := range options[i] {
			if used[e.nodeID] {
				continue
			}
			if f := p.m.fit(e.nodeID, ranks[i].demand, ov, freed); f.ok {
				fits = append(fits, planSlot{executor: e, rank: ranks[i].rank, fit: f})
			}
		}
		sort.SliceStable(fits, func(a, b int) bool { return p.m.better(fits[a].fit, fits[b].fit, ov) })
		for _, s := range fits {
			next := ov.clone()
			// Taken for a rank of this placement: the other ranks must see
			// its devices as held (a GPU shared by two nodes is one device,
			// and an empty holder would not count as one).
			next.take(s.fit, assignedRank)
			used[s.executor.nodeID] = true
			slots[i] = s
			if walk(depth+1, next) {
				return true
			}
			delete(used, s.executor.nodeID)
		}
		return false
	}
	return slots, walk(0, ov)
}

func (p *planner) commit(c *planCandidate, slots []planSlot) {
	for _, s := range slots {
		p.ov.take(s.fit, c.execution.ID)
		for _, d := range s.fit.devices {
			_ = d
			p.account(c.execution.ProjectScopeID, "gpu", 1)
		}
		for _, cc := range s.fit.capacity {
			p.account(c.execution.ProjectScopeID, cc.key.kind, float64(cc.quantity))
		}
	}
}

// makeRoom finds the fewest lower-priority cooperative leases whose suspension
// lets every rank of c fit on its target node.
func (p *planner) makeRoom(c *planCandidate, ranks []planRank, target []planSlot, ov planOverlay) ([]*planLease, []planSlot, bool) {
	pool := p.victimPool(c, target)
	sort.SliceStable(pool, func(a, b int) bool {
		x, y := pool[a], pool[b]
		if x.suspending != y.suspending {
			return x.suspending // already going: costs nothing
		}
		if x.priority != y.priority {
			return x.priority < y.priority
		}
		if x.gang != y.gang {
			return !x.gang // a gang victim stops every rank
		}
		if x.startedAt != y.startedAt {
			return x.startedAt > y.startedAt
		}
		return x.id < y.id
	})
	needs := roomNeeds(ranks)
	fits := func(chosen map[string]bool) ([]planSlot, bool) {
		freed := func(leaseID string) bool { return chosen[leaseID] }
		slots := make([]planSlot, len(target))
		for i, s := range target {
			f := p.m.fit(s.fit.node, needs[i], ov, freed)
			if !f.ok {
				return nil, false
			}
			slots[i] = planSlot{executor: s.executor, rank: s.rank, fit: f}
		}
		return slots, true
	}
	chosen := map[string]bool{}
	var picked []*planLease
	slots, ok := fits(chosen)
	for _, l := range pool {
		if ok {
			break
		}
		chosen[l.id] = true
		picked = append(picked, l)
		slots, ok = fits(chosen)
	}
	if !ok {
		return nil, nil, false
	}
	// Drop any pick the others make unnecessary.
	for i := 0; i < len(picked); i++ {
		delete(chosen, picked[i].id)
		if s, still := fits(chosen); still {
			slots = s
			picked = append(picked[:i], picked[i+1:]...)
			i--
			continue
		}
		chosen[picked[i].id] = true
	}
	return picked, slots, true
}

// victimPool lists the leases c may suspend to go to target: of lower
// priority, cooperative (or already suspending), holding something there,
// and not chosen for another execution of this pass.
func (p *planner) victimPool(c *planCandidate, target []planSlot) []*planLease {
	var pool []*planLease
	nodes := map[string]bool{}
	for _, s := range target {
		nodes[s.fit.node] = true
	}
	for _, id := range p.m.leaseIDs {
		l := p.m.leases[id]
		if p.victims[id] || l.priority >= c.execution.Priority || !(l.victim || l.suspending) || !p.reaches(l, nodes) {
			continue
		}
		pool = append(pool, l)
	}
	return pool
}

// roomNeeds is what victims are chosen for: GPUs and RAM only; CPU and disk
// the waiting execution waits for, or borrows, as without preemption.
func roomNeeds(ranks []planRank) []planDemand {
	needs := make([]planDemand, len(ranks))
	for i, r := range ranks {
		needs[i] = planDemand{priority: r.demand.priority, gpus: r.demand.gpus}
		for _, c := range r.demand.capacity {
			if c.kind == "ram" {
				needs[i].capacity = append(needs[i].capacity, c)
			}
		}
	}
	return needs
}

// stillShort says, for c that may suspend leases to go to target, what would
// still be short there were all of them suspended (a GPU it could have
// coming free, but not the RAM others hold, say): suspending them would not
// make room, and so none is. It is empty when that would make room, or c may
// suspend nothing (what is in its way is then the reason).
func (p *planner) stillShort(c *planCandidate, ranks []planRank, target []planSlot, ov planOverlay) planWait {
	pool := p.victimPool(c, target)
	if len(pool) == 0 {
		return planWait{}
	}
	all := map[string]bool{}
	labels := make([]string, 0, 4)
	for i, l := range pool {
		all[l.id] = true
		switch {
		case i < 3:
			labels = append(labels, p.m.leaseLabel(l.id))
		case i == 3:
			labels = append(labels, fmt.Sprintf("%d more", len(pool)-3))
		}
	}
	needs := roomNeeds(ranks)
	for i, s := range target {
		if f := p.m.fit(s.fit.node, needs[i], ov, func(leaseID string) bool { return all[leaseID] }); !f.ok {
			return planWait{reason: f.reason, node: f.node, message: rankPrefix(c, ranks[i]) + f.message + " even were " + strings.Join(labels, ", ") + " suspended"}
		}
	}
	return planWait{}
}

// reaches reports whether lease l holds anything on one of nodes.
func (p *planner) reaches(l *planLease, nodes map[string]bool) bool {
	if nodes[l.nodeID] {
		return true
	}
	for node := range nodes {
		for _, g := range p.m.gpus[node] {
			if slices.Contains(l.devices, g.device) {
				return true
			}
		}
	}
	return false
}

// loadPlanCandidates returns every waiting execution that may be admitted
// now (its scope chain open, no live lease; a gang by its leader).
//
// It costs a fixed number of queries whatever the number of candidates (one
// round trip each, which matters with the database across a network): the
// candidates with their gangs, the members of those gangs, and the resource
// requests of all of them.
func loadPlanCandidates(ctx context.Context, tx *sql.Tx, m *planModel) ([]*planCandidate, error) {
	var candidates []*planCandidate
	var gangIDs []string
	err := eachRow(ctx, tx, `SELECT `+candidateColumns+`,COALESCE(gm.gang_id,'')
		FROM execution_requests e LEFT JOIN gang_members gm ON gm.execution_id=e.id
		WHERE e.state='waiting'
		AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.execution_id=e.id AND l.state!='released')
		AND NOT EXISTS (
			SELECT 1 FROM admission_gates g
			WHERE g.scope_id IN(e.project_scope_id,e.queue_scope_id,e.task_scope_id) AND g.state!='open'
		)
		AND (gm.rank IS NULL OR gm.rank=0)
		ORDER BY e.priority DESC,e.submitted_at,e.id`, nil, func(rows *sql.Rows) error {
		c := &planCandidate{}
		if err := scanCandidate(rows, &c.executionCandidate, &c.gangID); err != nil {
			return err
		}
		if c.gangID != "" {
			gangIDs = append(gangIDs, c.gangID)
		}
		candidates = append(candidates, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	members := map[string][]gangMember{}
	if len(gangIDs) > 0 {
		err = eachRow(ctx, tx, `SELECT `+candidateColumns+`,gm.gang_id,gm.rank FROM gang_members gm JOIN execution_requests e ON e.id=gm.execution_id
			WHERE gm.gang_id=ANY($1::text[]) ORDER BY gm.gang_id,gm.rank`, []any{gangIDs}, func(rows *sql.Rows) error {
			var member gangMember
			var gangID string
			if err := scanCandidate(rows, &member.candidate, &gangID, &member.rank); err != nil {
				return err
			}
			members[gangID] = append(members[gangID], member)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.execution.ID)
	}
	if err = loadInputsWaits(ctx, tx, candidates, ids); err != nil {
		return nil, err
	}
	for _, gang := range members {
		for _, member := range gang {
			ids = append(ids, member.candidate.execution.ID)
		}
	}
	demands, err := loadDemandsTx(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	var scopes []string
	for _, c := range candidates {
		scopes = append(scopes, c.execution.ProjectScopeID)
		if c.execution.TaskScopeID != nil {
			scopes = append(scopes, *c.execution.TaskScopeID)
		}
	}
	if err = m.loadLabels(ctx, tx, scopes); err != nil {
		return nil, err
	}
	for _, c := range candidates {
		task := c.execution.ProjectScopeID
		if c.execution.TaskScopeID != nil {
			task = *c.execution.TaskScopeID
		}
		c.label = m.labels[c.execution.ProjectScopeID] + "/" + m.labels[task]
		c.demand = demands[c.execution.ID]
		c.demand.priority = c.execution.Priority
		for _, member := range members[c.gangID] {
			demand := demands[member.candidate.execution.ID]
			demand.priority = c.execution.Priority
			c.ranks = append(c.ranks, planRank{rank: member.rank, candidate: member.candidate, demand: demand})
		}
	}
	return candidates, nil
}

// loadDemandsTx returns the demand of each execution (demandOf), in one
// query.
func loadDemandsTx(ctx context.Context, tx *sql.Tx, executionIDs []string) (map[string]planDemand, error) {
	requests := map[string][]requestRow{}
	err := eachRow(ctx, tx, `SELECT execution_id,id,request_type,kind,quantity,filesystem,constraints_json,policy_json FROM resource_requests
		WHERE execution_id=ANY($1::text[]) ORDER BY execution_id,request_type DESC,id`, []any{executionIDs}, func(rows *sql.Rows) error {
		var executionID string
		var r requestRow
		if err := rows.Scan(&executionID, &r.ID, &r.Type, &r.Kind, &r.Quantity, &r.Filesystem, &r.Constraints, &r.Policy); err != nil {
			return err
		}
		requests[executionID] = append(requests[executionID], r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	demands := make(map[string]planDemand, len(executionIDs))
	for _, id := range executionIDs {
		demands[id] = demandOf(requests[id])
	}
	return demands, nil
}

// demandOf groups an execution's resource requests (in loadResourceRequestsTx
// order): exclusive requests one by one, capacity requests summed per kind
// and filesystem.
func demandOf(requests []requestRow) planDemand {
	var d planDemand
	groups := map[string]int{}
	for _, r := range requests {
		if r.Type == "exclusive" {
			g := gpuDemand{count: r.Quantity}
			_ = json.Unmarshal([]byte(r.Constraints), &g.constraints)
			_ = json.Unmarshal([]byte(r.Policy), &g.policy)
			d.gpus = append(d.gpus, g)
			continue
		}
		var constraints map[string]int64
		_ = json.Unmarshal([]byte(r.Constraints), &constraints)
		key := r.Kind + "\x00" + r.Filesystem
		if i, ok := groups[key]; ok {
			d.capacity[i].quantity += r.Quantity
			d.capacity[i].minFreeAfter = max(d.capacity[i].minFreeAfter, constraints["min_free_after_bytes"])
			continue
		}
		groups[key] = len(d.capacity)
		d.capacity = append(d.capacity, capDemand{kind: r.Kind, filesystem: r.Filesystem, quantity: r.Quantity, minFreeAfter: constraints["min_free_after_bytes"]})
	}
	sort.SliceStable(d.capacity, func(i, j int) bool {
		if d.capacity[i].kind != d.capacity[j].kind {
			return d.capacity[i].kind < d.capacity[j].kind
		}
		return d.capacity[i].filesystem < d.capacity[j].filesystem
	})
	return d
}

// waitRefresh: a wait whose reason and node stay the same is rewritten (for
// numbers in its message) at most this often.
const waitRefresh = 30 * time.Second

// saveWaitsTx records the pass's wait reasons: a row is rewritten when its
// reason changes (its message at most every waitRefresh) and, after a
// complete pass, dropped for executions that no longer wait.
func saveWaitsTx(ctx context.Context, tx *sql.Tx, waits map[string]planWait, complete bool) error {
	rows, err := tx.QueryContext(ctx, `SELECT execution_id,reason_code,COALESCE(node_id,''),detail_json,updated_at FROM admission_waits`)
	if err != nil {
		return err
	}
	existing := map[string]planWait{}
	fresh := map[string]bool{}
	for rows.Next() {
		var executionID, detail, updated string
		var w planWait
		if err = rows.Scan(&executionID, &w.reason, &w.node, &detail, &updated); err != nil {
			rows.Close()
			return err
		}
		if at, parseErr := time.Parse(time.RFC3339Nano, updated); parseErr == nil && time.Since(at) < waitRefresh {
			fresh[executionID] = true
		}
		var d map[string]string
		_ = json.Unmarshal([]byte(detail), &d)
		w.message = d["message"]
		existing[executionID] = w
	}
	if err = rows.Close(); err != nil {
		return err
	}
	stamp := now()
	for executionID := range existing {
		if _, still := waits[executionID]; complete && !still {
			if _, err = tx.ExecContext(ctx, `DELETE FROM admission_waits WHERE execution_id=$1`, executionID); err != nil {
				return err
			}
		}
	}
	for executionID, w := range waits {
		if old, ok := existing[executionID]; ok && (old == w || fresh[executionID] && old.reason == w.reason && old.node == w.node) {
			continue
		}
		detail, _ := json.Marshal(map[string]string{"message": w.message})
		var node any
		if w.node != "" {
			node = w.node
		}
		since := stamp
		if old, ok := existing[executionID]; ok && old.reason == w.reason {
			if err = tx.QueryRowContext(ctx, `SELECT since FROM admission_waits WHERE execution_id=$1`, executionID).Scan(&since); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO admission_waits(execution_id,reason_code,node_id,detail_json,since,updated_at) VALUES($1,$2,$3,$4,$5,$6)
			ON CONFLICT(execution_id) DO UPDATE SET reason_code=excluded.reason_code,node_id=excluded.node_id,detail_json=excluded.detail_json,since=excluded.since,updated_at=excluded.updated_at`,
			executionID, w.reason, node, string(detail), since, stamp); err != nil {
			return err
		}
	}
	return nil
}

// saveNodeAlertsTx keeps node_alerts in step with the low filesystems the
// pass found, with an event when one starts or ends.
func saveNodeAlertsTx(ctx context.Context, tx *sql.Tx, m *planModel) error {
	current := map[[2]string]string{}
	for node, messages := range m.lowDisk {
		for _, message := range messages {
			filesystem, _, _ := strings.Cut(message, " has ")
			current[[2]string{node, filesystem}] = message
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT node_id,subject FROM node_alerts WHERE kind='disk_low_watermark'`)
	if err != nil {
		return err
	}
	existing := map[[2]string]bool{}
	for rows.Next() {
		var k [2]string
		if err = rows.Scan(&k[0], &k[1]); err != nil {
			rows.Close()
			return err
		}
		existing[k] = true
	}
	if err = rows.Close(); err != nil {
		return err
	}
	stamp := now()
	for k := range existing {
		if _, still := current[k]; still {
			continue
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM node_alerts WHERE node_id=$1 AND kind='disk_low_watermark' AND subject=$2`, k[0], k[1]); err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "disk_watermark_cleared", nil, "node", k[0], nil, map[string]string{"filesystem": k[1]}); err != nil {
			return err
		}
	}
	for k, message := range current {
		if existing[k] {
			continue
		}
		detail, _ := json.Marshal(map[string]string{"message": message})
		if _, err = tx.ExecContext(ctx, `INSERT INTO node_alerts(node_id,kind,subject,detail_json,since) VALUES($1,'disk_low_watermark',$2,$3,$4)`, k[0], k[1], string(detail), stamp); err != nil {
			return err
		}
		if err = appendCoordinationEventTx(ctx, tx, "disk_low_watermark", nil, "node", k[0], nil, map[string]string{"filesystem": k[1], "message": message}); err != nil {
			return err
		}
	}
	return nil
}

// RecordAttemptUsage stores what an attempt's process tree uses now. A nil
// field is not known.
func (s *Store) RecordAttemptUsage(ctx context.Context, attemptID string, ramBytes, diskWrittenBytes *int64) error {
	result, err := s.exec(ctx, `INSERT INTO attempt_usage(attempt_id,ram_bytes,disk_written_bytes,observed_at) SELECT id,$1,$2,$3 FROM attempts WHERE id=$4
		ON CONFLICT(attempt_id) DO UPDATE SET ram_bytes=excluded.ram_bytes,disk_written_bytes=excluded.disk_written_bytes,observed_at=excluded.observed_at`,
		ramBytes, diskWrittenBytes, now(), attemptID)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// AdmissionWait is why a waiting execution has not been admitted yet.
type AdmissionWait struct {
	ExecutionID string  `json:"execution_id"`
	Reason      string  `json:"reason"`
	NodeID      *string `json:"node_id,omitempty"`
	Message     string  `json:"message"`
	Since       string  `json:"since"`
}

// AdmissionWaits returns the recorded wait of each of executionIDs that has
// one.
func (s *Store) AdmissionWaits(ctx context.Context, executionIDs []string) (map[string]AdmissionWait, error) {
	waits := map[string]AdmissionWait{}
	err := eachRow(ctx, s.db, `SELECT execution_id,reason_code,node_id,detail_json,since FROM admission_waits WHERE execution_id=ANY($1::text[])`, []any{executionIDs}, func(rows *sql.Rows) error {
		var w AdmissionWait
		var detail string
		if err := rows.Scan(&w.ExecutionID, &w.Reason, &w.NodeID, &detail, &w.Since); err != nil {
			return err
		}
		var d map[string]string
		_ = json.Unmarshal([]byte(detail), &d)
		w.Message = d["message"]
		waits[w.ExecutionID] = w
		return nil
	})
	return waits, err
}

func formatBytes(n int64) string {
	return fmt.Sprintf("%.1f GiB", float64(n)/float64(1<<30))
}

func formatMillis(n int64) string {
	return fmt.Sprintf("%.1f", float64(n)/1000)
}

func unique(values []string) []string {
	seen := map[string]bool{}
	out := values[:0]
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
