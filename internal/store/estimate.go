package store

// Run-time estimates for backfill (planner.go). How long a run takes in all
// (a Task's executions together, or a direct execution) is estimated from
// the runs like it that succeeded before: the median of the latest
// runHistoryDepth that share its most specific key, falling back to coarser
// keys:
//
//   - project, queue, program, subcommand or script, config file (digits masked)
//   - project, queue, program, subcommand or script
//   - project, queue, Task name (digits and hashes masked)
//   - project, queue
//   - project
//
// A run past its estimate is expected to take as long again as it has run
// so far.
//
// The RAM a running lease is expected to take is likewise read from the runs
// like it that succeeded: the most its attempts used (attempt_usage) in the
// latest runHistoryDepth runs of its most specific key with at least
// ramHistoryMin of them, among the first three keys only (a queue or a
// project mixes work of very different sizes), times 1.5 plus 512 MiB
// (samples are 15 s apart and miss short peaks), never more than it
// reserved (planner.go counts it so). Replayed on the installation's history, half the runs land within
// twice their estimate and long runs are mostly underestimated, which
// backfill tolerates: it only asks whether short work ends before the
// execution it would delay could start, and an estimate corrected as a run
// overruns it costs that execution little.

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// runHistoryDepth is how many of the latest runs per key an estimate is
	// the median of.
	runHistoryDepth = 5
	// runHistoryMaxAge: the history is read again at least this often, and
	// after any attempt exits.
	runHistoryMaxAge = 10 * time.Minute
	// overrunFloor is the least an overrunning run is still expected to take.
	overrunFloor = time.Minute
	// suspendSettle is how long a lease with a live suspend command is
	// expected to keep what it has (suspends measured take seconds to two
	// minutes from the command to the lease's release).
	suspendSettle = 2 * time.Minute
	// ramHistoryMin is how many runs of a key the RAM they took is read
	// from; ramHeadroom is added to the most of them, after half again.
	ramHistoryMin = 3
	ramHeadroom   = 512 << 20
)

// runHistory is the run time of the runs that succeeded, by estimate key,
// and the most RAM their attempts were seen to use.
type runHistory struct {
	runs map[string][]float64 // key -> seconds of the latest runs, oldest first
	ram  map[string][]int64   // key -> bytes of the latest runs that were sampled, oldest first
}

// runHistoryCache keeps the history between passes.
type runHistoryCache struct {
	mu      sync.Mutex
	history *runHistory
	loaded  time.Time
	stale   atomic.Bool
}

// invalidate makes the next pass read the history again (an attempt exited).
func (c *runHistoryCache) invalidate() { c.stale.Store(true) }

// runHistory returns the history, read again when it is stale or old. A
// read that fails leaves the previous history in use, or none: without
// estimates, the planner holds as it did without backfill.
func (s *Store) runHistory(ctx context.Context) *runHistory {
	c := &s.runs
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.history != nil && !c.stale.Load() && time.Since(c.loaded) < runHistoryMaxAge {
		return c.history
	}
	c.stale.Store(false)
	h, err := loadRunHistory(ctx, s.db)
	if err != nil {
		c.stale.Store(true)
		return c.history
	}
	c.history, c.loaded = h, time.Now()
	return h
}

// loadRunHistory reads every run whose last execution exited 0 without a
// signal: what its attempts ran in all (gang ranks other than 0 not
// counted twice) and the keys of its last execution, in the order the runs
// ended.
func loadRunHistory(ctx context.Context, db *sql.DB) (*runHistory, error) {
	h := &runHistory{runs: map[string][]float64{}, ram: map[string][]int64{}}
	err := eachRow(ctx, db, `WITH runs AS MATERIALIZED (
			SELECT COALESCE(e.task_scope_id,e.id) AS unit,
				SUM(EXTRACT(EPOCH FROM (a.exited_at::timestamptz-a.started_at::timestamptz))) AS seconds,
				MAX(a.exited_at) AS ended,MAX(u.ram_bytes) AS ram
			FROM execution_requests e JOIN attempts a ON a.execution_id=e.id LEFT JOIN attempt_usage u ON u.attempt_id=a.id
			WHERE a.started_at IS NOT NULL AND a.exited_at IS NOT NULL
			AND NOT EXISTS(SELECT 1 FROM gang_members gm WHERE gm.execution_id=e.id AND gm.rank>0)
			GROUP BY 1
		), last AS MATERIALIZED (
			SELECT DISTINCT ON (COALESCE(e.task_scope_id,e.id)) COALESCE(e.task_scope_id,e.id) AS unit,e.id,e.project_scope_id,
				COALESCE(e.queue_scope_id,'') AS queue,e.task_scope_id AS task,e.argv_json,e.state,e.terminal_cause
			FROM execution_requests e
			ORDER BY COALESCE(e.task_scope_id,e.id),e.submitted_at DESC,e.id DESC
		)
		SELECT l.project_scope_id,l.queue,COALESCE(s.external_key,''),l.argv_json,r.seconds::double precision,r.ram
		FROM last l JOIN runs r ON r.unit=l.unit JOIN attempts a ON a.execution_id=l.id
		LEFT JOIN coordination_scopes s ON s.id=l.task
		WHERE l.state='terminal' AND l.terminal_cause='process_exit' AND a.exit_code=0 AND a.exit_signal IS NULL AND r.seconds>0
		ORDER BY r.ended,l.unit`, nil, func(rows *sql.Rows) error {
		var project, queue, task, argv string
		var seconds float64
		var ram sql.NullInt64
		if err := rows.Scan(&project, &queue, &task, &argv, &seconds, &ram); err != nil {
			return err
		}
		for _, key := range estimateKeys(project, queue, task, decodeArgv(argv)) {
			runs := append(h.runs[key], seconds)
			if len(runs) > runHistoryDepth {
				runs = runs[1:]
			}
			h.runs[key] = runs
			if ram.Valid && ram.Int64 > 0 {
				peaks := append(h.ram[key], ram.Int64)
				if len(peaks) > runHistoryDepth {
					peaks = peaks[1:]
				}
				h.ram[key] = peaks
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return h, nil
}

// estimate is the run time of a run with keys: the median of the latest runs
// of its most specific key that has any.
func (h *runHistory) estimate(keys []string) (time.Duration, bool) {
	if h == nil {
		return 0, false
	}
	for _, key := range keys {
		if runs := h.runs[key]; len(runs) != 0 {
			return time.Duration(median(runs) * float64(time.Second)), true
		}
	}
	return 0, false
}

// expectedRAM is the RAM a run with keys is expected to take at most: the
// most its like took, of the first of its three most specific keys with
// ramHistoryMin runs sampled, times 1.5 plus ramHeadroom.
func (h *runHistory) expectedRAM(keys []string) (int64, bool) {
	if h == nil {
		return 0, false
	}
	for _, key := range keys[:min(3, len(keys))] {
		if peaks := h.ram[key]; len(peaks) >= ramHistoryMin {
			return slices.Max(peaks)/2*3 + ramHeadroom, true
		}
	}
	return 0, false
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// remaining is what a run estimated to take total is still expected to take
// once it has run done: the rest of its estimate, or, past it, as long again
// as it has run (at least overrunFloor).
func remaining(total, done time.Duration) time.Duration {
	if left := total - done; left > 0 {
		return left
	}
	return max(done, overrunFloor)
}

func decodeArgv(argvJSON string) []string {
	var argv []string
	_ = json.Unmarshal([]byte(argvJSON), &argv)
	return argv
}

// runners are words of a command line that only start what does the work.
var runners = map[string]bool{
	"uv": true, "uv.exe": true, "run": true, "--no-sync": true, "python": true, "python.exe": true, "python3": true, "-m": true,
	"pwsh.exe": true, "pwsh": true, "-noprofile": true, "-command": true, "-c": true, "cmd": true, "/c": true,
	"bash": true, "sh": true, "wsl.exe": true, "--exec": true, "env": true,
}

var (
	hexWord      = regexp.MustCompile(`\b[0-9a-f]{7,40}\b`)
	digitRun     = regexp.MustCompile(`\d+`)
	maskedSuffix = regexp.MustCompile(`-#$`)
	knownExt     = regexp.MustCompile(`\.(exe|py|toml|json|ps1|sh)$`)
	commandSplit = regexp.MustCompile(`[\s;&|]+`)
)

// commandBase is a command-line word without its directory, extension and
// hashes: `scripts\train.py` is `train`, `bench-f8ce6a2` is `bench`.
func commandBase(word string) string {
	word = strings.TrimRight(strings.ReplaceAll(word, `\`, "/"), "/")
	if i := strings.LastIndex(word, "/"); i >= 0 {
		word = word[i+1:]
	}
	word = knownExt.ReplaceAllString(strings.ToLower(word), "")
	word = hexWord.ReplaceAllString(word, "#")
	return maskedSuffix.ReplaceAllString(word, "")
}

// maskNumbers replaces hashes and digit runs with #.
func maskNumbers(text string) string {
	return digitRun.ReplaceAllString(hexWord.ReplaceAllString(strings.ToLower(text), "#"), "#")
}

// estimateKeys are a run's keys, most specific first. A PowerShell -Command
// line is read as the words of its last statement.
func estimateKeys(project, queue, task string, argv []string) []string {
	for _, a := range argv {
		if strings.EqualFold(a, "-command") && len(argv) != 0 {
			var words []string
			for _, w := range commandSplit.Split(argv[len(argv)-1], -1) {
				if w != "" && !strings.HasPrefix(w, "$") {
					words = append(words, strings.Trim(w, `'"`))
				}
			}
			argv = words
			break
		}
	}
	var words []string
	for _, a := range argv {
		lower := strings.ToLower(a)
		if runners[lower] || strings.HasSuffix(lower, "uv") || strings.HasSuffix(lower, "uv.exe") || strings.HasSuffix(lower, "pwsh.exe") {
			continue
		}
		words = append(words, a)
	}
	program, sub, config := "", "", ""
	if len(words) != 0 {
		program = commandBase(words[0])
	}
	for i := 1; i < len(words); i++ {
		w := words[i]
		if strings.HasPrefix(w, "-") {
			if (w == "--config" || w == "-c") && i+1 < len(words) {
				config = maskNumbers(commandBase(words[i+1]))
			}
			continue
		}
		if sub == "" {
			sub = commandBase(w)
		}
		if config == "" && (strings.HasSuffix(w, ".toml") || strings.HasSuffix(w, ".json") || strings.HasSuffix(w, ".yaml")) {
			config = maskNumbers(commandBase(w))
		}
	}
	join := func(parts ...string) string { return strings.Join(parts, "\x00") }
	return []string{
		join("4", project, queue, program, sub, config),
		join("3", project, queue, program, sub),
		join("2", project, queue, maskNumbers(task)),
		join("1", project, queue),
		join("0", project),
	}
}

// planRun is what the planner needs to estimate one run: its keys and what
// it has run so far.
type planRun struct {
	unit string // the Task scope, or the execution for a direct one
	keys []string
}

// loadEstimates sets the estimated end of every live lease (m.leaseEnds),
// the RAM it is expected to take (ramExpected), and the estimated run time
// left of every candidate (endsIn), from history and from what each run's
// earlier attempts ran, read in one query.
func loadEstimates(ctx context.Context, tx *sql.Tx, m *planModel, candidates []*planCandidate, history *runHistory) error {
	m.leaseEnds = map[string]time.Time{}
	if history == nil {
		return nil
	}
	runOf := func(projectID string, queueID, taskID *string, executionID, argvJSON string) planRun {
		queue, unit, task := "", executionID, ""
		if queueID != nil {
			queue = *queueID
		}
		if taskID != nil {
			unit, task = *taskID, m.labels[*taskID]
		}
		return planRun{unit: unit, keys: estimateKeys(projectID, queue, task, decodeArgv(argvJSON))}
	}
	var units []string
	leaseRuns := map[string]planRun{}
	for _, id := range m.leaseIDs {
		l := m.leases[id]
		r := runOf(l.projectID, l.queueID, l.taskID, l.executionID, l.argvJSON)
		leaseRuns[id] = r
		units = append(units, r.unit)
		if ram, known := history.expectedRAM(r.keys); known {
			l.ramExpected = ram
		}
	}
	candidateRuns := make([]planRun, len(candidates))
	for i, c := range candidates {
		candidateRuns[i] = runOf(c.execution.ProjectScopeID, c.execution.QueueScopeID, c.execution.TaskScopeID, c.execution.ID, c.argvJSON)
		units = append(units, candidateRuns[i].unit)
	}
	if len(units) == 0 {
		return nil
	}
	// What each run's finished attempts ran.
	done := map[string]time.Duration{}
	err := eachRow(ctx, tx, `SELECT COALESCE(e.task_scope_id,e.id),SUM(EXTRACT(EPOCH FROM (a.exited_at::timestamptz-a.started_at::timestamptz)))::double precision
		FROM execution_requests e JOIN attempts a ON a.execution_id=e.id
		WHERE (e.task_scope_id=ANY($1::text[]) OR e.id=ANY($1::text[])) AND a.started_at IS NOT NULL AND a.exited_at IS NOT NULL
		AND NOT EXISTS(SELECT 1 FROM gang_members gm WHERE gm.execution_id=e.id AND gm.rank>0)
		GROUP BY 1`, []any{units}, func(rows *sql.Rows) error {
		var unit string
		var seconds float64
		if err := rows.Scan(&unit, &seconds); err != nil {
			return err
		}
		done[unit] = time.Duration(max(0, seconds) * float64(time.Second))
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range m.leaseIDs {
		l := m.leases[id]
		if l.suspending {
			m.leaseEnds[id] = m.stamp.Add(suspendSettle)
			continue
		}
		if l.attemptExited {
			// Exited, still holding what it left running: no telling when.
			continue
		}
		r := leaseRuns[id]
		total, known := history.estimate(r.keys)
		if !known {
			continue
		}
		ran := done[r.unit]
		if started, err := time.Parse(time.RFC3339Nano, l.attemptStarted); err == nil && l.attemptStarted != "" {
			ran += max(0, m.stamp.Sub(started))
		}
		m.leaseEnds[id] = m.stamp.Add(remaining(total, ran))
	}
	for i, c := range candidates {
		r := candidateRuns[i]
		if total, known := history.estimate(r.keys); known {
			c.endsIn = remaining(total, done[r.unit])
		}
	}
	return nil
}
