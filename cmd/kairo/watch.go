package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// kairo task wait and kairo project watch poll the project status and print
// one line per change ("HH:MM:SS TASK OLD -> NEW note"), so an agent or a
// Monitor tool can follow a run without parsing JSON.

// Exit codes of task wait.
const (
	waitOK       = 0
	waitFailed   = 1
	waitTimeout  = 2
	waitNotFound = 3
)

// observed is what a poller last saw of a task.
type observed struct {
	state      string
	ordinal    int // of the latest execution, -1 without one
	progress   string
	progressAt time.Time
}

// tracker prints the changes between polls.
type tracker struct {
	seen map[string]*observed
}

func newTracker() *tracker { return &tracker{seen: map[string]*observed{}} }

// update prints the first sight of t and every change of state (or of
// execution); it returns the previous observation (nil on first sight).
func (tr *tracker) update(t taskStatus, now time.Time) *observed {
	ordinal := -1 // no execution yet
	if e := t.latest(); e != nil {
		ordinal = e.Ordinal
	}
	note := taskNote(t)
	previous, ok := tr.seen[t.Name]
	if !ok {
		stdoutLine("%s %s %s", clock(now), t.Name, withNote(t.State, note))
		tr.seen[t.Name] = &observed{state: t.State, ordinal: ordinal, progress: progressOf(t), progressAt: now}
		return nil
	}
	before := *previous
	if previous.state != t.State || previous.ordinal != ordinal {
		if previous.ordinal != ordinal && previous.ordinal >= 0 {
			// A new execution (a rerun or retry), not the first one.
			note = strings.TrimSpace("exec #" + strconv.Itoa(ordinal) + " " + note)
		}
		stdoutLine("%s %s %s -> %s", clock(now), t.Name, previous.state, withNote(t.State, note))
		previous.state, previous.ordinal = t.State, ordinal
	}
	return &before
}

func withNote(state, note string) string {
	if note == "" {
		return state
	}
	return state + " " + note
}

// progressOf is the progress text of a running task ("" otherwise).
func progressOf(t taskStatus) string {
	if t.State != "running" {
		return ""
	}
	if e := t.latest(); e != nil {
		return progressText(e.Progress)
	}
	return ""
}

// transient reports whether a poll error is worth retrying (the daemon is
// down, restarting or busy) rather than a mistake of the command.
func transient(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status >= 500 || ae.Status == http.StatusTooManyRequests || ae.Status == http.StatusRequestTimeout
	}
	return !errors.Is(err, errNoToken)
}

// outage prints DOWN once when polls start failing and UP when they work again.
type outage struct{ down bool }

func (o *outage) failed(err error, now time.Time) {
	if !o.down {
		stdoutLine("%s DOWN %v", clock(now), err)
		o.down = true
	}
}

func (o *outage) ok(now time.Time) {
	if o.down {
		stdoutLine("%s UP", clock(now))
		o.down = false
	}
}

// taskWait: kairo task wait PROJECT [QUEUE] TASK [TASK...] | PROJECT --match PREFIX.
func taskWait(args []string) error {
	fs := newFlags("task wait")
	api := apiFlag(fs)
	match := fs.String("match", "", "wait for every task whose name starts with PREFIX (tasks declared later included)")
	until := fs.String("until", "terminal", "terminal (ended) or started (running or ended)")
	timeout := fs.Duration("timeout", 0, "give up after this long, exit 2 (0: never)")
	interval := fs.Duration("interval", 10*time.Second, "time between polls")
	stall := fs.Duration("stall", 0, "report a running task whose logs did not grow for this long (0: off)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return withCode(err, waitNotFound)
	}
	switch {
	case *until != "terminal" && *until != "started":
		return &cliError{code: waitNotFound, msg: fmt.Sprintf("--until %q: want terminal or started", *until)}
	case len(positional) == 0:
		return &cliError{code: waitNotFound, msg: "task wait requires PROJECT [QUEUE] TASK [TASK...] or PROJECT --match PREFIX"}
	case *match == "" && len(positional) < 2:
		return &cliError{code: waitNotFound, msg: "task wait requires PROJECT [QUEUE] TASK [TASK...] or PROJECT --match PREFIX"}
	case *match != "" && len(positional) > 1:
		return &cliError{code: waitNotFound, msg: "task wait takes either TASK names or --match, not both"}
	case *interval <= 0:
		return &cliError{code: waitNotFound, msg: "--interval must be positive"}
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return withCode(err, waitNotFound)
	}
	ctx, stop := interruptContext()
	defer stop()
	client.ctx = ctx
	w := &waiter{client: client, project: positional[0], names: positional[1:], match: *match,
		started: *until == "started", timeout: *timeout, interval: *interval, stall: *stall}
	return w.run(ctx)
}

func withCode(err error, code int) error {
	var ce *cliError
	if errors.As(err, &ce) {
		return &cliError{code: code, msg: ce.msg, raw: ce.raw}
	}
	if errors.Is(err, errHelpShown) {
		return err
	}
	return &cliError{code: code, msg: err.Error()}
}

type waiter struct {
	client   *apiClient
	project  string
	names    []string
	match    string
	started  bool
	timeout  time.Duration
	interval time.Duration
	stall    time.Duration

	stalls map[string]*stallState
}

// stallState follows the log growth of a running task.
type stallState struct {
	attemptID string
	bytes     int64
	since     time.Time
	reported  bool
}

func (w *waiter) query() projectQuery {
	return projectQuery{tasks: w.names, match: w.match}
}

func (w *waiter) run(ctx context.Context) error {
	start := nowFunc()
	tr := newTracker()
	var out outage
	first := true
	var last []taskStatus
	w.stalls = map[string]*stallState{}
	for {
		now := nowFunc()
		resp, err := fetchProject(w.client, w.project, w.query())
		switch {
		case ctx.Err() != nil:
			return &cliError{code: 130, msg: ""}
		case err != nil && notFound(err):
			return &cliError{code: waitNotFound, msg: fmt.Sprintf("project %s not found (%v)", w.project, err)}
		case err != nil && transient(err):
			out.failed(err, now)
		case err != nil:
			return &cliError{code: waitNotFound, msg: err.Error()}
		default:
			out.ok(now)
			if first {
				if err := w.checkNames(resp); err != nil {
					return err
				}
				first = false
			}
			tasks := resp.Project.Tasks
			sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })
			for _, t := range tasks {
				tr.update(t, now)
			}
			last = tasks
			if w.stall > 0 {
				w.checkStalls(tasks, now)
			}
			if code, done := w.done(tasks, now); done {
				return exitWith(code)
			}
		}
		if w.timeout > 0 && now.Sub(start) >= w.timeout {
			var pending []string
			for _, t := range last {
				if !w.reached(t) {
					pending = append(pending, t.Name+" ("+t.State+")")
				}
			}
			stdoutLine("%s TIMEOUT after %s: %s", clock(now), compactDuration(w.timeout), strings.Join(pending, ", "))
			return exitWith(waitTimeout)
		}
		if !sleepFunc(ctx, w.interval) {
			return &cliError{code: 130, msg: ""}
		}
	}
}

func exitWith(code int) error {
	if code == waitOK {
		return nil
	}
	return &cliError{code: code}
}

// checkNames: every named task must exist; a name that is the queue of the
// named tasks (PROJECT QUEUE TASK) is dropped. With --match, something must match.
func (w *waiter) checkNames(resp *projectResponse) error {
	if w.match != "" {
		if len(resp.Project.Tasks) == 0 {
			return &cliError{code: waitNotFound, msg: fmt.Sprintf("no task of project %s matches %q", w.project, w.match)}
		}
		return nil
	}
	found := map[string]bool{}
	queues := map[string]bool{}
	for _, t := range resp.Project.Tasks {
		found[t.Name] = true
		queues[t.Queue] = true
	}
	var names, missing []string
	for i, name := range w.names {
		switch {
		case found[name]:
			names = append(names, name)
		case i == 0 && len(w.names) > 1 && queues[name]:
			// PROJECT QUEUE TASK: the queue is not a task.
		default:
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return &cliError{code: waitNotFound, msg: fmt.Sprintf("task %s not found in project %s", strings.Join(missing, ", "), w.project)}
	}
	w.names = names
	return nil
}

// reached: the task ended, or (--until started) its latest execution has an
// attempt (a running task's execution may still wait for resources).
func (w *waiter) reached(t taskStatus) bool {
	if terminalState(t.State) {
		return true
	}
	if w.started && t.State == "running" {
		e := t.latest()
		return e != nil && e.AttemptID != nil
	}
	return false
}

// done reports whether every task reached the target, and the exit code.
func (w *waiter) done(tasks []taskStatus, now time.Time) (int, bool) {
	var bad []string
	for _, t := range tasks {
		if !w.reached(t) {
			return 0, false
		}
		ok := t.State == "succeeded" || (w.started && t.State == "running")
		if w.started && !ok {
			// Ended after it started (failed or stopped) still counts as started.
			if e := t.latest(); e != nil && e.AttemptID != nil && (t.State == "failed" || t.State == "stopped") {
				ok = true
			}
		}
		if !ok {
			bad = append(bad, withNote(t.Name, "("+t.State+")"))
		}
	}
	if len(bad) > 0 {
		stdoutLine("%s DONE not succeeded: %s", clock(now), strings.Join(bad, ", "))
		return waitFailed, true
	}
	what := "succeeded"
	if w.started {
		what = "started"
	}
	stdoutLine("%s DONE %d task(s) %s", clock(now), len(tasks), what)
	return waitOK, true
}

// checkStalls reports running tasks whose latest attempt's logs did not
// grow for --stall (once per stall).
func (w *waiter) checkStalls(tasks []taskStatus, now time.Time) {
	for _, t := range tasks {
		if t.State != "running" {
			delete(w.stalls, t.Name)
			continue
		}
		detail, _, err := fetchTask(w.client, taskRef{project: w.project, task: t.Name})
		if err != nil || len(detail.Executions) == 0 {
			continue
		}
		latest := detail.Executions[len(detail.Executions)-1]
		attempts := append([]attemptInfo(nil), latest.Attempts...)
		sortAttempts(attempts)
		if len(attempts) == 0 {
			continue
		}
		a := attempts[len(attempts)-1]
		size, ok := a.logBytes()
		if !ok {
			continue
		}
		s := w.stalls[t.Name]
		if s == nil || s.attemptID != a.ID || size != s.bytes {
			w.stalls[t.Name] = &stallState{attemptID: a.ID, bytes: size, since: now}
			continue
		}
		if !s.reported && now.Sub(s.since) >= w.stall {
			stdoutLine("%s %s STALL no log output for %s", clock(now), t.Name, compactDuration(now.Sub(s.since)))
			s.reported = true
		}
	}
}

// projectWatch: kairo project watch PROJECT [--match PREFIX] [--interval D] [--exit-when-idle].
func projectWatch(args []string) error {
	fs := newFlags("project watch")
	api := apiFlag(fs)
	match := fs.String("match", "", "only tasks whose name starts with PREFIX")
	interval := fs.Duration("interval", 10*time.Second, "time between polls")
	exitWhenIdle := fs.Bool("exit-when-idle", false, "exit once no task is pending or running (exit 1 if a task seen ending did not succeed)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("project watch requires PROJECT")
	}
	if *interval <= 0 {
		return usageError("--interval must be positive")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	ctx, stop := interruptContext()
	defer stop()
	client.ctx = ctx
	return watchProject(ctx, client, positional[0], *match, *interval, *exitWhenIdle)
}

const (
	progressEvery = time.Minute
	summaryEvery  = 10 * time.Minute
)

func watchProject(ctx context.Context, client *apiClient, project, match string, interval time.Duration, exitWhenIdle bool) error {
	start := nowFunc()
	since := start.Add(-interval).UTC().Format(time.RFC3339)
	tr := newTracker()
	var out outage
	var lastSummary time.Time
	first := true
	for {
		now := nowFunc()
		live, err := fetchProject(client, project, projectQuery{match: match, live: true})
		var recent *projectResponse
		if err == nil {
			recent, err = fetchProject(client, project, projectQuery{match: match, since: since})
		}
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil && notFound(err):
			return fmt.Errorf("project %s not found (%v)", project, err)
		case err != nil && transient(err):
			out.failed(err, now)
		case err != nil:
			return err
		default:
			out.ok(now)
			idle := len(live.Project.Tasks) == 0
			counts := live.Project.StateCounts
			live.merge(recent)
			tasks := live.Project.Tasks
			sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })
			for _, t := range tasks {
				before := tr.update(t, now)
				o := tr.seen[t.Name]
				p := progressOf(t)
				if before != nil && p != "" && p != o.progress && now.Sub(o.progressAt) >= progressEvery {
					stdoutLine("%s %s PROGRESS %s", clock(now), t.Name, p)
					o.progress, o.progressAt = p, now
				}
			}
			if first || now.Sub(lastSummary) >= summaryEvery {
				if counts == nil {
					counts = countStates(tasks)
				}
				stdoutLine("%s SUMMARY %s: %s", clock(now), project, stateSummary(counts))
				lastSummary, first = now, false
			}
			if exitWhenIdle && idle {
				var bad []string
				for name, o := range tr.seen {
					if terminalState(o.state) && o.state != "succeeded" {
						bad = append(bad, name+" ("+o.state+")")
					}
				}
				sort.Strings(bad)
				if len(bad) > 0 {
					stdoutLine("%s IDLE not succeeded: %s", clock(now), strings.Join(bad, ", "))
					return &cliError{code: 1}
				}
				stdoutLine("%s IDLE nothing pending or running", clock(now))
				return nil
			}
		}
		if !sleepFunc(ctx, interval) {
			return nil
		}
	}
}
