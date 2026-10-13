package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
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
	// progressEvery, when set, also prints the progress of a running task
	// when it changed, at most this often.
	progressEvery time.Duration
}

func newTracker() *tracker { return &tracker{seen: map[string]*observed{}} }

// updateAll sorts the tasks by name and updates each.
func (tr *tracker) updateAll(tasks []taskStatus, now time.Time) {
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })
	for _, t := range tasks {
		tr.update(t, now)
	}
}

// update prints the first sight of t, every change of state (or of
// execution) and, with progressEvery, its progress.
func (tr *tracker) update(t taskStatus, now time.Time) {
	ordinal := -1 // no execution yet
	if e := t.latest(); e != nil {
		ordinal = e.Ordinal
	}
	note := taskNote(t)
	progress := progressOf(t)
	previous, ok := tr.seen[t.Name]
	if !ok {
		stdoutLine("%s %s %s", clock(now), t.Name, withNote(t.State, note))
		tr.seen[t.Name] = &observed{state: t.State, ordinal: ordinal, progress: progress, progressAt: now}
		return
	}
	if previous.state != t.State || previous.ordinal != ordinal {
		if previous.ordinal != ordinal && previous.ordinal >= 0 {
			// A new execution (a rerun or retry), not the first one.
			note = strings.TrimSpace("exec #" + strconv.Itoa(ordinal) + " " + note)
		}
		stdoutLine("%s %s %s -> %s", clock(now), t.Name, previous.state, withNote(t.State, note))
		previous.state, previous.ordinal = t.State, ordinal
	}
	if tr.progressEvery > 0 && progress != "" && progress != previous.progress && now.Sub(previous.progressAt) >= tr.progressEvery {
		stdoutLine("%s %s PROGRESS %s", clock(now), t.Name, progress)
		previous.progress, previous.progressAt = progress, now
	}
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

// poll is the loop of task wait and project watch: every interval it reads
// the project and hands the response to handle, until handle or after ends
// it. A read that fails for a transient reason is retried, with DOWN and UP
// lines around the outage; another failure ends the loop with fatal(err).
// Ctrl+C ends it with interrupted.
type poll struct {
	interval time.Duration
	fetch    func() (*projectResponse, error)
	handle   func(resp *projectResponse, now time.Time) (done bool, err error)
	// after, if set, runs after every poll, failed or not.
	after       func(now time.Time) (done bool, err error)
	fatal       func(err error) error
	interrupted error
}

func (p poll) run(ctx context.Context) error {
	var out outage
	for {
		now := nowFunc()
		resp, err := p.fetch()
		switch {
		case ctx.Err() != nil:
			return p.interrupted
		case err != nil && transient(err):
			out.failed(err, now)
		case err != nil:
			return p.fatal(err)
		default:
			out.ok(now)
			if done, err := p.handle(resp, now); done {
				return err
			}
		}
		if p.after != nil {
			if done, err := p.after(now); done {
				return err
			}
		}
		if !sleepFunc(ctx, p.interval) {
			return p.interrupted
		}
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
	ctx, client, stop, err := newInterruptibleClient(*api)
	if err != nil {
		return withCode(err, waitNotFound)
	}
	defer stop()
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

func (w *waiter) run(ctx context.Context) error {
	start := nowFunc()
	tr := newTracker()
	first := true
	var last []taskStatus
	w.stalls = map[string]*stallState{}
	return poll{
		interval: w.interval,
		fetch: func() (*projectResponse, error) {
			return fetchProject(w.client, w.project, projectQuery{tasks: w.names, match: w.match})
		},
		handle: func(resp *projectResponse, now time.Time) (bool, error) {
			if first {
				if err := w.checkNames(resp); err != nil {
					return true, err
				}
				first = false
			}
			last = resp.Project.Tasks
			tr.updateAll(last, now)
			if w.stall > 0 {
				w.checkStalls(last, now)
			}
			code, done := w.done(last, now)
			return done, exitWith(code)
		},
		after: func(now time.Time) (bool, error) {
			if w.timeout <= 0 || now.Sub(start) < w.timeout {
				return false, nil
			}
			var pending []string
			for _, t := range last {
				if !w.reached(t) {
					pending = append(pending, t.Name+" ("+t.State+")")
				}
			}
			stdoutLine("%s TIMEOUT after %s: %s", clock(now), compactDuration(w.timeout), strings.Join(pending, ", "))
			return true, exitWith(waitTimeout)
		},
		fatal: func(err error) error {
			if notFound(err) {
				return &cliError{code: waitNotFound, msg: fmt.Sprintf("project %s not found (%v)", w.project, err)}
			}
			return &cliError{code: waitNotFound, msg: err.Error()}
		},
		interrupted: &cliError{code: 130},
	}.run(ctx)
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
		e := t.latest()
		if e == nil || e.AttemptID == nil {
			continue
		}
		attemptID := *e.AttemptID
		size, ok := logSize(w.client, attemptID)
		if !ok {
			continue
		}
		s := w.stalls[t.Name]
		if s == nil || s.attemptID != attemptID || size != s.bytes {
			w.stalls[t.Name] = &stallState{attemptID: attemptID, bytes: size, since: now}
			continue
		}
		if !s.reported && now.Sub(s.since) >= w.stall {
			stdoutLine("%s %s STALL no log output for %s", clock(now), t.Name, compactDuration(now.Sub(s.since)))
			s.reported = true
		}
	}
}

// logSize is the size of an attempt's stdout plus stderr, and whether the
// daemon has either log; it reads no log bytes.
func logSize(client *apiClient, attemptID string) (int64, bool) {
	var size int64
	found := false
	for _, stream := range []string{"stdout", "stderr"} {
		if chunk, err := fetchTail(client, attemptID, stream, 0); err == nil {
			size += chunk.size
			found = true
		}
	}
	return size, found
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
	ctx, client, stop, err := newInterruptibleClient(*api)
	if err != nil {
		return err
	}
	defer stop()
	return watchProject(ctx, client, positional[0], *match, *interval, *exitWhenIdle)
}

const (
	progressEvery = time.Minute
	summaryEvery  = 10 * time.Minute
	// sinceMargin: each poll of watch asks for the tasks updated since the
	// latest update it saw (the daemon's clock) less this margin.
	sinceMargin = time.Minute
)

func watchProject(ctx context.Context, client *apiClient, project, match string, interval time.Duration, exitWhenIdle bool) error {
	since := nowFunc().Add(-interval)
	tr := newTracker()
	tr.progressEvery = progressEvery
	var lastSummary time.Time
	first := true
	return poll{
		interval: interval,
		fetch: func() (*projectResponse, error) {
			return fetchLiveAndRecent(client, project, projectQuery{match: match}, since)
		},
		handle: func(resp *projectResponse, now time.Time) (bool, error) {
			tasks := resp.Project.Tasks
			tr.updateAll(tasks, now)
			for _, t := range tasks {
				if updated, ok := parseStamp(&t.UpdatedAt); ok && updated.Add(-sinceMargin).After(since) {
					since = updated.Add(-sinceMargin)
				}
			}
			if first || now.Sub(lastSummary) >= summaryEvery {
				counts := resp.Project.StateCounts
				if counts == nil {
					counts = countStates(tasks)
				}
				stdoutLine("%s SUMMARY %s: %s", clock(now), project, stateSummary(counts))
				lastSummary, first = now, false
			}
			if !exitWhenIdle || slices.ContainsFunc(tasks, func(t taskStatus) bool { return liveState(t.State) }) {
				return false, nil
			}
			var bad []string
			for name, o := range tr.seen {
				if terminalState(o.state) && o.state != "succeeded" {
					bad = append(bad, name+" ("+o.state+")")
				}
			}
			sort.Strings(bad)
			if len(bad) > 0 {
				stdoutLine("%s IDLE not succeeded: %s", clock(now), strings.Join(bad, ", "))
				return true, &cliError{code: 1}
			}
			stdoutLine("%s IDLE nothing pending or running", clock(now))
			return true, nil
		},
		fatal: func(err error) error {
			if notFound(err) {
				return fmt.Errorf("project %s not found (%v)", project, err)
			}
			return err
		},
	}.run(ctx)
}
