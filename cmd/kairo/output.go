package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"kairo/internal/orchestration"
	"kairo/internal/store"
)

// The project and task views of the API (GET /api/projects/{p} and
// GET /api/projects/{p}/tasks/{t}) as the CLI reads them, and the
// formatting shared by list, show, wait and watch.

type projectStatus struct {
	Name        string         `json:"name"`
	StateCounts map[string]int `json:"state_counts"`
	Tasks       []taskStatus   `json:"tasks"`
}

type taskStatus struct {
	Name       string          `json:"name"`
	Queue      string          `json:"queue"`
	Policy     string          `json:"policy"`
	State      string          `json:"state"`
	SpecDigest string          `json:"spec_digest"`
	DependsOn  []string        `json:"depends_on"`
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
	Result     json.RawMessage `json:"result,omitempty"`
	Executions []taskExecution `json:"executions"`
}

type taskExecution struct {
	Ordinal          int             `json:"ordinal"`
	ExecutionID      string          `json:"execution_id"`
	Reason           string          `json:"reason"`
	State            string          `json:"state"`
	ExitCode         *int            `json:"exit_code"`
	ExitSignal       *string         `json:"exit_signal"`
	TerminalCause    *string         `json:"terminal_cause"`
	AttemptID        *string         `json:"attempt_id"`
	ExecutorID       *string         `json:"executor_id"`
	SubmittedAt      *string         `json:"submitted_at"`
	StartedAt        *string         `json:"started_at"`
	TerminalAt       *string         `json:"terminal_at"`
	AttemptStartedAt *string         `json:"attempt_started_at"`
	ExitedAt         *string         `json:"exited_at"`
	Progress         json.RawMessage `json:"progress"`
	AdmissionWait    *string         `json:"admission_wait"`
	// Task detail only.
	Attempts []attemptInfo        `json:"attempts"` // at most one
	Gang     []store.TaskGangRank `json:"gang"`
	Waiting  []map[string]any     `json:"waiting"`
}

type attemptInfo struct {
	ID              string          `json:"id"`
	State           string          `json:"state"`
	ExecutorID      string          `json:"executor_id"`
	NodeID          string          `json:"node_id"`
	PID             *int            `json:"pid"`
	ExitCode        *int            `json:"exit_code"`
	ExitSignal      *string         `json:"exit_signal"`
	Progress        json.RawMessage `json:"progress"`
	LastHeartbeatAt *string         `json:"last_heartbeat_at"`
	ExitedAt        *string         `json:"exited_at"`
	QuiescedAt      *string         `json:"quiesced_at"`
	StdoutPath      *string         `json:"stdout_path"`
	StderrPath      *string         `json:"stderr_path"`
	StdoutBytes     *int64          `json:"stdout_bytes"`
	StderrBytes     *int64          `json:"stderr_bytes"`
}

// ended reports whether the attempt's process is gone for good.
func (a attemptInfo) ended() bool {
	switch a.State {
	case "exited", "lost", "quiesced":
		return true
	}
	return a.ExitedAt != nil || a.QuiescedAt != nil
}

type taskDetail struct {
	taskStatus
	Spec         *orchestration.Task `json:"spec"`
	Dependencies []store.TaskRef     `json:"dependencies"`
	Dependents   []store.TaskRef     `json:"dependents"`
	Gates        []store.TaskGate    `json:"gates"`
}

// taskResult is the part of a task's result the CLI shows.
type taskResult struct {
	Reason        string  `json:"reason"`
	ExitCode      *int    `json:"exit_code"`
	ExitSignal    *string `json:"exit_signal"`
	TerminalCause *string `json:"terminal_cause"`
}

func (t taskStatus) result() taskResult {
	var r taskResult
	if len(t.Result) > 0 {
		_ = json.Unmarshal(t.Result, &r)
	}
	return r
}

// latest is the execution with the highest ordinal (nil when none).
func (t taskStatus) latest() *taskExecution {
	var best *taskExecution
	for i := range t.Executions {
		if best == nil || t.Executions[i].Ordinal > best.Ordinal {
			best = &t.Executions[i]
		}
	}
	return best
}

// execution is the execution `--attempt which` names: the latest, or #N
// (nil when there is none).
func (t taskStatus) execution(which string) *taskExecution {
	if which == "latest" {
		return t.latest()
	}
	n, err := strconv.Atoi(which)
	if err != nil {
		return nil
	}
	for i := range t.Executions {
		if t.Executions[i].Ordinal == n {
			return &t.Executions[i]
		}
	}
	return nil
}

// attemptID is the execution's attempt, "" before it starts: an execution
// has at most one.
func (e *taskExecution) attemptID() string {
	if len(e.Attempts) > 0 {
		return e.Attempts[0].ID
	}
	if e.AttemptID != nil {
		return *e.AttemptID
	}
	return ""
}

// liveState: a task in it can still run; terminalState: it ended.
func liveState(state string) bool { return state == "pending" || state == "running" }

func terminalState(state string) bool {
	return !liveState(state) && slices.Contains(store.TaskStates, state)
}

// projectQuery is the filter of GET /api/projects/{p}.
type projectQuery struct {
	tasks  []string
	match  string
	states []string
	live   bool
	since  string // RFC3339
	// raw keeps the response as the daemon sent it (task list --json).
	raw bool
}

func (q projectQuery) path(project string) string {
	values := url.Values{"task": q.tasks, "match": {q.match}, "state": {strings.Join(q.states, ",")}, "since": {q.since}}
	if q.live {
		values.Set("live", "1")
	}
	return withQuery("/api/projects/"+url.PathEscape(project), values)
}

// keep applies the filter on the client: a daemon that predates the filters
// returns every task.
func (q projectQuery) keep(t taskStatus) bool {
	if len(q.tasks) > 0 && !slices.Contains(q.tasks, t.Name) {
		return false
	}
	if q.match != "" && !strings.HasPrefix(t.Name, q.match) {
		return false
	}
	if len(q.states) > 0 && !slices.Contains(q.states, t.State) {
		return false
	}
	if q.live && !liveState(t.State) {
		return false
	}
	if since, ok := parseStamp(&q.since); ok {
		if updated, ok := parseStamp(&t.UpdatedAt); !ok || updated.Before(since) {
			return false
		}
	}
	return true
}

// projectResponse is GET /api/projects/{p}: the parsed status and, with
// projectQuery.raw, the body and each task as the daemon sent them.
type projectResponse struct {
	Project  projectStatus
	payload  []byte
	rawTasks map[string]json.RawMessage
}

func fetchProject(client *apiClient, project string, q projectQuery) (*projectResponse, error) {
	payload, err := client.call(http.MethodGet, q.path(project), nil)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Project struct {
			Name        string            `json:"name"`
			StateCounts map[string]int    `json:"state_counts"`
			Tasks       []json.RawMessage `json:"tasks"`
		} `json:"project"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		return nil, fmt.Errorf("decode project status: %w", err)
	}
	resp := &projectResponse{Project: projectStatus{Name: wire.Project.Name, StateCounts: wire.Project.StateCounts}}
	if q.raw {
		resp.payload, resp.rawTasks = payload, map[string]json.RawMessage{}
	}
	// A daemon that counts the states also applies the filters.
	filtered := wire.Project.StateCounts != nil
	for _, raw := range wire.Project.Tasks {
		var t taskStatus
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, fmt.Errorf("decode project status: %w", err)
		}
		if !filtered && !q.keep(t) {
			continue
		}
		resp.Project.Tasks = append(resp.Project.Tasks, t)
		if q.raw {
			resp.rawTasks[t.Name] = raw
		}
	}
	return resp, nil
}

// fetchLiveAndRecent is the tasks of q that are live plus those updated
// since: two calls, made at once, merged.
func fetchLiveAndRecent(client *apiClient, project string, q projectQuery, since time.Time) (*projectResponse, error) {
	liveQuery, recentQuery := q, q
	liveQuery.live = true
	recentQuery.since = since.UTC().Format(time.RFC3339)
	var recent *projectResponse
	var recentErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		recent, recentErr = fetchProject(client, project, recentQuery)
	}()
	live, err := fetchProject(client, project, liveQuery)
	<-done
	if err == nil {
		err = recentErr
	}
	if err != nil {
		return nil, err
	}
	live.merge(recent)
	return live, nil
}

// merge adds the tasks of other not already in r.
func (r *projectResponse) merge(other *projectResponse) {
	seen := map[string]bool{}
	for _, t := range r.Project.Tasks {
		seen[t.Name] = true
	}
	for _, t := range other.Project.Tasks {
		if !seen[t.Name] {
			r.Project.Tasks = append(r.Project.Tasks, t)
			if r.rawTasks != nil {
				r.rawTasks[t.Name] = other.rawTasks[t.Name]
			}
		}
	}
}

// json is the response (fetched with projectQuery.raw) with only the kept
// tasks, in their current order.
func (r *projectResponse) json() ([]byte, error) {
	var wire struct {
		Project map[string]json.RawMessage `json:"project"`
	}
	if err := json.Unmarshal(r.payload, &wire); err != nil {
		return nil, err
	}
	tasks := make([]json.RawMessage, 0, len(r.Project.Tasks))
	for _, t := range r.Project.Tasks {
		tasks = append(tasks, r.rawTasks[t.Name])
	}
	encoded, err := json.Marshal(tasks)
	if err != nil {
		return nil, err
	}
	wire.Project["tasks"] = encoded
	return json.MarshalIndent(map[string]any{"project": wire.Project}, "", "  ")
}

// stateOrder is how summaries list the states.
var stateOrder = []string{"running", "pending", "succeeded", "failed", "blocked", "stopped", "cancelled"}

// stateSummary is "3 running, 12 pending, 2373 succeeded".
func stateSummary(counts map[string]int) string {
	var parts []string
	done := map[string]bool{}
	for _, s := range stateOrder {
		done[s] = true
		if counts[s] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[s], s))
		}
	}
	var rest []string
	for s, n := range counts {
		if !done[s] && n > 0 {
			rest = append(rest, s)
		}
	}
	sort.Strings(rest)
	for _, s := range rest {
		parts = append(parts, fmt.Sprintf("%d %s", counts[s], s))
	}
	if len(parts) == 0 {
		return "no tasks"
	}
	return strings.Join(parts, ", ")
}

func countStates(tasks []taskStatus) map[string]int {
	counts := map[string]int{}
	for _, t := range tasks {
		counts[t.State]++
	}
	return counts
}

// Time and size formatting. Every timestamp of the API is RFC 3339 (UTC).

// nowFunc and sleepFunc are the clock of polling commands (tests replace them).
var (
	nowFunc   = time.Now
	sleepFunc = func(ctx context.Context, d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
)

func parseStamp(s *string) (time.Time, bool) {
	if s == nil || *s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// clock is the HH:MM:SS of event lines.
func clock(t time.Time) string { return t.Local().Format("15:04:05") }

// shortTime is the MM-DD HH:MM of tables; shortStamp is that of a
// timestamp, "-" when unknown.
func shortTime(t time.Time) string { return t.Local().Format("01-02 15:04") }

func shortStamp(s *string) string {
	if t, ok := parseStamp(s); ok {
		return shortTime(t)
	}
	return "-"
}

// longTime is a full local time for task show; longStamp is that of a
// timestamp, "-" when unknown.
func longTime(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05") }

func longStamp(s *string) string {
	if t, ok := parseStamp(s); ok {
		return longTime(t)
	}
	return "-"
}

// compactDuration is 45s, 12m5s, 3h12m, 2d4h.
func compactDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d/time.Second))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d/time.Minute), int(d%time.Minute/time.Second))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d/time.Hour), int(d%time.Hour/time.Minute))
	default:
		return fmt.Sprintf("%dd%dh", int(d/(24*time.Hour)), int(d%(24*time.Hour)/time.Hour))
	}
}

func byteSize(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	case n < 1024*1024*1024:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1024*1024))
	default:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1024*1024*1024))
	}
}

// startedAt is when the execution's (latest) attempt started.
func (e *taskExecution) startedAt() (time.Time, bool) {
	if t, ok := parseStamp(e.AttemptStartedAt); ok {
		return t, true
	}
	return parseStamp(e.StartedAt)
}

// endedAt is when the execution ended (exit or terminal).
func (e *taskExecution) endedAt() (time.Time, bool) {
	if t, ok := parseStamp(e.ExitedAt); ok {
		return t, true
	}
	return parseStamp(e.TerminalAt)
}

// elapsed is the run time of a task's latest execution: so far while it
// runs, start to end once it ended; "" when it never started.
func elapsed(t taskStatus, now time.Time) string {
	e := t.latest()
	if e == nil {
		return ""
	}
	start, ok := e.startedAt()
	if !ok {
		return ""
	}
	if end, ok := e.endedAt(); ok {
		return compactDuration(end.Sub(start))
	}
	if t.State == "running" {
		return compactDuration(now.Sub(start))
	}
	return ""
}

// progressText is "120/500 steps" (plus the message), "" without progress.
func progressText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var p struct {
		Current *float64 `json:"current"`
		Total   *float64 `json:"total"`
		Unit    string   `json:"unit"`
		Message string   `json:"message"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return ""
	}
	var parts []string
	if p.Current != nil {
		text := number(*p.Current)
		if p.Total != nil {
			text += "/" + number(*p.Total)
		}
		if p.Unit != "" {
			text += " " + p.Unit
		}
		parts = append(parts, text)
	}
	if p.Message != "" {
		parts = append(parts, p.Message)
	}
	return strings.Join(parts, " ")
}

func number(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// exitText is "exit 1", "signal SIGKILL" or "".
func exitText(code *int, signal *string) string {
	switch {
	case signal != nil && *signal != "":
		return "signal " + *signal
	case code != nil:
		return fmt.Sprintf("exit %d", *code)
	}
	return ""
}

// taskNote is the short explanation of a task's state used by list, wait
// and watch: progress while running, the exit or reason once ended.
func taskNote(t taskStatus) string {
	e := t.latest()
	switch {
	case t.State == "running":
		if e != nil {
			if p := progressText(e.Progress); p != "" {
				return p
			}
			if e.AttemptID == nil && e.State != "" && e.State != "started" {
				// Submitted, not started yet (waiting for resources or a gate).
				if e.AdmissionWait != nil {
					return "execution " + e.State + ": " + *e.AdmissionWait
				}
				return "execution " + e.State
			}
		}
		return ""
	case t.State == "pending":
		if e != nil && e.State != "" {
			return "execution " + e.State
		}
		return ""
	case t.State == "succeeded":
		return ""
	}
	r := t.result()
	exit := exitText(r.ExitCode, r.ExitSignal)
	if exit == "" && e != nil {
		exit = exitText(e.ExitCode, e.ExitSignal)
	}
	switch {
	case exit != "" && r.Reason != "" && !strings.Contains(r.Reason, "exit"):
		return exit + " (" + r.Reason + ")"
	case exit != "":
		return exit
	case r.Reason != "":
		if r.TerminalCause != nil && *r.TerminalCause != "" {
			return r.Reason + " (" + *r.TerminalCause + ")"
		}
		return r.Reason
	case e != nil && e.TerminalCause != nil:
		return *e.TerminalCause
	}
	return ""
}

// stdoutLine writes one event line; stdout is unbuffered, so every line
// reaches a pipe (or a Monitor tool) as soon as it is printed.
func stdoutLine(format string, args ...any) {
	fmt.Fprintf(os.Stdout, format+"\n", args...)
}
