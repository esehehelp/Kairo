package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"kairo/internal/orchestration"
)

// The project and task views of the API (GET /api/projects/{p} and
// GET /api/projects/{p}/tasks/{t}) as the CLI reads them, and the
// formatting shared by list, show, wait and watch.

type projectStatus struct {
	Name        string         `json:"name"`
	SpecDigests []string       `json:"spec_digests"`
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
	// Task detail only.
	Attempts []attemptInfo    `json:"attempts"`
	Gang     []gangRankInfo   `json:"gang"`
	Waiting  []map[string]any `json:"waiting"`
}

type attemptInfo struct {
	ID              string          `json:"id"`
	ExecutionID     string          `json:"execution_id"`
	State           string          `json:"state"`
	ExecutorID      string          `json:"executor_id"`
	NodeID          string          `json:"node_id"`
	PID             *int            `json:"pid"`
	ExitCode        *int            `json:"exit_code"`
	ExitSignal      *string         `json:"exit_signal"`
	Progress        json.RawMessage `json:"progress"`
	StartedAt       *string         `json:"started_at"`
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

// logBytes is stdout+stderr size, and whether the daemon reported either.
func (a attemptInfo) logBytes() (int64, bool) {
	if a.StdoutBytes == nil && a.StderrBytes == nil {
		return 0, false
	}
	var n int64
	if a.StdoutBytes != nil {
		n += *a.StdoutBytes
	}
	if a.StderrBytes != nil {
		n += *a.StderrBytes
	}
	return n, true
}

type gangRankInfo struct {
	Rank        int     `json:"rank"`
	ExecutionID string  `json:"execution_id"`
	State       string  `json:"state"`
	AttemptID   *string `json:"attempt_id"`
	ExecutorID  *string `json:"executor_id"`
	NodeID      *string `json:"node_id"`
}

type nameState struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type gateInfo struct {
	Scope string `json:"scope"`
	State string `json:"state"`
}

type taskSpec struct {
	Name      string        `json:"name"`
	Queue     string        `json:"queue"`
	DependsOn []string      `json:"depends_on"`
	Policy    string        `json:"policy"`
	Execution specExecution `json:"execution"`
}

type specExecution struct {
	orchestration.Execution
	Env map[string]string `json:"env,omitempty"`
}

type taskDetail struct {
	taskStatus
	Spec         *taskSpec   `json:"spec"`
	Dependencies []nameState `json:"dependencies"`
	Dependents   []nameState `json:"dependents"`
	Gates        []gateInfo  `json:"gates"`
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

// Task states.
func terminalState(state string) bool {
	switch state {
	case "succeeded", "failed", "blocked", "stopped", "cancelled":
		return true
	}
	return false
}

func liveState(state string) bool { return state == "pending" || state == "running" }

// projectQuery is the filter of GET /api/projects/{p}.
type projectQuery struct {
	tasks  []string
	match  string
	states []string
	live   bool
	since  string // RFC3339
}

func (q projectQuery) path(project string) string {
	values := url.Values{}
	for _, t := range q.tasks {
		values.Add("task", t)
	}
	if q.match != "" {
		values.Set("match", q.match)
	}
	if len(q.states) > 0 {
		values.Set("state", strings.Join(q.states, ","))
	}
	if q.live {
		values.Set("live", "1")
	}
	if q.since != "" {
		values.Set("since", q.since)
	}
	path := "/api/projects/" + url.PathEscape(project)
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path
}

// keep applies the filter on the client too: a daemon that predates the
// filters returns every task.
func (q projectQuery) keep(t taskStatus) bool {
	if len(q.tasks) > 0 && !contains(q.tasks, t.Name) {
		return false
	}
	if q.match != "" && !strings.HasPrefix(t.Name, q.match) {
		return false
	}
	if len(q.states) > 0 && !contains(q.states, t.State) {
		return false
	}
	if q.live && !liveState(t.State) {
		return false
	}
	if q.since != "" {
		since, err1 := time.Parse(time.RFC3339Nano, q.since)
		updated, err2 := time.Parse(time.RFC3339Nano, t.UpdatedAt)
		if err1 == nil && (err2 != nil || updated.Before(since)) {
			return false
		}
	}
	return true
}

func contains(list []string, value string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// projectResponse is GET /api/projects/{p}: the parsed status and each task
// as the daemon sent it (for --json).
type projectResponse struct {
	Project  projectStatus
	RawTasks map[string]json.RawMessage
	raw      map[string]json.RawMessage
}

func fetchProject(client *apiClient, project string, q projectQuery) (*projectResponse, error) {
	payload, err := client.call("GET", q.path(project), nil)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Project json.RawMessage `json:"project"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		return nil, fmt.Errorf("decode project status: %w", err)
	}
	resp := &projectResponse{RawTasks: map[string]json.RawMessage{}}
	if err := json.Unmarshal(wire.Project, &resp.Project); err != nil {
		return nil, fmt.Errorf("decode project status: %w", err)
	}
	if err := json.Unmarshal(wire.Project, &resp.raw); err != nil {
		return nil, fmt.Errorf("decode project status: %w", err)
	}
	var rawTasks []json.RawMessage
	_ = json.Unmarshal(resp.raw["tasks"], &rawTasks)
	kept := resp.Project.Tasks[:0]
	for i, t := range resp.Project.Tasks {
		if !q.keep(t) {
			continue
		}
		kept = append(kept, t)
		if i < len(rawTasks) {
			resp.RawTasks[t.Name] = rawTasks[i]
		}
	}
	resp.Project.Tasks = kept
	return resp, nil
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
			r.RawTasks[t.Name] = other.RawTasks[t.Name]
		}
	}
}

// json is the response with only the kept tasks, in their current order.
func (r *projectResponse) json() ([]byte, error) {
	out := map[string]json.RawMessage{}
	for k, v := range r.raw {
		out[k] = v
	}
	tasks := make([]json.RawMessage, 0, len(r.Project.Tasks))
	for _, t := range r.Project.Tasks {
		tasks = append(tasks, r.RawTasks[t.Name])
	}
	encoded, err := json.Marshal(tasks)
	if err != nil {
		return nil, err
	}
	out["tasks"] = encoded
	return json.MarshalIndent(map[string]any{"project": out}, "", "  ")
}

// stateOrder is how summaries list the states.
var stateOrder = []string{"running", "pending", "paused", "succeeded", "failed", "blocked", "stopped", "cancelled"}

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

// shortStamp is the MM-DD HH:MM of tables, "-" when unknown.
func shortStamp(s *string) string {
	if t, ok := parseStamp(s); ok {
		return t.Local().Format("01-02 15:04")
	}
	return "-"
}

// longStamp is a full local time for task show, "-" when unknown.
func longStamp(s *string) string {
	if t, ok := parseStamp(s); ok {
		return t.Local().Format("2006-01-02 15:04:05")
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
