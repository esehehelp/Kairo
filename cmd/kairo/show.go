package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"kairo/internal/orchestration"
)

// renderTask prints `kairo task show`: one "label: value" line per fact, so
// the output greps well.
func renderTask(w io.Writer, project string, t *taskDetail, now time.Time) {
	line := func(label, format string, args ...any) {
		fmt.Fprintf(w, "%-12s %s\n", label+":", fmt.Sprintf(format, args...))
	}
	line("task", "%s", t.Name)
	line("project", "%s  queue: %s  policy: %s", project, t.Queue, t.Policy)
	state := t.State
	if note := taskNote(t.taskStatus); note != "" {
		state += " (" + note + ")"
	}
	line("state", "%s", state)
	if t.CreatedAt != "" || t.UpdatedAt != "" {
		line("declared", "%s  updated: %s", longStamp(&t.CreatedAt), longStamp(&t.UpdatedAt))
	}
	if t.SpecDigest != "" {
		line("digest", "%s", shortDigest(t.SpecDigest))
	}
	if s := t.Spec; s != nil {
		e := s.Execution
		line("argv", "%s", quoteArgv(e.Argv))
		if e.CWD != "" {
			line("cwd", "%s", e.CWD)
		}
		line("priority", "%d  checkpointable: %s  preemptible: %s", e.Priority, yesNo(e.Checkpointable), yesNo(e.Preemptible))
		if len(e.Executor.Labels) > 0 {
			line("executor", "%s", keyValues(e.Executor.Labels))
		}
		if r := resourceSummary(e); r != "" {
			line("resources", "%s", r)
		}
		if g := e.Gang; g != nil {
			text := fmt.Sprintf("size %d", g.Size)
			if g.Port != 0 {
				text += fmt.Sprintf(", port %d", g.Port)
			}
			line("gang", "%s", text)
		}
		if len(e.Env) > 0 {
			line("env", "%s", keyValues(e.Env))
		}
	}
	if len(t.Dependencies) > 0 {
		line("depends on", "%s", nameStates(t.Dependencies))
	} else if len(t.DependsOn) > 0 {
		line("depends on", "%s", strings.Join(t.DependsOn, ", "))
	}
	if len(t.Dependents) > 0 {
		line("dependents", "%s", nameStates(t.Dependents))
	}
	if len(t.Gates) > 0 {
		var parts []string
		for _, g := range t.Gates {
			parts = append(parts, g.Scope+" "+g.State)
		}
		line("gates", "%s", strings.Join(parts, ", "))
	}
	if len(t.Executions) == 0 {
		fmt.Fprintln(w, "executions:  none yet")
		return
	}
	for _, e := range t.Executions {
		renderExecution(w, e, now)
	}
}

func renderExecution(w io.Writer, e taskExecution, now time.Time) {
	head := fmt.Sprintf("execution #%d", e.Ordinal)
	if e.Reason != "" {
		head += " (" + e.Reason + ")"
	}
	head += "  " + e.State
	if exit := exitText(e.ExitCode, e.ExitSignal); exit != "" {
		head += "  " + exit
	}
	if e.TerminalCause != nil && *e.TerminalCause != "" {
		head += "  [" + *e.TerminalCause + "]"
	}
	fmt.Fprintln(w, head)
	line := func(label, format string, args ...any) {
		fmt.Fprintf(w, "  %-11s %s\n", label+":", fmt.Sprintf(format, args...))
	}
	line("id", "%s", e.ExecutionID)
	var times []string
	if e.SubmittedAt != nil {
		times = append(times, "submitted "+longStamp(e.SubmittedAt))
	}
	if start, ok := e.startedAt(); ok {
		stamp := start.Format(time.RFC3339Nano)
		times = append(times, "started "+longStamp(&stamp))
		if end, ok := e.endedAt(); ok {
			stamp := end.Format(time.RFC3339Nano)
			times = append(times, "ended "+longStamp(&stamp)+" ("+compactDuration(end.Sub(start))+")")
		} else if e.State != "terminal" {
			times = append(times, "running for "+compactDuration(now.Sub(start)))
		}
	} else if e.TerminalAt != nil {
		times = append(times, "ended "+longStamp(e.TerminalAt))
	}
	if len(times) > 0 {
		line("time", "%s", strings.Join(times, "  "))
	}
	attempts := e.Attempts
	if len(attempts) == 0 && e.AttemptID != nil {
		executor := ""
		if e.ExecutorID != nil {
			executor = *e.ExecutorID
		}
		attempts = []attemptInfo{{ID: *e.AttemptID, ExecutorID: executor, Progress: e.Progress}}
	}
	for _, a := range attempts {
		renderAttempt(w, a, len(attempts) > 1)
	}
	if len(e.Attempts) == 0 && e.AttemptID == nil {
		if p := progressText(e.Progress); p != "" {
			line("progress", "%s", p)
		}
	}
	for _, r := range e.Gang {
		text := fmt.Sprintf("rank %d %s %s", r.Rank, r.ExecutionID, r.State)
		if r.AttemptID != nil {
			text += " " + *r.AttemptID
		}
		if r.NodeID != nil {
			text += " node " + *r.NodeID
		}
		if r.ExecutorID != nil {
			text += " executor " + *r.ExecutorID
		}
		line("gang", "%s", text)
	}
	for _, reason := range e.Waiting {
		line("waiting", "%s", waitingText(reason))
	}
}

func renderAttempt(w io.Writer, a attemptInfo, several bool) {
	line := func(label, format string, args ...any) {
		fmt.Fprintf(w, "  %-11s %s\n", label+":", fmt.Sprintf(format, args...))
	}
	text := a.ID
	if a.State != "" {
		text += " " + a.State
	}
	if a.PID != nil {
		text += fmt.Sprintf(" pid %d", *a.PID)
	}
	if a.NodeID != "" {
		text += " node " + a.NodeID
	}
	if a.ExecutorID != "" {
		text += " executor " + a.ExecutorID
	}
	if exit := exitText(a.ExitCode, a.ExitSignal); exit != "" {
		text += " " + exit
	}
	line("attempt", "%s", text)
	if several && a.StartedAt != nil {
		times := "started " + longStamp(a.StartedAt)
		if a.ExitedAt != nil {
			times += "  exited " + longStamp(a.ExitedAt)
		} else if a.QuiescedAt != nil {
			times += "  quiesced " + longStamp(a.QuiescedAt)
		}
		line("time", "%s", times)
	}
	if a.LastHeartbeatAt != nil && !a.ended() {
		line("heartbeat", "%s", longStamp(a.LastHeartbeatAt))
	}
	if p := progressText(a.Progress); p != "" {
		line("progress", "%s", p)
	}
	logLine := func(stream string, path *string, size *int64) {
		if path == nil || *path == "" {
			return
		}
		text := *path
		if size != nil {
			text += " (" + byteSize(*size) + ")"
		}
		line(stream, "%s", text)
	}
	logLine("stdout", a.StdoutPath, a.StdoutBytes)
	logLine("stderr", a.StderrPath, a.StderrBytes)
}

// waitingText makes a waiting reason readable:
// "external_claim on GPU-1234 (pid=99)", "scope_gate_closed on scope p/q".
func waitingText(reason map[string]any) string {
	rest := map[string]any{}
	for k, v := range reason {
		if k != "detail" {
			rest[k] = v
		}
	}
	// The daemon nests the block's facts in a "detail" object.
	switch detail := reason["detail"].(type) {
	case map[string]any:
		for k, v := range detail {
			if _, taken := rest[k]; !taken {
				rest[k] = v
			}
		}
	case nil:
	default:
		rest["detail"] = detail
	}
	text := fmt.Sprint(rest["reason_code"])
	delete(rest, "reason_code")
	if text == "<nil>" || text == "" {
		text = "unknown"
	}
	since := ""
	if created, ok := rest["created_at"].(string); ok {
		since = shortStamp(&created)
		delete(rest, "created_at")
	}
	if scope, ok := rest["scope"].(string); ok && scope != "" && text == "scope_gate_closed" {
		text += ": the " + scope + " is paused"
		delete(rest, "scope")
	}
	for _, key := range []string{"resource_id", "resource", "gpu", "gpu_uuid", "device", "uuid", "scope", "node_id", "executor_id"} {
		if v, ok := rest[key]; ok && v != nil && fmt.Sprint(v) != "" {
			label := ""
			if key == "scope" {
				label = "scope "
			}
			text += " on " + label + plain(v)
			delete(rest, key)
			break
		}
	}
	message := ""
	for _, key := range []string{"message", "detail"} {
		if v, ok := rest[key].(string); ok && v != "" {
			message = v
			delete(rest, key)
			break
		}
	}
	var keys []string
	for k := range rest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		if rest[k] == nil {
			continue
		}
		parts = append(parts, k+"="+plain(rest[k]))
	}
	if len(parts) > 0 {
		text += " (" + strings.Join(parts, ", ") + ")"
	}
	if message != "" {
		text += ": " + message
	}
	if since != "" && since != "-" {
		text += " (since " + since + ")"
	}
	return text
}

// plain formats a JSON value without exponents for whole numbers.
func plain(v any) string {
	if f, ok := v.(float64); ok {
		return number(f)
	}
	return fmt.Sprint(v)
}

func nameStates(list []nameState) string {
	var parts []string
	for _, d := range list {
		parts = append(parts, d.Name+" ("+d.State+")")
	}
	return strings.Join(parts, ", ")
}

func keyValues(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + m[k]
	}
	return strings.Join(parts, " ")
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// quoteArgv joins argv, quoting the arguments that need it.
func quoteArgv(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		if a == "" || strings.ContainsAny(a, " \t\"'") {
			parts[i] = strconv.Quote(a)
		} else {
			parts[i] = a
		}
	}
	return strings.Join(parts, " ")
}

// resourceSummary is "gpu x1 (same node); cpu 4000m; ram 16.0 GiB; disk data 10.0 GiB".
func resourceSummary(e orchestration.Execution) string {
	var parts []string
	for _, x := range e.Exclusive {
		text := fmt.Sprintf("%s x%d", x.Kind, x.Count)
		if x.SameNode {
			text += " same node"
		}
		if x.MinTotalMemoryBytes > 0 {
			text += " >=" + byteSize(x.MinTotalMemoryBytes)
		}
		parts = append(parts, text)
	}
	c := e.Capacity
	if c.CPUMillis > 0 {
		parts = append(parts, fmt.Sprintf("cpu %dm", c.CPUMillis))
	}
	if c.RAMBytes > 0 {
		parts = append(parts, "ram "+byteSize(c.RAMBytes))
	}
	for _, d := range c.Disks {
		parts = append(parts, "disk "+d.Filesystem+" "+byteSize(d.ReserveBytes))
	}
	return strings.Join(parts, "; ")
}
