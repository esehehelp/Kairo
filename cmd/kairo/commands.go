package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	manifest "kairo/internal/execution"
	"kairo/internal/id"
)

func executionCommand(args []string) error {
	return dispatch(findGroup("execution"), args, map[string]func([]string) error{
		"submit":   executionSubmit,
		"list":     executionList,
		"show":     func(a []string) error { return executionByID("show", a) },
		"withdraw": func(a []string) error { return executionByID("withdraw", a) },
		"logs":     executionLogs,
	})
}

func executionSubmit(args []string) error {
	fs := newFlags("execution submit")
	api := apiFlag(fs)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("execution submit requires an execution TOML file")
	}
	source, err := os.ReadFile(positional[0])
	if err != nil {
		return err
	}
	parsed, err := manifest.Parse(source)
	if err != nil {
		return err
	}
	body, err := submitBody(parsed.Manifest)
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	return client.print(http.MethodPost, "/api/executions", body)
}

func executionList(args []string) error {
	fs := newFlags("execution list")
	api := apiFlag(fs)
	project := fs.String("project", "", "filter by project")
	queue := fs.String("queue", "", "filter by queue")
	task := fs.String("task", "", "filter by task")
	state := fs.String("state", "", "filter by coordination state")
	limit := fs.Int("limit", 100, "maximum results")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("execution list accepts only flags")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("project", *project)
	query.Set("queue", *queue)
	query.Set("task", *task)
	query.Set("state", *state)
	query.Set("limit", fmt.Sprint(*limit))
	return client.print(http.MethodGet, withQuery("/api/executions", query), nil)
}

// executionByID: execution show|withdraw EXECUTION_ID.
func executionByID(action string, args []string) error {
	fs := newFlags("execution " + action)
	api := apiFlag(fs)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("execution %s requires EXECUTION_ID", action)
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	path := "/api/executions/" + url.PathEscape(positional[0])
	if action == "withdraw" {
		return client.print(http.MethodPost, path+"/withdraw", nil)
	}
	return client.print(http.MethodGet, path, nil)
}

// submitBody is the execution file as the API takes it: schema_version
// checks the file format and is not part of the request.
func submitBody(m manifest.Manifest) (map[string]any, error) {
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		return nil, err
	}
	delete(body, "schema_version")
	return body, nil
}

func projectCommand(args []string) error {
	return dispatch(findGroup("project"), args, map[string]func([]string) error{
		"validate": projectValidate,
		"apply":    projectApply,
		"status":   projectStatusCommand,
		"watch":    projectWatch,
		"prune":    projectPrune,
		"pause":    func(a []string) error { return scopeAction("project", "pause", a) },
		"resume":   func(a []string) error { return scopeAction("project", "resume", a) },
	})
}

func queueCommand(args []string) error {
	return dispatch(findGroup("queue"), args, map[string]func([]string) error{
		"pause":  func(a []string) error { return scopeAction("queue", "pause", a) },
		"resume": func(a []string) error { return scopeAction("queue", "resume", a) },
	})
}

// scopeAction pauses or resumes a project, a queue or a task. A task may be
// named without its queue: it is looked up.
func scopeAction(kind, action string, args []string) error {
	fs := newFlags(kind + " " + action)
	api := apiFlag(fs)
	var noWait, force *bool
	var waitTimeout, grace *time.Duration
	var requestID, reason *string
	actor := fs.String("actor", defaultActor(), "audit actor")
	if action == "pause" {
		noWait = fs.Bool("no-wait", false, "return after the pause operation is persisted")
		waitTimeout = fs.Duration("timeout", 30*time.Minute, "maximum time to wait for pause convergence")
		requestID = fs.String("request-id", id.New("cli"), "idempotency request ID")
		// --force / -f: end the captured executions instead of suspending them.
		force = fs.Bool("force", false, "force stop: terminate running process trees (graceful stop, then kill after --grace) and withdraw unstarted executions; the task ends stopped")
		fs.BoolVar(force, "f", false, "shorthand for --force")
		grace = fs.Duration("grace", 30*time.Second, "with --force: time between the graceful stop and the kill")
		reason = fs.String("reason", "", "with --force: why the work is stopped (recorded)")
	}
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	var ref taskRef
	switch kind {
	case "project":
		if len(positional) != 1 {
			return usageError("project %s requires PROJECT", action)
		}
		ref.project = positional[0]
	case "queue":
		if len(positional) != 2 {
			return usageError("queue %s requires PROJECT QUEUE", action)
		}
		ref.project, ref.queue = positional[0], positional[1]
	default:
		if ref, err = parseTaskRef("task "+action, positional); err != nil {
			return err
		}
	}
	if action == "pause" {
		graceSet, reasonSet := false, false
		fs.Visit(func(f *flag.Flag) {
			graceSet = graceSet || f.Name == "grace"
			reasonSet = reasonSet || f.Name == "reason"
		})
		if !*force && (graceSet || reasonSet) {
			return usageError("--grace and --reason apply only to pause --force")
		}
		if *grace < 0 || *grace > time.Hour {
			return usageError("--grace must be between 0 and 1h")
		}
	}

	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	if kind == "task" {
		if err := ref.resolveQueue(client); err != nil {
			return err
		}
	}
	query := url.Values{"project": {ref.project}}
	if kind != "project" {
		query.Set("queue", ref.queue)
	}
	if kind == "task" {
		query.Set("task", ref.task)
	}
	var listed struct {
		Scopes []struct {
			ID string `json:"id"`
		} `json:"scopes"`
	}
	if err := client.callJSON(http.MethodGet, withQuery("/api/scopes", query), nil, &listed); err != nil {
		return err
	}
	if len(listed.Scopes) != 1 {
		return fmt.Errorf("coordination scope not found or ambiguous (%d matches)", len(listed.Scopes))
	}
	path := "/api/scopes/" + url.PathEscape(listed.Scopes[0].ID) + "/" + action
	var body any
	switch {
	case action == "pause" && *force:
		// Only a forced pause sends these fields.
		body = map[string]any{"actor": *actor, "request_id": *requestID, "force": true,
			"grace_seconds": int((*grace + time.Second - 1) / time.Second), "reason": *reason}
	case action == "pause":
		body = map[string]string{"actor": *actor, "request_id": *requestID}
	default:
		body = map[string]string{"actor": *actor}
	}
	payload, err := client.call(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	if action == "resume" || *noWait {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var started struct {
		OperationID string `json:"operation_id"`
	}
	if err := json.Unmarshal(payload, &started); err != nil {
		return fmt.Errorf("decode pause response: %w", err)
	}
	if started.OperationID == "" {
		return errors.New("pause response did not include operation_id")
	}
	return waitForPause(client, started.OperationID, *waitTimeout)
}

func waitForPause(client *apiClient, operationID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		payload, err := client.call(http.MethodGet, "/api/pause-operations/"+url.PathEscape(operationID), nil)
		if err != nil {
			return err
		}
		var response pauseOperationResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			return fmt.Errorf("decode pause operation: %w", err)
		}
		switch response.Operation.State {
		case "quiesced":
			_, err = os.Stdout.Write(payload)
			for _, line := range response.forceSummary() {
				fmt.Fprintln(os.Stderr, line)
			}
			return err
		case "blocked":
			_, _ = os.Stdout.Write(payload)
			return fmt.Errorf("pause operation %s%s", response.Operation.State, response.blockers())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for pause operation %s", operationID)
		}
		time.Sleep(time.Second)
	}
}

type pauseOperationResponse struct {
	Operation struct {
		State string `json:"state"`
	} `json:"operation"`
	Targets []struct {
		ExecutionID   string  `json:"execution_id"`
		State         string  `json:"state"`
		BlockerReason *string `json:"blocker_reason"`
	} `json:"targets"`
	ForceStops []struct {
		ExecutionID string          `json:"execution_id"`
		ExecutorID  *string         `json:"executor_id"`
		State       string          `json:"state"`
		Detail      json.RawMessage `json:"detail"`
	} `json:"force_stops"`
}

// blockerHints explain the blocker reasons an operator can act on.
var blockerHints = map[string]string{
	"execution_not_checkpointable": "it cannot checkpoint; end it with pause --force",
	"checkpoint_rejected":          "it rejected the checkpoint; end it with pause --force",
	"force_stop_not_picked_up":     "its executor is polling but has not picked up the force stop; check that node's agent log",
	"executor_unreachable":         "its executor has not been seen recently: check that node's agent",
}

// blockers lists the blocked targets of an operation for an error message.
func (r pauseOperationResponse) blockers() string {
	var parts []string
	for _, t := range r.Targets {
		if t.State != "blocked" || t.BlockerReason == nil {
			continue
		}
		part := t.ExecutionID + ": " + *t.BlockerReason
		if hint := blockerHints[*t.BlockerReason]; hint != "" {
			part += " (" + hint + ")"
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return ""
	}
	return ": " + strings.Join(parts, "; ")
}

// forceSummary says how each execution of a forced pause ended.
func (r pauseOperationResponse) forceSummary() []string {
	var lines []string
	for _, f := range r.ForceStops {
		var detail struct {
			Killed      bool `json:"killed"`
			AlreadyGone bool `json:"already_gone"`
		}
		_ = json.Unmarshal(f.Detail, &detail)
		how := "stopped after the graceful stop"
		switch {
		case f.State == "withdrawn":
			how = "withdrawn before it started"
		case detail.AlreadyGone:
			how = "had already exited"
		case detail.Killed:
			how = "killed"
		}
		lines = append(lines, fmt.Sprintf("force stop: %s %s", f.ExecutionID, how))
	}
	return lines
}

func resourceCommand(args []string) error {
	handler := func(action string) func([]string) error {
		return func(a []string) error { return resourceAction(action, a) }
	}
	return dispatch(findGroup("resource"), args, map[string]func([]string) error{
		"status": handler("status"), "quarantine": handler("quarantine"),
		"enable": handler("enable"), "reconcile": handler("reconcile"),
	})
}

func resourceAction(action string, args []string) error {
	fs := newFlags("resource " + action)
	api := apiFlag(fs)
	var reason *string
	var confirmAbsent *bool
	switch action {
	case "quarantine":
		reason = fs.String("reason", "operator request", "quarantine reason")
	case "reconcile":
		confirmAbsent = fs.Bool("confirm-process-absent", false, "confirm provider/OS evidence that stale attempt processes are absent")
	}
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if action == "status" {
		if len(positional) != 0 {
			return usageError("resource status accepts no arguments")
		}
		client, err := newAPIClient(*api)
		if err != nil {
			return err
		}
		return printResources(client)
	}
	if len(positional) != 1 {
		return usageError("resource %s requires RESOURCE_ID", action)
	}
	var body any
	if action == "quarantine" {
		body = map[string]string{"reason": *reason}
	} else if action == "reconcile" {
		if !*confirmAbsent {
			return usageError("resource reconcile requires --confirm-process-absent")
		}
		body = map[string]bool{"confirm_process_absent": true}
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	return client.print(http.MethodPost, "/api/resources/"+url.PathEscape(positional[0])+"/"+action, body)
}

func printResources(client *apiClient) error {
	return client.print(http.MethodGet, "/api/resources", nil)
}

func doctorCommand(args []string) error {
	fs := newFlags("doctor")
	api := apiFlag(fs)
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("doctor accepts no arguments")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	var who struct {
		Name    string `json:"name"`
		Role    string `json:"role"`
		NodeID  string `json:"node_id"`
		Version string `json:"version"`
	}
	if err := client.callJSON(http.MethodGet, "/api/whoami", nil, &who); err != nil {
		return fmt.Errorf("%s: %w", client.baseURL, err)
	}
	line := fmt.Sprintf("authenticated as %s (%s)", who.Name, who.Role)
	if who.NodeID != "" {
		line += " for node " + who.NodeID
	}
	fmt.Println(line)
	printVersions(who.Version, nil)
	return printResources(client)
}

func withQuery(path string, values url.Values) string {
	for key, entries := range values {
		if len(entries) == 0 || entries[0] == "" {
			values.Del(key)
		}
	}
	if encoded := values.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}
