package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	manifest "kairo/internal/execution"
	"kairo/internal/id"
)

func executionCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo execution <submit|list|show|withdraw>")
	}
	action := args[0]
	fs := flag.NewFlagSet("execution "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	project := fs.String("project", "", "filter by project")
	queue := fs.String("queue", "", "filter by queue")
	task := fs.String("task", "", "filter by task")
	state := fs.String("state", "", "filter by coordination state")
	limit := fs.Int("limit", 100, "maximum results")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch action {
	case "submit":
		if fs.NArg() != 1 {
			return errors.New("execution submit requires an execution TOML file")
		}
		source, err := os.ReadFile(fs.Arg(0))
		if err != nil {
			return err
		}
		parsed, err := manifest.Parse(source)
		if err != nil {
			return err
		}
		return request(*api, http.MethodPost, "/v2/executions", parsed.Manifest)
	case "list":
		if fs.NArg() != 0 {
			return errors.New("execution list accepts only flags")
		}
		query := url.Values{}
		query.Set("project", *project)
		query.Set("queue", *queue)
		query.Set("task", *task)
		query.Set("state", *state)
		query.Set("limit", fmt.Sprint(*limit))
		return request(*api, http.MethodGet, withQuery("/v2/executions", query), nil)
	case "show", "withdraw":
		if fs.NArg() != 1 {
			return fmt.Errorf("execution %s requires EXECUTION_ID", action)
		}
		path := "/v2/executions/" + url.PathEscape(fs.Arg(0))
		if action == "withdraw" {
			return request(*api, http.MethodPost, path+"/withdraw", nil)
		}
		return request(*api, http.MethodGet, path, nil)
	default:
		return fmt.Errorf("unknown execution command %q", action)
	}
}

func projectCommand(args []string) error {
	return scopeCommand("project", args)
}

func queueCommand(args []string) error {
	return scopeCommand("queue", args)
}

func taskCommand(args []string) error {
	return scopeCommand("task", args)
}

func scopeCommand(kind string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: kairo %s <pause|resume>", kind)
	}
	action := args[0]
	if action != "pause" && action != "resume" {
		return fmt.Errorf("unknown %s command %q", kind, action)
	}
	fs := flag.NewFlagSet(kind+" "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	noWait := fs.Bool("no-wait", false, "return after the pause operation is persisted")
	waitTimeout := fs.Duration("timeout", 30*time.Minute, "maximum time to wait for pause convergence")
	actor := fs.String("actor", os.Getenv("USERNAME"), "audit actor")
	requestID := fs.String("request-id", id.New("cli"), "idempotency request ID")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	want := map[string]int{"project": 1, "queue": 2, "task": 3}[kind]
	if fs.NArg() != want {
		return fmt.Errorf("%s %s requires %s", kind, action, scopeArguments(kind))
	}
	if action == "resume" && *noWait {
		return errors.New("--no-wait applies only to pause")
	}

	query := url.Values{"project": {fs.Arg(0)}}
	if want > 1 {
		query.Set("queue", fs.Arg(1))
	}
	if want > 2 {
		query.Set("task", fs.Arg(2))
	}
	payload, err := requestBytes(*api, http.MethodGet, withQuery("/v2/scopes", query), nil)
	if err != nil {
		return err
	}
	var listed struct {
		Scopes []struct {
			ID string `json:"id"`
		} `json:"scopes"`
	}
	if err := json.Unmarshal(payload, &listed); err != nil {
		return fmt.Errorf("decode scope response: %w", err)
	}
	if len(listed.Scopes) != 1 {
		return fmt.Errorf("coordination scope not found or ambiguous (%d matches)", len(listed.Scopes))
	}
	path := "/v2/scopes/" + url.PathEscape(listed.Scopes[0].ID) + "/" + action
	var body any
	if action == "pause" {
		body = map[string]string{"actor": *actor, "request_id": *requestID}
	} else {
		body = map[string]string{"actor": *actor}
	}
	payload, err = requestBytes(*api, http.MethodPost, path, body)
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
	return waitForPause(*api, started.OperationID, *waitTimeout)
}

func waitForPause(apiURL, operationID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		payload, err := requestBytes(apiURL, http.MethodGet, "/v2/pause-operations/"+url.PathEscape(operationID), nil)
		if err != nil {
			return err
		}
		var response struct {
			Operation struct {
				State string `json:"state"`
			} `json:"operation"`
		}
		if err := json.Unmarshal(payload, &response); err != nil {
			return fmt.Errorf("decode pause operation: %w", err)
		}
		switch response.Operation.State {
		case "quiesced":
			_, err = os.Stdout.Write(payload)
			return err
		case "blocked":
			_, _ = os.Stdout.Write(payload)
			return fmt.Errorf("pause operation %s", response.Operation.State)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for pause operation %s", operationID)
		}
		time.Sleep(time.Second)
	}
}

func scopeArguments(kind string) string {
	switch kind {
	case "project":
		return "PROJECT"
	case "queue":
		return "PROJECT QUEUE"
	default:
		return "PROJECT QUEUE TASK"
	}
}

func resourceCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo resource <status|quarantine|enable|reconcile>")
	}
	action := args[0]
	fs := flag.NewFlagSet("resource "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	reason := fs.String("reason", "operator request", "quarantine reason")
	confirmAbsent := fs.Bool("confirm-process-absent", false, "confirm provider/OS evidence that stale attempt processes are absent")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if action == "status" {
		if fs.NArg() != 0 {
			return errors.New("resource status accepts no arguments")
		}
		return request(*api, http.MethodGet, "/v2/resources/status", nil)
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("resource %s requires RESOURCE_ID", action)
	}
	if action != "quarantine" && action != "enable" && action != "reconcile" {
		return fmt.Errorf("unknown resource command %q", action)
	}
	var body any
	if action == "quarantine" {
		body = map[string]string{"reason": *reason}
	} else if action == "reconcile" {
		if !*confirmAbsent {
			return errors.New("resource reconcile requires --confirm-process-absent")
		}
		body = map[string]bool{"confirm_process_absent": true}
	}
	return request(*api, http.MethodPost, "/v2/resources/"+url.PathEscape(fs.Arg(0))+"/"+action, body)
}

func doctorCommand(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	api := apiFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("doctor accepts no arguments")
	}
	return request(*api, http.MethodGet, "/v2/resources/status", nil)
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

func requestBytes(apiURL, method, path string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, strings.TrimRight(apiURL, "/")+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = os.Stdout.Write(payload)
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	return payload, nil
}
