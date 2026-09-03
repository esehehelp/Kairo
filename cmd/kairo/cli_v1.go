package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"kairo/internal/id"
	"kairo/internal/plan"
)

func planCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo plan <validate|apply|export>")
	}
	switch args[0] {
	case "validate":
		fs := flag.NewFlagSet("plan validate", flag.ContinueOnError)
		asJSON := fs.Bool("json", false, "emit normalized JSON")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("plan validate requires a TOML file")
		}
		body, err := os.ReadFile(fs.Arg(0))
		if err != nil {
			return err
		}
		validated, err := plan.Parse(body)
		if err != nil {
			return err
		}
		if *asJSON {
			_, err = os.Stdout.Write(append(validated.Normalized, '\n'))
			return err
		}
		fmt.Printf("valid: %s/%s digest=%s tasks=%d\n", validated.Manifest.Project, validated.Manifest.Queue, validated.Digest, len(validated.Manifest.Tasks))
		return nil
	case "apply":
		fs := flag.NewFlagSet("plan apply", flag.ContinueOnError)
		api := apiFlag(fs)
		expected := fs.Int("expected-revision", -1, "required current revision")
		create := fs.Bool("create", false, "create a new queue")
		actor := fs.String("actor", os.Getenv("USERNAME"), "audit actor")
		requestID := fs.String("request-id", id.New("cli"), "idempotency request ID")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 1 || (!*create && *expected < 0) {
			return errors.New(
				"plan apply requires a file and --expected-revision (or --create)",
			)
		}
		if *create && *expected < 0 {
			*expected = 0
		}
		body, err := os.ReadFile(fs.Arg(0))
		if err != nil {
			return err
		}
		validated, err := plan.Parse(body)
		if err != nil {
			return err
		}
		path := "/v1/queues/" + url.PathEscape(validated.Manifest.Project) + "/" + url.PathEscape(validated.Manifest.Queue) + "/plan"
		return request(*api, http.MethodPost, path, map[string]any{"source": string(body), "expected_revision": *expected, "create": *create, "actor": *actor, "request_id": *requestID})
	case "export":
		fs := flag.NewFlagSet("plan export", flag.ContinueOnError)
		api := apiFlag(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errors.New("plan export requires PROJECT QUEUE")
		}
		return request(*api, http.MethodGet, queuePath(fs.Arg(0), fs.Arg(1))+"/export", nil)
	default:
		return fmt.Errorf("unknown plan command %q", args[0])
	}
}

func queuePath(project, queue string) string {
	return "/v1/queues/" + url.PathEscape(project) + "/" + url.PathEscape(queue)
}
func queueCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo queue <list|show|history|pause|resume>")
	}
	action := args[0]
	fs := flag.NewFlagSet("queue "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	_ = fs.Bool("json", true, "emit JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if action == "list" {
		return request(*api, http.MethodGet, "/v1/queues", nil)
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("queue %s requires PROJECT QUEUE", action)
	}
	path := queuePath(fs.Arg(0), fs.Arg(1))
	switch action {
	case "show":
		return request(*api, http.MethodGet, path, nil)
	case "history":
		return request(*api, http.MethodGet, path+"/history", nil)
	case "pause", "resume":
		return request(*api, http.MethodPost, path+"/"+action, nil)
	default:
		return fmt.Errorf("unknown queue command %q", action)
	}
}
func taskCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo task <show|pause|resume|cancel|retry>")
	}
	action := args[0]
	fs := flag.NewFlagSet("task "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	_ = fs.Bool("json", true, "emit JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("task %s requires TASK_ID", action)
	}
	path := "/v1/tasks/" + url.PathEscape(fs.Arg(0))
	if action == "show" {
		return request(*api, http.MethodGet, path, nil)
	}
	if !contains(action, "pause", "resume", "cancel", "retry") {
		return fmt.Errorf("unknown task command %q", action)
	}
	return request(*api, http.MethodPost, path+"/"+action, nil)
}
func resourceCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: kairo resource <status|quarantine|enable>")
	}
	action := args[0]
	fs := flag.NewFlagSet("resource "+action, flag.ContinueOnError)
	api := apiFlag(fs)
	reason := fs.String("reason", "operator request", "quarantine reason")
	confirmAbsent := fs.Bool("confirm-process-absent", false, "confirm provider/OS evidence that stale attempt processes are absent")
	_ = fs.Bool("json", true, "emit JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if action == "status" {
		return request(*api, http.MethodGet, "/v1/resources/status", nil)
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
	return request(*api, http.MethodPost, "/v1/resources/"+url.PathEscape(fs.Arg(0))+"/"+action, body)
}
func doctorCommand(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	api := apiFlag(fs)
	_ = fs.Bool("json", true, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return request(*api, http.MethodGet, "/v1/status", nil)
}
func contains(value string, options ...string) bool {
	for _, x := range options {
		if strings.EqualFold(value, x) {
			return true
		}
	}
	return false
}
