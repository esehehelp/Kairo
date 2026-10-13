package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"kairo/internal/orchestration"
)

// projectValidate: kairo project validate FILE [--json].
func projectValidate(args []string) error {
	fs := newFlags("project validate")
	asJSON := fs.Bool("json", false, "print the normalized tasks as JSON")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("project validate requires a project TOML file")
	}
	validated, err := readProjectSpec(positional[0])
	if err != nil {
		return err
	}
	if !*asJSON {
		fmt.Printf("ok %s: %d tasks, digest %s\n", validated.Manifest.Project.Name, len(validated.Manifest.Tasks), shortDigest(validated.Digest))
		return nil
	}
	body, err := json.MarshalIndent(map[string]any{
		"ok":          true,
		"project":     validated.Manifest.Project.Name,
		"spec_digest": validated.Digest,
		"tasks":       validated.Manifest.Tasks,
	}, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	_, err = os.Stdout.Write(body)
	return err
}

func readProjectSpec(path string) (orchestration.Validated, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return orchestration.Validated{}, err
	}
	return orchestration.Parse(source)
}

// shortDigest is the first 12 hex digits of a digest.
func shortDigest(digest string) string {
	digest = strings.TrimPrefix(digest, "sha256:")
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

const immutableHint = "a task's content is immutable once declared: give the changed task a new name, or use 'kairo task rerun' to run the same declaration again"

// projectApply: kairo project apply FILE [--json].
func projectApply(args []string) error {
	fs := newFlags("project apply")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("project apply requires a project TOML file")
	}
	validated, err := readProjectSpec(positional[0])
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/projects", validated.Manifest)
	if err != nil {
		if ae := (*apiError)(nil); errors.As(err, &ae) && ae.Status == http.StatusConflict {
			return fmt.Errorf("%w\nhint: %s", err, immutableHint)
		}
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var applied struct {
		Project    json.RawMessage `json:"project"`
		SpecDigest string          `json:"spec_digest"`
		Idempotent bool            `json:"idempotent"`
		Created    *[]string       `json:"created"`
		Unchanged  *int            `json:"unchanged"`
	}
	if err := json.Unmarshal(payload, &applied); err != nil {
		return fmt.Errorf("decode apply response: %w", err)
	}
	name := validated.Manifest.Project.Name
	var named string
	if json.Unmarshal(applied.Project, &named) == nil && named != "" {
		name = named
	}
	switch {
	case applied.Created == nil:
		// A daemon older than the summary fields.
		if applied.Idempotent {
			fmt.Printf("applied %s: no changes (idempotent)\n", name)
		} else {
			fmt.Printf("applied %s: %d tasks in the spec, digest %s\n", name, len(validated.Manifest.Tasks), shortDigest(applied.SpecDigest))
		}
	case len(*applied.Created) == 0 && (applied.Idempotent || applied.Unchanged == nil || *applied.Unchanged == len(validated.Manifest.Tasks)):
		fmt.Printf("applied %s: no changes (idempotent)\n", name)
	default:
		unchanged := len(validated.Manifest.Tasks) - len(*applied.Created)
		if applied.Unchanged != nil {
			unchanged = *applied.Unchanged
		}
		fmt.Printf("applied %s: %d new task(s), %d unchanged\n", name, len(*applied.Created), unchanged)
		for _, task := range *applied.Created {
			fmt.Printf("  + %s\n", task)
		}
	}
	return nil
}

// projectStatusCommand: kairo project status PROJECT [filters] prints the
// daemon's JSON as it is (scripts read it).
func projectStatusCommand(args []string) error {
	fs := newFlags("project status")
	api := apiFlag(fs)
	var tasks stringList
	fs.Var(&tasks, "task", "only this task (repeatable)")
	match := fs.String("match", "", "only tasks whose name starts with PREFIX")
	state := fs.String("state", "", "only tasks in these states (comma separated)")
	live := fs.Bool("live", false, "only pending and running tasks")
	since := fs.String("since", "", "only tasks updated since DURATION ago (e.g. 2h) or an RFC 3339 time")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("project status requires PROJECT")
	}
	q := projectQuery{tasks: tasks, match: *match, states: splitList(*state), live: *live}
	if q.since, err = sinceStamp(*since); err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	return client.print(http.MethodGet, q.path(positional[0]), nil)
}

func splitList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
