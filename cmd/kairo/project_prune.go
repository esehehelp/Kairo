package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"kairo/internal/orchestration"
)

// liveTaskStates are the states in which a declared Task can still run.
var liveTaskStates = map[string]bool{"pending": true, "running": true, "paused": true}

var (
	taskBlockStart = regexp.MustCompile(`(?m)^\[\[tasks\]\][ \t]*\r?$`)
	taskBlockName  = regexp.MustCompile(`(?m)^name[ \t]*=[ \t]*"([^"]+)"`)
)

// pruneResult is what prune keeps of a ProjectSpec file.
type pruneResult struct {
	Text    string   // head + the kept [[tasks]] blocks, verbatim
	Kept    []string // sorted
	Live    []string // sorted
	Dropped int
}

// pruneSpec keeps the Tasks that can still run (a live state, or not declared yet) and every Task they depend
// on, transitively; the other Tasks' blocks are dropped. `apply` is additive, so a dropped Task is neither
// cancelled nor forgotten: its declaration and result stay in the daemon. Kept blocks are copied verbatim, so
// their digests do not change (checked by parsing the result).
func pruneSpec(source []byte, states map[string]string) (pruneResult, error) {
	validated, err := orchestration.Parse(source)
	if err != nil {
		return pruneResult{}, err
	}
	tasks := map[string]orchestration.Task{}
	for _, t := range validated.Manifest.Tasks {
		tasks[t.Name] = t
	}
	text := string(source)
	starts := taskBlockStart.FindAllStringIndex(text, -1)
	if len(starts) != len(tasks) {
		return pruneResult{}, fmt.Errorf("found %d [[tasks]] headers for %d tasks; prune needs one header line per task", len(starts), len(tasks))
	}
	head := text[:starts[0][0]]
	blocks := map[string]string{}
	order := []string{}
	for i, s := range starts {
		end := len(text)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		block := text[s[0]:end]
		m := taskBlockName.FindStringSubmatch(block)
		if m == nil {
			return pruneResult{}, fmt.Errorf("[[tasks]] block %d has no name line", i+1)
		}
		if _, ok := tasks[m[1]]; !ok {
			return pruneResult{}, fmt.Errorf("block name %q is not a task of the spec", m[1])
		}
		blocks[m[1]] = block
		order = append(order, m[1])
	}
	keep := map[string]bool{}
	var live, todo []string
	for _, name := range order {
		state, declared := states[name]
		if !declared || liveTaskStates[state] {
			live = append(live, name)
			todo = append(todo, name)
		}
	}
	for len(todo) > 0 {
		name := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		if keep[name] {
			continue
		}
		keep[name] = true
		todo = append(todo, tasks[name].DependsOn...)
	}
	var out strings.Builder
	out.WriteString(head)
	var kept []string
	for _, name := range order {
		if keep[name] {
			out.WriteString(blocks[name])
			kept = append(kept, name)
		}
	}
	sort.Strings(kept)
	sort.Strings(live)
	result := pruneResult{Text: strings.TrimRight(out.String(), "\r\n") + "\n", Kept: kept, Live: live, Dropped: len(order) - len(kept)}
	if len(kept) == 0 {
		return result, nil
	}
	pruned, err := orchestration.Parse([]byte(result.Text))
	if err != nil {
		return pruneResult{}, fmt.Errorf("pruned spec does not parse: %w", err)
	}
	for _, name := range kept {
		if pruned.TaskDigests[name] != validated.TaskDigests[name] {
			return pruneResult{}, fmt.Errorf("task %q changed its digest in the pruned spec", name)
		}
	}
	return result, nil
}

// projectPrune: `kairo project prune [--api URL] [--write] FILE` asks the daemon for the project's task states
// and rewrites FILE (with --write) to the Tasks that can still run and their dependencies.
func projectPrune(args []string) error {
	fs := flag.NewFlagSet("project prune", flag.ContinueOnError)
	api := apiFlag(fs)
	write := fs.Bool("write", false, "rewrite the file (default: only report)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("project prune requires a project TOML file")
	}
	path := fs.Arg(0)
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	validated, err := orchestration.Parse(source)
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	states, err := projectTaskStates(client, validated.Manifest.Project.Name)
	if err != nil {
		return err
	}
	result, err := pruneSpec(source, states)
	if err != nil {
		return err
	}
	fmt.Printf("tasks in file %d, live %d, kept with dependencies %d, dropped %d\n",
		len(result.Kept)+result.Dropped, len(result.Live), len(result.Kept), result.Dropped)
	for _, name := range result.Kept {
		state, ok := states[name]
		if !ok {
			state = "undeclared"
		}
		fmt.Printf("  %-10s %s\n", state, name)
	}
	if !*write {
		return nil
	}
	if len(result.Kept) == 0 {
		fmt.Println("no task can still run; the file is left as it is (a ProjectSpec needs at least one task)")
		return nil
	}
	if err := os.WriteFile(path, []byte(result.Text), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", path)
	return nil
}

func projectTaskStates(client *apiClient, project string) (map[string]string, error) {
	status, body, err := client.do(http.MethodGet, "/api/projects/"+url.PathEscape(project), nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return map[string]string{}, nil // nothing applied yet: every task is undeclared
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("project status: %w", httpError(status, body))
	}
	var parsed struct {
		Project struct {
			Tasks []struct {
				Name  string `json:"name"`
				State string `json:"state"`
			} `json:"tasks"`
		} `json:"project"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("project status: %w", err)
	}
	states := map[string]string{}
	for _, t := range parsed.Project.Tasks {
		states[t.Name] = t.State
	}
	return states, nil
}
