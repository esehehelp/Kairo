package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"kairo/internal/store"
)

// projectMerge moves everything of a project into another (POST
// /api/projects/FROM/merge; README: Merging projects): its tasks and their
// history, its data objects, ids unchanged. A task name both have needs
// --rename OLD=NEW; --dry-run tells what would move.
func projectMerge(args []string) error {
	fs := newFlags("project merge")
	api := apiFlag(fs)
	actor := fs.String("actor", defaultActor(), "who acts")
	dryRun := fs.Bool("dry-run", false, "tell what would move, moving nothing")
	var renames stringList
	fs.Var(&renames, "rename", "OLD=NEW: give FROM's task OLD, a name INTO has too, the name NEW (repeatable)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return usageError("project merge takes FROM and INTO")
	}
	named := map[string]string{}
	for _, r := range renames {
		old, renamed, ok := strings.Cut(r, "=")
		if !ok || old == "" || renamed == "" {
			return usageError("--rename takes OLD=NEW, not %q", r)
		}
		named[old] = renamed
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	var result store.MergeResult
	body := map[string]any{"into": positional[1], "renames": named, "dry_run": *dryRun, "actor": *actor}
	if err = client.callJSON(http.MethodPost, "/api/projects/"+url.PathEscape(positional[0])+"/merge", body, &result); err != nil {
		return err
	}
	verb := "merged"
	if result.DryRun {
		verb = "would merge"
	}
	fmt.Printf("%s %s into %s: %d tasks, %d executions, %d data objects\n", verb, result.From, result.Into, result.Tasks, result.Executions, result.Objects)
	queues := make([]string, 0, len(result.Queues))
	for name := range result.Queues {
		queues = append(queues, name)
	}
	sort.Strings(queues)
	for _, name := range queues {
		fmt.Printf("queue %s: %s\n", name, result.Queues[name])
	}
	for old, renamed := range result.Renamed {
		fmt.Printf("task %s renamed %s\n", old, renamed)
	}
	for _, name := range result.SharedDataNames {
		fmt.Printf("data name %s: both had it; a data input naming it reads the newest of either\n", name)
	}
	return nil
}
