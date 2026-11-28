package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"kairo/internal/store"
)

// dataImport: kairo data import --project P --node N [--name NAME]
// [--delete-source] [--json] PATH...
func dataImport(args []string) error {
	fs := newFlags("data import")
	api := apiFlag(fs)
	project := fs.String("project", "", "the project the data belongs to (required)")
	node := fs.String("node", "", "the node the directories are on (required)")
	name := fs.String("name", "", "the object's name (with several directories, NAME/<directory>; default the directory's name)")
	deleteSource := fs.Bool("delete-source", false, "delete each directory once its copy on a durable store is verified (only on the volatile store's filesystem)")
	regenerableBy := fs.String("regenerable-by", "", "the directories are made by this task of the project (declared regenerable): temporal data, not copied to a durable store")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	paths, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if *project == "" || *node == "" || len(paths) == 0 {
		return usageError("data import needs --project, --node and at least one PATH (an absolute path on the node)")
	}
	for _, p := range paths {
		if msysRewritten(p) {
			return usageError("%s looks like a POSIX path Git Bash rewrote into its own directory: run with MSYS_NO_PATHCONV=1 (or write //srv/...)", p)
		}
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/data/imports", store.ImportRequest{Project: *project, Node: *node, Paths: paths, Name: *name, DeleteSource: *deleteSource, RegenerableBy: *regenerableBy})
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not import data: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Objects []store.ImportedObject `json:"objects"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode imports: %w", err)
	}
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tNAME\tPATH\tPACKED ON\tBY")
	for _, o := range body.Objects {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", o.ObjectID, o.Name, o.Path, o.StoreID, o.ExecutorID)
	}
	w.Flush()
	fmt.Print(b.String())
	switch {
	case *regenerableBy != "":
		fmt.Println("temporal data: only each directory's identity (its file list) is kept; it stays where it is, made again by task " + *regenerableBy + " when needed (kairo data list)")
	case *deleteSource:
		fmt.Println("each directory is deleted once its copy on a durable store is verified, if it still holds only what was packed (kairo data list)")
	default:
		fmt.Println("the directories stay where they are; only the packs go once copied (kairo data list)")
	}
	return nil
}

// dataCancel: kairo data cancel OBJECT...: abandon imports not packed yet.
func dataCancel(args []string) error {
	fs := newFlags("data cancel")
	api := apiFlag(fs)
	objects, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return usageError("data cancel takes the objects to cancel")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	for _, objectID := range objects {
		if _, err = client.call(http.MethodPost, "/api/data/objects/"+url.PathEscape(objectID)+"/cancel", map[string]string{}); err != nil {
			return fmt.Errorf("%s: %w", objectID, err)
		}
		fmt.Println(objectID, "abandoned")
	}
	return nil
}

// dataRelease: kairo data release [--all] [--confirm] [--json] [OBJECT...].
func dataRelease(args []string) error {
	fs := newFlags("data release")
	api := apiFlag(fs)
	all := fs.Bool("all", false, "every kept run output and scratch")
	confirm := fs.Bool("confirm", false, "release them: without it, nothing changes")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	objects, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(objects) == 0 && !*all {
		return usageError("data release needs the objects, or --all")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/data/releases", store.ReleaseRequest{Objects: objects, All: *all, Confirm: *confirm})
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not release sources: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Sources []store.ReleasedSource `json:"sources"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode releases: %w", err)
	}
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tKIND\tSTORE\tSIZE\tPATH\tKEPT BECAUSE\tSTATE")
	var bytes int64
	released, refused := 0, 0
	for _, s := range body.Sources {
		state := "to release"
		switch {
		case s.Refused != "":
			state, refused = "not released: "+s.Refused, refused+1
		case s.Gone && *confirm:
			state = "settled deleted (its node is retired)"
		case s.Gone:
			state = "to settle deleted (its node is retired)"
		case *confirm:
			state = "released: its executor deletes it"
		}
		if s.Refused == "" {
			released++
			bytes += s.Bytes
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.ObjectID, s.Kind, s.StoreID, formatSize(s.Bytes), s.Path, strings.TrimPrefix(s.Kept, "not deleted: "), state)
	}
	w.Flush()
	fmt.Print(b.String())
	if *confirm {
		fmt.Printf("%d released (%s), %d not\n", released, formatSize(bytes), refused)
	} else {
		fmt.Printf("%d to release (%s), %d not; nothing changed: run again with --confirm to release them (what their packs do not hold is lost)\n", released, formatSize(bytes), refused)
	}
	return nil
}

// dataDrop: kairo data drop --project P [--match NAME_PREFIX] [--task-match
// TASK_PREFIX] [--reason TEXT] [--confirm] [--json] [OBJECT|NAME...].
func dataDrop(args []string) error {
	fs := newFlags("data drop")
	api := apiFlag(fs)
	project := fs.String("project", "", "the project the data belongs to (required)")
	match := fs.String("match", "", "the objects whose name starts with this")
	taskMatch := fs.String("task-match", "", "the outputs of the tasks whose name starts with this")
	reason := fs.String("reason", "", "why (recorded)")
	confirm := fs.Bool("confirm", false, "drop them: without it, nothing changes")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	objects, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if *project == "" || len(objects) == 0 && *match == "" && *taskMatch == "" {
		return usageError("data drop needs --project and the objects (ids or names), --match or --task-match")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/data/drops", store.DropRequest{Project: *project, Objects: objects, NamePrefix: *match, TaskPrefix: *taskMatch, Reason: *reason, Confirm: *confirm})
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not drop data: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Objects []store.DroppedObject `json:"objects"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode drops: %w", err)
	}
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tKIND\tNAME OR TASK\tPACK\tCOPIES\tBROUGHT TO\tSTATE")
	var bytes int64
	dropping, refused := 0, 0
	for _, o := range body.Objects {
		what := o.Name
		if what == "" {
			what = o.Task
		}
		state := "to drop"
		switch {
		case o.Refused != "":
			state, refused = "kept: "+o.Refused, refused+1
		case o.Dropped && *confirm:
			state = "dropped"
		case o.Dropped:
			state = "dropped already"
		}
		if o.Refused == "" {
			dropping++
			bytes += o.PackBytes
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\n", o.ObjectID, o.Kind, what, formatSize(o.PackBytes), o.Copies, o.Placements, state)
	}
	w.Flush()
	fmt.Print(b.String())
	if *confirm {
		fmt.Printf("%d dropped (%s packed), %d kept: only their file lists stay; their copies and the places they were brought to are deleted as their agents get to them\n", dropping, formatSize(bytes), refused)
	} else {
		fmt.Printf("%d to drop (%s packed), %d kept; nothing changed: run again with --confirm to drop them\n", dropping, formatSize(bytes), refused)
	}
	return nil
}

// dataList lists data objects, optionally narrowed to a task or execution's outputs.
func dataList(args []string) error {
	fs := newFlags("data list")
	api := apiFlag(fs)
	project := fs.String("project", "", "only this project's objects")
	kind := fs.String("kind", "", "only objects of this kind: output, scratch or import")
	task := fs.String("task", "", "only outputs of tasks with this name")
	execution := fs.String("execution", "", "only outputs of the run this execution belongs to")
	limit := fs.Int("limit", 200, "at most this many objects, newest first")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("data list accepts no arguments")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	query := url.Values{"limit": {strconv.Itoa(*limit)}}
	if *project != "" {
		query.Set("project", *project)
	}
	if *kind != "" {
		query.Set("kind", *kind)
	}
	if *task != "" {
		query.Set("task", *task)
	}
	if *execution != "" {
		query.Set("execution", *execution)
	}
	payload, err := client.call(http.MethodGet, withQuery("/api/data/objects", query), nil)
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not list data objects: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Objects []store.DataObject `json:"objects"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode data objects: %w", err)
	}
	fmt.Print(formatDataObjects(body.Objects))
	return nil
}

// formatDataObjects is one line an object: what it was made from, its pack,
// its verified copies and what became of its source.
func formatDataObjects(objects []store.DataObject) string {
	if len(objects) == 0 {
		return "no data objects\n"
	}
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tKIND\tLIFECYCLE\tPROJECT\tNAME\tNODE\tSTATE\tFILES\tCONTENT\tPACK\tCOPIES\tSOURCE\tPATH")
	for _, o := range objects {
		files := "-"
		if o.Files != nil {
			files = strconv.FormatInt(*o.Files, 10)
		}
		source := o.SourceState
		if o.Kind == store.ObjectImport && o.DeleteSource && source == "present" {
			source = "present (delete once copied)"
		}
		if o.RegeneratedAs != nil {
			source += " (made again as " + *o.RegeneratedAs + ")"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", o.ID, o.Kind, dash(o.Lifecycle), dash(valueOr(o.Project)), dash(valueOr(o.Name)), o.Node, o.State, files,
			optionalSize(o.ContentBytes), optionalSize(o.PackBytes), o.Copies, source, o.SourcePath)
	}
	w.Flush()
	for _, o := range objects {
		if o.SourceReason != nil && *o.SourceReason != "" {
			fmt.Fprintf(&b, "%s: %s\n", o.ID, *o.SourceReason)
		}
		if o.RegenerationProblem != nil {
			fmt.Fprintf(&b, "%s: %s\n", o.ID, *o.RegenerationProblem)
		}
	}
	return b.String()
}

// valueOr is *p, or "" for nil.
func valueOr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// dataFetch orders objects brought to a node and can wait for their paths.
func dataFetch(args []string) error {
	fs := newFlags("data fetch")
	api := apiFlag(fs)
	project := fs.String("project", "", "the project the data belongs to (required)")
	node := fs.String("node", "", "the node to bring it to (required)")
	wait := fs.Bool("wait", false, "wait until the objects are unpacked and print their paths")
	timeout := fs.Duration("timeout", 0, "give up waiting after this long, exit 2 (0: never)")
	var names stringList
	fs.Var(&names, "data", "a data object by name (the newest packed); repeatable")
	objects, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if *project == "" || *node == "" || len(names)+len(objects) == 0 {
		return usageError("data fetch needs --project, --node and --data NAME or an OBJECT id")
	}
	if *timeout < 0 || (*timeout != 0 && !*wait) {
		return usageError("data fetch --timeout must be non-negative and used with --wait")
	}
	var client *apiClient
	if *wait {
		var stop context.CancelFunc
		_, client, stop, err = newInterruptibleClient(*api)
		if err == nil {
			defer stop()
		}
	} else {
		client, err = newAPIClient(*api)
	}
	if err != nil {
		return err
	}
	var body struct {
		Objects []store.FetchedObject `json:"objects"`
	}
	err = client.callJSON(http.MethodPost, "/api/data/fetches", store.FetchRequest{Project: *project, Node: *node, Data: names, Objects: objects}, &body)
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not bring data to nodes: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *wait {
		return waitForFetches(client, *node, *timeout, body.Objects)
	}
	for _, o := range body.Objects {
		state := "ordered"
		if !o.Ordered {
			state = "there or on its way already"
		}
		fmt.Println(o.ObjectID + ": " + state)
	}
	return nil
}

func waitForFetches(client *apiClient, node string, timeout time.Duration, pending []store.FetchedObject) error {
	ctx := client.ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		client.ctx = ctx
	}
	stopped := func() error {
		if ctx.Err() == context.DeadlineExceeded {
			return withCode(fmt.Errorf("timed out waiting for data on node %s", node), waitTimeout)
		}
		return exitWith(130)
	}
	query := url.Values{"node": {node}}
	for len(pending) != 0 {
		still := pending[:0]
		for _, o := range pending {
			var status store.PlacementStatus
			path := withQuery("/api/data/objects/"+url.PathEscape(o.ObjectID)+"/placement", query)
			err := client.callJSON(http.MethodGet, path, nil, &status)
			switch {
			case ctx.Err() != nil:
				return stopped()
			case routeMissing(err):
				return fmt.Errorf("the daemon does not report fetch status: %s", olderDaemonHint())
			case err != nil:
				return err
			}
			switch status.State {
			case "ready":
				fmt.Println(o.ObjectID + ": " + status.Path)
			case "fetching":
				still = append(still, o)
			default:
				problem := status.Problem
				if problem == "" {
					problem = status.State
				}
				return fmt.Errorf("fetch %s on node %s: %s", o.ObjectID, node, problem)
			}
		}
		pending = still
		if len(pending) > 0 && !sleepFunc(ctx, 2*time.Second) {
			return stopped()
		}
	}
	return nil
}

// msysRewritten reports whether p is a POSIX path (/srv/x) that Git Bash
// (MSYS) turned into a path under its own install directory
// (C:/Program Files/Git/srv/x) before the CLI saw it.
func msysRewritten(p string) bool {
	if os.Getenv("MSYSTEM") == "" {
		return false
	}
	q := strings.ToLower(strings.ReplaceAll(p, `\`, "/"))
	i := strings.Index(q, "/git/")
	if len(q) < 3 || q[1] != ':' || i < 0 {
		return false
	}
	rest := q[i+len("/git/"):]
	for _, top := range []string{"srv/", "root/", "home/", "var/", "mnt/", "opt/", "tmp/", "data/", "media/"} {
		if strings.HasPrefix(rest, top) {
			return true
		}
	}
	return false
}
