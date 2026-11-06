package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

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
	asJSON := fs.Bool("json", false, "print the daemon's response")
	paths, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if *project == "" || *node == "" || len(paths) == 0 {
		return usageError("data import needs --project, --node and at least one PATH (an absolute path on the node)")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/data/imports", store.ImportRequest{Project: *project, Node: *node, Paths: paths, Name: *name, DeleteSource: *deleteSource})
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
	if *deleteSource {
		fmt.Println("each directory is deleted once its copy on a durable store is verified, if it still holds only what was packed (kairo data list)")
	} else {
		fmt.Println("the directories stay where they are; only the packs go once copied (kairo data list)")
	}
	return nil
}

// dataList: kairo data list [--project P] [--kind K] [--limit N] [--json].
func dataList(args []string) error {
	fs := newFlags("data list")
	api := apiFlag(fs)
	project := fs.String("project", "", "only this project's objects")
	kind := fs.String("kind", "", "only objects of this kind: output, scratch or import")
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
	fmt.Fprintln(w, "OBJECT\tKIND\tPROJECT\tNAME\tNODE\tSTATE\tFILES\tCONTENT\tPACK\tCOPIES\tSOURCE\tPATH")
	for _, o := range objects {
		files := "-"
		if o.Files != nil {
			files = strconv.FormatInt(*o.Files, 10)
		}
		source := o.SourceState
		if o.Kind == store.ObjectImport && o.DeleteSource && source == "present" {
			source = "present (delete once copied)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", o.ID, o.Kind, dash(valueOr(o.Project)), dash(valueOr(o.Name)), o.Node, o.State, files,
			optionalSize(o.ContentBytes), optionalSize(o.PackBytes), o.Copies, source, o.SourcePath)
	}
	w.Flush()
	for _, o := range objects {
		if o.SourceReason != nil && *o.SourceReason != "" {
			fmt.Fprintf(&b, "%s: %s\n", o.ID, *o.SourceReason)
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
