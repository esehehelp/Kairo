package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"kairo/internal/api"
	"kairo/internal/executor"
	"kairo/internal/orchestration"
	"kairo/internal/sourcesnap"
	"kairo/internal/store"
)

// snapshotSources makes a snapshot of each source a task of the manifest
// runs in (its paths under its root, relative to the project file's
// directory), sends the daemon those it lacks, and names them in the
// manifest's Snapshots, which bind the tasks this apply declares.
func snapshotSources(client *apiClient, specPath string, m *orchestration.Manifest) error {
	used := map[string]bool{}
	for _, t := range m.Tasks {
		if t.Execution.Source != "" {
			used[t.Execution.Source] = true
		}
	}
	if len(used) == 0 {
		return nil
	}
	names := make([]string, 0, len(used))
	for name := range used {
		names = append(names, name)
	}
	sort.Strings(names)
	tmp, err := os.MkdirTemp("", "kairo-source-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	host, _ := os.Hostname()
	m.Snapshots = map[string]string{}
	for _, name := range names {
		src := m.Sources[name]
		root := src.Root
		if root == "" {
			root = "."
		}
		if !filepath.IsAbs(root) {
			root = filepath.Join(filepath.Dir(specPath), root)
		}
		if abs, err := filepath.Abs(root); err == nil {
			root = abs
		}
		sp, err := executor.PackSource(context.Background(), root, src.Paths, src.Exclude, tmp)
		if err != nil {
			return fmt.Errorf("source %s (%s): %w", name, root, err)
		}
		meta := api.SourceUpload{ID: sp.ID, List: sp.List, ListSHA256: sp.ListSHA256, PackBytes: sp.PackBytes, PackSHA256: sp.PackSHA256,
			Files: sp.Files, ContentBytes: sp.ContentBytes, Skipped: sp.Skipped, Project: m.Project.Name, SourceName: name, OriginHost: host, OriginRoot: root}
		meta.GitHead, meta.GitDirty = gitState(root)
		stored, err := uploadSource(client, meta, sp.PackPath)
		if err != nil {
			return fmt.Errorf("source %s: %w", name, err)
		}
		m.Snapshots[name] = sp.ID
		how := "kept already"
		if stored {
			how = "sent"
		}
		fmt.Fprintf(os.Stderr, "source %s: snapshot %s (%d files, %s; %s)\n", name, sp.ID, sp.Files, byteSize(sp.ContentBytes), how)
	}
	return nil
}

// gitState is the git HEAD of dir and whether its work tree has changes
// ("" and nil when it is no git work tree): for reference only.
func gitState(dir string) (string, *bool) {
	head, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", nil
	}
	status, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output()
	if err != nil {
		return strings.TrimSpace(string(head)), nil
	}
	dirty := len(bytes.TrimSpace(status)) > 0
	return strings.TrimSpace(string(head)), &dirty
}

// uploadSource sends a snapshot the daemon does not keep yet (POST
// /api/sources, multipart: meta, then the pack); stored is false when it
// kept it already.
func uploadSource(client *apiClient, meta api.SourceUpload, packPath string) (bool, error) {
	if client.token == "" {
		return false, errNoToken
	}
	body, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	go func() {
		err := func() error {
			part, err := form.CreateFormField("meta")
			if err != nil {
				return err
			}
			if err = json.NewEncoder(part).Encode(meta); err != nil {
				return err
			}
			pack, err := os.Open(packPath)
			if err != nil {
				return err
			}
			defer pack.Close()
			part, err = form.CreateFormFile("pack", filepath.Base(packPath))
			if err != nil {
				return err
			}
			if _, err = io.Copy(part, pack); err != nil {
				return err
			}
			return form.Close()
		}()
		writer.CloseWithError(err)
	}()
	ctx := client.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/api/sources", body)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+client.token)
	resp, err := client.http.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return false, httpError(resp.StatusCode, payload)
	}
	return resp.StatusCode == http.StatusCreated, nil
}

// sourceList: kairo source list [--project P] [--limit N] [--json].
func sourceList(args []string) error {
	fs := newFlags("source list")
	apiURL := apiFlag(fs)
	project := fs.String("project", "", "only the snapshots of this project")
	limit := fs.Int("limit", 50, "at most this many (newest first)")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	client, err := newAPIClient(*apiURL)
	if err != nil {
		return err
	}
	query := url.Values{"limit": {fmt.Sprint(*limit)}}
	if *project != "" {
		query.Set("project", *project)
	}
	if *asJSON {
		return client.print(http.MethodGet, withQuery("/api/sources", query), nil)
	}
	var listed struct {
		Sources []store.SourceSnapshot `json:"sources"`
	}
	if err := client.callJSON(http.MethodGet, withQuery("/api/sources", query), nil, &listed); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tPROJECT\tSOURCE\tFILES\tSIZE\tGIT\tCREATED")
	for _, s := range listed.Sources {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", s.ID, dash(s.Project), dash(s.SourceName), s.Files, byteSize(s.ContentBytes), gitLabel(s), s.CreatedAt)
	}
	return w.Flush()
}

// sourceShow: kairo source show ID [--files] [--json].
func sourceShow(args []string) error {
	fs := newFlags("source show")
	apiURL := apiFlag(fs)
	files := fs.Bool("files", false, "list its files (path, bytes, sha256)")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("source show requires a snapshot id")
	}
	client, err := newAPIClient(*apiURL)
	if err != nil {
		return err
	}
	path := "/api/sources/" + url.PathEscape(positional[0])
	if *asJSON {
		return client.print(http.MethodGet, path, nil)
	}
	snap, err := getSource(client, positional[0])
	if err != nil {
		return err
	}
	fmt.Printf("snapshot %s\n", snap.ID)
	fmt.Printf("  project %s, source %s, made %s by %s\n", dash(snap.Project), dash(snap.SourceName), snap.CreatedAt, dash(snap.CreatedBy))
	fmt.Printf("  from %s:%s, git %s\n", dash(snap.OriginHost), dash(snap.OriginRoot), gitLabel(snap))
	fmt.Printf("  %d files, %s (pack %s, sha256 %s)", snap.Files, byteSize(snap.ContentBytes), byteSize(snap.PackBytes), snap.PackSHA256)
	if snap.Skipped > 0 {
		fmt.Printf("; %d links or special files not taken", snap.Skipped)
	}
	fmt.Println()
	fmt.Printf("  tasks bound to it: %d\n", len(snap.Tasks))
	for _, t := range snap.Tasks {
		fmt.Printf("    %s\n", t)
	}
	if *files {
		listed, err := sourcesnap.Parse(snap.List)
		if err != nil {
			return err
		}
		for _, f := range listed {
			fmt.Printf("  %s\t%s\t%s\n", f.Path, f.Bytes, f.SHA256)
		}
	}
	return nil
}

// sourceDiff: kairo source diff A B: the paths added, removed and changed
// from snapshot A to B.
func sourceDiff(args []string) error {
	fs := newFlags("source diff")
	apiURL := apiFlag(fs)
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return usageError("source diff requires two snapshot ids")
	}
	client, err := newAPIClient(*apiURL)
	if err != nil {
		return err
	}
	a, err := getSource(client, positional[0])
	if err != nil {
		return err
	}
	b, err := getSource(client, positional[1])
	if err != nil {
		return err
	}
	d, err := sourcesnap.Compare(a.List, b.List)
	if err != nil {
		return err
	}
	for _, p := range d.Added {
		fmt.Printf("+ %s\n", p)
	}
	for _, p := range d.Removed {
		fmt.Printf("- %s\n", p)
	}
	for _, p := range d.Changed {
		fmt.Printf("M %s\n", p)
	}
	fmt.Fprintf(os.Stderr, "%d added, %d removed, %d changed\n", len(d.Added), len(d.Removed), len(d.Changed))
	return nil
}

func getSource(client *apiClient, id string) (store.SourceSnapshot, error) {
	var got struct {
		Source store.SourceSnapshot `json:"source"`
	}
	err := client.callJSON(http.MethodGet, "/api/sources/"+url.PathEscape(id), nil, &got)
	return got.Source, err
}

func gitLabel(s store.SourceSnapshot) string {
	if s.GitHead == "" {
		return "-"
	}
	label := s.GitHead
	if len(label) > 12 {
		label = label[:12]
	}
	if s.GitDirty != nil && *s.GitDirty {
		label += " (changes not committed)"
	}
	return label
}
