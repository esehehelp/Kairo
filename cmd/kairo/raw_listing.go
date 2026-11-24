package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"time"

	"kairo/internal/orchestration"
)

// hfAPI is where Hugging Face repositories are listed (a test's server
// instead).
var hfAPI = "https://huggingface.co/api/"

// hfEntry is a file of a Hugging Face repository's tree listing.
type hfEntry struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Size int64  `json:"size"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs"`
}

var nextLink = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// resolveRawListings lists the files of each Hugging Face raw of m at its
// commit (those matching its patterns, by path, with each LFS file's size
// and sha256), as the apply sends them (Manifest.RawFiles).
func resolveRawListings(m *orchestration.Manifest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: time.Minute}
	for name, raw := range m.Raws {
		if raw.HF == nil {
			continue
		}
		h := *raw.HF
		kind := "datasets"
		if h.Type == "model" {
			kind = "models"
		}
		next := hfAPI + kind + "/" + h.Repo + "/tree/" + h.Revision + "?recursive=true"
		var files []orchestration.RawFile
		for pages := 0; next != ""; pages++ {
			if pages == 1000 {
				return fmt.Errorf("raw %s: %s lists more than 1000 pages", name, h.Repo)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
			if err != nil {
				return err
			}
			if token := os.Getenv("HF_TOKEN"); token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("raw %s: listing %s: %w", name, h.Repo, err)
			}
			var entries []hfEntry
			err = json.NewDecoder(resp.Body).Decode(&entries)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("raw %s: listing %s at %s: %s", name, h.Repo, h.Revision, resp.Status)
			}
			if err != nil {
				return fmt.Errorf("raw %s: listing %s: %w", name, h.Repo, err)
			}
			for _, e := range entries {
				if e.Type != "file" || !matchesAny(h.Files, e.Path) {
					continue
				}
				f := orchestration.RawFile{Name: e.Path, URL: orchestration.HFFileURL(h, e.Path), Size: e.Size}
				if e.LFS != nil {
					f.Size, f.SHA256 = e.LFS.Size, e.LFS.OID
				}
				files = append(files, f)
			}
			next = ""
			if m := nextLink.FindStringSubmatch(resp.Header.Get("Link")); m != nil {
				next = m[1]
			}
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
		if m.RawFiles == nil {
			m.RawFiles = map[string][]orchestration.RawFile{}
		}
		m.RawFiles[name] = files
		fmt.Fprintf(os.Stderr, "raw %s: %d files of %s at %s\n", name, len(files), h.Repo, h.Revision[:12])
	}
	return nil
}

func matchesAny(patterns []string, p string) bool {
	for _, pattern := range patterns {
		if ok, _ := path.Match(pattern, p); ok {
			return true
		}
	}
	return false
}
