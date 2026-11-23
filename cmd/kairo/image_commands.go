package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"kairo/internal/imageref"
	"kairo/internal/orchestration"
)

// resolveImages pins each image a task of the manifest runs in to the
// digest its reference names now (asking its registry; a reference with a
// digest names it already) and names them in the manifest's ImagePins,
// which bind the tasks this apply declares (README: Containers).
func resolveImages(m *orchestration.Manifest) error {
	used := map[string]bool{}
	for _, t := range m.Tasks {
		if t.Execution.Image != "" {
			used[t.Execution.Image] = true
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: time.Minute}
	m.ImagePins = map[string]string{}
	for _, name := range names {
		ref, err := imageref.Parse(m.Images[name].Ref)
		if err != nil {
			return fmt.Errorf("image %s: %w", name, err)
		}
		pinned, err := imageref.Resolve(ctx, client, ref)
		if err != nil {
			return fmt.Errorf("image %s: %w", name, err)
		}
		m.ImagePins[name] = pinned.Pinned()
		fmt.Fprintf(os.Stderr, "image %s: %s is %s\n", name, ref, pinned.Digest)
	}
	return nil
}
