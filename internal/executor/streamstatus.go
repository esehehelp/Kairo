package executor

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"kairo/internal/loader"
	"kairo/internal/store"
)

// Stream status (issue #43): the loader of each stream an attempt reads
// writes its status to /kairo/run/streams/<name>/StreamStatusFile (kairo
// loader serve --status, every few seconds); the agent reads it from the
// node's side with each round of usage samples and reports it
// (store.RecordAttemptStreams), for task show.

// StreamStatusFile is the file a stream's loader writes its status to, in
// the stream's directory under /kairo/run/streams.
const StreamStatusFile = "status.json"

// streamStatusLimit bounds a status file read.
const streamStatusLimit = 1 << 20

// streamStatuses is the status each stream's loader of a container attempt
// wrote last (none: nil). A file not there yet or not whole is left out.
func (e *Local) streamStatuses(attemptID string) []store.AttemptStream {
	dir := filepath.Join(e.containerDir(attemptID), "run", "streams")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []store.AttemptStream
	for _, d := range entries {
		if !d.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(dir, d.Name(), StreamStatusFile))
		if err != nil {
			continue
		}
		body, err := io.ReadAll(io.LimitReader(f, streamStatusLimit))
		f.Close()
		var st loader.Status
		if err != nil || json.Unmarshal(body, &st) != nil {
			continue
		}
		out = append(out, store.AttemptStream{Stream: d.Name(), Status: st})
	}
	return out
}
