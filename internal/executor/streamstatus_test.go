package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/loader"
)

// The agent reads what each stream's loader of a container attempt wrote
// last, from the node's side of /kairo/run; a file not there or not whole is
// left out.
func TestTheAgentReadsWhatAttemptsLoadersSaid(t *testing.T) {
	e := &Local{LogDir: t.TempDir()}
	if got := e.streamStatuses("att_1"); got != nil {
		t.Fatalf("no container: %+v", got)
	}
	streams := filepath.Join(e.containerDir("att_1"), "run", "streams")
	for name, body := range map[string]string{"whole": "", "torn": `{"world":2,"ra`, "none": "-"} {
		if err := os.MkdirAll(filepath.Join(streams, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if body == "" {
			b, _ := json.Marshal(loader.Status{World: 1, Ranks: []loader.RankStatus{{Taken: 5}}})
			body = string(b)
		}
		if body != "-" {
			if err := os.WriteFile(filepath.Join(streams, name, StreamStatusFile), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := e.streamStatuses("att_1")
	if len(got) != 1 || got[0].Stream != "whole" || got[0].Status.World != 1 || got[0].Status.Ranks[0].Taken != 5 {
		t.Fatalf("read %+v", got)
	}
}
