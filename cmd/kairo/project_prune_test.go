package main

import (
	"strings"
	"testing"

	"kairo/internal/orchestration"
)

const pruneSpecSource = `# header comment stays
schema_version = 1

[project]
name = "p"

[defaults]
queue = "eval"
policy = "RunToCompletion@v1"

[defaults.execution]
schema_version = 2
cwd = "."

[defaults.execution.capacity]
cpu_millis = 1000
ram_bytes = 1048576
strength = "admitted"

[[tasks]]
name = "old"

[tasks.execution]
argv = ["echo", "old"]

[[tasks]]
name = "export"

[tasks.execution]
argv = ["echo", "export"]

[[tasks]]
name = "bench"
depends_on = ["export"]

[tasks.execution]
argv = ["echo", "bench"]

[[tasks]]
name = "new"

[tasks.execution]
argv = ["echo", "new"]
`

func TestPruneKeepsLiveTasksTheirDependenciesAndDigests(t *testing.T) {
	// bench runs (its succeeded dependency export stays), new was never applied, old is done
	states := map[string]string{"old": "succeeded", "export": "succeeded", "bench": "running"}
	got, err := pruneSpec([]byte(pruneSpecSource), states)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Kept, ",") != "bench,export,new" || strings.Join(got.Live, ",") != "bench,new" || got.Dropped != 1 {
		t.Fatalf("kept %v live %v dropped %d", got.Kept, got.Live, got.Dropped)
	}
	if strings.Contains(got.Text, `name = "old"`) || !strings.HasPrefix(got.Text, "# header comment stays") {
		t.Fatalf("pruned text:\n%s", got.Text)
	}
	before, _ := orchestration.Parse([]byte(pruneSpecSource))
	after, err := orchestration.Parse([]byte(got.Text))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range got.Kept {
		if before.TaskDigests[name] != after.TaskDigests[name] {
			t.Fatalf("digest of %s changed", name)
		}
	}
}

func TestPruneWithNothingLiveKeepsNothing(t *testing.T) {
	states := map[string]string{"old": "succeeded", "export": "succeeded", "bench": "failed", "new": "cancelled"}
	got, err := pruneSpec([]byte(pruneSpecSource), states)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kept) != 0 || got.Dropped != 4 {
		t.Fatalf("kept %v dropped %d", got.Kept, got.Dropped)
	}
}
