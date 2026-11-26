//go:build linux || windows

package provider

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/internal/cpupower"
	"kairo/internal/store"
)

// pmonUM is pmon -s um as a current driver prints it: two header lines,
// "-" where a process has no sample, command names with spaces.
const pmonUM = `# gpu         pid   type     sm    mem    enc    dec    jpg    ofa     fb   ccpm    command
# Idx           #    C/G      %      %      %      %      %      %     MB     MB    name
    0       2804   C+G      -      -      -      -      -      -      0      0    Discord.exe
    0       5960   C+G     %s      3      -      -      -      -      0      0    Game.exe
    0      17012   C+G      4      1      -      -      -      -      0      0    NVIDIA Overlay.
    0      32028   C+G     60     20      -      -      -      -      0      0    vmmemwslc-cli-a
    0       9000     C     55     10      -      -      -      -   4000      0    python
`

func ownerNVIDIA(t *testing.T, sm string, owner *OwnerUse) Snapshot {
	t.Helper()
	p := &NVIDIA{ProviderID: "p", NodeID: "n", Owner: owner,
		ProcessIdentity: func(pid int) (string, error) { return "pid:9000:start:x", nil },
		Command: func(_ context.Context, args ...string) ([]byte, error) {
			if args[0] == "pmon" {
				return []byte(strings.Replace(pmonUM, "%s", sm, 1)), nil
			}
			return queryAnswer(args, inventory("GPU-a, 0, 10240, 4000, 90, 60")), nil
		}}
	snap, err := p.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func ownerClaims(snap Snapshot) []store.ExternalClaim {
	var out []store.ExternalClaim
	for _, c := range snap.Claims {
		if c.ClaimKind == ClaimOwnerUse {
			out = append(out, c)
		}
	}
	return out
}

// writeInput writes the input reporter's record: the last input that long
// ago, written now.
func writeInput(t *testing.T, path string, ago time.Duration) {
	t.Helper()
	body, _ := json.Marshal(InputRecord{LastInput: time.Now().Add(-ago).UTC().Format(time.RFC3339Nano), Written: time.Now().UTC().Format(time.RFC3339Nano)})
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A GPU is its owner's while graphics processes use enough of its SMs:
// their shares summed, the WSL VM's (Kairo's work on Windows) and compute
// processes left out, the threshold lower while the owner is at the
// keyboard. The pmon columns are read from its header.
func TestAGPUIsItsOwnersWhileGraphicsUseItsSMs(t *testing.T) {
	input := filepath.Join(t.TempDir(), "input.json")
	owner := &OwnerUse{InputFile: input}
	if got := ownerClaims(ownerNVIDIA(t, "20", owner)); len(got) != 0 {
		t.Fatalf("24%% with the owner away (threshold 30): %+v", got)
	}
	got := ownerClaims(ownerNVIDIA(t, "40", owner))
	if len(got) != 1 || got[0].ID != "owner-gpu-a" || got[0].ResourceID != "gpu-a" {
		t.Fatalf("44%%: %+v", got)
	}
	var evidence struct {
		SM        float64 `json:"sm_percent"`
		Present   bool    `json:"owner_present"`
		Processes []ownerProcess
	}
	if err := json.Unmarshal(got[0].Evidence, &evidence); err != nil || evidence.SM != 44 || evidence.Present || len(evidence.Processes) != 2 ||
		evidence.Processes[0].Name != "Game.exe" || evidence.Processes[1].Name != "NVIDIA Overlay." {
		t.Fatalf("evidence %s %v", got[0].Evidence, err)
	}
	// at the keyboard: 10%
	writeInput(t, input, time.Minute)
	if got := ownerClaims(ownerNVIDIA(t, "8", owner)); len(got) != 1 {
		t.Fatalf("12%% with the owner at the keyboard: %+v", got)
	}
	// input long ago, or a record its reporter stopped writing: away
	writeInput(t, input, time.Hour)
	if got := ownerClaims(ownerNVIDIA(t, "8", owner)); len(got) != 0 {
		t.Fatalf("12%% with the owner away an hour: %+v", got)
	}
	writeInput(t, input, time.Minute)
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(input, old, old); err != nil {
		t.Fatal(err)
	}
	stale := &OwnerUse{InputFile: input, Now: func() time.Time { return time.Now().Add(time.Hour) }}
	if got := ownerClaims(ownerNVIDIA(t, "8", stale)); len(got) != 0 {
		t.Fatalf("a stale record kept the owner at the keyboard: %+v", got)
	}
	// off: nothing
	if got := ownerClaims(ownerNVIDIA(t, "90", &OwnerUse{Disabled: true})); len(got) != 0 {
		t.Fatalf("disabled: %+v", got)
	}
	if got := ownerClaims(ownerNVIDIA(t, "90", nil)); len(got) != 0 {
		t.Fatalf("no owner settings: %+v", got)
	}
	// the compute process is still a claim of its own
	snap := ownerNVIDIA(t, "0", nil)
	if len(snap.Claims) != 1 || snap.Claims[0].ClaimKind != "external_process" {
		t.Fatalf("claims %+v", snap.Claims)
	}
}

// The node's CPU is its owner's when its cores are busy and what keeps them
// so is not Kairo's: busy less Kairo's containers' share at least the
// threshold, and the cores at least 90% busy. Without Kairo's share the CPU
// is never its owner's (Kairo's own work would be taken for it). The RAM is
// its owner's when less is available than the floor.
func TestTheNodesCPUAndRAMAreItsOwnersWhenTaken(t *testing.T) {
	readings := []cpuTimes{}
	next := 0
	h := &Host{ProviderID: "h", NodeID: "n", Owner: &OwnerUse{}, Meter: noMeter{}, CPUTimes: func() (cpuTimes, error) {
		r := readings[next]
		next++
		return r, nil
	}}
	claimsOf := func(prev, cur cpuTimes, free int64) []store.ExternalClaim {
		t.Helper()
		readings, next = []cpuTimes{prev, cur}, 0
		h.cpuHave = false
		h.snapshot(64<<30, free)
		return h.snapshot(64<<30, free).Claims
	}
	base := cpuTimes{total: 1000, idle: 500, kairo: 100, kairoKnown: true}
	at := func(total, idle, kairo float64) cpuTimes {
		return cpuTimes{total: base.total + total, idle: base.idle + idle, kairo: base.kairo + kairo, kairoKnown: true}
	}
	// 95% busy, Kairo 30%: its owner 65%
	if got := claimsOf(base, at(100, 5, 30), 32<<30); len(got) != 1 || got[0].ResourceID != "n-cpu" {
		t.Fatalf("owner at 65%%: %+v", got)
	}
	// 95% busy, Kairo 80%: its owner 15%
	if got := claimsOf(base, at(100, 5, 80), 32<<30); len(got) != 0 {
		t.Fatalf("Kairo's own work: %+v", got)
	}
	// its owner 60%, the cores 70% busy: nothing waits
	if got := claimsOf(base, at(100, 30, 10), 32<<30); len(got) != 0 {
		t.Fatalf("cores not short: %+v", got)
	}
	// Kairo's share unknown
	unknown := at(100, 5, 0)
	unknown.kairoKnown = false
	if got := claimsOf(base, unknown, 32<<30); len(got) != 0 {
		t.Fatalf("Kairo's share unknown: %+v", got)
	}
	// RAM: the floor of 64 GiB is the larger of 2 GiB and 5%: 3.2 GiB
	if got := claimsOf(base, at(100, 50, 0), 3<<30); len(got) != 1 || got[0].ResourceID != "n-ram" {
		t.Fatalf("3 GiB available: %+v", got)
	}
	if got := claimsOf(base, at(100, 50, 0), 4<<30); len(got) != 0 {
		t.Fatalf("4 GiB available: %+v", got)
	}
	// the floor goes with the RAM's observation (admission leaves it free);
	// none without owner use
	h.CPUTimes = func() (cpuTimes, error) { return base, nil }
	if ev := string(h.snapshot(64<<30, 32<<30).Observations[1].Evidence); ev != `{"floor_bytes":3435973836}` {
		t.Fatalf("the RAM's evidence: %s", ev)
	}
	h.Owner = &OwnerUse{Disabled: true}
	if ev := string(h.snapshot(64<<30, 32<<30).Observations[1].Evidence); ev != `{}` {
		t.Fatalf("without owner use: %s", ev)
	}
}

type noMeter struct{}

func (noMeter) Read() (cpupower.Reading, bool) { return cpupower.Reading{}, false }
