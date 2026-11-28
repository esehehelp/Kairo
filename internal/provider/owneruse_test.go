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

// ownerGPU is an NVIDIA provider whose pmon shows Game.exe using the share
// of the SMs *sm holds (beside NVIDIA Overlay. at 4%, the WSL VM, and a
// compute process).
func ownerGPU(owner *OwnerUse, sm *string) *NVIDIA {
	return &NVIDIA{ProviderID: "p", NodeID: "n", Owner: owner,
		ProcessIdentity: func(pid int) (string, error) { return "pid:9000:start:x", nil },
		Command: func(_ context.Context, args ...string) ([]byte, error) {
			if args[0] == "pmon" {
				return []byte(strings.Replace(pmonUM, "%s", *sm, 1)), nil
			}
			return queryAnswer(args, inventory("GPU-a, 0, 10240, 4000, 90, 60")), nil
		}}
}

// testClock is a test's clock (OwnerUse.Now).
type testClock struct{ now time.Time }

func (c *testClock) read() time.Time { return c.now }

// readings reads p every 5 s on c, Game.exe at each of shares in turn, and
// returns the last snapshot. With ago, the owner's last input was that long
// before each reading (the input reporter's record written then).
func (c *testClock) readings(t *testing.T, p *NVIDIA, sm *string, input string, ago time.Duration, shares ...string) Snapshot {
	t.Helper()
	var snap Snapshot
	for _, share := range shares {
		c.now = c.now.Add(5 * time.Second)
		if ago > 0 {
			writeInput(t, input, c.now, ago)
		}
		*sm = share
		var err error
		if snap, err = p.Observe(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return snap
}

// repeat is share n times.
func repeat(share string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = share
	}
	return out
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

// writeInput writes the input reporter's record: written then, the last
// input that long before.
func writeInput(t *testing.T, path string, written time.Time, ago time.Duration) {
	t.Helper()
	body, _ := json.Marshal(InputRecord{LastInput: written.Add(-ago).UTC().Format(time.RFC3339Nano), Written: written.UTC().Format(time.RFC3339Nano)})
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A GPU is its owner's while graphics processes use enough of its SMs over
// the last two minutes: their shares summed and averaged over the readings,
// the WSL VM's (Kairo's work on Windows) and compute processes left out.
// By default half of them, at the keyboard or not: a game, not a browser;
// a threshold set lower for the owner at the keyboard applies while they
// are. A desktop's bursts (a page drawn for a second) are not its owner's
// use, nor is a reading alone. The pmon columns are read from its header.
func TestAGPUIsItsOwnersWhileGraphicsUseItsSMs(t *testing.T) {
	input := filepath.Join(t.TempDir(), "input.json")
	c := &testClock{now: time.Now()}
	owner := &OwnerUse{InputFile: input, Now: c.read}
	sm := "0"

	// a game at 90% (94% with the overlay), the owner away: one reading
	// tells nothing, a minute of them does
	p := ownerGPU(owner, &sm)
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, "90")); len(got) != 0 {
		t.Fatalf("one reading: %+v", got)
	}
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, repeat("90", 11)...)); len(got) != 0 {
		t.Fatalf("55 seconds of readings: %+v", got)
	}
	got := ownerClaims(c.readings(t, p, &sm, input, 0, "90"))
	if len(got) != 1 || got[0].ID != "owner-gpu-a" || got[0].ResourceID != "gpu-a" {
		t.Fatalf("a minute at 94%%: %+v", got)
	}
	var evidence struct {
		SM        float64 `json:"sm_percent"`
		Threshold float64 `json:"threshold_percent"`
		Present   bool    `json:"owner_present"`
		Window    float64 `json:"window_seconds"`
		Processes []ownerProcess
	}
	if err := json.Unmarshal(got[0].Evidence, &evidence); err != nil || evidence.SM != 94 || evidence.Threshold != 50 || evidence.Present || evidence.Window != 120 ||
		len(evidence.Processes) != 2 || evidence.Processes[0].Name != "Game.exe" || evidence.Processes[0].SM != 90 || evidence.Processes[1].Name != "NVIDIA Overlay." {
		t.Fatalf("evidence %s %v", got[0].Evidence, err)
	}
	// the game ended: once the window holds its readings no more, nothing
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, repeat("0", 24)...)); len(got) != 0 {
		t.Fatalf("two minutes after the game: %+v", got)
	}

	// 44% for minutes, a browser at the keyboard: not the owner's by default
	p = ownerGPU(owner, &sm)
	if got := ownerClaims(c.readings(t, p, &sm, input, time.Minute, repeat("40", 30)...)); len(got) != 0 {
		t.Fatalf("44%% with the owner at the keyboard, by default: %+v", got)
	}

	// with gpu_sm_percent_present 25: not the owner's while away (50%),
	// theirs at the keyboard
	present := &OwnerUse{InputFile: input, Now: c.read, GPUSMPercentPresent: 25}
	p = ownerGPU(present, &sm)
	writeInput(t, input, c.now, time.Hour)
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, repeat("40", 30)...)); len(got) != 0 {
		t.Fatalf("44%% with the owner away: %+v", got)
	}
	if got := ownerClaims(c.readings(t, p, &sm, input, time.Minute, "40")); len(got) != 1 {
		t.Fatalf("44%% with the owner at the keyboard: %+v", got)
	}
	// input an hour ago, or a record its reporter stopped writing: away
	if got := ownerClaims(c.readings(t, p, &sm, input, time.Hour, "40")); len(got) != 0 {
		t.Fatalf("44%% with the owner away an hour: %+v", got)
	}
	writeInput(t, input, c.now.Add(-3*time.Minute), time.Second)
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, "40")); len(got) != 0 {
		t.Fatalf("a stale record kept the owner at the keyboard: %+v", got)
	}

	// a desktop at the keyboard: 94% for one reading a minute, the overlay's
	// 4% between (the old rule took each burst for the owner's use), under
	// 25% too
	p = ownerGPU(present, &sm)
	bursts := []string{}
	for i := 0; i < 36; i++ {
		bursts = append(bursts, map[bool]string{true: "90", false: "0"}[i%12 == 0])
	}
	if got := ownerClaims(c.readings(t, p, &sm, input, time.Minute, bursts...)); len(got) != 0 {
		t.Fatalf("a desktop's bursts: %+v", got)
	}

	// off: nothing
	off := &OwnerUse{Disabled: true, Now: c.read}
	p = ownerGPU(off, &sm)
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, repeat("90", 30)...)); len(got) != 0 {
		t.Fatalf("disabled: %+v", got)
	}
	p = ownerGPU(nil, &sm)
	if got := ownerClaims(c.readings(t, p, &sm, input, 0, repeat("90", 30)...)); len(got) != 0 {
		t.Fatalf("no owner settings: %+v", got)
	}
	// the compute process is still a claim of its own
	snap := c.readings(t, p, &sm, input, 0, "0")
	if len(snap.Claims) != 1 || snap.Claims[0].ClaimKind != "external_process" {
		t.Fatalf("claims %+v", snap.Claims)
	}
}

// The node's CPU is its owner's when its cores are busy over the last two
// minutes and what keeps them so is not Kairo's: busy less Kairo's
// containers' share at least the threshold, and the cores at least 90%
// busy. Readings over less than a minute tell nothing, a burst of seconds
// is not its owner's use, and a counter that went back starts the window
// over. Without Kairo's share the CPU is never its owner's (Kairo's own
// work would be taken for it). The RAM is never its owner's (who took it is
// not known): its observation tells the floor and since when less than it
// is available.
func TestTheNodesCPUIsItsOwnersWhenTaken(t *testing.T) {
	readings := []cpuTimes{}
	next := 0
	c := &testClock{now: time.Now()}
	h := &Host{ProviderID: "h", NodeID: "n", Owner: &OwnerUse{Now: c.read, InputFile: filepath.Join(t.TempDir(), "input.json")}, Meter: noMeter{}, CPUTimes: func() (cpuTimes, error) {
		r := readings[next]
		next++
		return r, nil
	}}
	// claimsOf reads each of rs, after each the time in steps (none before
	// the first), and returns the last reading's claims
	claimsOf := func(steps []time.Duration, free int64, rs ...cpuTimes) []store.ExternalClaim {
		t.Helper()
		readings, next = rs, 0
		h.cpuReadings.readings = nil
		var claims []store.ExternalClaim
		for i := range rs {
			if i > 0 {
				c.now = c.now.Add(steps[i-1])
			}
			claims = h.snapshot(64<<30, free).Claims
		}
		return claims
	}
	minute := []time.Duration{time.Minute, time.Minute}
	base := cpuTimes{total: 1000, idle: 500, kairo: 100, kairoKnown: true}
	at := func(total, idle, kairo float64) cpuTimes {
		return cpuTimes{total: base.total + total, idle: base.idle + idle, kairo: base.kairo + kairo, kairoKnown: true}
	}
	// 95% busy, Kairo 30%: its owner 65%
	if got := claimsOf(minute, 32<<30, base, at(100, 5, 30)); len(got) != 1 || got[0].ResourceID != "n-cpu" {
		t.Fatalf("owner at 65%%: %+v", got)
	}
	// the same over 5 seconds only: nothing yet
	if got := claimsOf([]time.Duration{5 * time.Second}, 32<<30, base, at(100, 5, 30)); len(got) != 0 {
		t.Fatalf("5 seconds: %+v", got)
	}
	// 5% busy for 55 seconds, then 95% for 5: a burst, 12.5% over the minute
	if got := claimsOf([]time.Duration{55 * time.Second, 5 * time.Second}, 32<<30, base, at(1100, 1045, 0), at(1200, 1050, 0)); len(got) != 0 {
		t.Fatalf("a burst: %+v", got)
	}
	// Kairo's counter went back (its VM started again): the window starts
	// over at that reading
	if got := claimsOf(minute, 32<<30, base, at(100, 5, -50)); len(got) != 0 {
		t.Fatalf("a counter that went back: %+v", got)
	}
	if got := claimsOf(minute, 32<<30, base, at(100, 5, -50), at(200, 10, -50)); len(got) != 1 {
		t.Fatalf("a minute after the counter went back: %+v", got)
	}
	// 95% busy, Kairo 80%: its owner 15%
	if got := claimsOf(minute, 32<<30, base, at(100, 5, 80)); len(got) != 0 {
		t.Fatalf("Kairo's own work: %+v", got)
	}
	// its owner 60%, the cores 70% busy: nothing waits
	if got := claimsOf(minute, 32<<30, base, at(100, 30, 10)); len(got) != 0 {
		t.Fatalf("cores not short: %+v", got)
	}
	// Kairo's share unknown
	unknown := at(100, 5, 0)
	unknown.kairoKnown = false
	if got := claimsOf(minute, 32<<30, base, unknown); len(got) != 0 {
		t.Fatalf("Kairo's share unknown: %+v", got)
	}
	// RAM: the floor of 64 GiB is the larger of 2 GiB and 5%: 3.2 GiB; under
	// it, no claim
	if got := claimsOf(minute, 3<<30, base, at(100, 50, 0)); len(got) != 0 {
		t.Fatalf("3 GiB available: %+v", got)
	}
	// the floor goes with the RAM's observation (admission leaves it free),
	// and under it, since when (the first observation under it); none
	// without owner use
	h.CPUTimes = func() (cpuTimes, error) { return base, nil }
	if ev := string(h.snapshot(64<<30, 32<<30).Observations[1].Evidence); ev != `{"floor_bytes":3435973836}` {
		t.Fatalf("the RAM's evidence: %s", ev)
	}
	var first, second struct {
		Floor int64  `json:"floor_bytes"`
		Since string `json:"under_floor_since"`
	}
	_ = json.Unmarshal(h.snapshot(64<<30, 3<<30).Observations[1].Evidence, &first)
	time.Sleep(time.Millisecond)
	_ = json.Unmarshal(h.snapshot(64<<30, 3<<30).Observations[1].Evidence, &second)
	if first.Floor != 3435973836 || first.Since == "" || second.Since != first.Since {
		t.Fatalf("under the floor: %+v then %+v", first, second)
	}
	if ev := string(h.snapshot(64<<30, 4<<30).Observations[1].Evidence); ev != `{"floor_bytes":3435973836}` {
		t.Fatalf("over the floor again: %s", ev)
	}
	_ = json.Unmarshal(h.snapshot(64<<30, 3<<30).Observations[1].Evidence, &second)
	if second.Since == first.Since {
		t.Fatalf("under the floor again: still since %s", second.Since)
	}
	h.Owner = &OwnerUse{Disabled: true}
	if ev := string(h.snapshot(64<<30, 32<<30).Observations[1].Evidence); ev != `{}` {
		t.Fatalf("without owner use: %s", ev)
	}
}

type noMeter struct{}

func (noMeter) Read() (cpupower.Reading, bool) { return cpupower.Reading{}, false }
