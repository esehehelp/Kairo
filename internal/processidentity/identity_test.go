package processidentity

import (
	"errors"
	"reflect"
	"testing"

	"kairo/internal/conformance"
)

// The identity strings are compared byte for byte with what workers send, so
// they follow sdk/conformance/identity.json exactly, on every OS.
func TestIdentityConformance(t *testing.T) {
	var fixture struct {
		Linux []struct {
			PID    int     `json:"pid"`
			Stat   string  `json:"stat"`
			Expect *string `json:"expect"`
		} `json:"linux"`
		Windows []struct {
			PID      int    `json:"pid"`
			Filetime uint64 `json:"creation_filetime"`
			Expect   string `json:"expect"`
		} `json:"windows"`
	}
	conformance.Load(t, "identity.json", &fixture)
	if len(fixture.Linux) == 0 || len(fixture.Windows) == 0 {
		t.Fatal("identity.json has no cases")
	}
	for _, c := range fixture.Linux {
		got, err := LinuxIdentityFromStat(c.PID, c.Stat)
		switch {
		case c.Expect == nil && err == nil:
			t.Errorf("linux %q: got %q, want an error", c.Stat, got)
		case c.Expect != nil && (err != nil || got != *c.Expect):
			t.Errorf("linux %q: got %q, %v; want %q", c.Stat, got, err, *c.Expect)
		}
	}
	for _, c := range fixture.Windows {
		if got := WindowsIdentityFromFiletime(c.PID, c.Filetime); got != c.Expect {
			t.Errorf("windows %d: got %q, want %q", c.Filetime, got, c.Expect)
		}
	}
}

func TestLinuxIdentityFromStatHandlesSpacesAndParenthesesInComm(t *testing.T) {
	body := "42 (worker name) with ) paren) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 98765 20 21\n"
	got, err := LinuxIdentityFromStat(42, body)
	if err != nil {
		t.Fatal(err)
	}
	if want := "proc:42:starttime:98765"; got != want {
		t.Fatalf("identity = %q, want %q", got, want)
	}
}

func TestLinuxIdentityFromStatRejectsMalformedInput(t *testing.T) {
	for _, body := range []string{
		"42 (worker) S 1 2",
		"42 worker S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 98765 20 21",
		"42 (worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 -5 20 21",
		"42 (worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 x 20 21",
	} {
		if got, err := LinuxIdentityFromStat(42, body); err == nil {
			t.Errorf("%q: got %q, want an error", body, got)
		}
	}
}

func TestLinuxProcessFromStatReadsParentAndExit(t *testing.T) {
	for _, tc := range []struct {
		stat   string
		parent int
		exited bool
	}{
		{"19774 (maya) S 19770 19770 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 13733900 0 0", 19770, false},
		{"19774 (maya) Z 19770 19770 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 13733900 0 0", 19770, true},
	} {
		p, err := linuxProcessFromStat(19774, tc.stat)
		if err != nil || p.identity != "proc:19774:starttime:13733900" || p.parent != tc.parent || p.start != 13733900 || p.exited != tc.exited {
			t.Errorf("%q: %+v %v", tc.stat, p, err)
		}
	}
}

// fakeProcesses reads processes from a table, as /proc or the Windows process
// list would show them.
func fakeProcesses(table map[int]process) func(int) (process, error) {
	return func(pid int) (process, error) {
		p, ok := table[pid]
		if !ok {
			return process{}, errors.New("no such process")
		}
		return p, nil
	}
}

func TestLineageFollowsLivingParents(t *testing.T) {
	launcher := process{identity: "launcher", parent: 1, start: 100}
	table := map[int]process{
		1:  {identity: "init", start: 1},
		10: launcher,
		20: {identity: "maya", parent: 10, start: 200},
		30: {identity: "render", parent: 20, start: 300},
	}
	environ := func(pid int) ([]byte, error) {
		return []byte("PATH=/bin\x00KAIRO_ATTEMPT_ID_X=no\x00KAIRO_ATTEMPT_ID=att_1\x00"), nil
	}
	got := lineage(30, "render", fakeProcesses(table), environ)
	if want := (Lineage{AttemptID: "att_1", Ancestors: []string{"maya", "launcher", "init"}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("lineage = %+v, want %+v", got, want)
	}
	for name, change := range map[string]func(map[int]process){
		"a parent that started after its child (a reused pid)": func(m map[int]process) { m[10] = process{identity: "later", parent: 1, start: 250} },
		"an exited parent": func(m map[int]process) { m[10] = process{identity: "launcher", parent: 1, start: 100, exited: true} },
		"an unreadable parent": func(m map[int]process) { delete(m, 10) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := map[int]process{}
			for pid, p := range table {
				changed[pid] = p
			}
			change(changed)
			if got := lineage(30, "render", fakeProcesses(changed), nil); !reflect.DeepEqual(got.Ancestors, []string{"maya"}) {
				t.Fatalf("ancestors = %v, want the walk to stop at maya", got.Ancestors)
			}
		})
	}
}

func TestLineageFailsClosed(t *testing.T) {
	table := map[int]process{10: {identity: "launcher", start: 100}, 20: {identity: "maya", parent: 10, start: 200}}
	environ := func(int) ([]byte, error) { return []byte("KAIRO_ATTEMPT_ID=att_1"), nil }
	// pid 20 is no longer the process the claim names
	if got := lineage(20, "maya-before", fakeProcesses(table), environ); !reflect.DeepEqual(got, Lineage{}) {
		t.Fatalf("lineage of a replaced process: %+v", got)
	}
	// pid 20 was replaced while its environment was read: the environment may be the newcomer's
	reads := 0
	replacing := func(pid int) (process, error) {
		if reads++; pid == 20 && reads > 1 {
			return process{identity: "newcomer", start: 900}, nil
		}
		return fakeProcesses(table)(pid)
	}
	if got := lineage(20, "maya", replacing, environ); got.AttemptID != "" {
		t.Fatalf("attempt id read across a pid reuse: %+v", got)
	}
	// an exited process, and an environment that cannot be read
	exited := map[int]process{20: {identity: "maya", parent: 10, start: 200, exited: true}}
	if got := lineage(20, "maya", fakeProcesses(exited), environ); !reflect.DeepEqual(got, Lineage{}) {
		t.Fatalf("lineage of an exited process: %+v", got)
	}
	unreadable := func(int) ([]byte, error) { return nil, errors.New("permission denied") }
	if got := lineage(20, "maya", fakeProcesses(table), unreadable); got.AttemptID != "" || !reflect.DeepEqual(got.Ancestors, []string{"launcher"}) {
		t.Fatalf("lineage with an unreadable environment: %+v", got)
	}
	// a parent chain that loops ends
	loop := map[int]process{5: {identity: "self-parent", parent: 5, start: 1}}
	if got := lineage(5, "self-parent", fakeProcesses(loop), nil); len(got.Ancestors) != maxAncestors {
		t.Fatalf("looping chain: %d ancestors", len(got.Ancestors))
	}
}
