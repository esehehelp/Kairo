package processidentity

import (
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
