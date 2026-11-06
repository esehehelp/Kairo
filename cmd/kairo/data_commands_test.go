package main

import "testing"

// A POSIX path Git Bash rewrote into its own directory is told apart from a
// Windows path the user meant.
func TestMsysRewrittenPaths(t *testing.T) {
	t.Setenv("MSYSTEM", "MINGW64")
	for p, want := range map[string]bool{
		"C:/Program Files/Git/srv/kairo-import-smoke": true,
		`C:\Program Files\Git\root\data`:              true,
		"D:/Dev/neo-ime/local/nvme-datasets":          false,
		"F:/Strata-data":                              false,
		"/srv/models":                                 false,
	} {
		if got := msysRewritten(p); got != want {
			t.Fatalf("%s: %v, want %v", p, got, want)
		}
	}
	t.Setenv("MSYSTEM", "")
	if msysRewritten("C:/Program Files/Git/srv/x") {
		t.Fatal("outside Git Bash nothing is rewritten")
	}
}
