package store

import (
	"slices"
	"strings"
	"testing"
)

// The data lifecycle's states are exactly those the database allows (the
// latest CHECK constraint of each column, migrations 0011-0025), every one
// can be reached from a record not there, and every change leads to a state
// of the record.
func TestTheDataLifecycleWalks(t *testing.T) {
	allowed := map[lifecycleRecord][]string{
		recObject:    {"packing", "packed", "abandoned"},
		recSource:    {"present", "deleting", "deleted", "kept", "evicting", "evicted"},
		recTransfer:  {"packing", "packed", "dropping"},
		recCopy:      {"copying", "verified"},
		recPlacement: {"fetching", "ready", "evicting"},
		recScratch:   {"active", "kept", "deleted", "missing", "deleting"},
		recRunOutput: {"active", "missing", "deleted"},
	}
	for r, states := range lifecycle {
		want, ok := allowed[r]
		if !ok {
			t.Fatalf("%s has no states the database allows", r)
		}
		var have []string
		for from, tos := range states {
			if from != noRecord {
				have = append(have, from)
			}
			for _, to := range tos {
				if to != noRecord && !slices.Contains(want, to) {
					t.Errorf("%s changes to %q, which the database does not allow", r, to)
				}
				if _, ok := states[to]; !ok && to != noRecord {
					t.Errorf("%s changes to %q, a state with no row in the table", r, to)
				}
			}
		}
		slices.Sort(have)
		slices.Sort(want)
		if !slices.Equal(have, want) {
			t.Errorf("%s states %v, the database allows %v", r, have, want)
		}
		// every state reached from a record not there
		reached := map[string]bool{noRecord: true}
		for frontier := []string{noRecord}; len(frontier) > 0; {
			from := frontier[0]
			frontier = frontier[1:]
			for _, to := range states[from] {
				if !reached[to] {
					reached[to] = true
					frontier = append(frontier, to)
				}
			}
		}
		for _, s := range want {
			if !reached[s] {
				t.Errorf("%s state %q cannot be reached", r, s)
			}
		}
	}
	if len(allowed) != len(lifecycle) {
		t.Fatalf("the lifecycle has %d records, the database %d", len(lifecycle), len(allowed))
	}
}

// What the lifecycle refuses is said, naming the record and both states;
// staying in a state is no change.
func TestTheDataLifecycleRefusesWhatItDoesNotHave(t *testing.T) {
	for _, c := range []struct {
		r        lifecycleRecord
		from, to string
	}{
		{recSource, SourceEvicted, SourcePresent}, // made again is another object
		{recSource, SourceDeleted, SourcePresent},
		{recScratch, "active", "deleted"}, // deleted only once claimed
		{recCopy, "verified", "copying"},
		{recObject, "packed", "packing"},
		{recRunOutput, "deleted", "active"},
	} {
		err := checkTransition(c.r, c.from, c.to)
		if err == nil || !strings.Contains(err.Error(), string(c.r)) {
			t.Errorf("%s %s -> %s: %v", c.r, c.from, c.to, err)
		}
	}
	if err := checkTransition(recSource, SourceKept, SourceKept); err != nil {
		t.Fatalf("staying kept: %v", err)
	}
	if err := checkAdopted(); err != nil {
		t.Fatal(err)
	}
}
