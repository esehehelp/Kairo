package executor

import (
	"slices"
	"testing"
)

func TestParseAttemptMembers(t *testing.T) {
	pids, found, err := parseAttemptMembers(" 12:345 67:8\n")
	if err != nil || !slices.Equal(pids, []int{12, 67}) || found[12] != "345" || found[67] != "8" {
		t.Fatalf("%v %v %v", pids, found, err)
	}
	if pids, found, err = parseAttemptMembers("\n"); err != nil || len(pids) != 0 || len(found) != 0 {
		t.Fatalf("nothing: %v %v %v", pids, found, err)
	}
	for _, bad := range []string{"12", "12:", "x:3", "12:y", "sh: syntax error"} {
		if _, _, err := parseAttemptMembers(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}
