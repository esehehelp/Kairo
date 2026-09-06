package main

import "testing"

func TestRemovedLegacyCommandsFallBackToUsage(t *testing.T) {
	for _, command := range []string{"plan", "resource-add", "resource-ready", "submit", "status", "pause", "resume"} {
		if err := run([]string{command}); err == nil {
			t.Fatalf("legacy command %q was accepted", command)
		}
	}
}

func TestV2CommandGroupsAreDispatched(t *testing.T) {
	for _, command := range []string{"execution", "project", "queue", "task", "resource"} {
		err := run([]string{command})
		if err == nil || err.Error() == usage().Error() {
			t.Fatalf("V2 command group %q was not dispatched: %v", command, err)
		}
	}
}

func TestServeRequiresConfiguration(t *testing.T) {
	if err := run([]string{"serve"}); err == nil {
		t.Fatal("serve without --config was accepted")
	}
}
