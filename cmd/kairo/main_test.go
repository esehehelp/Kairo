package main

import "testing"

func TestRemovedLegacyCommandsFallBackToUsage(t *testing.T) {
	for _, command := range []string{"resource-add", "resource-ready", "submit", "status", "pause", "resume"} {
		if err := run([]string{command}); err == nil {
			t.Fatalf("legacy command %q was accepted", command)
		}
	}
}

func TestServeRequiresConfiguration(t *testing.T) {
	if err := run([]string{"serve"}); err == nil {
		t.Fatal("serve without --config was accepted")
	}
}
