package main

import (
	"strings"
	"testing"
)

func TestCommandGroupsAreDispatched(t *testing.T) {
	isolateClientEnv(t)
	t.Setenv("KAIRO_API", "http://127.0.0.1:1")
	for _, command := range []string{"execution", "project", "queue", "task", "resource", "node", "token", "tls", "db", "doctor"} {
		err := run([]string{command})
		if err == nil || err.Error() == usage().Error() {
			t.Fatalf("command group %q was not dispatched: %v", command, err)
		}
	}
	if err := run([]string{"nonsense"}); err == nil || err.Error() != usage().Error() {
		t.Fatalf("unknown command: %v", err)
	}
}

func TestProjectUsageListsEverySubcommand(t *testing.T) {
	for _, args := range [][]string{{"project"}, {"project", "nonsense"}} {
		err := run(args)
		if err == nil || !strings.Contains(err.Error(), "validate|apply|status|prune|pause|resume") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestServeRequiresConfiguration(t *testing.T) {
	if err := run([]string{"serve"}); err == nil {
		t.Fatal("serve without --config was accepted")
	}
}
