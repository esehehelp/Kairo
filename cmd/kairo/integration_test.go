package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/api"
	"kairo/internal/pgtest"
	"kairo/internal/store"
)

// The task commands against the real API and store: apply, list, show,
// cancel a task waiting for its dependency, wait, rerun.
func TestTaskCommandsAgainstTheRealAPI(t *testing.T) {
	isolateClientEnv(t)
	st, err := store.Open(pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	_, token, err := st.CreateAPIToken(ctx, "operator", store.RoleAdmin, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAIRO_TOKEN", token)
	server := httptest.NewServer((&api.Server{Store: st}).Handler())
	defer server.Close()
	cli := func(args ...string) (string, error) {
		t.Helper()
		return captureStdout(t, func() error { return run(append(args, "--api", server.URL)) })
	}

	path := filepath.Join(t.TempDir(), "p.toml")
	if err := os.WriteFile(path, []byte(applySpec), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := cli("project", "apply", path)
	if err != nil || out != "applied p: 2 new task(s), 0 unchanged\n  + t\n  + u\n" {
		t.Fatalf("apply: %v %q", err, out)
	}
	if out, err = cli("project", "apply", path); err != nil || out != "applied p: no changes (idempotent)\n" {
		t.Fatalf("apply again: %v %q", err, out)
	}
	if err := st.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	out, err = cli("task", "list", "p")
	if err != nil || !strings.Contains(out, "\nt ") || !strings.Contains(out, "\nu ") || !strings.HasSuffix(out, "p: 1 running, 1 pending (2 shown)\n") || !strings.Contains(out, "execution waiting") {
		t.Fatalf("list: %v\n%s", err, out)
	}
	out, err = cli("task", "show", "p", "u")
	if err != nil || !strings.Contains(out, "depends on:  t (running)\n") || !strings.Contains(out, "argv:        worker\n") {
		t.Fatalf("show: %v\n%s", err, out)
	}
	if out, err = cli("task", "show", "p", "t"); err != nil || !strings.Contains(out, "dependents:  u (pending)\n") || !strings.Contains(out, "execution #0 (initial)  waiting\n") {
		t.Fatalf("show t: %v\n%s", err, out)
	}
	if out, err = cli("task", "cancel", "p", "u"); err != nil || !strings.HasSuffix(out, "cancelled u\n") {
		t.Fatalf("cancel: %v %q", err, out)
	}
	useFakeClock(t, base)
	out, err = cli("task", "wait", "p", "q", "u")
	if exitCode(err) != waitFailed || !strings.Contains(out, "u cancelled") || !strings.Contains(out, "DONE not succeeded: u (cancelled)") {
		t.Fatalf("wait: %v\n%s", err, out)
	}
	if out, err = cli("task", "rerun", "p", "u"); err != nil || !strings.HasPrefix(out, "rerun u (execution #") {
		t.Fatalf("rerun: %v %q", err, out)
	}
	if _, err = cli("task", "rerun", "p", "t"); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("rerun of a pending task: %v", err)
	}
	if _, err = cli("task", "logs", "p", "nope"); err == nil || !strings.Contains(err.Error(), "task nope not found in project p") {
		t.Fatalf("logs of an unknown task: %v", err)
	}
	out, err = cli("version")
	if err != nil || !strings.Contains(out, "\ndaemon ") {
		t.Fatalf("version: %v %q", err, out)
	}
}
