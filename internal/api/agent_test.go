package api

import (
	"database/sql"
	"net/http"
	"testing"
	"time"

	"kairo/internal/store"
)

func TestAgentRegistrationSetsBootTimeFromUptime(t *testing.T) {
	d := newTestDaemon(t, false)
	token := d.token(t, "agent", store.RoleNode, "remote")
	body := map[string]any{"node": store.Node{ID: "remote", Name: "remote", OS: "linux", Architecture: "amd64"}, "uptime_seconds": 3600.25}
	db, err := sql.Open("pgx", d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	register := func() string {
		t.Helper()
		if status, response := d.call(t, http.MethodPost, "/api/agent/register", token, body); status != http.StatusOK {
			t.Fatalf("register: %d %s", status, response)
		}
		var bootedAt string
		if err := db.QueryRow(`SELECT booted_at FROM nodes WHERE id='remote'`).Scan(&bootedAt); err != nil {
			t.Fatal(err)
		}
		return bootedAt
	}
	const uptime = time.Hour + 250*time.Millisecond
	before := time.Now().Add(-uptime)
	stamp := register()
	after := time.Now().Add(-uptime)
	bootedAt, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil || bootedAt.Before(before) || bootedAt.After(after) {
		t.Fatalf("booted_at = %q (%v); want between %s and %s", stamp, err, before, after)
	}
	delete(body, "uptime_seconds")
	if got := register(); got != stamp {
		t.Fatalf("registration without uptime changed booted_at: %q -> %q", stamp, got)
	}
}
