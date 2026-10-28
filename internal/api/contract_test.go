package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kairo/internal/pgtest"
	"kairo/internal/store"

	"github.com/jackc/pgx/v5/pgconn"
)

// These tests pin what the SDK worker scenarios (sdk/conformance) assume of
// the server, against the real store.

type wireError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// expectError fails unless the response is status with the wire code.
func expectError(t *testing.T, what string, status int, payload []byte, wantStatus int, wantCode string) wireError {
	t.Helper()
	var e wireError
	if status != wantStatus || json.Unmarshal(payload, &e) != nil || e.Code != wantCode || e.Error == "" {
		t.Fatalf("%s: %d %s, want %d %s", what, status, payload, wantStatus, wantCode)
	}
	return e
}

func TestHealthReportsTheAPIVersion(t *testing.T) {
	d := newTestDaemon(t, false)
	status, payload := d.call(t, http.MethodGet, "/health", "", nil)
	var body struct {
		OK  bool `json:"ok"`
		API *int `json:"api"`
	}
	if status != http.StatusOK || json.Unmarshal(payload, &body) != nil || !body.OK || body.API == nil || *body.API != APIVersion {
		t.Fatalf("health: %d %s", status, payload)
	}
	if APIVersion != 1 {
		t.Fatalf("APIVersion = %d; changing it breaks every client", APIVersion)
	}
}

func TestWorkerAckContract(t *testing.T) {
	d := newTestDaemon(t, false)
	launch := d.workerLaunch(t)
	token := launch.WorkerToken
	ack := func(commandPath, phase string, payload any) (int, []byte) {
		body := map[string]any{"phase": phase}
		if payload != nil {
			body["payload"] = payload
		}
		return d.call(t, http.MethodPost, "/api/worker/commands/"+commandPath+"/acks", token, body)
	}

	status, payload := ack("cmd_unknown", "accepted", nil)
	expectError(t, "ack of an unknown command", status, payload, http.StatusNotFound, "not_found")

	commandID, err := d.store.EnqueueSuspend(t.Context(), launch.Attempt.ID, "scope_pause", "test")
	if err != nil {
		t.Fatal(err)
	}
	status, payload = d.call(t, http.MethodGet, "/api/worker/commands", token, nil)
	if status != http.StatusOK || !strings.Contains(string(payload), `"id":"`+commandID+`"`) {
		t.Fatalf("poll: %d %s", status, payload)
	}

	// The router percent-decodes the command id.
	escaped := fmt.Sprintf("%%%02X%s", commandID[0], commandID[1:])
	if status, payload = ack(escaped, "accepted", nil); status != http.StatusAccepted {
		t.Fatalf("accepted (as %s): %d %s", escaped, status, payload)
	}
	status, payload = ack(commandID, "checkpointed", map[string]any{})
	if e := expectError(t, "checkpointed without continuation_ref", status, payload, http.StatusBadRequest, "bad_request"); !strings.Contains(e.Error, "continuation_ref") {
		t.Fatalf("checkpointed without continuation_ref: %s", payload)
	}
	status, payload = ack(commandID, "checkpointed", nil)
	expectError(t, "checkpointed without payload", status, payload, http.StatusBadRequest, "bad_request")
	if status, payload = ack(commandID, "rejected", map[string]any{"reason": "not now"}); status != http.StatusAccepted {
		t.Fatalf("rejected after accepted: %d %s", status, payload)
	}
	status, payload = ack(commandID, "bogus", nil)
	expectError(t, "unknown phase", status, payload, http.StatusBadRequest, "bad_request")
}

func TestWorkerTokenIsRetiredOnceTheAttemptQuiesced(t *testing.T) {
	d := newTestDaemon(t, false)
	launch := d.workerLaunch(t)
	ctx := t.Context()
	if err := d.store.RecordTerminal(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch, 0, ""); err != nil {
		t.Fatal(err)
	}
	// Between exit and quiescence the token still authenticates.
	if status, payload := d.call(t, http.MethodGet, "/api/whoami", launch.WorkerToken, nil); status != http.StatusOK {
		t.Fatalf("whoami after exit: %d %s", status, payload)
	}
	if err := d.store.FinalizeQuiescence(ctx, launch.Attempt.ID, launch.Lease.ID, launch.Lease.CoordinationEpoch); err != nil {
		t.Fatal(err)
	}
	status, payload := d.call(t, http.MethodGet, "/api/worker/commands", launch.WorkerToken, nil)
	expectError(t, "poll after quiescence", status, payload, http.StatusUnauthorized, "unauthorized")
	status, payload = d.call(t, http.MethodPost, "/api/worker/heartbeat", launch.WorkerToken, map[string]any{"progress": map[string]any{}})
	expectError(t, "heartbeat after quiescence", status, payload, http.StatusUnauthorized, "unauthorized")
}

func TestWorkerTokenIsFencedWhenTheExecutorIsUnknown(t *testing.T) {
	d := newTestDaemon(t, false)
	launch := d.workerLaunch(t)
	if lost, err := d.store.MarkExecutorUnknown(t.Context(), "executor"); err != nil || lost == 0 {
		t.Fatalf("fence: %d %v", lost, err)
	}
	status, payload := d.call(t, http.MethodGet, "/api/worker/commands", launch.WorkerToken, nil)
	expectError(t, "poll after fence", status, payload, http.StatusUnauthorized, "unauthorized")
}

// A database failure is the daemon's problem, not the request's: it is
// reported as code "internal", never as 400 bad_request.
func TestDatabaseFailureIsInternal(t *testing.T) {
	d := newTestDaemon(t, false)
	token := d.workerToken(t)
	db, err := sql.Open("pgx", d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`DROP TABLE attempt_tokens`); err != nil {
		t.Fatal(err)
	}
	status, payload := d.call(t, http.MethodGet, "/api/worker/commands", token, nil)
	expectError(t, "poll with a broken database", status, payload, http.StatusInternalServerError, "internal")
}

// lockTimeoutError returns a real PostgreSQL lock_not_available (55P03): a
// second connection that will not wait asks for a lock another holds.
func lockTimeoutError(t *testing.T) error {
	t.Helper()
	db, err := sql.Open("pgx", pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { holder.Rollback() })
	if _, err = holder.Exec(`SELECT pg_advisory_xact_lock(1)`); err != nil {
		t.Fatal(err)
	}
	waiter, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { waiter.Rollback() })
	if _, err = waiter.Exec(`SET LOCAL lock_timeout = '10ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = waiter.Exec(`SELECT pg_advisory_xact_lock(1)`)
	if err == nil {
		t.Fatal("a lock held by another transaction was granted")
	}
	return err
}

// unreachableError returns a real connection failure: nothing listens.
func unreachableError(t *testing.T) error {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://kairo@127.0.0.1:1/kairo?connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.PingContext(t.Context())
	if err == nil {
		t.Fatal("connected to a port nothing listens on")
	}
	return err
}

func TestErrorStatusClassification(t *testing.T) {
	for _, c := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"store sentinel", fmt.Errorf("ack: %w", store.ErrNotFound), http.StatusNotFound, "not_found"},
		{"validation", errors.New("invalid command acknowledgement phase"), http.StatusBadRequest, "bad_request"},
		{"lock timeout", fmt.Errorf("poll: %w", lockTimeoutError(t)), http.StatusServiceUnavailable, "internal"},
		{"database unreachable", fmt.Errorf("poll: %w", unreachableError(t)), http.StatusServiceUnavailable, "internal"},
		{"database error", fmt.Errorf("poll: %w", &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}), http.StatusInternalServerError, "internal"},
		{"deadline", fmt.Errorf("poll: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, "internal"},
		{"canceled", context.Canceled, http.StatusServiceUnavailable, "internal"},
		{"transaction done", sql.ErrTxDone, http.StatusInternalServerError, "internal"},
	} {
		recorder := httptest.NewRecorder()
		writeError(recorder, c.err)
		var e wireError
		if recorder.Code != c.status || json.Unmarshal(recorder.Body.Bytes(), &e) != nil || e.Code != c.code || e.Error != c.err.Error() {
			t.Errorf("%s (%v): %d %s, want %d %s", c.name, c.err, recorder.Code, recorder.Body, c.status, c.code)
		}
	}
}
