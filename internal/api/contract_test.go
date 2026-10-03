package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"kairo/internal/store"
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
	db, err := sql.Open("sqlite", d.path)
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

// busyError returns a real SQLITE_BUSY: a second connection that will not
// wait tries to write while another holds the write lock.
func busyError(t *testing.T) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "busy.db")
	open := func() *sql.Conn {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if _, err = conn.ExecContext(t.Context(), `PRAGMA busy_timeout = 0`); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	holder, waiter := open(), open()
	if _, err := holder.ExecContext(t.Context(), `CREATE TABLE t(x)`); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { holder.ExecContext(context.Background(), `ROLLBACK`) })
	_, err := waiter.ExecContext(t.Context(), `INSERT INTO t VALUES(1)`)
	if err == nil {
		t.Fatal("write under another connection's write lock succeeded")
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
		{"sqlite busy", fmt.Errorf("poll: %w", busyError(t)), http.StatusServiceUnavailable, "internal"},
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
