package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"kairo/internal/store"
)

func TestExecutionEnvIsSubmittedAndShown(t *testing.T) {
	d := newTestDaemon(t, false)
	body := submitBody("with-env")
	body["env"] = map[string]string{"PYTHONUTF8": "1"}
	status, payload := d.call(t, http.MethodPost, "/api/executions", d.admin, body)
	var submitted struct {
		Execution store.ExecutionRequest `json:"execution"`
	}
	if status != http.StatusCreated || json.Unmarshal(payload, &submitted) != nil || submitted.Execution.Env["PYTHONUTF8"] != "1" {
		t.Fatalf("submit returned %d: %s", status, payload)
	}
	status, payload = d.call(t, http.MethodGet, "/api/executions/"+submitted.Execution.ID, d.admin, nil)
	if status != http.StatusOK || !bytes.Contains(payload, []byte(`"env":{"PYTHONUTF8":"1"}`)) {
		t.Fatalf("show returned %d: %s", status, payload)
	}
	body = submitBody("kairo-env")
	body["env"] = map[string]string{"KAIRO_TOKEN": "x"}
	if status, payload = d.call(t, http.MethodPost, "/api/executions", d.admin, body); status != http.StatusBadRequest {
		t.Fatalf("KAIRO_ env accepted: %d %s", status, payload)
	}
}
