package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"kairo/internal/store"
)

// A read token reads the hardware history: its series with the step and
// window used, and the lease intervals; a bad window or step is refused.
func TestHardwareHistoryRoutes(t *testing.T) {
	d := newTestDaemon(t, false)
	ctx := context.Background()
	if err := d.store.UpsertNode(ctx, store.Node{ID: "n", Name: "n", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertProvider(ctx, "n-host", "n", "host", nil); err != nil {
		t.Fatal(err)
	}
	if err := d.store.UpsertResourceInstance(ctx, store.ResourceInstance{ID: "n-ram", NodeID: "n", ProviderID: "n-host", Kind: "ram", StableIdentity: "physical-memory", Binding: json.RawMessage(`{}`), AdminState: "enabled"}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Minute).UTC()
	total, free := int64(32<<30), int64(20<<30)
	if err := d.store.RecordObservation(ctx, store.Observation{ResourceID: "n-ram", ObservedAt: at.Format(time.RFC3339Nano), ValidUntil: at.Add(time.Minute).Format(time.RFC3339Nano), TotalBytes: &total, FreeBytes: &free}); err != nil {
		t.Fatal(err)
	}
	read := d.token(t, "dashboard", store.RoleRead, "")
	status, body := d.call(t, "GET", "/api/hardware/samples?key=n-ram&step=30s", read, nil)
	var samples store.HardwareSamples
	if status != http.StatusOK || json.Unmarshal(body, &samples) != nil {
		t.Fatalf("samples: %d %s", status, body)
	}
	if samples.StepSeconds < 43 || samples.Since == "" || samples.Until == "" {
		t.Fatalf("a day in at most %d buckets needs a step of 44 s at least, got %d (%s - %s)", maxPoints, samples.StepSeconds, samples.Since, samples.Until)
	}
	if len(samples.Series) != 1 || samples.Series[0].Key != "n-ram" || len(samples.Series[0].T) != 1 || *samples.Series[0].Metrics["used"][0] != float64(12<<30) {
		t.Fatalf("series %s", body)
	}
	if status, body = d.call(t, "GET", "/api/hardware/leases?since="+url.QueryEscape(time.Now().Add(-time.Hour).Format(time.RFC3339)), read, nil); status != http.StatusOK || !strings.Contains(string(body), `"leases":[]`) {
		t.Fatalf("leases: %d %s", status, body)
	}
	for _, bad := range []string{"step=fast", "since=yesterday", "since=" + url.QueryEscape(time.Now().Add(time.Hour).Format(time.RFC3339))} {
		if status, body = d.call(t, "GET", "/api/hardware/samples?"+bad, read, nil); status != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", bad, status, body)
		}
	}
}
