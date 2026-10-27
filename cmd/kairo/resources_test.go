package main

import (
	"net/http"
	"strings"
	"testing"
)

const resourcesPayload = `{"resources":[
 {"id":"pve0-disk-/srv","node_id":"pve0","provider_id":"fs","kind":"disk","stable_identity":"/srv","admin_state":"enabled","derived_state":"available","total_bytes":536870912000,"free_bytes":2147483648,"reserved":10737418240,
  "holders":[{"execution_id":"exe_1","lease_state":"active","quantity":10737418240,"project":"neo","queue":"bench","task":"lora-v4"}]},
 {"id":"pve0-gpu0","node_id":"pve0","provider_id":"nv","kind":"gpu","stable_identity":"GPU-aaaa","admin_state":"enabled","derived_state":"leased","total_bytes":25769803776,"free_bytes":4294967296,"utilization":97,
  "device":{"observed_at":"2026-10-27T01:05:00.000000000Z","power_w":289.4,"power_limit_w":320,"sm_clock_mhz":1905,"pstate":"P2","event_reasons":4,"max_power_limit_w":350},
  "holders":[{"execution_id":"exe_1","lease_state":"active","quantity":1,"project":"neo","queue":"bench","task":"lora-v4"}]},
 {"id":"pve0-cpu","node_id":"pve0","provider_id":"host","kind":"cpu","stable_identity":"logical-cpu","admin_state":"enabled","derived_state":"available","total_bytes":16000,"free_bytes":16000,"reserved":4500,
  "device":{"observed_at":"2026-10-27T01:05:00.000000000Z","power_w":42.311},
  "holders":[{"execution_id":"exe_1","lease_state":"active","quantity":4000,"project":"neo","queue":"bench","task":"lora-v4"},{"execution_id":"exe_2","lease_state":"reserved","quantity":500,"project":"neo","queue":"bench","task":"lora-v5"}]},
 {"id":"host-gpu0","node_id":"host","provider_id":"nv","kind":"gpu","stable_identity":"GPU-bbbb","admin_state":"quarantined","quarantine_reason":"fan noise","derived_state":"quarantined"},
 {"id":"pve0-gpu1","node_id":"pve0","provider_id":"nv","kind":"gpu","stable_identity":"GPU-cccc","admin_state":"enabled","derived_state":"conflicted","total_bytes":25769803776,"free_bytes":1073741824,"utilization":0,
  "device":{"observed_at":"2026-10-27T01:05:00.000000000Z","power_w":21.7,"pstate":"P8","event_reasons":2113}}
],
"external_claims":[{"id":"claim-1","resource_id":"pve0-gpu1","claim_kind":"external_process","evidence":{"pid":"4242","process_name":"python","gpu_uuid":"GPU-cccc"},"first_observed_at":"2026-10-27T01:02:03Z","last_observed_at":"2026-10-27T01:05:03Z"}],
"undeclared_gpus":[{"uuid":"GPU-dddd","node_id":"pve0","provider_id":"nv","first_observed_at":"2026-10-27T01:02:03Z","last_observed_at":"2026-10-27T01:05:03Z"}]}
`

func TestResourceStatusIsATableByNodeAndJSONOnRequest(t *testing.T) {
	server := fakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(resourcesPayload))
	}))
	defer server.Close()
	out, err := captureStdout(t, func() error { return run([]string{"resource", "status", "--api", server.URL}) })
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(out, "\n")
	// The header, then host before pve0, and per node GPUs, cpu, ram, disk.
	want := [][]string{
		{"NODE", "KIND", "RESOURCE", "STATE", "TOTAL", "FREE", "RESERVED", "POWER", "CLOCKS", "HELD", "BY"},
		{"host", "gpu", "GPU-bbbb", "quarantined", "(fan", "noise)", "-", "-", "-", "-", "-", "-"},
		{"pve0", "gpu", "GPU-aaaa", "leased", "24.0", "GiB", "4.0", "GiB", "(util", "97%)", "-", "289/320", "W", "1905", "MHz", "P2", "sw-power", "neo/bench/lora-v4"},
		{"pve0", "gpu", "GPU-cccc", "conflicted", "24.0", "GiB", "1.0", "GiB", "(util", "0%)", "-", "22", "W", "P8", "idle,hw-thermal,0x800", "-"},
		{"pve0", "cpu", "logical-cpu", "available", "16", "cores", "-", "4.5", "cores", "42", "W", "-", "neo/bench/lora-v4,", "neo/bench/lora-v5", "(reserved)"},
		{"pve0", "disk", "/srv", "available", "500.0", "GiB", "2.0", "GiB", "10.0", "GiB", "-", "-", "neo/bench/lora-v4"},
	}
	for i, fields := range want {
		if got := strings.Fields(lines[i]); strings.Join(got, " ") != strings.Join(fields, " ") {
			t.Fatalf("line %d = %q, want %q\n%s", i, lines[i], strings.Join(fields, " "), out)
		}
	}
	for _, part := range []string{
		"\nexternal claims (not Kairo's; the resource is not scheduled while they last):\n  pve0 gpu GPU-cccc  external_process pid=4242 process_name=python  since ",
		"\nreported but not declared for the node (not scheduled; kairo machine show):\n  GPU-dddd  on pve0 (nv), declared for no node\n",
	} {
		if !strings.Contains(out, part) {
			t.Fatalf("missing %q in\n%s", part, out)
		}
	}

	out, err = captureStdout(t, func() error { return run([]string{"resource", "status", "--json", "--api", server.URL}) })
	if err != nil || out != resourcesPayload {
		t.Fatalf("--json: %v\n%s", err, out)
	}
}
