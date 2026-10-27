package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// declareHost registers node "work" and the controller's node "ctl", declares
// host "h" over "work" with "ctl" as its controller, and returns ctl's token.
func declareHost(t *testing.T, d *testDaemon) string {
	t.Helper()
	ctx := context.Background()
	if err := d.store.UpsertNode(ctx, store.Node{ID: "work", Name: "work", OS: "linux", Architecture: "amd64", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	token := d.token(t, "ctl", store.RoleNode, "ctl")
	status, body := d.call(t, "POST", "/api/agent/controller-register", token, map[string]any{"node": store.Node{ID: "ctl", Name: "ctl", OS: "linux", Architecture: "amd64"}, "version": "test"})
	if status != http.StatusOK {
		t.Fatalf("controller register: %d %s", status, body)
	}
	spec := machine.Spec{Hosts: []machine.Host{{ID: "h", Nodes: []string{"work"}, Controller: "ctl", MaxSlowdown: 0.05}}}
	if status, body = d.call(t, "POST", "/api/machines", d.admin, spec); status != http.StatusOK {
		t.Fatalf("machine apply: %d %s", status, body)
	}
	return token
}

func TestControllerPollsItsDevicesAndReports(t *testing.T) {
	d := newTestDaemon(t, false)
	token := declareHost(t, d)
	status, body := d.call(t, "POST", "/api/agent/device-settings", token, map[string]string{"node_id": "ctl"})
	var poll struct {
		Devices []store.ControlledDevice `json:"devices"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &poll) != nil || len(poll.Devices) != 1 || poll.Devices[0].DeviceID != "cpu" {
		t.Fatalf("poll: %d %s", status, body)
	}
	// Another node's token may neither poll nor report for ctl.
	other := d.token(t, "work", store.RoleNode, "work")
	if status, _ = d.call(t, "POST", "/api/agent/device-settings", other, map[string]string{"node_id": "ctl"}); status != http.StatusForbidden {
		t.Fatalf("poll for another node: %d", status)
	}
	maxFreq := 5000
	report := store.DeviceReport{HostID: "h", DeviceID: "cpu", Applied: &store.DeviceSettings{}, Capabilities: &store.DeviceCapabilities{Settings: []string{store.SettingMaxFreq}, MaxFreqMaxMHz: &maxFreq}}
	if status, body = d.call(t, "POST", "/api/agent/device-report", token, map[string]any{"node_id": "ctl", "reports": []store.DeviceReport{report}}); status != http.StatusOK {
		t.Fatalf("report: %d %s", status, body)
	}
	// The operator sets a manual setting; the controller sees it.
	status, body = d.call(t, "POST", "/api/hardware/h/cpu/settings", d.admin, map[string]any{"settings": map[string]int{"max_freq_mhz": 4000}})
	var result store.DeviceSettingResult
	if status != http.StatusOK || json.Unmarshal(body, &result) != nil || result.Generation != 1 || result.Effective.Owner.Ref != "operator" {
		t.Fatalf("set: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/hardware/h/cpu/settings", d.admin, map[string]any{"settings": map[string]int{"max_freq_mhz": 9000}}); status != http.StatusBadRequest || !strings.Contains(string(body), "outside the device's range") {
		t.Fatalf("out of range: %d %s", status, body)
	}
	if status, body = d.call(t, "POST", "/api/hardware/nowhere/cpu/settings", d.admin, map[string]any{"settings": map[string]int{"max_freq_mhz": 4000}}); status != http.StatusNotFound {
		t.Fatalf("unknown host: %d %s", status, body)
	}
	_, body = d.call(t, "POST", "/api/agent/device-settings", token, map[string]string{"node_id": "ctl"})
	if json.Unmarshal(body, &poll) != nil || poll.Devices[0].Settings.MaxFreqMHz == nil || *poll.Devices[0].Settings.MaxFreqMHz != 4000 {
		t.Fatalf("poll after set: %s", body)
	}
	status, body = d.call(t, "GET", "/api/hardware?host=h", d.admin, nil)
	var hardware struct {
		Hosts []store.HardwareHost `json:"hosts"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &hardware) != nil || len(hardware.Hosts) != 1 || hardware.Hosts[0].Devices[0].Effective == nil || hardware.Hosts[0].ControllerVersion == nil {
		t.Fatalf("hardware: %d %s", status, body)
	}
	if status, body = d.call(t, "DELETE", "/api/hardware/h/cpu/settings", d.admin, nil); status != http.StatusOK || !strings.Contains(string(body), `"cleared":true`) {
		t.Fatalf("reset: %d %s", status, body)
	}
	// A read token may look but not set.
	reader := d.token(t, "dashboard", store.RoleRead, "")
	if status, _ = d.call(t, "GET", "/api/hardware", reader, nil); status != http.StatusOK {
		t.Fatalf("read hardware: %d", status)
	}
	if status, _ = d.call(t, "POST", "/api/hardware/h/cpu/settings", reader, map[string]any{"settings": map[string]int{"max_freq_mhz": 4000}}); status != http.StatusForbidden {
		t.Fatalf("read token set: %d", status)
	}
}

func TestAnObserveOnlyDaemonGivesAControllerNothingToChange(t *testing.T) {
	d := newTestDaemon(t, true)
	token := declareHost(t, d)
	status, body := d.call(t, "POST", "/api/agent/device-settings", token, map[string]string{"node_id": "ctl"})
	if status != http.StatusOK || !strings.Contains(string(body), `"devices":[]`) {
		t.Fatalf("observe-only poll: %d %s", status, body)
	}
}
