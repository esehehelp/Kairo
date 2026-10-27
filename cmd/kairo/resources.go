package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"kairo/internal/api"
	"kairo/internal/store"
)

// resourceStatus is GET /api/resources.
type resourceStatus struct {
	Resources      []api.ResourceStatusEntry `json:"resources"`
	ExternalClaims []store.ExternalClaim     `json:"external_claims"`
	UndeclaredGPUs []store.UndeclaredGPU     `json:"undeclared_gpus"`
}

// printResources prints `kairo resource status` (and the end of `kairo
// doctor`): a table, or the daemon's response with asJSON.
func printResources(client *apiClient, asJSON bool) error {
	payload, err := client.call(http.MethodGet, "/api/resources", nil)
	if err != nil {
		return err
	}
	if asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var status resourceStatus
	if err := json.Unmarshal(payload, &status); err != nil {
		return fmt.Errorf("decode resource status: %w", err)
	}
	return renderResources(os.Stdout, status)
}

// kindOrder is how the table lists the resources of a node.
var kindOrder = map[string]int{"gpu": 0, "cpu": 1, "ram": 2, "disk": 3}

// renderResources is one row per resource, by node: its state, total, free
// (as last observed), what live leases reserve of it and who holds it; then
// the external claims and the GPUs reported but not declared.
func renderResources(w io.Writer, s resourceStatus) error {
	if len(s.Resources) == 0 {
		fmt.Fprintln(w, "no resources registered")
	} else {
		resources := append([]api.ResourceStatusEntry(nil), s.Resources...)
		sort.SliceStable(resources, func(i, j int) bool {
			a, b := resources[i], resources[j]
			if a.NodeID != b.NodeID {
				return a.NodeID < b.NodeID
			}
			ka, oka := kindOrder[a.Kind]
			kb, okb := kindOrder[b.Kind]
			if !oka {
				ka = len(kindOrder)
			}
			if !okb {
				kb = len(kindOrder)
			}
			if ka != kb {
				return ka < kb
			}
			return a.StableIdentity < b.StableIdentity
		})
		rows := []string{"NODE\tKIND\tRESOURCE\tSTATE\tTOTAL\tFREE\tRESERVED\tHELD BY"}
		for _, r := range resources {
			state := r.DerivedState
			if r.QuarantineReason != nil && *r.QuarantineReason != "" {
				state += " (" + *r.QuarantineReason + ")"
			}
			total, free, reserved := resourceAmounts(r)
			rows = append(rows, strings.Join([]string{r.NodeID, r.Kind, r.StableIdentity, state, total, free, reserved, dash(holdersText(r.Holders))}, "\t"))
		}
		if err := writeTable(w, rows); err != nil {
			return err
		}
	}
	if len(s.ExternalClaims) > 0 {
		names := map[string]string{}
		for _, r := range s.Resources {
			names[r.ID] = r.NodeID + " " + r.Kind + " " + r.StableIdentity
		}
		fmt.Fprintln(w, "\nexternal claims (not Kairo's; the resource is not scheduled while they last):")
		for _, c := range s.ExternalClaims {
			name := names[c.ResourceID]
			if name == "" {
				name = c.ResourceID
			}
			text := "  " + name + "  " + c.ClaimKind
			if e := evidenceText(c.Evidence); e != "" {
				text += " " + e
			}
			fmt.Fprintf(w, "%s  since %s\n", text, shortStamp(&c.FirstObservedAt))
		}
	}
	if len(s.UndeclaredGPUs) > 0 {
		fmt.Fprintln(w, "\nreported but not declared for the node (not scheduled; kairo machine show):")
		for _, gpu := range s.UndeclaredGPUs {
			declared := "declared for no node"
			if gpu.DeclaredNodeID != nil {
				declared = "declared for " + *gpu.DeclaredNodeID
			}
			fmt.Fprintf(w, "  %s  on %s (%s), %s\n", gpu.UUID, gpu.NodeID, gpu.ProviderID, declared)
		}
	}
	return nil
}

// resourceAmounts formats a resource's total, observed free and reserved
// amounts: a cpu in cores, the rest in bytes (a GPU's are its memory). A GPU
// is held whole, so it reserves nothing by amount.
func resourceAmounts(r api.ResourceStatusEntry) (total, free, reserved string) {
	format := func(v int64) string { return byteSize(v) }
	if r.Kind == "cpu" {
		format = func(v int64) string { return strconv.FormatFloat(float64(v)/1000, 'f', -1, 64) + " cores" }
	}
	total, free, reserved = "-", "-", "-"
	if r.TotalBytes != nil {
		total = format(*r.TotalBytes)
	}
	// A cpu's free amount is not observed (its provider reports the total).
	if r.FreeBytes != nil && r.Kind != "cpu" {
		free = format(*r.FreeBytes)
		if r.Kind == "gpu" && r.Utilization != nil {
			free += fmt.Sprintf(" (util %s%%)", number(*r.Utilization))
		}
	}
	if r.Kind != "gpu" && r.Reserved > 0 {
		reserved = format(r.Reserved)
	}
	return total, free, reserved
}

// holdersText names the tasks of a resource's live leases: "p/q/t", with
// the lease state unless active; more than two are counted.
func holdersText(holders []store.ResourceHolder) string {
	var parts []string
	for i, h := range holders {
		if i == 2 {
			parts = append(parts, fmt.Sprintf("+%d more", len(holders)-2))
			break
		}
		var path []string
		for _, name := range []string{h.Project, h.Queue, h.Task} {
			if name != "" {
				path = append(path, name)
			}
		}
		text := strings.Join(path, "/")
		if text == "" {
			text = h.ExecutionID
		}
		if h.LeaseState != "active" {
			text += " (" + h.LeaseState + ")"
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, ", ")
}

// evidenceText is a claim's evidence as "pid=4242 process_name=python": the
// identifying keys first, then the rest by name.
func evidenceText(raw json.RawMessage) string {
	var evidence map[string]any
	if json.Unmarshal(raw, &evidence) != nil || len(evidence) == 0 {
		return ""
	}
	delete(evidence, "gpu_uuid") // the resource column names it
	var parts []string
	for _, key := range []string{"pid", "process_name", "used_memory_bytes", "reason"} {
		if v, ok := evidence[key]; ok {
			if f, isNumber := v.(float64); isNumber && key == "used_memory_bytes" {
				parts = append(parts, "using "+byteSize(int64(f)))
			} else {
				parts = append(parts, key+"="+plain(v))
			}
			delete(evidence, key)
		}
	}
	keys := make([]string, 0, len(evidence))
	for k := range evidence {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, k+"="+plain(evidence[k]))
	}
	return strings.Join(parts, " ")
}
