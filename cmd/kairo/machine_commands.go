package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"

	"kairo/internal/machine"
	"kairo/internal/store"
)

// machineApply: kairo machine apply FILE [--dry-run] [--json].
func machineApply(args []string) error {
	fs := newFlags("machine apply")
	api := apiFlag(fs)
	dryRun := fs.Bool("dry-run", false, "print the changes without applying them")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("machine apply requires a machines TOML file")
	}
	body, err := os.ReadFile(positional[0])
	if err != nil {
		return err
	}
	spec, err := machine.Parse(string(body))
	if err != nil {
		return fmt.Errorf("%s: %w", positional[0], err)
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/machines", struct {
		machine.Spec
		DryRun bool `json:"dry_run"`
	}{spec, *dryRun})
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var result store.MachineApplyResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return fmt.Errorf("decode apply response: %w", err)
	}
	verb := "applied"
	if result.DryRun {
		verb = "would apply"
	}
	fmt.Print(formatApply(verb, result))
	return nil
}

// formatApply is the changes of an apply: GPUs (+ declared, ~ moved, -
// dropped), then hosts, then the device settings it clears.
func formatApply(verb string, result store.MachineApplyResult) string {
	var b strings.Builder
	changes := len(result.Changes) + len(result.HostChanges)
	switch {
	case changes == 0 && result.UnchangedHosts == 0:
		fmt.Fprintf(&b, "%s: no changes (%d GPUs declared)\n", verb, result.Unchanged)
	case changes == 0:
		fmt.Fprintf(&b, "%s: no changes (%d GPUs, %d hosts declared)\n", verb, result.Unchanged, result.UnchangedHosts)
	default:
		fmt.Fprintf(&b, "%s: %d change(s), %d unchanged\n", verb, changes, result.Unchanged+result.UnchangedHosts)
	}
	for _, c := range result.Changes {
		switch {
		case c.From == "":
			fmt.Fprintf(&b, "  + %s  %s\n", c.UUID, c.To)
		case c.To == "":
			fmt.Fprintf(&b, "  - %s  %s\n", c.UUID, c.From)
		default:
			fmt.Fprintf(&b, "  ~ %s  %s -> %s\n", c.UUID, c.From, c.To)
		}
	}
	for _, c := range result.HostChanges {
		switch {
		case c.From == nil:
			fmt.Fprintf(&b, "  + host %s  %s\n", c.Host, strings.Join(hostFields(*c.To), "; "))
		case c.To == nil:
			fmt.Fprintf(&b, "  - host %s\n", c.Host)
		default:
			from, to := hostFields(*c.From), hostFields(*c.To)
			var diff []string
			for i := range from {
				if from[i] != to[i] {
					name, before, _ := strings.Cut(from[i], " ")
					_, after, _ := strings.Cut(to[i], " ")
					diff = append(diff, name+" "+before+" -> "+after)
				}
			}
			fmt.Fprintf(&b, "  ~ host %s  %s\n", c.Host, strings.Join(diff, "; "))
		}
	}
	for _, s := range result.ClearedSettings {
		fmt.Fprintf(&b, "  - setting %s/%s  %s: %s (no longer controllable)\n", s.HostID, s.DeviceID, s.Owner.String(), s.Settings.String())
	}
	return b.String()
}

// hostFields are a host's declaration as "name value" texts, always the same
// fields in the same order.
func hostFields(h machine.Host) []string {
	devices := []string{}
	for _, d := range h.Devices {
		var overrides []string
		if d.MaxSlowdown != nil {
			overrides = append(overrides, "max_slowdown="+number(*d.MaxSlowdown))
		}
		if d.Control != nil {
			overrides = append(overrides, fmt.Sprintf("control=%t", *d.Control))
		}
		devices = append(devices, d.ID+"("+strings.Join(overrides, ",")+")")
	}
	return []string{
		"nodes " + strings.Join(h.Nodes, ","),
		"controller " + dash(h.Controller),
		"max_slowdown " + number(h.MaxSlowdown),
		"devices " + dash(strings.Join(devices, ",")),
	}
}

// machineShow: kairo machine show [--json].
func machineShow(args []string) error {
	fs := newFlags("machine show")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("machine show accepts no arguments")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodGet, "/api/machines", nil)
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var machines store.Machines
	if err := json.Unmarshal(payload, &machines); err != nil {
		return fmt.Errorf("decode machines: %w", err)
	}
	fmt.Print(formatMachines(machines))
	return nil
}

// formatMachines is the declared GPUs as a table, then the GPUs that
// providers report but may not register, then the hosts and the effective
// policy of each of their devices.
func formatMachines(m store.Machines) string {
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tGPU\tSTATE")
	for _, gpu := range m.GPUs {
		fmt.Fprintf(w, "%s\t%s\t%s\n", gpu.NodeID, gpu.UUID, gpu.State)
	}
	w.Flush()
	if len(m.Undeclared) != 0 {
		fmt.Fprintln(&b, "\nreported but not declared for the node (not scheduled):")
		for _, gpu := range m.Undeclared {
			declared := "declared for no node"
			if gpu.DeclaredNodeID != nil {
				declared = "declared for " + *gpu.DeclaredNodeID
			}
			fmt.Fprintf(&b, "  %s  on %s (%s), %s\n", gpu.UUID, gpu.NodeID, gpu.ProviderID, declared)
		}
	}
	if len(m.Hosts) != 0 {
		fmt.Fprintln(&b)
		w = tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "HOST\tNODES\tCONTROLLER")
		for _, host := range m.Hosts {
			fmt.Fprintf(w, "%s\t%s\t%s\n", host.ID, strings.Join(host.Nodes, ","), dash(host.Controller))
		}
		w.Flush()
		fmt.Fprintln(&b)
		w = tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "HOST\tDEVICE\tMAX_SLOWDOWN\tCONTROL")
		for _, device := range m.Devices {
			control := "no"
			switch {
			case device.Controllable():
				control = "yes"
			case device.Control:
				control = "no (no controller)"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", device.HostID, device.DeviceID, number(device.MaxSlowdown), control)
		}
		w.Flush()
	}
	return b.String()
}
