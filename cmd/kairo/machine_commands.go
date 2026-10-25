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
	if len(result.Changes) == 0 {
		fmt.Printf("%s: no changes (%d GPUs declared)\n", verb, result.Unchanged)
		return nil
	}
	fmt.Printf("%s: %d change(s), %d unchanged\n", verb, len(result.Changes), result.Unchanged)
	for _, c := range result.Changes {
		switch {
		case c.From == "":
			fmt.Printf("  + %s  %s\n", c.UUID, c.To)
		case c.To == "":
			fmt.Printf("  - %s  %s\n", c.UUID, c.From)
		default:
			fmt.Printf("  ~ %s  %s -> %s\n", c.UUID, c.From, c.To)
		}
	}
	return nil
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
// providers report but may not register.
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
	return b.String()
}
