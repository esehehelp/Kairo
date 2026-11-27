package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"

	"kairo/internal/store"
)

// fabricList: kairo fabric list [--json]: the fabric's prefix and its nodes
// (issue #47).
func fabricList(args []string) error {
	fs := newFlags("fabric list")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("fabric list accepts no arguments")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodGet, "/api/fabric", nil)
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not know the fabric: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Prefix string             `json:"prefix"`
		Nodes  []store.FabricNode `json:"nodes"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode the fabric: %w", err)
	}
	fmt.Print(formatFabric(body.Prefix, body.Nodes))
	return nil
}

// formatFabric is the fabric's prefix, then a line a node: its address, the
// start of its public key, its port and endpoints, and when it last said
// something new.
func formatFabric(prefix string, nodes []store.FabricNode) string {
	if prefix == "" {
		return "no fabric (the daemon's [fabric] prefix)\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "fabric %s\n", prefix)
	if len(nodes) == 0 {
		b.WriteString("no node on it yet (an agent's [fabric] enabled)\n")
		return b.String()
	}
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tADDRESS\tKEY\tPORT\tENDPOINTS\tUPDATED")
	for _, n := range nodes {
		key := n.PublicKey
		if len(key) > 10 {
			key = key[:10] + "…"
		}
		endpoints := strings.Join(n.Endpoints, ",")
		if endpoints == "" {
			endpoints = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n", n.NodeID, n.Address, key, n.ListenPort, endpoints, shortStamp(&n.UpdatedAt))
	}
	w.Flush()
	return b.String()
}
