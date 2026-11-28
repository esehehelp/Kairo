package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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
		Prefix   string                `json:"prefix"`
		Nodes    []store.FabricNode    `json:"nodes"`
		Links    []store.FabricLink    `json:"links"`
		Segments []store.FabricSegment `json:"segments"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode the fabric: %w", err)
	}
	fmt.Print(formatFabric(body.Prefix, body.Nodes))
	fmt.Print(formatFabricLinks(body.Links))
	fmt.Print(formatFabricSegments(body.Segments))
	return nil
}

// formatFabricSegments is a line a segment: its VNI, subnet, MTU and nodes.
func formatFabricSegments(segments []store.FabricSegment) string {
	if len(segments) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nsegments:\n")
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VNI\tSUBNET\tMTU\tPURPOSE\tNODES\tCREATED")
	for _, s := range segments {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\n", s.VNI, s.Subnet, s.MTU, s.Purpose, strings.Join(s.Nodes, ","), shortStamp(&s.Created))
	}
	w.Flush()
	return b.String()
}

// fabricSegmentAdd: kairo fabric segment-add NODE... [--mtu N] [--json]: a
// segment made by hand on those nodes (gangs get their own).
func fabricSegmentAdd(args []string) error {
	fs := newFlags("fabric segment-add")
	api := apiFlag(fs)
	mtu := fs.Int("mtu", 0, "the segment's MTU (default 1370: the fabric's less VXLAN's)")
	asJSON := fs.Bool("json", false, "print the daemon's response")
	nodes, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return usageError("fabric segment-add needs the nodes it is on")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, "/api/fabric/segments", map[string]any{"nodes": nodes, "mtu": *mtu})
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not make segments: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var seg store.FabricSegment
	if err := json.Unmarshal(payload, &seg); err != nil {
		return fmt.Errorf("decode the segment: %w", err)
	}
	fmt.Printf("segment %d: %s, MTU %d, on %s\n", seg.VNI, seg.Subnet, seg.MTU, strings.Join(seg.Nodes, ", "))
	return nil
}

// fabricSegmentRemove: kairo fabric segment-remove VNI...
func fabricSegmentRemove(args []string) error {
	fs := newFlags("fabric segment-remove")
	api := apiFlag(fs)
	vnis, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(vnis) == 0 {
		return usageError("fabric segment-remove needs VNIs")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	for _, vni := range vnis {
		if _, err := client.call(http.MethodDelete, "/api/fabric/segments/"+url.PathEscape(vni), nil); err != nil {
			return err
		}
		fmt.Printf("segment %s removed\n", vni)
	}
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

// formatFabricLinks is a line a directed link: its path and endpoint, the
// round trip, the bandwidth its transfers took, and when each was measured.
func formatFabricLinks(links []store.FabricLink) string {
	if len(links) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nlinks (as the first node sees its path to the second):\n")
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FROM\tTO\tPATH\tENDPOINT\tHANDSHAKE\tRTT\tBANDWIDTH\tMEASURED")
	for _, l := range links {
		rtt, bw, measured := "-", "-", "-"
		if l.RTTMillis != nil {
			rtt = fmt.Sprintf("%.2f ms", *l.RTTMillis)
		}
		if l.BytesPerSecond != nil {
			bw = formatSize(int64(*l.BytesPerSecond)) + "/s"
			measured = shortStamp(l.BandwidthAt)
		}
		endpoint := l.Endpoint
		if endpoint == "" {
			endpoint = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", l.Src, l.Dst, l.Path, endpoint, shortStamp(l.HandshakeAt), rtt, bw, measured)
	}
	w.Flush()
	return b.String()
}
