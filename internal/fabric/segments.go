package fabric

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"kairo/internal/store"
)

// Segments and routing (issue #47, phase 2). A segment is an L2 network
// over VXLAN: on each of its nodes a bridge with a VXLAN device as a port,
// made by the agent (named with Manager.Names: kfb10000, kfv10000). FRR,
// run by the agent in a container in the host's network namespace,
// advertises the local VNIs and MACs over EVPN to the route reflectors and
// installs the others' (iBGP over the fabric, BFD). The order a node is put
// back in: WireGuard, then the segments, then FRR.

// FRRImage is the FRR the agents run, pinned (quay.io/frrouting/frr:10.7.1).
const (
	FRRRepository = "quay.io/frrouting/frr"
	FRRDigest     = "sha256:e995beaa50fdc9edb35eadcfefa29b7f062cc06f2b812613789b68fa541554d2"
)

// DefaultNames prefixes a segment's devices when [fabric] names is unset.
const DefaultNames = "kf"

// segmentDevices are segment vni's bridge and VXLAN device, named with names.
func segmentDevices(names string, vni int) (bridge, vxlan string) {
	return names + "b" + strconv.Itoa(vni), names + "v" + strconv.Itoa(vni)
}

// SegmentNetwork is the wslc network of segment vni on a Windows node (the
// network a container joins to be on the segment).
func SegmentNetwork(names string, vni int) string {
	return names + "-seg-" + strconv.Itoa(vni)
}

// router runs FRR on this node.
type router interface {
	// apply keeps FRR running with these files in /etc/frr, restarted when
	// they changed.
	apply(ctx context.Context, files map[string]string) error
	// remove takes FRR away (a node that no longer routes).
	remove(ctx context.Context) error
	// show is vtysh -c command.
	show(ctx context.Context, command string) ([]byte, error)
}

// Show runs vtysh -c command in this node's FRR.
func (m *Manager) Show(ctx context.Context, command string) ([]byte, error) {
	if m.router == nil {
		return nil, fmt.Errorf("no FRR on this node")
	}
	return m.router.show(ctx, command)
}

// applyRouting puts the segments of view on this node (taking down those
// it is no longer on), then FRR as view says (none: removed).
func (m *Manager) applyRouting(ctx context.Context, view store.FabricView) error {
	if !m.Routing {
		return nil
	}
	names := m.Names
	if names == "" {
		names = DefaultNames
	}
	r := view.Routing
	if r == nil {
		if m.router != nil {
			return m.router.remove(ctx)
		}
		return nil
	}
	want := map[int]bool{}
	for _, seg := range view.Segments {
		want[seg.VNI] = true
		if err := m.link.ensureSegment(ctx, names, seg, view.Address, r.VXLANPort); err != nil {
			return fmt.Errorf("segment %d: %w", seg.VNI, err)
		}
	}
	have, err := m.link.segments(ctx, names)
	if err != nil {
		return err
	}
	for _, vni := range have {
		if !want[vni] {
			if err := m.link.removeSegment(ctx, names, vni); err != nil {
				m.logger().Warn("segment not taken down (taken down at a later sync)", "vni", vni, "error", err)
			}
		}
	}
	if m.router == nil {
		m.router = m.link.router(m.RouterName, m.NodeID, m.KeyFile)
	}
	if m.router == nil {
		return fmt.Errorf("FRR cannot run on this node")
	}
	return m.router.apply(ctx, frrFiles(m.NodeID, view))
}

// frrFiles are /etc/frr for node with view: iBGP in the fabric's ASN over
// the fabric, to the reflectors (or, on a reflector, from every address of
// the fabric), carrying EVPN for every local VNI, with BFD.
func frrFiles(node string, view store.FabricView) map[string]string {
	r := view.Routing
	address := view.Address
	var c strings.Builder
	fmt.Fprintf(&c, "! made by the Kairo agent (issue #47): changes are overwritten\n")
	c.WriteString("frr defaults datacenter\n")
	fmt.Fprintf(&c, "hostname %s\n", node)
	c.WriteString("log stdout informational\nservice integrated-vtysh-config\n!\n")
	fmt.Fprintf(&c, "router bgp %d\n", r.ASN)
	fmt.Fprintf(&c, " bgp router-id %s\n", address)
	c.WriteString(" no bgp default ipv4-unicast\n")
	if r.Reflector && len(r.Reflectors) > 0 {
		fmt.Fprintf(&c, " bgp cluster-id %s\n", r.Reflectors[0])
	}
	c.WriteString(" neighbor fabric peer-group\n neighbor fabric remote-as internal\n")
	fmt.Fprintf(&c, " neighbor fabric update-source %s\n", address)
	c.WriteString(" neighbor fabric bfd\n")
	if r.Reflector {
		fmt.Fprintf(&c, " bgp listen range %s peer-group fabric\n", view.Prefix)
	}
	reflectors := slices.Clone(r.Reflectors)
	slices.Sort(reflectors)
	for _, rr := range reflectors {
		if rr != address {
			fmt.Fprintf(&c, " neighbor %s peer-group fabric\n", rr)
		}
	}
	c.WriteString(" !\n address-family l2vpn evpn\n  neighbor fabric activate\n")
	if r.Reflector {
		c.WriteString("  neighbor fabric route-reflector-client\n")
	}
	c.WriteString("  advertise-all-vni\n exit-address-family\nexit\n!\n")
	daemons := "zebra=yes\nbgpd=yes\nbfdd=yes\nvtysh_enable=yes\n" +
		"zebra_options=\"  -A 127.0.0.1 -s 90000000\"\n" +
		"bgpd_options=\"   -A 127.0.0.1 -l " + address + "\"\n" +
		"bfdd_options=\"   -A 127.0.0.1\"\n"
	return map[string]string{
		"frr.conf":   c.String(),
		"daemons":    daemons,
		"vtysh.conf": "service integrated-vtysh-config\n",
	}
}
