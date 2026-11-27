package fabric

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"kairo/internal/store"
)

// errSegmentInUse: a segment is not taken down while a container is on it
// (a port of its bridge other than its VXLAN device); it is at a later sync.
var errSegmentInUse = errors.New("a container is on the segment")

// launchedSegmentKept: a segment put on the node for a launch
// (EnsureSegment) is not taken down for this long by a sync whose view
// does not have it yet (one asked for before the gang was placed).
const launchedSegmentKept = 10 * time.Minute

func (m *Manager) names() string {
	if m.Names == "" {
		return DefaultNames
	}
	return m.Names
}

// EnsureSegment puts seg on this node now, its devices (on Windows its
// network too), for a gang member's launch, before a sync would; it
// returns the device a container joins it by (segmentDevice).
func (m *Manager) EnsureSegment(ctx context.Context, seg store.FabricSegment) (string, error) {
	m.mu.Lock()
	view, address := m.view, m.address
	m.mu.Unlock()
	if !m.Routing || view.Routing == nil || address == "" || m.link == nil {
		return "", errors.New("this node carries no segments yet (not on the fabric, or not told to route)")
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if err := m.link.ensureSegment(ctx, m.names(), seg, address, view.Routing.VXLANPort); err != nil {
		return "", err
	}
	if m.launched == nil {
		m.launched = map[int]time.Time{}
	}
	m.launched[seg.VNI] = m.now()
	return segmentDevice(m.names(), seg.VNI), nil
}

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
	names := m.names()
	r := view.Routing
	if !m.Routing || r == nil {
		// a node that routes no more (or was told it does not): its FRR and
		// segments go, as a node's left to keep advertising would
		return m.stopRouting(ctx, names)
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	want := map[int]bool{}
	for _, seg := range view.Segments {
		want[seg.VNI] = true
	}
	// the segments no longer wanted go first: one left (a wslc network) may
	// stand in the way of a new one with its subnet
	if err := m.removeSegments(ctx, names, want, true); err != nil {
		return err
	}
	var failed []error
	for _, seg := range view.Segments {
		if err := m.link.ensureSegment(ctx, names, seg, view.Address, r.VXLANPort); err != nil {
			failed = append(failed, fmt.Errorf("segment %d: %w", seg.VNI, err))
		}
	}
	// VXLAN and BFD only from the fabric: a frame that did not come over
	// WireGuard is not a peer's (ingressGuard)
	if g, ok := m.link.(ingressGuard); ok {
		if err := g.guardIngress(ctx, names, m.Interface, r.VXLANPort); err != nil {
			failed = append(failed, fmt.Errorf("ingress guard: %w", err))
		}
	}
	if m.router == nil {
		m.router = m.link.router(m.RouterName, m.NodeID, m.KeyFile)
	}
	if m.router == nil {
		return errors.Join(append(failed, fmt.Errorf("FRR cannot run on this node"))...)
	}
	if err := m.router.apply(ctx, frrFiles(m.NodeID, view)); err != nil {
		failed = append(failed, err)
	}
	return errors.Join(failed...)
}

// removeSegments takes down the segments made here that want does not have
// (but, keepLaunched, one put here for a launch lately), none while a
// container is on it.
func (m *Manager) removeSegments(ctx context.Context, names string, want map[int]bool, keepLaunched bool) error {
	have, err := m.link.segments(ctx, names)
	if err != nil {
		return err
	}
	for _, vni := range have {
		if want[vni] {
			continue
		}
		if at, ok := m.launched[vni]; keepLaunched && ok && m.now().Sub(at) < launchedSegmentKept {
			continue
		}
		delete(m.launched, vni)
		err := m.link.removeSegment(ctx, names, vni)
		switch {
		case errors.Is(err, errSegmentInUse):
			m.logger().Debug("segment kept while a container is on it", "vni", vni)
		case err != nil:
			m.logger().Warn("segment not taken down (taken down at a later sync)", "vni", vni, "error", err)
		}
	}
	return nil
}

// stopRouting takes FRR and the segments (those no container is on) off a
// node that does not route.
func (m *Manager) stopRouting(ctx context.Context, names string) error {
	if m.link == nil {
		return nil
	}
	if m.router == nil && m.RouterName != "" {
		m.router = m.link.router(m.RouterName, m.NodeID, m.KeyFile)
	}
	var err error
	if m.router != nil {
		err = m.router.remove(ctx)
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	return errors.Join(err, m.removeSegments(ctx, names, nil, false))
}

// ingressGuard is a link that keeps VXLAN and BFD to what comes over the
// fabric's interface (a host takes UDP to any of its addresses on any of
// its interfaces).
type ingressGuard interface {
	guardIngress(ctx context.Context, names, iface string, vxlanPort int) error
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

// ingressRules is the nftables ruleset that keeps the fabric's UDP (VXLAN
// on vxlanPort, BFD) to what comes in on iface: its own table (one per
// installation: names), replaced whole each time.
func ingressRules(names, iface string, vxlanPort int) string {
	table := names + "fabric"
	return fmt.Sprintf(`table inet %[1]s {}
delete table inet %[1]s
table inet %[1]s {
	chain input {
		type filter hook input priority -10; policy accept;
		udp dport { %[3]d, 3784, 3785, 4784 } iifname != %[2]q drop
	}
}
`, table, iface, vxlanPort)
}
