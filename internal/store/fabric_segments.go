package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Routing and segments of the fabric (issue #47, phase 2). Every node that
// runs FRR (FabricSelf.Routing) speaks iBGP over the fabric to the route
// reflectors, with EVPN carrying where each segment's MACs are: the agent
// makes a segment's bridge and VXLAN device, FRR advertises them and learns
// the others'. A segment is made by hand for now (kairo fabric segment-add);
// gangs get their own, one per generation (phase 3).

// FabricPlan is how the daemon runs the fabric ([fabric] in its config).
type FabricPlan struct {
	Prefix string
	// ASN is the fabric's one autonomous system (iBGP).
	ASN uint32
	// VXLANPort is the UDP port of every segment's VXLAN.
	VXLANPort int
}

// The plan's defaults: a private 4-byte ASN, VXLAN's port.
const (
	DefaultFabricASN       = 4200000000
	DefaultFabricVXLANPort = 4789
)

func (p FabricPlan) withDefaults() FabricPlan {
	if p.ASN == 0 {
		p.ASN = DefaultFabricASN
	}
	if p.VXLANPort == 0 {
		p.VXLANPort = DefaultFabricVXLANPort
	}
	return p
}

// FabricRouting is what a node that runs FRR is told: the ASN, VXLAN's
// port, the route reflectors' fabric addresses, and whether it is one.
type FabricRouting struct {
	ASN        uint32   `json:"asn"`
	VXLANPort  int      `json:"vxlan_port"`
	Reflectors []string `json:"reflectors"`
	Reflector  bool     `json:"reflector,omitempty"`
}

// FabricSegment is a segment: its VNI, subnet and MTU (and, listed, its
// nodes).
type FabricSegment struct {
	VNI     int      `json:"vni"`
	Subnet  string   `json:"subnet"`
	MTU     int      `json:"mtu"`
	Purpose string   `json:"purpose,omitempty"`
	Nodes   []string `json:"nodes,omitempty"`
	Created string   `json:"created_at,omitempty"`
}

// The reflectors are this many, chosen among nodes that run FRR and said
// how they see their links this recently.
const (
	fabricReflectors   = 2
	reflectorLiveness  = 10 * time.Minute
	firstSegmentVNI    = 10000
	segmentSubnetBits  = 24
	DefaultSegmentMTU  = 1370 // the fabric's MTU (1420) less VXLAN's 50
	minimumSegmentMTU  = 1280 // Incus refuses a NIC under it
	maximumSegmentMTUs = 9000
)

// routingTx is node's routing: nil unless it runs FRR. The reflectors are
// the first nodes (those with endpoints, which take connections, first;
// then by id) that run FRR and reported their links recently: every node is
// told the same ones (a node not yet reporting is none).
func routingTx(ctx context.Context, tx *sql.Tx, nodeID string, plan FabricPlan) (*FabricRouting, error) {
	var routing bool
	if err := tx.QueryRowContext(ctx, `SELECT routing FROM fabric_nodes WHERE node_id=$1`, nodeID).Scan(&routing); err != nil || !routing {
		return nil, err
	}
	since := time.Now().Add(-reflectorLiveness).UTC().Format(time.RFC3339Nano)
	r := &FabricRouting{ASN: plan.ASN, VXLANPort: plan.VXLANPort, Reflectors: []string{}}
	var ids []string
	err := eachRow(ctx, tx, `SELECT f.node_id,f.address FROM fabric_nodes f JOIN nodes n ON n.id=f.node_id
		WHERE n.enabled AND f.routing AND EXISTS(SELECT 1 FROM fabric_links l WHERE l.src_node=f.node_id AND l.updated_at::timestamptz>$1::timestamptz)
		ORDER BY (f.endpoints_json<>'[]') DESC, f.node_id LIMIT $2`, []any{since, fabricReflectors}, func(rows *sql.Rows) error {
		var id, address string
		if err := rows.Scan(&id, &address); err != nil {
			return err
		}
		ids = append(ids, id)
		r.Reflectors = append(r.Reflectors, address)
		return nil
	})
	if err != nil {
		return nil, err
	}
	r.Reflector = slices.Contains(ids, nodeID)
	return r, nil
}

// segmentsOfTx are the segments node is on.
func segmentsOfTx(ctx context.Context, tx *sql.Tx, nodeID string) ([]FabricSegment, error) {
	var out []FabricSegment
	err := eachRow(ctx, tx, `SELECT s.vni,s.subnet,s.mtu FROM fabric_segments s JOIN fabric_segment_members m ON m.vni=s.vni
		WHERE m.node_id=$1 ORDER BY s.vni`, []any{nodeID}, func(rows *sql.Rows) error {
		var seg FabricSegment
		if err := rows.Scan(&seg.VNI, &seg.Subnet, &seg.MTU); err != nil {
			return err
		}
		out = append(out, seg)
		return nil
	})
	return out, err
}

// CreateFabricSegment makes a segment on nodes (each on the fabric, running
// FRR): the lowest free VNI from 10000, the lowest free /24 of
// segmentPrefix, and mtu (0: the default).
func (s *Store) CreateFabricSegment(ctx context.Context, segmentPrefix string, nodes []string, mtu int) (FabricSegment, error) {
	p, err := netip.ParsePrefix(segmentPrefix)
	if err != nil || !p.Addr().Is4() || p.Masked() != p || p.Bits() > segmentSubnetBits {
		return FabricSegment{}, fmt.Errorf("%w: segment prefix %q: an IPv4 network of /%d or more (the daemon's [fabric] segment_prefix)", ErrFabric, segmentPrefix, segmentSubnetBits)
	}
	if mtu == 0 {
		mtu = DefaultSegmentMTU
	}
	if mtu < minimumSegmentMTU || mtu > maximumSegmentMTUs {
		return FabricSegment{}, fmt.Errorf("%w: segment MTU %d (from %d to %d)", ErrFabric, mtu, minimumSegmentMTU, maximumSegmentMTUs)
	}
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)
	if len(nodes) == 0 {
		return FabricSegment{}, fmt.Errorf("%w: a segment needs nodes", ErrFabric)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return FabricSegment{}, err
	}
	defer tx.Rollback()
	for _, n := range nodes {
		var routing bool
		err := tx.QueryRowContext(ctx, `SELECT routing FROM fabric_nodes WHERE node_id=$1`, n).Scan(&routing)
		if errors.Is(err, sql.ErrNoRows) {
			return FabricSegment{}, fmt.Errorf("%w: node %s is not on the fabric", ErrFabric, n)
		}
		if err != nil {
			return FabricSegment{}, err
		}
		if !routing {
			return FabricSegment{}, fmt.Errorf("%w: node %s runs no FRR (only an agent's node carries segments)", ErrFabric, n)
		}
	}
	usedVNI := map[int]bool{}
	usedSubnet := map[string]bool{}
	err = eachRow(ctx, tx, `SELECT vni,subnet FROM fabric_segments`, nil, func(rows *sql.Rows) error {
		var vni int
		var subnet string
		if err := rows.Scan(&vni, &subnet); err != nil {
			return err
		}
		usedVNI[vni], usedSubnet[subnet] = true, true
		return nil
	})
	if err != nil {
		return FabricSegment{}, err
	}
	seg := FabricSegment{VNI: firstSegmentVNI, MTU: mtu, Purpose: "manual", Nodes: nodes, Created: now()}
	for usedVNI[seg.VNI] {
		seg.VNI++
	}
	base := p.Addr().As4()
	count := 1 << (segmentSubnetBits - p.Bits())
	for i := 0; i < count; i++ {
		a := base
		a[1] += byte(i >> 8)
		a[2] += byte(i)
		subnet := netip.PrefixFrom(netip.AddrFrom4(a), segmentSubnetBits).Masked().String()
		if !usedSubnet[subnet] && p.Contains(netip.AddrFrom4(a)) {
			seg.Subnet = subnet
			break
		}
	}
	if seg.Subnet == "" {
		return FabricSegment{}, fmt.Errorf("%w: every /%d of %s is a segment's", ErrFabric, segmentSubnetBits, segmentPrefix)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_segments(vni,subnet,mtu,purpose,created_at) VALUES($1,$2,$3,$4,$5)`, seg.VNI, seg.Subnet, seg.MTU, seg.Purpose, seg.Created); err != nil {
		return FabricSegment{}, err
	}
	for _, n := range nodes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_segment_members(vni,node_id) VALUES($1,$2)`, seg.VNI, n); err != nil {
			return FabricSegment{}, err
		}
	}
	if err := appendCoordinationEventTx(ctx, tx, "fabric_segment_created", nil, "fabric_segment", strconv.Itoa(seg.VNI), nil, map[string]any{"vni": seg.VNI, "subnet": seg.Subnet, "mtu": seg.MTU, "nodes": nodes}); err != nil {
		return FabricSegment{}, err
	}
	return seg, tx.Commit()
}

// DeleteFabricSegment removes segment vni: its nodes take down its bridge
// and VXLAN device at their next sync.
func (s *Store) DeleteFabricSegment(ctx context.Context, vni int) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM fabric_segments WHERE vni=$1`, vni)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: segment %d", ErrNotFound, vni)
	}
	if err := appendCoordinationEventTx(ctx, tx, "fabric_segment_deleted", nil, "fabric_segment", strconv.Itoa(vni), nil, map[string]any{"vni": vni}); err != nil {
		return err
	}
	return tx.Commit()
}

// FabricSegments are the fabric's segments with their nodes.
func (s *Store) FabricSegments(ctx context.Context) ([]FabricSegment, error) {
	out := []FabricSegment{}
	err := eachRow(ctx, s.db, `SELECT s.vni,s.subnet,s.mtu,s.purpose,s.created_at,COALESCE(string_agg(m.node_id,',' ORDER BY m.node_id),'')
		FROM fabric_segments s LEFT JOIN fabric_segment_members m ON m.vni=s.vni GROUP BY s.vni ORDER BY s.vni`, nil, func(rows *sql.Rows) error {
		var seg FabricSegment
		var nodes string
		if err := rows.Scan(&seg.VNI, &seg.Subnet, &seg.MTU, &seg.Purpose, &seg.Created, &nodes); err != nil {
			return err
		}
		if nodes != "" {
			seg.Nodes = strings.Split(nodes, ",")
		}
		out = append(out, seg)
		return nil
	})
	return out, err
}
