package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

// Placement from the fabric's evidence (issue #47, phase 5). A gang may say
// what its members' links must be: a round trip of at most max_rtt_ms, at
// least min_mib_per_second each way, and metered links or not. It is placed
// only where measurements say every pair of its members' nodes meets that:
// the round trip from the probes (fresh: rttFresh), the bandwidth from
// transfers or samples each way. A link without the measurement holds the
// gang (fabric_unmeasured) and is asked for one: its destination fetches a
// sample from its source (FabricView.Measure); a link measured short of it
// holds it too (fabric_links_short).

// FabricMeasure is a peer a node is asked to time a link with: to fetch a
// sample from it (the link from it), or with Send to send it one (the link
// to it: a peer that serves none, as a Windows node's transfer endpoint off
// the fabric, measures both ways itself); its address on the fabric and
// the port its transfer endpoint serves samples on.
type FabricMeasure struct {
	Node    string `json:"node"`
	Address string `json:"address"`
	Port    int    `json:"port"`
	Send    bool   `json:"send,omitempty"`
}

// rttFresh: a link's round trip measured this recently is evidence.
const rttFresh = 10 * time.Minute

// measureAsked: a link asked for a sample this recently, and not measured
// since, is still asked for one.
const measureAsked = 15 * time.Minute

// gangLinks is what a gang says its members' links must be.
type gangLinks struct {
	maxRTT   float64 // ms; 0: any
	minBytes float64 // bytes per second each way; 0: any
	metered  bool
}

func (g *gangLinks) any() bool { return g == nil || (g.maxRTT == 0 && g.minBytes == 0 && g.metered) }

// linkEvidence is what was measured of a directed link (-1: nothing).
type linkEvidence struct {
	rtt, bytesPerSecond float64
}

// loadLinkEvidence reads what was measured of the links between routing
// nodes, and which nodes are metered.
func (m *planModel) loadLinkEvidence(ctx context.Context, tx *sql.Tx) error {
	m.links, m.metered = map[[2]string]linkEvidence{}, map[string]bool{}
	if err := eachRow(ctx, tx, `SELECT node_id FROM fabric_nodes WHERE metered`, nil, func(rows *sql.Rows) error {
		var node string
		if err := rows.Scan(&node); err != nil {
			return err
		}
		m.metered[node] = true
		return nil
	}); err != nil {
		return err
	}
	return eachRow(ctx, tx, `SELECT l.src_node,l.dst_node,l.rtt_ms,l.rtt_at,l.bytes_per_second FROM fabric_links l
		JOIN fabric_nodes a ON a.node_id=l.src_node AND a.routing JOIN fabric_nodes b ON b.node_id=l.dst_node AND b.routing`, nil, func(rows *sql.Rows) error {
		var src, dst string
		var rtt, bps sql.NullFloat64
		var rttAt sql.NullString
		if err := rows.Scan(&src, &dst, &rtt, &rttAt, &bps); err != nil {
			return err
		}
		e := linkEvidence{rtt: -1, bytesPerSecond: -1}
		if t, err := time.Parse(time.RFC3339Nano, rttAt.String); err == nil && rtt.Valid && m.stamp.Sub(t) < rttFresh {
			e.rtt = rtt.Float64
		}
		if bps.Valid {
			e.bytesPerSecond = bps.Float64
		}
		m.links[[2]string{src, dst}] = e
		return nil
	})
}

// linkVerdict says whether the pair a, b meets want: met, measured short of
// it, or not measured (missing: the directed links to ask a sample of).
type linkVerdict struct {
	met, short bool
	missing    [][2]string
}

func (m *planModel) linkMeets(a, b string, want *gangLinks) linkVerdict {
	if want.any() {
		return linkVerdict{met: true}
	}
	if !want.metered && (m.metered[a] || m.metered[b]) {
		return linkVerdict{short: true}
	}
	ab, okAB := m.links[[2]string{a, b}]
	ba, okBA := m.links[[2]string{b, a}]
	if !okAB {
		ab = linkEvidence{rtt: -1, bytesPerSecond: -1}
	}
	if !okBA {
		ba = linkEvidence{rtt: -1, bytesPerSecond: -1}
	}
	var v linkVerdict
	unknown := false
	if want.maxRTT > 0 {
		// a round trip is the same either way: the lower measured
		rtt := math.Inf(1)
		for _, e := range []linkEvidence{ab, ba} {
			if e.rtt >= 0 {
				rtt = math.Min(rtt, e.rtt)
			}
		}
		switch {
		case math.IsInf(rtt, 1):
			unknown = true
		case rtt > want.maxRTT:
			v.short = true
		}
	}
	if want.minBytes > 0 {
		for _, d := range []struct {
			e        linkEvidence
			src, dst string
		}{{ab, a, b}, {ba, b, a}} {
			switch {
			case d.e.bytesPerSecond < 0:
				unknown = true
				v.missing = append(v.missing, [2]string{d.src, d.dst})
			case d.e.bytesPerSecond < want.minBytes:
				v.short = true
			}
		}
	}
	v.met = !v.short && !unknown
	return v
}

// measureRequestsTx are the links node is asked to time: each asked for a
// measurement recently and not measured since. One to node it fetches a
// sample of from its source, when that serves samples (its transfer
// endpoint is on the fabric: it has a probe port); one from node whose
// destination serves samples and node does not, it sends a sample to.
func measureRequestsTx(ctx context.Context, tx *sql.Tx, node string) ([]FabricMeasure, error) {
	since := time.Now().Add(-measureAsked).UTC().Format(time.RFC3339Nano)
	var out []FabricMeasure
	err := eachRow(ctx, tx, `SELECT r.src_node,s.address,s.probe_port,false FROM fabric_measure_requests r JOIN fabric_nodes s ON s.node_id=r.src_node
		LEFT JOIN fabric_links l ON l.src_node=r.src_node AND l.dst_node=r.dst_node
		WHERE r.dst_node=$1 AND r.requested_at::timestamptz>$2::timestamptz AND s.probe_port IS NOT NULL
		AND (l.bandwidth_at IS NULL OR l.bandwidth_at::timestamptz<r.requested_at::timestamptz)
		UNION ALL
		SELECT r.dst_node,d.address,d.probe_port,true FROM fabric_measure_requests r JOIN fabric_nodes d ON d.node_id=r.dst_node JOIN fabric_nodes s ON s.node_id=r.src_node
		LEFT JOIN fabric_links l ON l.src_node=r.src_node AND l.dst_node=r.dst_node
		WHERE r.src_node=$1 AND r.requested_at::timestamptz>$2::timestamptz AND d.probe_port IS NOT NULL AND s.probe_port IS NULL
		AND (l.bandwidth_at IS NULL OR l.bandwidth_at::timestamptz<r.requested_at::timestamptz)
		ORDER BY 4,1`, []any{node, since}, func(rows *sql.Rows) error {
		var m FabricMeasure
		if err := rows.Scan(&m.Node, &m.Address, &m.Port, &m.Send); err != nil {
			return err
		}
		out = append(out, m)
		return nil
	})
	return out, err
}

// saveMeasureRequestsTx asks for samples of links (src, dst): asked again
// only once the last ask is no longer in force.
func saveMeasureRequestsTx(ctx context.Context, tx *sql.Tx, links map[[2]string]bool) error {
	stale := time.Now().Add(-measureAsked).UTC().Format(time.RFC3339Nano)
	for l := range links {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_measure_requests(src_node,dst_node,requested_at) VALUES($1,$2,$3)
			ON CONFLICT(src_node,dst_node) DO UPDATE SET requested_at=excluded.requested_at WHERE fabric_measure_requests.requested_at::timestamptz<$4::timestamptz`,
			l[0], l[1], now(), stale); err != nil {
			return err
		}
	}
	return nil
}

// describe is what a gang says its members' links must be, as a wait says
// it.
func (g *gangLinks) describe() string {
	var parts []string
	if g.maxRTT > 0 {
		parts = append(parts, fmt.Sprintf("a round trip of at most %g ms", g.maxRTT))
	}
	if g.minBytes > 0 {
		parts = append(parts, fmt.Sprintf("at least %g MiB/s each way", g.minBytes/(1<<20)))
	}
	if !g.metered {
		parts = append(parts, "not metered")
	}
	return strings.Join(parts, ", ")
}
