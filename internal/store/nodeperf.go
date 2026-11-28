package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"
)

// nodePerf is what hardware benches measured (hardware_bench.go), for the
// planner's estimate of where a continuation ends soonest (outputhome.go):
// each GPU's matrix multiply throughput (gemm, at its reference operating
// point), each node's CPU throughput (cpu-compute, at its reference), and
// the throughput of the path between two nodes' agents (the lesser of
// net-bandwidth-send and -recv), the path a pack takes. Each is the latest
// finished run's.
type nodePerf struct {
	gpu   map[string]float64    // work per second, by GPU UUID
	cpu   map[string]float64    // work per second, by node
	links map[[2]string]float64 // bytes per second, by the two nodes (sorted)
	// directed: bytes per second from the first node to the second, as the
	// fabric's transfers measured it (fabric_links, issue #47)
	directed map[[2]string]float64
}

// perfReload: benches are measured rarely; the planner reads them again
// this often.
const perfReload = 10 * time.Minute

// perfCache keeps nodePerf between passes.
type perfCache struct {
	mu     sync.Mutex
	perf   *nodePerf
	loaded time.Time
}

// nodePerf returns what benches measured, read again when perfReload old. A
// read that fails leaves the previous one in use (none: nodes are taken to
// be as fast as each other, links as defaultLinkBytesPerSecond).
func (s *Store) nodePerf(ctx context.Context) *nodePerf {
	c := &s.perf
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.perf != nil && time.Since(c.loaded) < perfReload {
		return c.perf
	}
	p, err := loadNodePerf(ctx, s.db)
	if err != nil {
		if c.perf == nil {
			return &nodePerf{}
		}
		return c.perf
	}
	c.perf, c.loaded = p, time.Now()
	return p
}

func loadNodePerf(ctx context.Context, q queryer) (*nodePerf, error) {
	p := &nodePerf{gpu: map[string]float64{}, cpu: map[string]float64{}, links: map[[2]string]float64{}, directed: map[[2]string]float64{}}
	// what the fabric's transfers measured, each way
	if err := eachRow(ctx, q, `SELECT src_node,dst_node,bytes_per_second FROM fabric_links WHERE bytes_per_second IS NOT NULL AND bytes_per_second>0`, nil, func(rows *sql.Rows) error {
		var src, dst string
		var bps float64
		if err := rows.Scan(&src, &dst, &bps); err != nil {
			return err
		}
		p.directed[[2]string{src, dst}] = bps
		return nil
	}); err != nil {
		return nil, err
	}
	// the latest valid point of each kind of each finished run, newest run first
	err := eachRow(ctx, q, `SELECT DISTINCT ON (r.class,r.device_id,r.node_id,COALESCE(r.peer_node_id,''),p.kind) r.class,r.device_id,r.node_id,COALESCE(r.peer_node_id,''),p.kind,p.metrics_json
		FROM bench_points p JOIN bench_runs r ON r.id=p.run_id
		WHERE r.state='finished' AND p.valid AND (r.class='gpu' AND p.kind='gemm' AND p.phase='reference' OR r.class='cpu' AND p.kind='cpu-compute' AND p.phase='reference'
			OR r.class='network' AND p.kind IN('net-bandwidth-send','net-bandwidth-recv'))
		ORDER BY r.class,r.device_id,r.node_id,COALESCE(r.peer_node_id,''),p.kind,r.created_at DESC,p.measured_at DESC`, nil, func(rows *sql.Rows) error {
		var class, device, node, peer, kind, body string
		if err := rows.Scan(&class, &device, &node, &peer, &kind, &body); err != nil {
			return err
		}
		var metrics struct {
			WorkPerSecond float64 `json:"work_per_second"`
		}
		if json.Unmarshal([]byte(body), &metrics) != nil || metrics.WorkPerSecond <= 0 {
			return nil
		}
		switch class {
		case BenchGPU:
			p.gpu[device] = metrics.WorkPerSecond
		case BenchCPU:
			p.cpu[node] = metrics.WorkPerSecond
		case BenchNetwork:
			if peer == "" || peer == node {
				return nil
			}
			key := linkKey(node, peer)
			if have, ok := p.links[key]; !ok || metrics.WorkPerSecond < have {
				p.links[key] = metrics.WorkPerSecond
			}
		}
		return nil
	})
	return p, err
}

// linkKey is the key of the path between two nodes, either way.
func linkKey(a, b string) [2]string {
	if b < a {
		a, b = b, a
	}
	return [2]string{a, b}
}

// defaultLinkBytesPerSecond is taken for a path no bench measured: 1 GbE
// less what TLS and the protocol take.
const defaultLinkBytesPerSecond = 100e6

// link is the bytes per second from node a's agent to node b's: what the
// fabric's transfers measured that way, else what a bench measured between
// them (either way), else defaultLinkBytesPerSecond.
func (p *nodePerf) link(a, b string) float64 {
	if p != nil {
		if v, ok := p.directed[[2]string{a, b}]; ok {
			return v
		}
		if v, ok := p.links[linkKey(a, b)]; ok {
			return v
		}
	}
	return defaultLinkBytesPerSecond
}
