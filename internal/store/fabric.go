package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// The fabric (issue #47): every node is on one WireGuard network, at an
// address of its own given once (from the daemon's fabric prefix) and never
// changed, whatever line it is on, so that what names a node (its transfer
// endpoint, pack_addr) names that address and the path under it is the
// agent's business. Each node keeps its own key; the daemon keeps each
// node's public key and the endpoints it says it is reached at, and hands
// every node the others'.

// ErrFabric: a node's fabric request that cannot be met (no prefix, no room
// in it, a malformed key or endpoint).
var ErrFabric = errors.New("fabric")

// maxFabricEndpoints bounds the endpoints a node gives.
const maxFabricEndpoints = 8

// FabricSelf is what a node says of itself: its WireGuard public key, the
// UDP port it listens on and the endpoints ("host:port") its peers may try,
// in its order of preference.
type FabricSelf struct {
	PublicKey  string   `json:"public_key"`
	ListenPort int      `json:"listen_port"`
	Endpoints  []string `json:"endpoints"`
	// ProbePort is where peers time a TCP connect to this node at its
	// fabric address (its transfer endpoint's port; 0: none).
	ProbePort int `json:"probe_port,omitempty"`
}

// FabricNode is a node on the fabric.
type FabricNode struct {
	NodeID     string   `json:"node_id"`
	Address    string   `json:"address"`
	PublicKey  string   `json:"public_key,omitempty"`
	ListenPort int      `json:"listen_port,omitempty"`
	Endpoints  []string `json:"endpoints"`
	ProbePort  int      `json:"probe_port,omitempty"`
	UpdatedAt  string   `json:"updated_at"`
}

// FabricView is what a node is told: its address, the fabric's prefix, and
// its peers (the other enabled nodes that have a key).
type FabricView struct {
	Address string       `json:"address"`
	Prefix  string       `json:"prefix"`
	Peers   []FabricNode `json:"peers"`
}

// ParseFabricPrefix checks a fabric prefix: an IPv4 network of at least 4
// addresses, given as its network address.
func ParseFabricPrefix(s string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(s)
	if err != nil || !p.Addr().Is4() || p.Bits() > 30 || p.Masked() != p {
		return netip.Prefix{}, fmt.Errorf("%w: prefix %q: an IPv4 network (a.b.c.d/n, n at most 30)", ErrFabric, s)
	}
	return p, nil
}

func checkFabricSelf(self FabricSelf) error {
	key, err := base64.StdEncoding.DecodeString(self.PublicKey)
	if err != nil || len(key) != 32 {
		return fmt.Errorf("%w: public key: 32 bytes of base64", ErrFabric)
	}
	if self.ListenPort < 1 || self.ListenPort > 65535 {
		return fmt.Errorf("%w: listen port %d", ErrFabric, self.ListenPort)
	}
	if self.ProbePort < 0 || self.ProbePort > 65535 {
		return fmt.Errorf("%w: probe port %d", ErrFabric, self.ProbePort)
	}
	if len(self.Endpoints) > maxFabricEndpoints {
		return fmt.Errorf("%w: %d endpoints, at most %d", ErrFabric, len(self.Endpoints), maxFabricEndpoints)
	}
	for _, e := range self.Endpoints {
		host, port, err := net.SplitHostPort(e)
		n, perr := strconv.Atoi(port)
		if err != nil || host == "" || perr != nil || n < 1 || n > 65535 {
			return fmt.Errorf("%w: endpoint %q: host:port", ErrFabric, e)
		}
	}
	return nil
}

// SyncFabric records what node says of itself on the fabric (its address
// given the first time, from prefix: the lowest free host address) and
// tells it where it is and who its peers are.
func (s *Store) SyncFabric(ctx context.Context, nodeID, prefix string, self FabricSelf) (FabricView, error) {
	p, err := ParseFabricPrefix(prefix)
	if err != nil {
		return FabricView{}, err
	}
	if err := checkFabricSelf(self); err != nil {
		return FabricView{}, err
	}
	endpoints, err := json.Marshal(append([]string{}, self.Endpoints...))
	if err != nil {
		return FabricView{}, err
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return FabricView{}, err
	}
	defer tx.Rollback()
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=$1)`, nodeID).Scan(&exists); err != nil {
		return FabricView{}, err
	}
	if !exists {
		return FabricView{}, fmt.Errorf("%w: node %s", ErrNotFound, nodeID)
	}
	t := now()
	var address string
	err = tx.QueryRowContext(ctx, `SELECT address FROM fabric_nodes WHERE node_id=$1`, nodeID).Scan(&address)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if address, err = freeFabricAddressTx(ctx, tx, p); err != nil {
			return FabricView{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_nodes(node_id,address,public_key,listen_port,endpoints_json,probe_port,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			nodeID, address, self.PublicKey, self.ListenPort, string(endpoints), nullablePort(self.ProbePort), t, t); err != nil {
			return FabricView{}, err
		}
		if err := appendCoordinationEventTx(ctx, tx, "fabric_joined", nil, "node", nodeID, nil, map[string]any{"address": address}); err != nil {
			return FabricView{}, err
		}
	case err != nil:
		return FabricView{}, err
	default:
		if a, perr := netip.ParseAddr(address); perr != nil || !p.Contains(a) {
			return FabricView{}, fmt.Errorf("%w: node %s has address %s, outside the fabric's prefix %s (a node's address is never changed)", ErrFabric, nodeID, address, prefix)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fabric_nodes SET public_key=$1,listen_port=$2,endpoints_json=$3,probe_port=$6,updated_at=$4 WHERE node_id=$5
			AND (public_key IS DISTINCT FROM $1 OR listen_port IS DISTINCT FROM $2 OR endpoints_json<>$3 OR probe_port IS DISTINCT FROM $6)`,
			self.PublicKey, self.ListenPort, string(endpoints), t, nodeID, nullablePort(self.ProbePort)); err != nil {
			return FabricView{}, err
		}
	}
	view := FabricView{Address: address, Prefix: p.String(), Peers: []FabricNode{}}
	err = eachRow(ctx, tx, `SELECT f.node_id,f.address,f.public_key,f.listen_port,f.endpoints_json,f.probe_port,f.updated_at FROM fabric_nodes f JOIN nodes n ON n.id=f.node_id
		WHERE f.node_id<>$1 AND n.enabled AND f.public_key IS NOT NULL ORDER BY f.node_id`, []any{nodeID}, func(rows *sql.Rows) error {
		peer, err := scanFabricNode(rows)
		if err != nil {
			return err
		}
		view.Peers = append(view.Peers, peer)
		return nil
	})
	if err != nil {
		return FabricView{}, err
	}
	return view, tx.Commit()
}

// freeFabricAddressTx is prefix's lowest host address no node has (its
// network and broadcast addresses aside).
func freeFabricAddressTx(ctx context.Context, tx *sql.Tx, p netip.Prefix) (string, error) {
	taken := map[string]bool{}
	err := eachRow(ctx, tx, `SELECT address FROM fabric_nodes`, nil, func(rows *sql.Rows) error {
		var a string
		if err := rows.Scan(&a); err != nil {
			return err
		}
		taken[a] = true
		return nil
	})
	if err != nil {
		return "", err
	}
	for a := p.Addr().Next(); p.Contains(a.Next()); a = a.Next() {
		if !taken[a.String()] {
			return a.String(), nil
		}
	}
	return "", fmt.Errorf("%w: no address left in %s", ErrFabric, p)
}

func scanFabricNode(rows *sql.Rows) (FabricNode, error) {
	var n FabricNode
	var key sql.NullString
	var port sql.NullInt64
	var endpoints string
	var probe sql.NullInt64
	if err := rows.Scan(&n.NodeID, &n.Address, &key, &port, &endpoints, &probe, &n.UpdatedAt); err != nil {
		return n, err
	}
	n.PublicKey, n.ListenPort, n.ProbePort = key.String, int(port.Int64), int(probe.Int64)
	if err := json.Unmarshal([]byte(endpoints), &n.Endpoints); err != nil {
		return n, err
	}
	return n, nil
}

// FabricNodes are the nodes on the fabric, by id.
func (s *Store) FabricNodes(ctx context.Context) ([]FabricNode, error) {
	out := []FabricNode{}
	err := eachRow(ctx, s.db, `SELECT node_id,address,public_key,listen_port,endpoints_json,probe_port,updated_at FROM fabric_nodes ORDER BY node_id`, nil, func(rows *sql.Rows) error {
		n, err := scanFabricNode(rows)
		if err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
}

func nullablePort(p int) any {
	if p == 0 {
		return nil
	}
	return p
}

// Directed links (issue #47): the path from one node to another as the
// first sees it, and how fast what the second received from the first came.
// The planner takes a transfer's speed from them (nodeperf.go).

// FabricPathDirect and FabricPathRelay are the kinds of path a link is on.
const (
	FabricPathDirect = "direct"
	FabricPathRelay  = "relay"
)

// minBandwidthSample: a transfer smaller than this says more about the
// round trip than about the link's bandwidth, and is not counted.
const minBandwidthSample = 4 << 20

// FabricPeerSeen is how a node sees its path to a peer now.
type FabricPeerSeen struct {
	Node        string   `json:"node"`
	Path        string   `json:"path"`
	Endpoint    string   `json:"endpoint,omitempty"`
	HandshakeAt string   `json:"handshake_at,omitempty"`
	RTTMillis   *float64 `json:"rtt_ms,omitempty"`
}

// FabricReceived is a transfer a node received from another: its bytes and
// the seconds they took.
type FabricReceived struct {
	From    string  `json:"from"`
	Bytes   int64   `json:"bytes"`
	Seconds float64 `json:"seconds"`
}

// FabricLinksReport is what a node says of its links.
type FabricLinksReport struct {
	Peers    []FabricPeerSeen `json:"peers"`
	Received []FabricReceived `json:"received,omitempty"`
}

// FabricLink is a directed link.
type FabricLink struct {
	Src            string   `json:"src"`
	Dst            string   `json:"dst"`
	Path           string   `json:"path"`
	Endpoint       string   `json:"endpoint,omitempty"`
	HandshakeAt    *string  `json:"handshake_at,omitempty"`
	RTTMillis      *float64 `json:"rtt_ms,omitempty"`
	RTTAt          *string  `json:"rtt_at,omitempty"`
	BytesPerSecond *float64 `json:"bytes_per_second,omitempty"`
	BandwidthAt    *string  `json:"bandwidth_at,omitempty"`
	UpdatedAt      string   `json:"updated_at"`
}

// RecordFabricLinks records what node says of its links: each path to a
// peer (a changed path, or endpoint, forgets the bandwidth measured over the
// old one), and what it received from its peers (each large transfer moves
// the link's bandwidth halfway to what it took).
func (s *Store) RecordFabricLinks(ctx context.Context, nodeID string, report FabricLinksReport) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t := now()
	known := func(node string) (bool, error) {
		var ok bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fabric_nodes WHERE node_id=$1)`, node).Scan(&ok)
		return ok, err
	}
	for _, p := range report.Peers {
		if p.Path != FabricPathDirect && p.Path != FabricPathRelay {
			return fmt.Errorf("%w: path %q to %s", ErrFabric, p.Path, p.Node)
		}
		if ok, err := known(p.Node); err != nil {
			return err
		} else if !ok || p.Node == nodeID {
			continue
		}
		var handshake any
		if p.HandshakeAt != "" {
			handshake = p.HandshakeAt
		}
		var rtt, rttAt any
		if p.RTTMillis != nil && *p.RTTMillis >= 0 {
			rtt, rttAt = *p.RTTMillis, t
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_links(src_node,dst_node,path,endpoint,handshake_at,rtt_ms,rtt_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT(src_node,dst_node) DO UPDATE SET
			bytes_per_second=CASE WHEN fabric_links.path<>excluded.path OR fabric_links.endpoint<>excluded.endpoint THEN NULL ELSE fabric_links.bytes_per_second END,
			bandwidth_at=CASE WHEN fabric_links.path<>excluded.path OR fabric_links.endpoint<>excluded.endpoint THEN NULL ELSE fabric_links.bandwidth_at END,
			path=excluded.path,endpoint=excluded.endpoint,handshake_at=COALESCE(excluded.handshake_at,fabric_links.handshake_at),
			rtt_ms=COALESCE(excluded.rtt_ms,fabric_links.rtt_ms),rtt_at=COALESCE(excluded.rtt_at,fabric_links.rtt_at),updated_at=excluded.updated_at`,
			nodeID, p.Node, p.Path, p.Endpoint, handshake, rtt, rttAt, t); err != nil {
			return err
		}
	}
	for _, r := range report.Received {
		if r.Bytes < minBandwidthSample || r.Seconds <= 0 || r.From == nodeID {
			continue
		}
		rate := float64(r.Bytes) / r.Seconds
		// the link from the sender to this node, as the sender reports its path
		if _, err := tx.ExecContext(ctx, `UPDATE fabric_links SET bytes_per_second=CASE WHEN bytes_per_second IS NULL THEN $1 ELSE (bytes_per_second+$1)/2 END,
			bandwidth_at=$2 WHERE src_node=$3 AND dst_node=$4`, rate, t, r.From, nodeID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// FabricLinks are the directed links, by source then destination.
func (s *Store) FabricLinks(ctx context.Context) ([]FabricLink, error) {
	out := []FabricLink{}
	err := eachRow(ctx, s.db, `SELECT src_node,dst_node,path,endpoint,handshake_at,rtt_ms,rtt_at,bytes_per_second,bandwidth_at,updated_at FROM fabric_links ORDER BY src_node,dst_node`, nil,
		func(rows *sql.Rows) error {
			var l FabricLink
			var handshake, rttAt, bwAt sql.NullString
			var rtt, bw sql.NullFloat64
			if err := rows.Scan(&l.Src, &l.Dst, &l.Path, &l.Endpoint, &handshake, &rtt, &rttAt, &bw, &bwAt, &l.UpdatedAt); err != nil {
				return err
			}
			if handshake.Valid {
				l.HandshakeAt = &handshake.String
			}
			if rtt.Valid {
				l.RTTMillis, l.RTTAt = &rtt.Float64, &rttAt.String
			}
			if bw.Valid {
				l.BytesPerSecond, l.BandwidthAt = &bw.Float64, &bwAt.String
			}
			out = append(out, l)
			return nil
		})
	return out, err
}
