package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strconv"
	"time"
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
	// RelayFrom and RelayTo are the UDP ports this node offers for relaying
	// other nodes' WireGuard datagrams (0: it relays none): a node with a
	// public endpoint.
	RelayFrom int `json:"relay_from,omitempty"`
	RelayTo   int `json:"relay_to,omitempty"`
	// Routing: this node runs FRR (an agent's: fabric_segments.go), so it
	// may be a route reflector and carries segments.
	Routing bool `json:"routing,omitempty"`
	// Metered: this node's uplink is paid by the byte (a mobile line): a
	// gang spans its links only when it says it may (fabric_evidence.go).
	Metered bool `json:"metered,omitempty"`
}

// FabricNode is a node on the fabric.
type FabricNode struct {
	NodeID     string   `json:"node_id"`
	Address    string   `json:"address"`
	PublicKey  string   `json:"public_key,omitempty"`
	ListenPort int      `json:"listen_port,omitempty"`
	Endpoints  []string `json:"endpoints"`
	ProbePort  int      `json:"probe_port,omitempty"`
	// RelayEndpoint, in a view, is where this node reaches the peer through
	// a relay ("": none), tried after its endpoints.
	RelayEndpoint string `json:"relay_endpoint,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}

// FabricRelay is a pair a node relays: the side of each node's datagrams
// (its port) and the address each is seen at ("": not yet), so that only
// its datagrams are taken for its side.
type FabricRelay struct {
	A     string `json:"a"`
	B     string `json:"b"`
	PortA int    `json:"port_a"`
	PortB int    `json:"port_b"`
	HostA string `json:"host_a,omitempty"`
	HostB string `json:"host_b,omitempty"`
}

// FabricView is what a node is told: its address, the fabric's prefix, and
// its peers (the other enabled nodes that have a key).
type FabricView struct {
	Address string       `json:"address"`
	Prefix  string       `json:"prefix"`
	Peers   []FabricNode `json:"peers"`
	// Relays are the pairs this node relays.
	Relays []FabricRelay `json:"relays,omitempty"`
	// Routing is how this node's FRR runs (nil: it runs none), and
	// Segments are the segments it is on (fabric_segments.go).
	Routing  *FabricRouting  `json:"routing,omitempty"`
	Segments []FabricSegment `json:"segments,omitempty"`
	// Measure are the peers this node is asked to fetch a sample from, for
	// the bandwidth of their links to it (fabric_evidence.go).
	Measure []FabricMeasure `json:"measure,omitempty"`
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
	if self.RelayFrom != 0 || self.RelayTo != 0 {
		if self.RelayFrom < 1024 || self.RelayTo > 65535 || self.RelayFrom+1 > self.RelayTo {
			return fmt.Errorf("%w: relay ports %d-%d (two or more, from 1024)", ErrFabric, self.RelayFrom, self.RelayTo)
		}
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
func (s *Store) SyncFabric(ctx context.Context, nodeID string, plan FabricPlan, self FabricSelf) (FabricView, error) {
	plan = plan.withDefaults()
	prefix := plan.Prefix
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_nodes(node_id,address,public_key,listen_port,endpoints_json,probe_port,relay_from,relay_to,routing,metered,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			nodeID, address, self.PublicKey, self.ListenPort, string(endpoints), nullablePort(self.ProbePort), nullablePort(self.RelayFrom), nullablePort(self.RelayTo), self.Routing, self.Metered, t, t); err != nil {
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
		// a node elsewhere now (other endpoints, or port): its pairs try a
		// direct path again, and are relayed again if it fails
		var wasEndpoints string
		var wasPort sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT endpoints_json,listen_port FROM fabric_nodes WHERE node_id=$1`, nodeID).Scan(&wasEndpoints, &wasPort); err != nil {
			return FabricView{}, err
		}
		if wasEndpoints != string(endpoints) || int(wasPort.Int64) != self.ListenPort {
			if err := unrelayTx(ctx, tx, `node_a=$1 OR node_b=$1`, nodeID); err != nil {
				return FabricView{}, err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fabric_nodes SET public_key=$1,listen_port=$2,endpoints_json=$3,probe_port=$6,relay_from=$7,relay_to=$8,routing=$9,metered=$10,updated_at=$4 WHERE node_id=$5
			AND (public_key IS DISTINCT FROM $1 OR listen_port IS DISTINCT FROM $2 OR endpoints_json<>$3 OR probe_port IS DISTINCT FROM $6
			OR relay_from IS DISTINCT FROM $7 OR relay_to IS DISTINCT FROM $8 OR routing<>$9 OR metered<>$10)`,
			self.PublicKey, self.ListenPort, string(endpoints), t, nodeID, nullablePort(self.ProbePort), nullablePort(self.RelayFrom), nullablePort(self.RelayTo), self.Routing, self.Metered); err != nil {
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
	for i := range view.Peers {
		if err := peerPathsTx(ctx, tx, nodeID, &view.Peers[i]); err != nil {
			return FabricView{}, err
		}
	}
	if view.Relays, err = relaysOfTx(ctx, tx, nodeID); err != nil {
		return FabricView{}, err
	}
	if view.Routing, err = routingTx(ctx, tx, nodeID, plan); err != nil {
		return FabricView{}, err
	}
	if view.Routing != nil {
		if view.Segments, err = segmentsOfTx(ctx, tx, nodeID); err != nil {
			return FabricView{}, err
		}
	}
	if view.Measure, err = measureRequestsTx(ctx, tx, nodeID); err != nil {
		return FabricView{}, err
	}
	return view, tx.Commit()
}

// observedFresh: an endpoint a node was seen at (by a peer's handshake)
// this recently is handed to the node's other peers.
const observedFresh = 10 * time.Minute

// peerPathsTx adds to peer's endpoints those its other peers see it at now
// (behind a NAT, its mapping: a peer that sends there punches through), and
// the relay endpoint self reaches it through, if a relay was given the pair.
func peerPathsTx(ctx context.Context, tx *sql.Tx, self string, peer *FabricNode) error {
	have := map[string]bool{}
	for _, e := range peer.Endpoints {
		have[e] = true
	}
	since := time.Now().Add(-observedFresh).UTC().Format(time.RFC3339Nano)
	err := eachRow(ctx, tx, `SELECT DISTINCT endpoint FROM fabric_links WHERE dst_node=$1 AND src_node<>$2 AND path='direct' AND endpoint<>''
		AND handshake_at IS NOT NULL AND handshake_at::timestamptz>$3::timestamptz ORDER BY endpoint`, []any{peer.NodeID, self, since}, func(rows *sql.Rows) error {
		var e string
		if err := rows.Scan(&e); err != nil {
			return err
		}
		if !have[e] && len(peer.Endpoints) < maxFabricEndpoints {
			have[e] = true
			peer.Endpoints = append(peer.Endpoints, e)
		}
		return nil
	})
	if err != nil {
		return err
	}
	a, b := self, peer.NodeID
	if b < a {
		a, b = b, a
	}
	var relay string
	var portA, portB int
	err = tx.QueryRowContext(ctx, `SELECT relay_node,port_a,port_b FROM fabric_relays WHERE node_a=$1 AND node_b=$2`, a, b).Scan(&relay, &portA, &portB)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var endpoints string
	if err := tx.QueryRowContext(ctx, `SELECT endpoints_json FROM fabric_nodes WHERE node_id=$1`, relay).Scan(&endpoints); err != nil {
		return err
	}
	var relayEndpoints []string
	if err := json.Unmarshal([]byte(endpoints), &relayEndpoints); err != nil || len(relayEndpoints) == 0 {
		return err
	}
	host, _, err := net.SplitHostPort(relayEndpoints[0])
	if err != nil {
		return nil
	}
	port := portA
	if self == b {
		port = portB
	}
	peer.RelayEndpoint = net.JoinHostPort(host, strconv.Itoa(port))
	return nil
}

// relaysOfTx are the pairs node relays, with where it sees each side.
func relaysOfTx(ctx context.Context, tx *sql.Tx, node string) ([]FabricRelay, error) {
	var out []FabricRelay
	err := eachRow(ctx, tx, `SELECT r.node_a,r.node_b,r.port_a,r.port_b,
		COALESCE((SELECT endpoint FROM fabric_links WHERE src_node=$1 AND dst_node=r.node_a),''),COALESCE((SELECT endpoint FROM fabric_links WHERE src_node=$1 AND dst_node=r.node_b),'')
		FROM fabric_relays r WHERE r.relay_node=$1 ORDER BY r.node_a,r.node_b`, []any{node}, func(rows *sql.Rows) error {
		var r FabricRelay
		var ea, eb string
		if err := rows.Scan(&r.A, &r.B, &r.PortA, &r.PortB, &ea, &eb); err != nil {
			return err
		}
		if h, _, err := net.SplitHostPort(ea); err == nil {
			r.HostA = h
		}
		if h, _, err := net.SplitHostPort(eb); err == nil {
			r.HostB = h
		}
		out = append(out, r)
		return nil
	})
	return out, err
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
	// Sent are samples this node sent to a peer (From: that peer): the
	// bandwidth of the link from this node to it (fabric_evidence.go).
	Sent     []FabricReceived `json:"sent,omitempty"`
	Peers    []FabricPeerSeen `json:"peers"`
	Received []FabricReceived `json:"received,omitempty"`
}

// fabricClockSkew is how far ahead of the daemon's clock a node's may be.
const fabricClockSkew = 5 * time.Minute

// check refuses what no node sees of a link (ErrFabric): a handshake time
// that is not one, or later than now by more than a clock's skew (it would
// keep a link up for good, and every view that compares it fail), an
// endpoint that is not an address and port (every peer would be given it),
// a round trip or a transfer that is not a measure.
func (r FabricLinksReport) check(now time.Time) error {
	for _, p := range r.Peers {
		if p.HandshakeAt != "" {
			at, err := time.Parse(time.RFC3339Nano, p.HandshakeAt)
			if err != nil {
				return fmt.Errorf("%w: handshake_at %q to %s: %v", ErrFabric, p.HandshakeAt, p.Node, err)
			}
			if at.After(now.Add(fabricClockSkew)) {
				return fmt.Errorf("%w: handshake_at %s to %s is ahead of the daemon's clock", ErrFabric, p.HandshakeAt, p.Node)
			}
		}
		if p.Endpoint != "" {
			if _, err := netip.ParseAddrPort(p.Endpoint); err != nil {
				return fmt.Errorf("%w: endpoint %q to %s: %v", ErrFabric, p.Endpoint, p.Node, err)
			}
		}
		if p.RTTMillis != nil && (math.IsNaN(*p.RTTMillis) || math.IsInf(*p.RTTMillis, 0) || *p.RTTMillis > 60_000) {
			return fmt.Errorf("%w: rtt_ms %v to %s", ErrFabric, *p.RTTMillis, p.Node)
		}
	}
	for _, x := range append(append([]FabricReceived{}, r.Received...), r.Sent...) {
		if x.Bytes < 0 || math.IsNaN(x.Seconds) || math.IsInf(x.Seconds, 0) || x.Seconds < 0 || x.Seconds > 86400 {
			return fmt.Errorf("%w: a transfer of %d bytes in %v s with %s", ErrFabric, x.Bytes, x.Seconds, x.From)
		}
	}
	return nil
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
	if err := report.check(time.Now()); err != nil {
		return err
	}
	t := now()
	// a relay given a day ago is given up: its pair tries a direct path again
	// (a firewall opened since), and is relayed again if it fails
	if err := unrelayTx(ctx, tx, `created_at::timestamptz<$1::timestamptz`, time.Now().Add(-relayTried).UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
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
		// no path to the peer, as at the report before: a relay for the pair
		var before sql.NullString
		var had bool
		if err := tx.QueryRowContext(ctx, `SELECT handshake_at,true FROM fabric_links WHERE src_node=$1 AND dst_node=$2`, nodeID, p.Node).Scan(&before, &had); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if had && !freshHandshake(before.String) && !freshHandshake(p.HandshakeAt) {
			if err := relayPairTx(ctx, tx, nodeID, p.Node); err != nil {
				return err
			}
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
	for _, s := range report.Sent {
		if s.Bytes < minBandwidthSample || s.Seconds <= 0 || s.From == nodeID {
			continue
		}
		// the link from this node to the peer it sent to
		if _, err := tx.ExecContext(ctx, `UPDATE fabric_links SET bytes_per_second=CASE WHEN bytes_per_second IS NULL THEN $1 ELSE (bytes_per_second+$1)/2 END,
			bandwidth_at=$2 WHERE src_node=$3 AND dst_node=$4`, float64(s.Bytes)/s.Seconds, t, nodeID, s.From); err != nil {
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

// handshakeFresh: a link whose latest handshake is this recent is up. A
// link carrying only keepalives renews its handshake every 2 minutes and
// up to a keepalive (25 s) more; its agent reports it every 30 to 90 s:
// a handshake of a link that is up can be 4 minutes old by then.
const handshakeFresh = 5 * time.Minute

func freshHandshake(at string) bool {
	t, err := time.Parse(time.RFC3339Nano, at)
	return err == nil && time.Since(t) < handshakeFresh
}

// relayPairTx gives the pair x, y a relay when it has none: the first node
// (by id) that offers relay ports, is neither, and has a fresh handshake
// with both; the two lowest of its ports no pair has.
func relayPairTx(ctx context.Context, tx *sql.Tx, x, y string) error {
	a, b := x, y
	if b < a {
		a, b = b, a
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fabric_relays WHERE node_a=$1 AND node_b=$2)`, a, b).Scan(&exists); err != nil || exists {
		return err
	}
	since := time.Now().Add(-handshakeFresh).UTC().Format(time.RFC3339Nano)
	type candidate struct {
		node     string
		from, to int
	}
	var candidates []candidate
	err := eachRow(ctx, tx, `SELECT f.node_id,f.relay_from,f.relay_to FROM fabric_nodes f JOIN nodes n ON n.id=f.node_id
		WHERE n.enabled AND f.relay_from IS NOT NULL AND f.relay_to IS NOT NULL AND f.node_id NOT IN($1,$2)
		AND EXISTS(SELECT 1 FROM fabric_links l WHERE l.src_node=f.node_id AND l.dst_node=$1 AND l.handshake_at IS NOT NULL AND l.handshake_at::timestamptz>$3::timestamptz)
		AND EXISTS(SELECT 1 FROM fabric_links l WHERE l.src_node=f.node_id AND l.dst_node=$2 AND l.handshake_at IS NOT NULL AND l.handshake_at::timestamptz>$3::timestamptz)
		ORDER BY f.node_id`, []any{a, b, since}, func(rows *sql.Rows) error {
		var c candidate
		if err := rows.Scan(&c.node, &c.from, &c.to); err != nil {
			return err
		}
		candidates = append(candidates, c)
		return nil
	})
	if err != nil {
		return err
	}
	for _, c := range candidates {
		used := map[int]bool{}
		err := eachRow(ctx, tx, `SELECT port_a,port_b FROM fabric_relays WHERE relay_node=$1`, []any{c.node}, func(rows *sql.Rows) error {
			var pa, pb int
			if err := rows.Scan(&pa, &pb); err != nil {
				return err
			}
			used[pa], used[pb] = true, true
			return nil
		})
		if err != nil {
			return err
		}
		var free []int
		for port := c.from; port <= c.to && len(free) < 2; port++ {
			if !used[port] {
				free = append(free, port)
			}
		}
		if len(free) < 2 {
			continue // its ports all taken
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_relays(node_a,node_b,relay_node,port_a,port_b,created_at) VALUES($1,$2,$3,$4,$5,$6)`, a, b, c.node, free[0], free[1], now()); err != nil {
			return err
		}
		return appendCoordinationEventTx(ctx, tx, "fabric_relayed", nil, "node", a, nil, map[string]any{"a": a, "b": b, "relay": c.node, "port_a": free[0], "port_b": free[1]})
	}
	return nil // no relay: the pair waits for one
}

// relayTried: a relay is given up after this long, so that its pair tries
// a direct path again.
const relayTried = 24 * time.Hour

// unrelayTx gives up the relays where says (with arg), each recorded (event
// fabric_unrelayed).
func unrelayTx(ctx context.Context, tx *sql.Tx, where string, arg any) error {
	var gone [][3]string
	err := eachRow(ctx, tx, `DELETE FROM fabric_relays WHERE `+where+` RETURNING node_a,node_b,relay_node`, []any{arg}, func(rows *sql.Rows) error {
		var g [3]string
		if err := rows.Scan(&g[0], &g[1], &g[2]); err != nil {
			return err
		}
		gone = append(gone, g)
		return nil
	})
	if err != nil {
		return err
	}
	for _, g := range gone {
		if err := appendCoordinationEventTx(ctx, tx, "fabric_unrelayed", nil, "node", g[0], nil, map[string]any{"a": g[0], "b": g[1], "relay": g[2]}); err != nil {
			return err
		}
	}
	return nil
}
