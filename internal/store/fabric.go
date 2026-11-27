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
}

// FabricNode is a node on the fabric.
type FabricNode struct {
	NodeID     string   `json:"node_id"`
	Address    string   `json:"address"`
	PublicKey  string   `json:"public_key,omitempty"`
	ListenPort int      `json:"listen_port,omitempty"`
	Endpoints  []string `json:"endpoints"`
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO fabric_nodes(node_id,address,public_key,listen_port,endpoints_json,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)`,
			nodeID, address, self.PublicKey, self.ListenPort, string(endpoints), t, t); err != nil {
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
		if _, err := tx.ExecContext(ctx, `UPDATE fabric_nodes SET public_key=$1,listen_port=$2,endpoints_json=$3,updated_at=$4 WHERE node_id=$5
			AND (public_key IS DISTINCT FROM $1 OR listen_port IS DISTINCT FROM $2 OR endpoints_json<>$3)`, self.PublicKey, self.ListenPort, string(endpoints), t, nodeID); err != nil {
			return FabricView{}, err
		}
	}
	view := FabricView{Address: address, Prefix: p.String(), Peers: []FabricNode{}}
	err = eachRow(ctx, tx, `SELECT f.node_id,f.address,f.public_key,f.listen_port,f.endpoints_json,f.updated_at FROM fabric_nodes f JOIN nodes n ON n.id=f.node_id
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
	if err := rows.Scan(&n.NodeID, &n.Address, &key, &port, &endpoints, &n.UpdatedAt); err != nil {
		return n, err
	}
	n.PublicKey, n.ListenPort = key.String, int(port.Int64)
	if err := json.Unmarshal([]byte(endpoints), &n.Endpoints); err != nil {
		return n, err
	}
	return n, nil
}

// FabricNodes are the nodes on the fabric, by id.
func (s *Store) FabricNodes(ctx context.Context) ([]FabricNode, error) {
	out := []FabricNode{}
	err := eachRow(ctx, s.db, `SELECT node_id,address,public_key,listen_port,endpoints_json,updated_at FROM fabric_nodes ORDER BY node_id`, nil, func(rows *sql.Rows) error {
		n, err := scanFabricNode(rows)
		if err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
}
