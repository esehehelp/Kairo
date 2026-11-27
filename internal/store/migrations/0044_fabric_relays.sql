-- Relays of the fabric (fabric.go, issue #47): a node with a public endpoint may forward WireGuard's datagrams between
-- two nodes that reach each other on no direct path, still encrypted end to end: a UDP port for each side, from the
-- range it offers (relay_from to relay_to). A pair is relayed by one node at a time.
ALTER TABLE fabric_nodes ADD COLUMN relay_from INTEGER;
ALTER TABLE fabric_nodes ADD COLUMN relay_to INTEGER;
CREATE TABLE fabric_relays(
 node_a TEXT NOT NULL REFERENCES nodes(id),
 node_b TEXT NOT NULL REFERENCES nodes(id),
 relay_node TEXT NOT NULL REFERENCES nodes(id),
 port_a INTEGER NOT NULL,
 port_b INTEGER NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(node_a, node_b),
 CHECK(node_a < node_b),
 UNIQUE(relay_node, port_a),
 UNIQUE(relay_node, port_b)
);
