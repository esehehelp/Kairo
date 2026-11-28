-- Directed links of the fabric (fabric.go, issue #47): the path from src_node to dst_node as src's agent sees it
-- (direct or through a relay, the endpoint WireGuard uses, the latest handshake, the round trip), and how fast what
-- dst received from src came (an average of its large transfers; forgotten when the path changes). probe_port is
-- where a node's peers time a TCP connect to it (its transfer endpoint's port).
ALTER TABLE fabric_nodes ADD COLUMN probe_port INTEGER;
CREATE TABLE fabric_links(
 src_node TEXT NOT NULL REFERENCES nodes(id),
 dst_node TEXT NOT NULL REFERENCES nodes(id),
 path TEXT NOT NULL CHECK(path IN('direct','relay')),
 endpoint TEXT NOT NULL DEFAULT '',
 handshake_at TEXT,
 rtt_ms DOUBLE PRECISION,
 rtt_at TEXT,
 bytes_per_second DOUBLE PRECISION,
 bandwidth_at TEXT,
 updated_at TEXT NOT NULL,
 PRIMARY KEY(src_node, dst_node)
);
