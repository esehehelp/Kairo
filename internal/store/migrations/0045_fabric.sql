-- The fabric (fabric.go, issue #47): each node's fixed address on it, given once and never changed, and what its
-- WireGuard peers need of it (its public key, its listen port, the endpoints it says it is reached at).
CREATE TABLE fabric_nodes(
 node_id TEXT PRIMARY KEY REFERENCES nodes(id),
 address TEXT NOT NULL UNIQUE,
 public_key TEXT,
 listen_port INTEGER,
 endpoints_json TEXT NOT NULL DEFAULT '[]',
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
