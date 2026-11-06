-- Data objects brought to a node for its executions to read (inputs.go):
-- fetched from a verified copy on a durable store (or from the pack where it
-- was made) and unpacked under <root>/inputs/<object id> on a volatile store
-- of the node by an executor serving it; fetching until that executor
-- reports it ready, with the bytes it holds. A fetch from another node's
-- agent is read from source_store_id at source_root (source_month: a durable
-- copy's month, NULL for a pack) on source_node_id, with a token issued for
-- it (its hash), valid until token_expires_at.
CREATE TABLE object_placements(
 object_id TEXT NOT NULL REFERENCES data_objects(id),
 store_id TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 root TEXT NOT NULL,
 path TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('fetching','ready')),
 bytes BIGINT,
 source_store_id TEXT,
 source_root TEXT,
 source_month TEXT,
 source_node_id TEXT,
 token_hash TEXT,
 token_expires_at TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 ready_at TEXT,
 PRIMARY KEY(object_id,store_id)
);
CREATE INDEX object_placements_executor_idx ON object_placements(executor_id,state);
