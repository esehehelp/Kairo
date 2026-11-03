-- Stores (`kairo machine apply`, [[stores]]): the places Kairo keeps data in.
-- A durable store keeps what is put there for good; a volatile one is
-- scratch space. Replaced as a whole with the machine declaration, like
-- machine_hosts; the declaration is kept as JSON with the columns it is
-- looked up by.
CREATE TABLE machine_stores(
 id TEXT PRIMARY KEY,
 node_id TEXT NOT NULL,
 executor_id TEXT,
 class TEXT NOT NULL CHECK(class IN('durable','volatile')),
 declaration_json TEXT NOT NULL,
 declared_at TEXT NOT NULL
);
CREATE INDEX machine_stores_node_idx ON machine_stores(node_id);
