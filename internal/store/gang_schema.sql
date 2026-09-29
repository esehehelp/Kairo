-- Multi-node gang executions (gang.go). A gang is Size ordinary execution
-- requests, one per rank, that are placed together on distinct nodes, launched
-- only when every rank is prepared, and stopped together. Applied idempotently
-- on every open, like the orchestration schema.
CREATE TABLE IF NOT EXISTS execution_gangs(
 id TEXT PRIMARY KEY,
 leader_execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 size INTEGER NOT NULL CHECK(size>=2),
 port INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gang_members(
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 rank INTEGER NOT NULL CHECK(rank>=0),
 execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 PRIMARY KEY(gang_id,rank)
);
-- One placement: a lease per rank, created in one transaction. 'placed' until
-- the first rank is authorized ('launched'); 'aborted' when a rank could not be
-- prepared in time or lost its lease before launch.
CREATE TABLE IF NOT EXISTS gang_placements(
 id TEXT PRIMARY KEY,
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 state TEXT NOT NULL CHECK(state IN('placed','launched','aborted')),
 master_addr TEXT NOT NULL,
 master_port INTEGER NOT NULL,
 master_node_id TEXT NOT NULL,
 abort_reason TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gang_placement_leases(
 placement_id TEXT NOT NULL REFERENCES gang_placements(id),
 rank INTEGER NOT NULL,
 lease_id TEXT NOT NULL UNIQUE REFERENCES leases(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 node_id TEXT NOT NULL,
 delivered_at TEXT,
 PRIMARY KEY(placement_id,rank)
);
-- A termination of a running rank because another rank of its gang stopped.
-- Carried out by the rank's executor like a quarantine termination.
CREATE TABLE IF NOT EXISTS gang_terminations(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 reason TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','terminated')),
 requested_at TEXT NOT NULL,
 terminated_at TEXT
);
CREATE INDEX IF NOT EXISTS gang_placement_leases_executor_idx ON gang_placement_leases(executor_id,delivered_at);
CREATE INDEX IF NOT EXISTS gang_placements_state_idx ON gang_placements(state,created_at);
CREATE INDEX IF NOT EXISTS gang_terminations_executor_idx ON gang_terminations(executor_id,state);
