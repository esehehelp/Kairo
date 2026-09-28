-- Node quarantine: while a node (or every node, node_id '*') is quarantined, its executors
-- reserve nothing, checkpointable attempts on it are suspended and the others are terminated.
-- Applied idempotently on every open, like the orchestration schema.
CREATE TABLE IF NOT EXISTS node_quarantines(
 node_id TEXT PRIMARY KEY,
 actor TEXT NOT NULL,
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL
);
-- A termination the node's executor must carry out on an attempt that cannot (or refused to)
-- checkpoint. signalled_at is set before the first signal is sent: from then on the termination
-- is no longer cancelled by a release, and the executor escalates to a forced kill after a grace
-- period. The row becomes 'terminated' once the executor sees the whole process tree gone.
CREATE TABLE IF NOT EXISTS quarantine_terminations(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 node_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','terminated')),
 requested_at TEXT NOT NULL,
 signalled_at TEXT,
 terminated_at TEXT
);
CREATE INDEX IF NOT EXISTS quarantine_terminations_executor_idx ON quarantine_terminations(executor_id,state);
