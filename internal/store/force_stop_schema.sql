-- Force stop (`pause --force`): a pause operation that ends the executions it
-- captures instead of asking them to checkpoint. Applied idempotently on every
-- open, like the orchestration schema.
CREATE TABLE IF NOT EXISTS force_stop_operations(
 operation_id TEXT PRIMARY KEY REFERENCES pause_operations(id),
 grace_seconds INTEGER NOT NULL CHECK(grace_seconds>=0),
 reason TEXT NOT NULL
);
-- One row per execution a force stop ended. 'withdrawn': it had not started
-- and was withdrawn. Otherwise the executor of its attempt stops the process
-- tree: 'pending' (not picked up yet) -> 'signalled' (graceful stop sent, the
-- grace period runs from signalled_at) -> 'terminated' (no process of the tree
-- is left). Lease release still goes through the ordinary quiescence proof.
CREATE TABLE IF NOT EXISTS force_stops(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 operation_id TEXT NOT NULL REFERENCES pause_operations(id),
 attempt_id TEXT REFERENCES attempts(id),
 executor_id TEXT,
 grace_seconds INTEGER NOT NULL CHECK(grace_seconds>=0),
 state TEXT NOT NULL CHECK(state IN('withdrawn','pending','signalled','terminated')),
 detail_json TEXT NOT NULL DEFAULT '{}',
 requested_at TEXT NOT NULL,
 signalled_at TEXT,
 terminated_at TEXT
);
CREATE INDEX IF NOT EXISTS force_stops_executor_idx ON force_stops(executor_id,state);
CREATE INDEX IF NOT EXISTS force_stops_operation_idx ON force_stops(operation_id);
