-- Facts about an attempt's logs that may mean its output was lost: the
-- filesystem holding them ran low on or out of space while it ran, or a node
-- agent could not ship them to the daemon. One row per attempt, problem and
-- stream ('' for both); a repeat updates it. Applied idempotently on every
-- open.
CREATE TABLE IF NOT EXISTS attempt_log_problems(
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 problem TEXT NOT NULL CHECK(problem IN('log_space_low','log_space_exhausted','log_forward_failed')),
 stream TEXT NOT NULL DEFAULT '' CHECK(stream IN('','stdout','stderr')),
 detail TEXT NOT NULL,
 occurrences INTEGER NOT NULL DEFAULT 1,
 first_seen_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL,
 resolved_at TEXT,
 PRIMARY KEY(attempt_id,problem,stream)
);
