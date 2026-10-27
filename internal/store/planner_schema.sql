-- Admission planning (planner.go). Applied idempotently on every open.
--
-- admission_waits: why each waiting execution is not running yet, as the last
-- planning pass found it. A row is rewritten only when the reason changes and
-- removed once the execution is reserved or no longer waits.
CREATE TABLE IF NOT EXISTS admission_waits(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 reason_code TEXT NOT NULL,
 node_id TEXT,
 detail_json TEXT NOT NULL,
 since TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
-- attempt_usage: what an attempt's process tree uses now, sampled by its
-- executor. Admission counts a lease's RAM as used, not twice, for the part
-- its attempt already uses; bytes written are kept for operators only.
CREATE TABLE IF NOT EXISTS attempt_usage(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 ram_bytes INTEGER,
 disk_written_bytes INTEGER,
 observed_at TEXT NOT NULL
);
-- node_alerts: conditions on a node that stop admission there (a filesystem
-- under its free-space watermark), one row while the condition holds.
CREATE TABLE IF NOT EXISTS node_alerts(
 node_id TEXT NOT NULL REFERENCES nodes(id),
 kind TEXT NOT NULL,
 subject TEXT NOT NULL,
 detail_json TEXT NOT NULL,
 since TEXT NOT NULL,
 PRIMARY KEY(node_id,kind,subject)
);
