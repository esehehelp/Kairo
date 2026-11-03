-- What the agent serving a store last saw of it: whether the store is usable
-- (its root exists and is on its declared filesystem), the filesystem's total
-- and free bytes, and the bytes Kairo knows there by class. One row a store,
-- replaced at every report.
CREATE TABLE store_observations(
 store_id TEXT PRIMARY KEY,
 executor_id TEXT NOT NULL,
 observed_at TEXT NOT NULL,
 usable BOOLEAN NOT NULL,
 problem TEXT,
 total_bytes BIGINT,
 free_bytes BIGINT,
 scratch_active_bytes BIGINT NOT NULL DEFAULT 0,
 scratch_kept_bytes BIGINT NOT NULL DEFAULT 0,
 pinned_bytes BIGINT NOT NULL DEFAULT 0,
 cache_bytes BIGINT NOT NULL DEFAULT 0
);

-- An attempt's scratch directory, <root>/scratch/<attempt id> on a volatile
-- store of its executor (KAIRO_SCRATCH). active while the attempt runs; then
-- deleted (the attempt succeeded) or kept (it failed or was lost). The root
-- and path are those at creation: a later declaration does not move them.
CREATE TABLE attempt_scratch(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 store_id TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 root TEXT NOT NULL,
 path TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('active','kept','deleted')),
 bytes BIGINT,
 reason TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 settled_at TEXT
);
CREATE INDEX attempt_scratch_store_idx ON attempt_scratch(store_id,state);
CREATE INDEX attempt_scratch_executor_idx ON attempt_scratch(executor_id,state);
