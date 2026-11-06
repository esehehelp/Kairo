-- A run's output directory, <root>/outputs/<run id> on a volatile store of
-- the executor that launched one of the run's attempts there
-- (KAIRO_OUTPUT_DIR). A run is an execution with the continuations that
-- follow it: a Task's initial or rerun execution and its continuations, or
-- a direct execution; a gang rank other than 0 has a run of its own,
-- <leader's run>-rank<r>. Every attempt of the run launched on that store
-- gets the same directory. active while it is there; missing when a whole
-- scan of the store no longer finds it (Kairo did not delete it). The root
-- and path are those at creation.
CREATE TABLE run_outputs(
 run_id TEXT NOT NULL,
 store_id TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 root TEXT NOT NULL,
 path TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('active','missing')),
 last_attempt_id TEXT NOT NULL REFERENCES attempts(id),
 bytes BIGINT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 missing_at TEXT,
 PRIMARY KEY(run_id,store_id)
);
CREATE INDEX run_outputs_store_idx ON run_outputs(store_id,state);
-- The bytes of the store's run output directories, as its last report found
-- them (recorded ones and any found without a record).
ALTER TABLE store_observations ADD COLUMN output_bytes BIGINT NOT NULL DEFAULT 0;
