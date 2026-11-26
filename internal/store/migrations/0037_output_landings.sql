-- A run's output directory brought to where an execution is to run, before it is launched there (outputlandings.go):
-- a packed object fetched to a volatile store of that node and unpacked as <root>/outputs/<run id> there. A
-- continuation the planner sends to another node than the one holding its run's output directory (it is expected to
-- end sooner there) has that directory packed where it is, as an object of kind output-move (temporal: never copied
-- to a durable store), and removed there once it has landed (reason move). A rerun from a checkpoint lands the output
-- of the run that published the checkpoint, from its packed output (reason checkpoint).
CREATE TABLE output_landings(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 run_id TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason IN('move','checkpoint')),
 from_run_id TEXT NOT NULL,
 from_store_id TEXT,
 object_id TEXT REFERENCES data_objects(id),
 node_id TEXT NOT NULL,
 store_id TEXT,
 executor_id TEXT,
 state TEXT NOT NULL CHECK(state IN('waiting','packing','bringing','placed','done','failed')),
 bytes BIGINT,
 estimate_json TEXT,
 problem TEXT,
 -- where the executor bringing it reads the pack from, and the token it reads it with (as object_placements)
 token_hash TEXT,
 token_expires_at TEXT,
 source_store_id TEXT,
 source_root TEXT,
 source_month TEXT,
 source_node_id TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE INDEX output_landings_run_idx ON output_landings(run_id,state);
CREATE INDEX output_landings_object_idx ON output_landings(object_id);
ALTER TABLE data_objects DROP CONSTRAINT data_objects_kind_check;
ALTER TABLE data_objects ADD CONSTRAINT data_objects_kind_check CHECK(kind IN('output','scratch','import','run-snapshot','output-move'));
-- a directory landed before any attempt of its run ran in it has no last attempt yet; one a move took elsewhere is
-- moved until its agent has removed it (then deleted)
ALTER TABLE run_outputs ALTER COLUMN last_attempt_id DROP NOT NULL;
ALTER TABLE run_outputs DROP CONSTRAINT run_outputs_state_check;
ALTER TABLE run_outputs ADD CONSTRAINT run_outputs_state_check CHECK(state IN('active','missing','deleted','moved'));
