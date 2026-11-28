-- A checkpoint a worker asks for of its own attempt (POST /api/worker/checkpoints, issue #49): after its last
-- step, so a run that ran to its end can be continued from where its streams stood (task rerun --from-checkpoint).
ALTER TABLE commands DROP CONSTRAINT commands_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_origin_check
 CHECK(origin IN('scope_pause','priority_preemption','node_quarantine','operator_checkpoint','data_protection','owner_use','memory_pressure','worker'));
ALTER TABLE commands DROP CONSTRAINT commands_kind_origin_check;
ALTER TABLE commands ADD CONSTRAINT commands_kind_origin_check
 CHECK((kind='checkpoint') = (origin IN('operator_checkpoint','data_protection','worker')));
-- The code snapshot a stream-reading execution's stages run in when it is not the one its program runs in: a
-- continuation of a checkpoint in another snapshot (task rerun --from-checkpoint --source) runs its program there
-- and its stages where the checkpoint's stages ran (they are part of where its streams stand).
ALTER TABLE execution_requests ADD COLUMN stream_snapshot_id TEXT REFERENCES source_snapshots(id);
