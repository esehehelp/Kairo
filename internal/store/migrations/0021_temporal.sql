-- Temporal (regenerable) data (temporal.go): an orchestrated task whose
-- output can be made again by running it again declares so (regenerable).
-- Its runs' output directories, and directories imported as made by such a
-- task (producer_task_scope_id), are data objects of lifecycle temporal: not
-- packed and not copied to a durable store, only their identity is kept (the
-- file list, manifest), they stay on their volatile store to be read in
-- place, are evicted when a store runs short of room, and are made again by
-- rerunning the producing task when an execution needs them.
ALTER TABLE execution_requests ADD COLUMN regenerable BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE data_objects ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'durable' CHECK(lifecycle IN('durable','temporal'));
ALTER TABLE data_objects ADD COLUMN producer_task_scope_id TEXT;
ALTER TABLE data_objects ADD COLUMN manifest TEXT;
ALTER TABLE data_objects ADD COLUMN last_used_at TEXT;
ALTER TABLE data_objects DROP CONSTRAINT data_objects_source_state_check;
ALTER TABLE data_objects ADD CONSTRAINT data_objects_source_state_check CHECK(source_state IN('present','deleting','deleted','kept','evicting','evicted'));
-- Evicting (evictions.go): what a volatile store holds that can be had again
-- (an object brought there, temporal data) is deleted when the store runs
-- short of room, least recently read first; the executor serving it claims
-- each (evicting) and settles it.
ALTER TABLE object_placements DROP CONSTRAINT object_placements_state_check;
ALTER TABLE object_placements ADD CONSTRAINT object_placements_state_check CHECK(state IN('fetching','ready','evicting'));
