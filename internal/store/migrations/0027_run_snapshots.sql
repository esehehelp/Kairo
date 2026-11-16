-- A run's output directory, packed while the run goes on: only the files changed since the run's previous
-- snapshot (snapshot_since), every run_snapshot_interval and after each checkpoint (runsnapshots.go).
ALTER TABLE data_objects DROP CONSTRAINT data_objects_kind_check;
ALTER TABLE data_objects ADD CONSTRAINT data_objects_kind_check CHECK(kind IN('output','scratch','import','run-snapshot'));
ALTER TABLE data_objects ADD COLUMN snapshot_since TEXT;
