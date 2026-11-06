-- Imported data (kairo data import): a directory on a node, outside every
-- store, packed by the node's executor onto its volatile store and copied to
-- a durable store like any data object (kind import, source_id the object
-- itself, source_path the directory). name labels an object in its project
-- (an import's own, or the directory's last component); delete_source: the
-- import asked for the directory to be deleted once a good copy exists (an
-- import without it keeps the directory, only its pack goes).
ALTER TABLE data_objects DROP CONSTRAINT data_objects_kind_check;
ALTER TABLE data_objects ADD CONSTRAINT data_objects_kind_check CHECK(kind IN('output','scratch','import'));
ALTER TABLE data_objects ADD COLUMN name TEXT;
ALTER TABLE data_objects ADD COLUMN delete_source BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX data_objects_name_idx ON data_objects(project_scope_id,name);
-- An execution's inputs (inputs.go): JSON [{name, task|data|object}], NULL
-- when it has none.
ALTER TABLE execution_requests ADD COLUMN inputs_json TEXT;
-- confine: its attempts may write only where Kairo hands them (scratch,
-- output directory, cache directory).
ALTER TABLE execution_requests ADD COLUMN confine BOOLEAN NOT NULL DEFAULT FALSE;
