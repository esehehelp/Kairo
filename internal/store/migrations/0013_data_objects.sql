-- Data objects: what Kairo keeps for good, each packed once on the store it
-- was written on into <root>/packs/<object id>.tar.zst (tar compressed with
-- zstd) with its file list <object id>.tar.zst.files.tsv (path, bytes,
-- sha256 and mtime_ns of each regular file). The source is a run's output
-- directory once its run has ended (kind output, source_id the run) or a
-- failed attempt's kept scratch (kind scratch, source_id the attempt).
-- packing from when its executor is told to pack it until it reports the
-- pack made; packed then (moving the pack to a durable store comes next).
CREATE TABLE data_objects(
 id TEXT PRIMARY KEY,
 kind TEXT NOT NULL CHECK(kind IN('output','scratch')),
 source_id TEXT NOT NULL,
 store_id TEXT NOT NULL,
 executor_id TEXT NOT NULL,
 root TEXT NOT NULL,
 source_path TEXT NOT NULL,
 project_scope_id TEXT,
 state TEXT NOT NULL CHECK(state IN('packing','packed')),
 pack_path TEXT,
 manifest_path TEXT,
 content_bytes BIGINT,
 files BIGINT,
 skipped BIGINT,
 pack_bytes BIGINT,
 pack_sha256 TEXT,
 manifest_sha256 TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 packed_at TEXT,
 UNIQUE(kind,source_id,store_id)
);
CREATE INDEX data_objects_executor_idx ON data_objects(executor_id,state);
CREATE INDEX data_objects_store_idx ON data_objects(store_id,state);
