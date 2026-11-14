-- Code snapshots (sources.go): the files of a project's declared source
-- paths at apply time, packed (the pack and file list kept by the daemon),
-- named by the sha256 of the file list without mtimes. A task declared with
-- a source is bound to the snapshot made when it was declared, its
-- executions run in it.
CREATE TABLE source_snapshots(
 id TEXT PRIMARY KEY,
 pack_bytes BIGINT NOT NULL, pack_sha256 TEXT NOT NULL,
 list_sha256 TEXT NOT NULL, list TEXT NOT NULL,
 files BIGINT NOT NULL, content_bytes BIGINT NOT NULL, skipped BIGINT NOT NULL DEFAULT 0,
 project TEXT, source_name TEXT,
 origin_host TEXT, origin_root TEXT, git_head TEXT, git_dirty BOOLEAN,
 created_by TEXT, created_at TEXT NOT NULL
);
ALTER TABLE orchestration_tasks ADD COLUMN source_snapshot_id TEXT REFERENCES source_snapshots(id);
ALTER TABLE execution_requests ADD COLUMN source_snapshot_id TEXT REFERENCES source_snapshots(id);
