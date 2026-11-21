-- The snapshot each source of a project was last applied with (kairo task rerun --source current), and the
-- snapshot a rerun runs in instead of its task's own.
CREATE TABLE project_sources(
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 source_name TEXT NOT NULL,
 snapshot_id TEXT NOT NULL REFERENCES source_snapshots(id),
 applied_at TEXT NOT NULL,
 PRIMARY KEY(project_scope_id, source_name)
);
ALTER TABLE orchestration_task_reruns ADD COLUMN source_snapshot_id TEXT REFERENCES source_snapshots(id);
