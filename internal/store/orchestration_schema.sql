PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS orchestration_schema_migrations(
 version INTEGER PRIMARY KEY,
 applied_at TEXT NOT NULL
);
INSERT OR IGNORE INTO orchestration_schema_migrations
 VALUES(1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));

-- This layer gives selected coordination scopes generic workflow meaning.
-- The lower coordination schema and immutable execution contract remain V3.
CREATE TABLE IF NOT EXISTS project_declarations(
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 spec_digest TEXT NOT NULL,
 spec_json TEXT NOT NULL,
 applied_at TEXT NOT NULL,
 PRIMARY KEY(project_scope_id,spec_digest)
);

CREATE TABLE IF NOT EXISTS orchestration_tasks(
 task_scope_id TEXT PRIMARY KEY REFERENCES coordination_scopes(id),
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 name TEXT NOT NULL,
 queue_name TEXT NOT NULL,
 policy_id TEXT NOT NULL,
 policy_version INTEGER NOT NULL,
 spec_digest TEXT NOT NULL,
 spec_json TEXT NOT NULL,
 execution_template_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','running','succeeded','failed','blocked')),
 result_json TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 UNIQUE(project_scope_id,name)
);

CREATE TABLE IF NOT EXISTS orchestration_task_dependencies(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 dependency_task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 PRIMARY KEY(task_scope_id,dependency_task_scope_id),
 CHECK(task_scope_id != dependency_task_scope_id)
);

CREATE TABLE IF NOT EXISTS orchestration_decisions(
 decision_key TEXT PRIMARY KEY,
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 reason TEXT NOT NULL CHECK(reason IN('initial','continuation')),
 trigger_json TEXT NOT NULL,
 execution_spec_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('planned','submitted')),
 execution_id TEXT REFERENCES execution_requests(id),
 created_at TEXT NOT NULL,
 submitted_at TEXT,
 UNIQUE(task_scope_id,execution_ordinal)
);

CREATE TABLE IF NOT EXISTS orchestration_task_executions(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 decision_key TEXT NOT NULL UNIQUE REFERENCES orchestration_decisions(decision_key),
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(task_scope_id,execution_ordinal)
);

CREATE INDEX IF NOT EXISTS orchestration_tasks_project_idx
 ON orchestration_tasks(project_scope_id,state,name);
CREATE INDEX IF NOT EXISTS orchestration_decisions_state_idx
 ON orchestration_decisions(state,created_at);
