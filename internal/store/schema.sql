PRAGMA foreign_keys = ON;

CREATE TABLE schema_migrations(
 version INTEGER PRIMARY KEY,
 applied_at TEXT NOT NULL
);
INSERT INTO schema_migrations VALUES(3, strftime('%Y-%m-%dT%H:%M:%fZ','now'));

-- Inventory and physical observations. These rows never imply ownership.
CREATE TABLE nodes(
 id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, os TEXT NOT NULL, architecture TEXT NOT NULL,
 attributes_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE executors(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), kind TEXT NOT NULL,
 attributes_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 last_seen_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE resource_providers(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), kind TEXT NOT NULL,
 config_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE resource_instances(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), provider_id TEXT NOT NULL REFERENCES resource_providers(id),
 kind TEXT NOT NULL, stable_identity TEXT NOT NULL, binding_json TEXT NOT NULL, attributes_json TEXT NOT NULL DEFAULT '{}',
 admin_state TEXT NOT NULL DEFAULT 'enabled' CHECK(admin_state IN('enabled','disabled','quarantined')),
 quarantine_reason TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(provider_id,stable_identity)
);
CREATE TABLE resource_observations(
 id INTEGER PRIMARY KEY AUTOINCREMENT, resource_id TEXT NOT NULL REFERENCES resource_instances(id),
 observed_at TEXT NOT NULL, valid_until TEXT NOT NULL, total_bytes INTEGER, free_bytes INTEGER,
 utilization REAL, temperature_c REAL, evidence_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE external_claims(
 id TEXT PRIMARY KEY, resource_id TEXT NOT NULL REFERENCES resource_instances(id),
 claim_kind TEXT NOT NULL CHECK(claim_kind IN('external_process','unattributed_activity')),
 process_identity TEXT, evidence_json TEXT NOT NULL, first_observed_at TEXT NOT NULL,
 last_observed_at TEXT NOT NULL, cleared_at TEXT
);

-- Project, queue, and task names are opaque coordination scopes only.
CREATE TABLE coordination_scopes(
 id TEXT PRIMARY KEY,
 kind TEXT NOT NULL CHECK(kind IN('project','queue','task')),
 parent_id TEXT REFERENCES coordination_scopes(id),
 external_key TEXT NOT NULL,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 CHECK((kind='project' AND parent_id IS NULL) OR (kind!='project' AND parent_id IS NOT NULL))
);
CREATE UNIQUE INDEX project_scope_key ON coordination_scopes(external_key) WHERE kind='project';
CREATE UNIQUE INDEX child_scope_key ON coordination_scopes(parent_id,kind,external_key) WHERE kind!='project';
CREATE TABLE admission_gates(
 scope_id TEXT PRIMARY KEY REFERENCES coordination_scopes(id),
 state TEXT NOT NULL DEFAULT 'open' CHECK(state IN('open','closed')),
 generation INTEGER NOT NULL DEFAULT 1 CHECK(generation>=1),
 updated_at TEXT NOT NULL
);

-- One immutable request permits at most one process execution. Terminal never
-- means success or failure and can never transition back to waiting.
CREATE TABLE execution_requests(
 id TEXT PRIMARY KEY,
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 queue_scope_id TEXT REFERENCES coordination_scopes(id),
 task_scope_id TEXT REFERENCES coordination_scopes(id),
 client_request_id TEXT NOT NULL,
 spec_digest TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'waiting' CHECK(state IN('waiting','authorized','started','terminal')),
 argv_json TEXT NOT NULL,
 cwd TEXT NOT NULL,
 executor_selector_json TEXT NOT NULL DEFAULT '{}',
 priority INTEGER NOT NULL DEFAULT 0,
 checkpointable INTEGER NOT NULL DEFAULT 0 CHECK(checkpointable IN(0,1)),
 preemptible INTEGER NOT NULL DEFAULT 0 CHECK(preemptible IN(0,1)),
 input_continuation_ref TEXT,
 terminal_cause TEXT,
 submitted_at TEXT NOT NULL,
 authorized_at TEXT,
 started_at TEXT,
 terminal_at TEXT,
 UNIQUE(project_scope_id,client_request_id),
 CHECK(preemptible=0 OR checkpointable=1)
);
CREATE TABLE resource_requests(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 request_type TEXT NOT NULL CHECK(request_type IN('exclusive','capacity')),
 kind TEXT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity>=0),
 filesystem TEXT NOT NULL DEFAULT '',
 constraints_json TEXT NOT NULL DEFAULT '{}',
 policy_json TEXT NOT NULL DEFAULT '{}',
 strength TEXT NOT NULL DEFAULT 'admitted' CHECK(strength IN('admitted','enforced'))
);
CREATE TABLE admission_blocks(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 reason_code TEXT NOT NULL,
 detail_json TEXT NOT NULL,
 created_at TEXT NOT NULL,
 cleared_at TEXT
);

-- Coordination facts.
CREATE TABLE attempts(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 state TEXT NOT NULL CHECK(state IN('authorized','running','quiescing','exited','quiesced','lost')),
 executor_id TEXT NOT NULL,
 coordination_epoch INTEGER NOT NULL DEFAULT 1,
 pid INTEGER,
 process_identity TEXT,
 exit_code INTEGER,
 exit_signal TEXT,
 continuation_ref TEXT,
 progress_json TEXT,
 stdout_path TEXT,
 stderr_path TEXT,
 authorized_at TEXT,
 started_at TEXT,
 last_heartbeat_at TEXT,
 checkpointed_at TEXT,
 exited_at TEXT,
 quiesced_at TEXT
);
CREATE TABLE attempt_processes(
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 role TEXT NOT NULL CHECK(role IN('launcher','worker')),
 namespace TEXT NOT NULL CHECK(namespace IN('host','executor')),
 rank INTEGER NOT NULL,
 pid INTEGER NOT NULL,
 process_identity TEXT NOT NULL,
 registered_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL,
 exited_at TEXT,
 PRIMARY KEY(attempt_id,role,rank),
 CHECK((role='launcher' AND rank=-1 AND namespace='host') OR (role='worker' AND rank>=0))
);
CREATE TABLE leases(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 attempt_id TEXT REFERENCES attempts(id),
 executor_id TEXT NOT NULL,
 coordination_epoch INTEGER NOT NULL DEFAULT 1,
 state TEXT NOT NULL CHECK(state IN('reserved','prepared','active','releasing','released','stale','revocation_requested')),
 expires_at TEXT,
 created_at TEXT NOT NULL,
 prepared_at TEXT,
 activated_at TEXT,
 releasing_at TEXT,
 revocation_requested_at TEXT,
 released_at TEXT,
 stale_at TEXT
);
CREATE TABLE lease_gate_snapshots(
 lease_id TEXT NOT NULL REFERENCES leases(id),
 scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 generation INTEGER NOT NULL,
 PRIMARY KEY(lease_id,scope_id)
);
CREATE TABLE lease_items(
 lease_id TEXT NOT NULL REFERENCES leases(id),
 resource_id TEXT REFERENCES resource_instances(id),
 kind TEXT NOT NULL DEFAULT 'exclusive',
 quantity INTEGER NOT NULL DEFAULT 1,
 filesystem TEXT NOT NULL DEFAULT '',
 prepared INTEGER NOT NULL DEFAULT 0 CHECK(prepared IN(0,1)),
 PRIMARY KEY(lease_id,kind,resource_id,filesystem)
);
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items
WHEN NEW.resource_id IS NOT NULL AND NEW.kind='gpu' AND EXISTS(
 SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
 WHERE li.resource_id=NEW.resource_id AND li.kind='gpu'
 AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
BEGIN SELECT RAISE(ABORT,'resource already leased'); END;
CREATE TABLE launch_authorizations(
 id TEXT PRIMARY KEY,
 lease_id TEXT NOT NULL UNIQUE REFERENCES leases(id),
 attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(id),
 coordination_epoch INTEGER NOT NULL,
 token_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('issued','consumed','revoked')),
 issued_at TEXT NOT NULL,
 consumed_at TEXT,
 revoked_at TEXT
);
CREATE TABLE commands(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 lease_id TEXT NOT NULL REFERENCES leases(id),
 coordination_epoch INTEGER NOT NULL,
 kind TEXT NOT NULL DEFAULT 'suspend' CHECK(kind='suspend'),
 origin TEXT NOT NULL CHECK(origin IN('scope_pause','priority_preemption')),
 reason TEXT NOT NULL,
 payload_json TEXT NOT NULL DEFAULT '{}',
 state TEXT NOT NULL CHECK(state IN('pending','accepted','checkpointing','checkpointed','rejected')),
 delivery_count INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX one_live_suspend_per_attempt ON commands(attempt_id)
 WHERE state IN('pending','accepted','checkpointing','checkpointed');
CREATE TABLE command_deliveries(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 command_id TEXT NOT NULL REFERENCES commands(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 coordination_epoch INTEGER NOT NULL,
 delivered_at TEXT NOT NULL
);
CREATE TABLE command_acks(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 command_id TEXT NOT NULL REFERENCES commands(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 coordination_epoch INTEGER NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN('accepted','checkpointing','checkpointed','rejected')),
 payload_json TEXT NOT NULL,
 created_at TEXT NOT NULL
);

CREATE TABLE pause_operations(
 id TEXT PRIMARY KEY,
 scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 scope_generation INTEGER NOT NULL,
 request_id TEXT NOT NULL,
 actor TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('requested','quiescing','quiesced','blocked')),
 detail_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 UNIQUE(scope_id,request_id)
);
CREATE TABLE pause_targets(
 operation_id TEXT NOT NULL REFERENCES pause_operations(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 attempt_id TEXT REFERENCES attempts(id),
 lease_id TEXT REFERENCES leases(id),
 command_id TEXT REFERENCES commands(id),
 state TEXT NOT NULL CHECK(state IN('pending','revoking','quiescing','quiesced','blocked')),
 blocker_reason TEXT,
 updated_at TEXT NOT NULL,
 PRIMARY KEY(operation_id,execution_id)
);

CREATE TABLE coordination_events(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 event_type TEXT NOT NULL,
 project_scope_id TEXT REFERENCES coordination_scopes(id),
 aggregate_type TEXT NOT NULL,
 aggregate_id TEXT NOT NULL,
 coordination_epoch INTEGER,
 payload_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL
);

CREATE INDEX observations_latest_idx ON resource_observations(resource_id,id DESC);
CREATE INDEX claims_active_idx ON external_claims(resource_id,cleared_at);
CREATE INDEX executions_admission_idx ON execution_requests(state,priority,submitted_at);
CREATE INDEX executions_project_idx ON execution_requests(project_scope_id,submitted_at);
CREATE INDEX resource_requests_execution_idx ON resource_requests(execution_id);
CREATE INDEX attempts_execution_idx ON attempts(execution_id);
CREATE INDEX leases_execution_idx ON leases(execution_id,state);
CREATE INDEX commands_attempt_idx ON commands(attempt_id,state,created_at);
CREATE INDEX pause_operations_scope_idx ON pause_operations(scope_id,state,created_at);
CREATE INDEX events_project_sequence_idx ON coordination_events(project_scope_id,sequence);
