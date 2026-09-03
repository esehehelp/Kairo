PRAGMA foreign_keys = ON;

CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
INSERT INTO schema_migrations VALUES(1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
INSERT INTO schema_migrations VALUES(2, strftime('%Y-%m-%dT%H:%M:%fZ','now'));

-- Inventory: only administrative state is persisted here.
CREATE TABLE nodes(
 id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, os TEXT NOT NULL, architecture TEXT NOT NULL,
 attributes_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE executors(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), kind TEXT NOT NULL,
 attributes_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 last_seen_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE resource_providers(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), kind TEXT NOT NULL,
 config_json TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN(0,1)),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE resource_instances(
 id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id), provider_id TEXT NOT NULL REFERENCES resource_providers(id),
 kind TEXT NOT NULL, stable_identity TEXT NOT NULL, binding_json TEXT NOT NULL, attributes_json TEXT NOT NULL DEFAULT '{}',
 admin_state TEXT NOT NULL DEFAULT 'enabled' CHECK(admin_state IN('enabled','disabled','quarantined')),
 quarantine_reason TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(provider_id,stable_identity));

-- Reality cache. Observation and claim rows never imply ownership.
CREATE TABLE resource_observations(
 id INTEGER PRIMARY KEY AUTOINCREMENT, resource_id TEXT NOT NULL REFERENCES resource_instances(id),
 observed_at TEXT NOT NULL, valid_until TEXT NOT NULL, total_bytes INTEGER, free_bytes INTEGER,
 utilization REAL, temperature_c REAL, evidence_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE external_claims(
 id TEXT PRIMARY KEY, resource_id TEXT NOT NULL REFERENCES resource_instances(id),
 claim_kind TEXT NOT NULL CHECK(claim_kind IN('external_process','unattributed_activity')),
 process_identity TEXT, evidence_json TEXT NOT NULL, first_observed_at TEXT NOT NULL,
 last_observed_at TEXT NOT NULL, cleared_at TEXT);

-- Durable, revisioned plan.
CREATE TABLE queues(
 id TEXT PRIMARY KEY, project TEXT NOT NULL, name TEXT NOT NULL, current_revision INTEGER NOT NULL DEFAULT 0,
 desired_state TEXT NOT NULL DEFAULT 'active' CHECK(desired_state IN('active','paused')),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(project,name));
CREATE TABLE queue_revisions(
 queue_id TEXT NOT NULL REFERENCES queues(id), revision INTEGER NOT NULL, digest TEXT NOT NULL,
 normalized_json TEXT NOT NULL, source_text TEXT NOT NULL, actor TEXT NOT NULL, request_id TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(queue_id,revision), UNIQUE(queue_id,request_id));
CREATE TABLE tasks(
 id TEXT PRIMARY KEY, queue_id TEXT NOT NULL REFERENCES queues(id), task_key TEXT NOT NULL,
 current_revision INTEGER NOT NULL DEFAULT 0, desired_state TEXT NOT NULL DEFAULT 'active'
 CHECK(desired_state IN('active','paused','cancelled')), scheduling_state TEXT NOT NULL DEFAULT 'pending'
 CHECK(scheduling_state IN('pending','running','preempting','backoff','paused','cancelled','succeeded','failed','failed_admission','blocked')),
 became_runnable_at TEXT, effective_priority INTEGER NOT NULL DEFAULT 0, effective_retry_json TEXT NOT NULL DEFAULT '{}',
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(queue_id,task_key));
CREATE TABLE task_revisions(
 task_id TEXT NOT NULL REFERENCES tasks(id), revision INTEGER NOT NULL, queue_revision INTEGER NOT NULL,
 spec_digest TEXT NOT NULL, argv_json TEXT NOT NULL, cwd TEXT NOT NULL, executor_selector_json TEXT NOT NULL DEFAULT '{}',
 priority INTEGER NOT NULL DEFAULT 0, checkpointable INTEGER NOT NULL DEFAULT 0 CHECK(checkpointable IN(0,1)),
 retry_json TEXT NOT NULL, desired_state TEXT NOT NULL DEFAULT 'active' CHECK(desired_state IN('active','paused','cancelled')),
 scheduling_state TEXT NOT NULL DEFAULT 'pending'
 CHECK(scheduling_state IN('pending','running','preempting','backoff','paused','cancelled','succeeded','failed','failed_admission','blocked')),
 became_runnable_at TEXT, next_retry_at TEXT, superseded INTEGER NOT NULL DEFAULT 0 CHECK(superseded IN(0,1)), created_at TEXT NOT NULL,
 PRIMARY KEY(task_id,revision));
CREATE TABLE task_dependencies(
 task_id TEXT NOT NULL, task_revision INTEGER NOT NULL, dependency_task_id TEXT NOT NULL, dependency_revision INTEGER NOT NULL,
 PRIMARY KEY(task_id,task_revision,dependency_task_id),
 FOREIGN KEY(task_id,task_revision) REFERENCES task_revisions(task_id,revision),
 FOREIGN KEY(dependency_task_id,dependency_revision) REFERENCES task_revisions(task_id,revision));
CREATE TABLE resource_requests(
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL, task_revision INTEGER NOT NULL,
 request_type TEXT NOT NULL CHECK(request_type IN('exclusive','capacity')), kind TEXT NOT NULL,
 quantity INTEGER NOT NULL CHECK(quantity>=0), filesystem TEXT, constraints_json TEXT NOT NULL DEFAULT '{}',
 policy_json TEXT NOT NULL DEFAULT '{}', strength TEXT NOT NULL DEFAULT 'admitted' CHECK(strength IN('admitted','enforced')),
 FOREIGN KEY(task_id,task_revision) REFERENCES task_revisions(task_id,revision));
CREATE TABLE admission_blocks(
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), task_revision INTEGER NOT NULL,
 reason_code TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL, cleared_at TEXT,
 FOREIGN KEY(task_id,task_revision) REFERENCES task_revisions(task_id,revision));

-- Coordination facts.
CREATE TABLE attempts(
 id TEXT PRIMARY KEY, task_id TEXT REFERENCES tasks(id), task_revision INTEGER, workload_id TEXT,
 ordinal INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN('authorized','running','suspend_requested','exited','quiesced','lost')),
 executor_id TEXT NOT NULL, coordination_epoch INTEGER NOT NULL DEFAULT 1, pid INTEGER, process_identity TEXT,
 exit_code INTEGER, failure_class TEXT, failure_counted INTEGER NOT NULL DEFAULT 0 CHECK(failure_counted IN(0,1)),
 terminal_disposition TEXT CHECK(terminal_disposition IN('close','requeue','hold')), terminal_payload_json TEXT,
 continuation_ref TEXT, progress_json TEXT, stdout_path TEXT, stderr_path TEXT, authorized_at TEXT,
 started_at TEXT, last_heartbeat_at TEXT, checkpointed_at TEXT, exited_at TEXT, quiesced_at TEXT,
 UNIQUE(task_id,task_revision,ordinal), UNIQUE(workload_id,ordinal));
CREATE TABLE attempt_processes(
 attempt_id TEXT NOT NULL REFERENCES attempts(id), rank INTEGER NOT NULL, pid INTEGER NOT NULL,
 process_identity TEXT NOT NULL, registered_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, exited_at TEXT,
 PRIMARY KEY(attempt_id,rank));
CREATE TABLE leases(
 id TEXT PRIMARY KEY, attempt_id TEXT REFERENCES attempts(id), task_id TEXT REFERENCES tasks(id), task_revision INTEGER,
 executor_id TEXT NOT NULL DEFAULT 'compat', coordination_epoch INTEGER NOT NULL DEFAULT 1,
 state TEXT NOT NULL CHECK(state IN('reserved','prepared','active','releasing','released','stale','revocation_requested')),
 expires_at TEXT, created_at TEXT NOT NULL, prepared_at TEXT, activated_at TEXT, releasing_at TEXT,
 revocation_requested_at TEXT, released_at TEXT, stale_at TEXT);
CREATE TABLE lease_items(
 lease_id TEXT NOT NULL REFERENCES leases(id), resource_id TEXT REFERENCES resource_instances(id), kind TEXT NOT NULL DEFAULT 'exclusive',
 quantity INTEGER NOT NULL DEFAULT 1, filesystem TEXT NOT NULL DEFAULT '', prepared INTEGER NOT NULL DEFAULT 0 CHECK(prepared IN(0,1)),
 PRIMARY KEY(lease_id,kind,resource_id,filesystem));
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items
WHEN NEW.resource_id IS NOT NULL AND NEW.kind='gpu' AND EXISTS(
 SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
 WHERE li.resource_id=NEW.resource_id AND li.kind='gpu' AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
BEGIN SELECT RAISE(ABORT,'resource already leased'); END;
CREATE TABLE launch_authorizations(
 id TEXT PRIMARY KEY, lease_id TEXT NOT NULL UNIQUE REFERENCES leases(id), attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(id),
 coordination_epoch INTEGER NOT NULL, token_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('issued','consumed','revoked')), issued_at TEXT NOT NULL, consumed_at TEXT);
CREATE TABLE commands(
 id TEXT PRIMARY KEY, task_id TEXT REFERENCES tasks(id), workload_id TEXT, attempt_id TEXT NOT NULL REFERENCES attempts(id),
 lease_id TEXT REFERENCES leases(id), coordination_epoch INTEGER NOT NULL DEFAULT 1,
 kind TEXT NOT NULL CHECK(kind IN('suspend','cancel')), reason TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{}',
 state TEXT NOT NULL CHECK(state IN('pending','accepted','checkpointing','checkpointed','completed','rejected')),
 delivery_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT);
CREATE TABLE command_deliveries(
 id INTEGER PRIMARY KEY AUTOINCREMENT, command_id TEXT NOT NULL REFERENCES commands(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id), coordination_epoch INTEGER NOT NULL, delivered_at TEXT NOT NULL);
CREATE TABLE command_acks(
 id INTEGER PRIMARY KEY AUTOINCREMENT, command_id TEXT NOT NULL REFERENCES commands(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id), coordination_epoch INTEGER NOT NULL DEFAULT 1,
 phase TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE coordination_events(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, aggregate_type TEXT NOT NULL,
 aggregate_id TEXT NOT NULL, coordination_epoch INTEGER, payload_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL);

CREATE INDEX tasks_schedule_idx ON tasks(desired_state,scheduling_state,became_runnable_at);
CREATE INDEX observations_latest_idx ON resource_observations(resource_id,observed_at DESC);
CREATE INDEX claims_active_idx ON external_claims(resource_id,cleared_at);
CREATE INDEX attempts_task_idx ON attempts(task_id,task_revision,ordinal DESC);
CREATE INDEX commands_attempt_idx ON commands(attempt_id,state,created_at);
