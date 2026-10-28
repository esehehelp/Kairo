-- The schema of a Kairo V3 SQLite database (kairo-v3.db) as the last SQLite
-- Kairo left it, for the import tests. Generated from sqlite_master.

CREATE TABLE schema_migrations(
 version INTEGER PRIMARY KEY,
 applied_at TEXT NOT NULL
);
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
CREATE TABLE coordination_scopes(
 id TEXT PRIMARY KEY,
 kind TEXT NOT NULL CHECK(kind IN('project','queue','task')),
 parent_id TEXT REFERENCES coordination_scopes(id),
 external_key TEXT NOT NULL,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 CHECK((kind='project' AND parent_id IS NULL) OR (kind!='project' AND parent_id IS NOT NULL))
);
CREATE TABLE admission_gates(
 scope_id TEXT PRIMARY KEY REFERENCES coordination_scopes(id),
 state TEXT NOT NULL DEFAULT 'open' CHECK(state IN('open','closed')),
 generation INTEGER NOT NULL DEFAULT 1 CHECK(generation>=1),
 updated_at TEXT NOT NULL
);
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
 terminal_at TEXT, env_json TEXT,
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
CREATE TABLE orchestration_schema_migrations(
 version INTEGER PRIMARY KEY,
 applied_at TEXT NOT NULL
);
CREATE TABLE project_declarations(
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 spec_digest TEXT NOT NULL,
 spec_json TEXT NOT NULL,
 applied_at TEXT NOT NULL,
 PRIMARY KEY(project_scope_id,spec_digest)
);
CREATE TABLE orchestration_task_dependencies(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 dependency_task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 PRIMARY KEY(task_scope_id,dependency_task_scope_id),
 CHECK(task_scope_id != dependency_task_scope_id)
);
CREATE TABLE orchestration_task_executions(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 decision_key TEXT NOT NULL UNIQUE REFERENCES orchestration_decisions(decision_key),
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(task_scope_id,execution_ordinal)
);
CREATE TABLE "commands"(
 id TEXT PRIMARY KEY,
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 lease_id TEXT NOT NULL REFERENCES leases(id),
 coordination_epoch INTEGER NOT NULL,
 kind TEXT NOT NULL DEFAULT 'suspend' CHECK(kind='suspend'),
 origin TEXT NOT NULL CHECK(origin IN('scope_pause','priority_preemption','node_quarantine')),
 reason TEXT NOT NULL,
 payload_json TEXT NOT NULL DEFAULT '{}',
 state TEXT NOT NULL CHECK(state IN('pending','accepted','checkpointing','checkpointed','rejected')),
 delivery_count INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE TABLE node_quarantines(
 node_id TEXT PRIMARY KEY,
 actor TEXT NOT NULL,
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE quarantine_terminations(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 node_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','terminated')),
 requested_at TEXT NOT NULL,
 terminated_at TEXT
);
CREATE TABLE execution_gangs(
 id TEXT PRIMARY KEY,
 leader_execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 size INTEGER NOT NULL CHECK(size>=2),
 port INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL
);
CREATE TABLE gang_members(
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 rank INTEGER NOT NULL CHECK(rank>=0),
 execution_id TEXT NOT NULL UNIQUE REFERENCES execution_requests(id),
 PRIMARY KEY(gang_id,rank)
);
CREATE TABLE gang_placements(
 id TEXT PRIMARY KEY,
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 state TEXT NOT NULL CHECK(state IN('placed','launched','aborted')),
 master_addr TEXT NOT NULL,
 master_port INTEGER NOT NULL,
 master_node_id TEXT NOT NULL,
 abort_reason TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE TABLE gang_placement_leases(
 placement_id TEXT NOT NULL REFERENCES gang_placements(id),
 rank INTEGER NOT NULL,
 lease_id TEXT NOT NULL UNIQUE REFERENCES leases(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 node_id TEXT NOT NULL,
 delivered_at TEXT,
 PRIMARY KEY(placement_id,rank)
);
CREATE TABLE gang_terminations(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 executor_id TEXT NOT NULL,
 gang_id TEXT NOT NULL REFERENCES execution_gangs(id),
 reason TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','terminated')),
 requested_at TEXT NOT NULL,
 terminated_at TEXT
);
CREATE TABLE force_stop_operations(
 operation_id TEXT PRIMARY KEY REFERENCES pause_operations(id),
 grace_seconds INTEGER NOT NULL CHECK(grace_seconds>=0),
 reason TEXT NOT NULL
);
CREATE TABLE force_stops(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 operation_id TEXT NOT NULL REFERENCES pause_operations(id),
 attempt_id TEXT REFERENCES attempts(id),
 executor_id TEXT,
 grace_seconds INTEGER NOT NULL CHECK(grace_seconds>=0),
 state TEXT NOT NULL CHECK(state IN('withdrawn','pending','signalled','terminated')),
 detail_json TEXT NOT NULL DEFAULT '{}',
 requested_at TEXT NOT NULL,
 signalled_at TEXT,
 terminated_at TEXT
);
CREATE TABLE api_tokens(
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 role TEXT NOT NULL CHECK(role IN('admin','read','node')),
 node_id TEXT,
 token_hash TEXT NOT NULL UNIQUE,
 created_at TEXT NOT NULL,
 last_used_at TEXT,
 revoked_at TEXT,
 CHECK((role='node')=(node_id IS NOT NULL))
);
CREATE TABLE attempt_tokens(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 lease_id TEXT NOT NULL REFERENCES leases(id),
 coordination_epoch INTEGER NOT NULL,
 token_hash TEXT NOT NULL UNIQUE,
 issued_at TEXT NOT NULL
);
CREATE TABLE "orchestration_tasks"(
 task_scope_id TEXT PRIMARY KEY REFERENCES coordination_scopes(id),
 project_scope_id TEXT NOT NULL REFERENCES coordination_scopes(id),
 name TEXT NOT NULL,
 queue_name TEXT NOT NULL,
 policy_id TEXT NOT NULL,
 policy_version INTEGER NOT NULL,
 spec_digest TEXT NOT NULL,
 spec_json TEXT NOT NULL,
 execution_template_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('pending','running','succeeded','failed','blocked','stopped','cancelled')),
 result_json TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL, rerun_ordinal INTEGER,
 UNIQUE(project_scope_id,name)
);
CREATE TABLE "orchestration_decisions"(
 decision_key TEXT PRIMARY KEY,
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 reason TEXT NOT NULL CHECK(reason IN('initial','continuation','quarantine_restart','gang_restart','rerun')),
 trigger_json TEXT NOT NULL,
 execution_spec_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('planned','submitted','abandoned')),
 execution_id TEXT REFERENCES execution_requests(id),
 created_at TEXT NOT NULL,
 submitted_at TEXT,
 UNIQUE(task_scope_id,execution_ordinal)
);
CREATE TABLE orchestration_task_reruns(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 request_id TEXT NOT NULL,
 actor TEXT NOT NULL,
 predecessor_execution_id TEXT REFERENCES execution_requests(id),
 response_json TEXT,
 created_at TEXT NOT NULL,
 PRIMARY KEY(task_scope_id,execution_ordinal),
 UNIQUE(task_scope_id,request_id)
);
CREATE TABLE undeclared_gpus(
 provider_id TEXT NOT NULL REFERENCES resource_providers(id),
 uuid TEXT NOT NULL,
 node_id TEXT NOT NULL REFERENCES nodes(id),
 first_observed_at TEXT NOT NULL,
 last_observed_at TEXT NOT NULL,
 PRIMARY KEY(provider_id,uuid)
);
CREATE TABLE "machine_gpus"(uuid TEXT NOT NULL,node_id TEXT NOT NULL REFERENCES nodes(id),declared_at TEXT NOT NULL,PRIMARY KEY(uuid,node_id));
CREATE TABLE machine_hosts(
 id TEXT PRIMARY KEY,
 declaration_json TEXT NOT NULL,
 declared_at TEXT NOT NULL
);
CREATE TABLE admission_waits(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 reason_code TEXT NOT NULL,
 node_id TEXT,
 detail_json TEXT NOT NULL,
 since TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
CREATE TABLE attempt_usage(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 ram_bytes INTEGER,
 disk_written_bytes INTEGER,
 observed_at TEXT NOT NULL
);
CREATE TABLE node_alerts(
 node_id TEXT NOT NULL REFERENCES nodes(id),
 kind TEXT NOT NULL,
 subject TEXT NOT NULL,
 detail_json TEXT NOT NULL,
 since TEXT NOT NULL,
 PRIMARY KEY(node_id,kind,subject)
);
CREATE TABLE attempt_log_problems(
 attempt_id TEXT NOT NULL REFERENCES attempts(id),
 problem TEXT NOT NULL CHECK(problem IN('log_space_low','log_space_exhausted','log_forward_failed')),
 stream TEXT NOT NULL DEFAULT '' CHECK(stream IN('','stdout','stderr')),
 detail TEXT NOT NULL,
 occurrences INTEGER NOT NULL DEFAULT 1,
 first_seen_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL,
 resolved_at TEXT,
 PRIMARY KEY(attempt_id,problem,stream)
);
CREATE TABLE device_settings(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 owner_kind TEXT NOT NULL CHECK(owner_kind IN('manual','bench','policy')),
 owner_ref TEXT NOT NULL,
 lease_id TEXT REFERENCES leases(id),
 settings_json TEXT NOT NULL,
 requested_by TEXT NOT NULL,
 requested_at TEXT NOT NULL,
 PRIMARY KEY(host_id,device_id,owner_kind),
 CHECK(owner_kind='policy' OR (owner_kind='manual')=(lease_id IS NULL))
);
CREATE TABLE device_state(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0,
 applied_generation INTEGER,
 applied_json TEXT,
 applied_note TEXT,
 applied_at TEXT,
 last_error TEXT,
 last_error_at TEXT,
 capabilities_json TEXT,
 capabilities_at TEXT,
 reported_at TEXT,
 PRIMARY KEY(host_id,device_id)
);
CREATE TABLE device_controllers(
 node_id TEXT PRIMARY KEY REFERENCES nodes(id),
 version TEXT NOT NULL DEFAULT '',
 started_at TEXT NOT NULL,
 last_seen_at TEXT NOT NULL
);
CREATE TABLE device_samples(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 device TEXT NOT NULL,
 resource_id TEXT REFERENCES resource_instances(id),
 observed_at TEXT NOT NULL,
 valid_until TEXT NOT NULL,
 power_w REAL,
 power_limit_w REAL,
 sm_clock_mhz INTEGER,
 mem_clock_mhz INTEGER,
 pstate TEXT,
 event_reasons INTEGER,
 mem_util REAL,
 fan_pct REAL,
 pcie_gen INTEGER,
 pcie_width INTEGER
);
CREATE TABLE device_properties(
 device TEXT PRIMARY KEY,
 default_power_limit_w REAL,
 min_power_limit_w REAL,
 max_power_limit_w REAL,
 max_sm_clock_mhz INTEGER,
 max_mem_clock_mhz INTEGER,
 pcie_gen_max INTEGER,
 pcie_width_max INTEGER,
 observed_at TEXT NOT NULL
);
CREATE TABLE attempt_energy(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
 gpu_joules REAL NOT NULL DEFAULT 0,
 gpu_seconds_covered REAL NOT NULL DEFAULT 0,
 partial INTEGER NOT NULL DEFAULT 0 CHECK(partial IN(0,1)),
 policy_seconds REAL NOT NULL DEFAULT 0,
 policy_saved_joules REAL NOT NULL DEFAULT 0,
 policy_unestimated_seconds REAL NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL
);
CREATE TABLE bench_runs(
 id TEXT PRIMARY KEY,
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 class TEXT NOT NULL CHECK(class IN('gpu','cpu','storage','network')),
 kinds_json TEXT NOT NULL,
 node_id TEXT NOT NULL,
 peer_node_id TEXT,
 target TEXT NOT NULL DEFAULT '',
 execution_id TEXT NOT NULL REFERENCES execution_requests(id),
 state TEXT NOT NULL CHECK(state IN('waiting','running','finished','invalid','failed')),
 requested_by TEXT NOT NULL,
 created_at TEXT NOT NULL,
 started_at TEXT,
 ended_at TEXT,
 driver_build TEXT,
 versions_json TEXT,
 driver_version TEXT,
 device_json TEXT,
 idle_temperature_c REAL,
 references_json TEXT,
 valid INTEGER CHECK(valid IN(0,1)),
 reason TEXT
);
CREATE TABLE bench_run_executions(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 run_id TEXT NOT NULL REFERENCES bench_runs(id),
 rank INTEGER NOT NULL
);
CREATE TABLE bench_points(
 run_id TEXT NOT NULL REFERENCES bench_runs(id),
 seq INTEGER NOT NULL,
 kind TEXT NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN('reference','sweep','measure')),
 settings_json TEXT NOT NULL,
 metrics_json TEXT NOT NULL,
 valid INTEGER NOT NULL CHECK(valid IN(0,1)),
 flags_json TEXT NOT NULL,
 measured_at TEXT NOT NULL,
 PRIMARY KEY(run_id,seq)
);
CREATE TABLE device_policy(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 decision_json TEXT NOT NULL,
 decided_at TEXT NOT NULL,
 PRIMARY KEY(host_id,device_id)
);
CREATE UNIQUE INDEX project_scope_key ON coordination_scopes(external_key) WHERE kind='project';
CREATE UNIQUE INDEX child_scope_key ON coordination_scopes(parent_id,kind,external_key) WHERE kind!='project';
CREATE INDEX observations_latest_idx ON resource_observations(resource_id,id DESC);
CREATE INDEX claims_active_idx ON external_claims(resource_id,cleared_at);
CREATE INDEX executions_admission_idx ON execution_requests(state,priority,submitted_at);
CREATE INDEX executions_project_idx ON execution_requests(project_scope_id,submitted_at);
CREATE INDEX resource_requests_execution_idx ON resource_requests(execution_id);
CREATE INDEX attempts_execution_idx ON attempts(execution_id);
CREATE INDEX leases_execution_idx ON leases(execution_id,state);
CREATE INDEX pause_operations_scope_idx ON pause_operations(scope_id,state,created_at);
CREATE INDEX events_project_sequence_idx ON coordination_events(project_scope_id,sequence);
CREATE UNIQUE INDEX one_live_suspend_per_attempt ON commands(attempt_id)
 WHERE state IN('pending','accepted','checkpointing','checkpointed');
CREATE INDEX commands_attempt_idx ON commands(attempt_id,state,created_at);
CREATE INDEX quarantine_terminations_executor_idx ON quarantine_terminations(executor_id,state);
CREATE INDEX gang_placement_leases_executor_idx ON gang_placement_leases(executor_id,delivered_at);
CREATE INDEX gang_placements_state_idx ON gang_placements(state,created_at);
CREATE INDEX gang_terminations_executor_idx ON gang_terminations(executor_id,state);
CREATE INDEX force_stops_executor_idx ON force_stops(executor_id,state);
CREATE INDEX force_stops_operation_idx ON force_stops(operation_id);
CREATE UNIQUE INDEX api_tokens_active_name ON api_tokens(name) WHERE revoked_at IS NULL;
CREATE INDEX orchestration_tasks_project_idx
 ON orchestration_tasks(project_scope_id,state,name);
CREATE INDEX orchestration_decisions_state_idx
 ON orchestration_decisions(state,created_at);
CREATE INDEX orchestration_task_dependents_idx
 ON orchestration_task_dependencies(dependency_task_scope_id);
CREATE INDEX executions_task_idx
 ON execution_requests(task_scope_id,state);
CREATE INDEX device_settings_lease_idx ON device_settings(lease_id);
CREATE INDEX device_samples_device_idx ON device_samples(device,observed_at);
CREATE INDEX lease_items_resource_idx ON lease_items(resource_id);
CREATE INDEX bench_runs_device_idx ON bench_runs(host_id,device_id,created_at);
CREATE INDEX bench_runs_state_idx ON bench_runs(state);
CREATE TRIGGER exclusive_resource_lease_guard BEFORE INSERT ON lease_items
WHEN NEW.resource_id IS NOT NULL AND NEW.kind='gpu' AND EXISTS(
 SELECT 1 FROM lease_items li JOIN leases l ON l.id=li.lease_id
 WHERE li.resource_id=NEW.resource_id AND li.kind='gpu'
 AND l.state IN('reserved','prepared','active','releasing','stale','revocation_requested'))
BEGIN SELECT RAISE(ABORT,'resource already leased'); END;
