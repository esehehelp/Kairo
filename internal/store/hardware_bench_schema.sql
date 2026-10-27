-- Hardware benches and the operating-point policy (hardware_bench.go,
-- device_policy.go). Applied idempotently on every open. Kept indefinitely: a
-- run is one row and a few hundred points.
--
-- bench_runs: one `kairo hardware bench` of a host's device, run as an
-- ordinary execution (project kairo, queue hardware-bench) whose driver
-- reports through the worker API. state: waiting (not started), running,
-- finished (valid), invalid (finished, but a reference or a kind could not be
-- trusted), failed (the execution ended without results).
CREATE TABLE IF NOT EXISTS bench_runs(
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
CREATE INDEX IF NOT EXISTS bench_runs_device_idx ON bench_runs(host_id,device_id,created_at);
CREATE INDEX IF NOT EXISTS bench_runs_state_idx ON bench_runs(state);
-- The executions of a run: the run's own, and the members of a network gang.
CREATE TABLE IF NOT EXISTS bench_run_executions(
 execution_id TEXT PRIMARY KEY REFERENCES execution_requests(id),
 run_id TEXT NOT NULL REFERENCES bench_runs(id),
 rank INTEGER NOT NULL
);
-- bench_points: one measurement of a run: a kind (workload) at a setting.
CREATE TABLE IF NOT EXISTS bench_points(
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
-- device_policy: the operating point the policy last chose for a device, with
-- its inputs (the history is in the device_policy_decision events).
CREATE TABLE IF NOT EXISTS device_policy(
 host_id TEXT NOT NULL,
 device_id TEXT NOT NULL,
 decision_json TEXT NOT NULL,
 decided_at TEXT NOT NULL,
 PRIMARY KEY(host_id,device_id)
);
