-- A hardware bench is a Task of project kairo (queue hardware-bench), declared
-- by `kairo hardware bench` (hardware_bench.go): preemptible and
-- checkpointable, it is suspended between points and resumed by its task's
-- continuation execution, which carries on the same run. Runs created before
-- this have no task (task_scope_id NULL) and keep their one execution.
--
-- execution_id is the run's latest execution (the leader of a network gang);
-- bench_run_executions keeps every execution of every attempt.
--
-- state partial: finished, with valid curves of some of its kinds only (the
-- reasons of the others in reason); invalid now means no kind was valid.
ALTER TABLE bench_runs ADD COLUMN task_scope_id TEXT REFERENCES coordination_scopes(id);
CREATE UNIQUE INDEX bench_runs_task_idx ON bench_runs(task_scope_id) WHERE task_scope_id IS NOT NULL;
ALTER TABLE bench_runs DROP CONSTRAINT bench_runs_state_check;
ALTER TABLE bench_runs ADD CONSTRAINT bench_runs_state_check CHECK(state IN('waiting','running','finished','partial','invalid','failed'));
UPDATE bench_runs SET state='partial' WHERE state='invalid' AND references_json IS NOT NULL
 AND EXISTS(SELECT 1 FROM jsonb_each(CASE WHEN jsonb_typeof(references_json::jsonb)='object' THEN references_json::jsonb ELSE '{}'::jsonb END) r WHERE r.value->>'valid'='true');
