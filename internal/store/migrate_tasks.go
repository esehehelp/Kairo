package store

import (
	"database/sql"
	"fmt"
)

// tasksSchema records each rerun of a task (task_control.go): the ordinal its
// new execution gets, the request that asked for it and the answer it got, so
// a repeated request is answered again instead of rerunning twice (a dependent
// rerun along with it has no answer of its own). Applied idempotently on every
// open, like the orchestration schema.
const tasksSchema = `CREATE TABLE IF NOT EXISTS orchestration_task_reruns(
 task_scope_id TEXT NOT NULL REFERENCES orchestration_tasks(task_scope_id),
 execution_ordinal INTEGER NOT NULL CHECK(execution_ordinal>=0),
 request_id TEXT NOT NULL,
 actor TEXT NOT NULL,
 predecessor_execution_id TEXT REFERENCES execution_requests(id),
 response_json TEXT,
 created_at TEXT NOT NULL,
 PRIMARY KEY(task_scope_id,execution_ordinal),
 UNIQUE(task_scope_id,request_id)
);`

// migrateTasks adds what task cancel and rerun need to an existing database:
// the terminal task state 'cancelled', the decision reason 'rerun', the
// decision state 'abandoned' (a planned decision that must never be
// submitted), orchestration_tasks.rerun_ordinal and the reruns table. It also
// renames what cancel used to look like: a task failed because its execution
// was withdrawn before it started is cancelled.
func migrateTasks(db *sql.DB) error {
	for _, w := range []struct{ table, old, new string }{
		{"orchestration_tasks", "state IN('pending','running','succeeded','failed','blocked','stopped')", "state IN('pending','running','succeeded','failed','blocked','stopped','cancelled')"},
		{"orchestration_decisions", "reason IN('initial','continuation','quarantine_restart','gang_restart')", "reason IN('initial','continuation','quarantine_restart','gang_restart','rerun')"},
		{"orchestration_decisions", "state IN('planned','submitted')", "state IN('planned','submitted','abandoned')"},
	} {
		if err := widenCheck(db, w.table, w.old, w.new); err != nil {
			return err
		}
	}
	var hasRerun int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('orchestration_tasks') WHERE name='rerun_ordinal'`).Scan(&hasRerun); err != nil {
		return err
	}
	if hasRerun == 0 {
		if _, err := db.Exec(`ALTER TABLE orchestration_tasks ADD COLUMN rerun_ordinal INTEGER`); err != nil {
			return fmt.Errorf("add rerun_ordinal: %w", err)
		}
	}
	if _, err := db.Exec(tasksSchema); err != nil {
		return fmt.Errorf("apply tasks schema: %w", err)
	}
	if _, err := db.Exec(`UPDATE orchestration_tasks SET state='cancelled',
	 result_json=json_object('reason','withdrawn before start','execution_id',json_extract(result_json,'$.execution_id'),'migrated',json('true'))
	 WHERE state='failed' AND json_valid(result_json)
	 AND json_extract(result_json,'$.reason')='execution terminal before attempt'
	 AND json_extract(result_json,'$.terminal_cause')='withdrawn_before_start'`); err != nil {
		return fmt.Errorf("migrate withdrawn tasks: %w", err)
	}
	return nil
}
