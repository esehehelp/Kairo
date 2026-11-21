package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	"kairo/internal/store"
)

func TestTaskOutputAndPlacementOverTheAPI(t *testing.T) {
	d := newTestDaemon(t, false)
	if code, body := d.call(t, http.MethodPost, "/api/projects", d.admin, twoTaskBody()); code != http.StatusCreated {
		t.Fatalf("apply: %d %s", code, body)
	}
	ctx := t.Context()
	if err := d.store.ReconcileProjectTasks(ctx); err != nil {
		t.Fatal(err)
	}
	launch := d.launchNext(t)
	db, err := sql.Open("pgx", d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `INSERT INTO data_objects(id,kind,source_id,store_id,executor_id,root,source_path,project_scope_id,state,created_at,updated_at)
		VALUES('obj_train','output',$1,'fast','executor','/store','/store/outputs/train',$2,'packed','2026-01-01','2026-01-01'),
		('obj_other','import','imported','fast','executor','/store','/other',$2,'packed','2026-01-01','2026-01-01')`, launch.RunID, launch.Execution.ProjectScopeID); err != nil {
		t.Fatal(err)
	}
	read := d.token(t, "dashboard", store.RoleRead, "")
	code, raw := d.call(t, http.MethodGet, "/api/projects/neo-ime/tasks/train", read, nil)
	var detail struct {
		Task store.TaskDetail `json:"task"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &detail) != nil || len(detail.Task.Executions) != 1 {
		t.Fatalf("task detail: %d %s", code, raw)
	}
	if outputs := detail.Task.Executions[0].Outputs; len(outputs) != 1 || outputs[0].ID != "obj_train" || outputs[0].State != "packed" {
		t.Fatalf("task outputs: %s", raw)
	}
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"task=train", 1}, {"task=export", 0}, {"execution=" + launch.Execution.ID, 1}, {"execution=exe_missing", 0},
	} {
		code, raw := d.call(t, http.MethodGet, "/api/data/objects?project=neo-ime&"+tc.query, read, nil)
		var body struct {
			Objects []store.DataObject `json:"objects"`
		}
		if code != http.StatusOK || json.Unmarshal(raw, &body) != nil || len(body.Objects) != tc.want || tc.want == 1 && body.Objects[0].ID != "obj_train" {
			t.Fatalf("list %s: %d %s", tc.query, code, raw)
		}
	}
	path := "/api/data/objects/obj_train/placement"
	code, raw = d.call(t, http.MethodGet, path, read, nil)
	expectError(t, "missing node", code, raw, http.StatusBadRequest, "bad_request")
	code, raw = d.call(t, http.MethodGet, "/api/data/objects/missing/placement?node=node", read, nil)
	expectError(t, "missing object", code, raw, http.StatusNotFound, "not_found")
	check := func(node, state, wantPath string) {
		t.Helper()
		code, raw := d.call(t, http.MethodGet, path+"?node="+node, read, nil)
		var p store.PlacementStatus
		if code != http.StatusOK || json.Unmarshal(raw, &p) != nil || p.State != state || p.Path != wantPath {
			t.Fatalf("placement on %s: %d %s, want %s %q", node, code, raw, state, wantPath)
		}
	}
	check("node", "failed", "")
	if _, err := db.ExecContext(ctx, `INSERT INTO object_placements(object_id,store_id,executor_id,root,path,state,created_at,updated_at)
		VALUES('obj_train','fast','executor','/store','/store/inputs/obj_train','fetching','2026-01-01','2026-01-01')`); err != nil {
		t.Fatal(err)
	}
	check("node", "fetching", "")
	if _, err := db.ExecContext(ctx, `UPDATE data_objects SET source_state='deleted' WHERE id='obj_train'`); err != nil {
		t.Fatal(err)
	}
	check("node", "failed", "")
	if err := d.store.RecordFetch(ctx, store.FetchMade{ObjectID: "obj_train", StoreID: "fast", ExecutorID: "executor", Path: "/store/inputs/obj_train"}); err != nil {
		t.Fatal(err)
	}
	check("node", "ready", "/store/inputs/obj_train")
	check("elsewhere", "failed", "")
}
