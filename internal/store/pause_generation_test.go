package store

import (
	"context"
	"testing"
)

// A pause whose gate was reopened by a resume captures nothing that runs or
// waits after the resume, also when it looked at the gate before the resume
// committed: the check is made again where the targets are taken.
func TestResumedPauseCapturesNothingAdmittedAfterTheResume(t *testing.T) {
	store := openCoordinationStore(t)
	ctx := context.Background()
	running, _, err := store.SubmitExecution(ctx, testExecutionSpec("after-resume"))
	if err != nil {
		t.Fatal(err)
	}
	waitingSpec := testExecutionSpec("waiting-after-resume")
	waitingSpec.Scope.Task = "eval"
	waiting, _, err := store.SubmitExecution(ctx, waitingSpec)
	if err != nil {
		t.Fatal(err)
	}
	operation := pauseProjectForTest(t, store, "pause-old")
	scopes, err := store.GetScopeByPath(ctx, ScopePath{Project: "llm-develop"})
	if err != nil {
		t.Fatal(err)
	}
	force, err := store.ForcePauseScope(ctx, scopes.Project.ID, "operator", "force-old", ForceStopSpec{GraceSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ResumeScope(ctx, scopes.Project.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	insertPauseTestRunning(t, store, running, true)
	targets, err := store.DiscoverPauseTargets(ctx, operation.ID)
	if err != nil || len(targets) != 0 {
		t.Fatalf("targets of a resumed pause: %+v %v", targets, err)
	}
	if err = store.forceWithdraw(ctx, force, waiting.ID); err != nil {
		t.Fatal(err)
	}
	var state string
	if err = store.db.QueryRow(`SELECT state FROM execution_requests WHERE id=$1`, waiting.ID).Scan(&state); err != nil || state != "waiting" {
		t.Fatalf("an execution waiting after the resume was withdrawn by the old forced pause: %s %v", state, err)
	}
}
