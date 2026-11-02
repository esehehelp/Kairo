package executor

import (
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"time"

	"kairo/internal/store"
)

// A reservation that is given up (its prepare, validation, authorization or
// launch failed) must have its lease released, or its resources stay held
// with nothing to run on them. Release can fail for a while (no fresh
// provider observation, an unreachable daemon), so a failed cleanup is kept
// and retried at every tick until the lease is released. A lease whose
// answer never reached this executor (a lost ReserveNext or AuthorizeLaunch
// response) is found by asking the store, between ticks, for the unlaunched
// leases it holds that this instance does not know.

// pendingCleanup is a reservation given up whose lease is not released yet.
type pendingCleanup struct {
	lease     store.Lease
	resources []store.ResourceInstance
	reason    string
	// attemptID is set once a launch was authorized: when its lease turns
	// out to be past release (the activation went through after all), the
	// attempt is ended through RecordTerminal and the ordinary quiescence.
	attemptID string
	retryAt   time.Time
}

const (
	// cleanupRetryInterval spaces out the retries of one cleanup.
	cleanupRetryInterval = 5 * time.Second
	// unlaunchedSweepInterval is how often the store is asked for leases
	// this instance does not hold; unlaunchedMinAge is how old such a lease
	// must be (a reservation being answered right now is younger).
	unlaunchedSweepInterval = 30 * time.Second
	unlaunchedMinAge        = 30 * time.Second
	// launchRetries is how many times in all a launch call is made while its
	// answer does not arrive.
	launchRetries = 3
)

// abandonReservation returns what a reservation prepared and releases its
// lease, or keeps it to retry (retryCleanups).
func (e *Local) abandonReservation(lease store.Lease, resources []store.ResourceInstance, reason, attemptID string) {
	c := pendingCleanup{lease: lease, resources: resources, reason: reason, attemptID: attemptID}
	if e.tryCleanup(context.Background(), c) {
		return
	}
	c.retryAt = time.Now().Add(cleanupRetryInterval)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pendingCleanups == nil {
		e.pendingCleanups = map[string]pendingCleanup{}
	}
	e.pendingCleanups[lease.ID] = c
}

// tryCleanup reports whether the cleanup is done: the lease released (now or
// before), or the attempt of a lease past release ended.
func (e *Local) tryCleanup(ctx context.Context, c pendingCleanup) bool {
	err := e.cleanupReservation(ctx, c.lease, c.resources, c.reason)
	if err == nil {
		return true
	}
	if errors.Is(err, store.ErrStaleEpoch) || errors.Is(err, store.ErrNotFound) {
		// The lease is no longer a reservation at this epoch: released
		// already, or activated after all.
		if c.attemptID == "" {
			return true
		}
		termErr := e.Store.RecordTerminal(ctx, c.attemptID, c.lease.ID, c.lease.CoordinationEpoch, -1, "launch_failed")
		if termErr == nil || errors.Is(termErr, store.ErrStaleEpoch) || errors.Is(termErr, store.ErrNotFound) {
			return true
		}
		err = termErr
	}
	e.Logger.Warn("reservation cleanup failed; retrying", "lease_id", c.lease.ID, "error", err)
	return false
}

// retryCleanups retries the cleanups that are due.
func (e *Local) retryCleanups(ctx context.Context) {
	now := time.Now()
	e.mu.Lock()
	var due []pendingCleanup
	for _, c := range e.pendingCleanups {
		if !now.Before(c.retryAt) {
			due = append(due, c)
		}
	}
	e.mu.Unlock()
	for _, c := range due {
		done := e.tryCleanup(ctx, c)
		e.mu.Lock()
		if done {
			delete(e.pendingCleanups, c.lease.ID)
		} else {
			c.retryAt = time.Now().Add(cleanupRetryInterval)
			e.pendingCleanups[c.lease.ID] = c
		}
		e.mu.Unlock()
	}
}

// sweepUnlaunched gives up the unlaunched leases of this executor that this
// instance does not hold (neither at the gang barrier nor in its cleanups).
// A daemon without the operation answers with an error, reported once.
func (e *Local) sweepUnlaunched(ctx context.Context) {
	if time.Since(e.lastUnlaunchedSweep) < unlaunchedSweepInterval {
		return
	}
	e.lastUnlaunchedSweep = time.Now()
	leases, err := e.Store.ListUnlaunchedLeases(ctx, e.ID, unlaunchedMinAge)
	if err != nil {
		if !e.unlaunchedSweepFailing {
			e.Logger.Warn("cannot list unlaunched leases", "executor_id", e.ID, "error", err)
		}
		e.unlaunchedSweepFailing = true
		return
	}
	e.unlaunchedSweepFailing = false
	for _, u := range leases {
		e.mu.Lock()
		_, held := e.pendingGang[u.Lease.ID]
		_, cleaning := e.pendingCleanups[u.Lease.ID]
		e.mu.Unlock()
		if held || cleaning {
			continue
		}
		e.Logger.Warn("releasing a reservation this executor does not hold (its answer never arrived)", "lease_id", u.Lease.ID, "execution_id", u.Lease.ExecutionID, "state", u.Lease.State)
		e.abandonReservation(u.Lease, u.Resources, "reservation its executor never received", u.AttemptID)
	}
}

// authorize is AuthorizeLaunch, asked again while no answer arrives (the
// store issues an unconsumed authorization anew).
func (e *Local) authorize(ctx context.Context, reservation *store.Reservation) (*store.Launch, error) {
	var launch *store.Launch
	err := e.retryUnanswered(ctx, func() error {
		var callErr error
		launch, callErr = e.Store.AuthorizeLaunch(ctx, reservation)
		return callErr
	})
	return launch, err
}

// retryUnanswered makes call, and again (launchRetries times in all) while
// it fails without an answer from the store (unanswered).
func (e *Local) retryUnanswered(ctx context.Context, call func() error) error {
	delay := e.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	var err error
	for i := 0; i < launchRetries; i++ {
		if err = call(); err == nil || !unanswered(err) {
			return err
		}
		if i == launchRetries-1 {
			break
		}
		e.Logger.Warn("coordination call got no answer; asking again", "error", err, "retry_in", delay.String())
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay *= 2
	}
	return err
}

// unanswered reports whether a coordination call failed without the store's
// answer: the connection failed or timed out, or the daemon failed (5xx).
// A store sentinel or a refused request is an answer.
func unanswered(err error) bool {
	var status interface{ HTTPStatus() int }
	var netErr net.Error
	switch {
	case errors.As(err, &status):
		return status.HTTPStatus() >= 500
	case errors.As(err, &netErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}

// killUnactivated ends a launcher whose activation failed together with
// everything it started: its job object or process group, and the
// processes carrying its KAIRO_ATTEMPT_ID (inside WSL too), not the launcher
// alone, whose children would otherwise run on unmanaged.
func (e *Local) killUnactivated(launch *store.Launch, cmd *exec.Cmd, tree *treeHandle, identity string) {
	t := processTree{attemptID: launch.Attempt.ID, pid: cmd.Process.Pid, identity: identity, handle: tree, wsl: e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2", distro: e.Attributes["distro"]}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := e.killTree(ctx, t); err != nil {
		e.Logger.Warn("process tree of an unactivated launch not killed", "attempt_id", launch.Attempt.ID, "error", err)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	tree.close()
}
