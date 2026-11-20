package executor

import (
	"context"
	"os"
	"time"
)

// renameRetries: how often, and how far apart at most, a rename that
// failed for a reason that passes is tried again (about 5 s in all).
const (
	renameRetries  = 10
	renameMaxDelay = time.Second
)

// renameSettled renames from to to in dir. On Windows a directory cannot be
// renamed while a file in it is held open without FILE_SHARE_DELETE, which
// a virus scanner or the search indexer does for a few moments to files just
// written: such a failure (renameTransient) is tried again, with backoff,
// unless done says the rename happened after all (another process made to
// meanwhile). Git for Windows retries its renames for the same reason
// (issue #2: a code snapshot unpacked and renamed at once failed with
// "Access is denied").
func renameSettled(ctx context.Context, dir *os.Root, from, to string, done func() bool) error {
	delay := 50 * time.Millisecond
	for try := 0; ; try++ {
		err := dir.Rename(from, to)
		if err == nil || try == renameRetries || !renameTransient(err) || done != nil && done() {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		if delay *= 2; delay > renameMaxDelay {
			delay = renameMaxDelay
		}
	}
}
