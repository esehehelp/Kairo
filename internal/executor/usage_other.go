//go:build !windows

package executor

import (
	"context"
	"os"
)

// usageJob: there is no job object outside Windows.
type usageJob struct{}

func duplicateJob(*treeHandle) usageJob { return usageJob{} }

func (usageJob) close() {}

// sampleTrees samples the attempts' trees from /proc (sampleProcTrees).
func (e *Local) sampleTrees(_ context.Context, targets []usageTarget) map[string]usageSample {
	return sampleProcTrees(os.DirFS("/proc"), os.Getpid(), targets)
}
