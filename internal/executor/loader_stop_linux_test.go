//go:build linux

package executor

import (
	"os"
	"testing"
)

// A loader that ends while its container is being stopped (the stop reached
// it too) is not started again; otherwise it is, up to loaderRestarts.
func TestALoaderIsNotStartedAgainWhileItsContainerStops(t *testing.T) {
	logs, err := os.CreateTemp(t.TempDir(), "logs")
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	p := &loaderProc{stream: "wiki", args: []string{"/bin/true"}, logs: logs, pid: 999999}
	l := loaderProcs{p}
	l.ended(999999, false)
	if p.pid != 0 || p.restarts != 0 {
		t.Fatalf("started again while stopping: pid %d, restarts %d", p.pid, p.restarts)
	}
	p.pid = 999998
	l.ended(999998, true)
	if p.restarts != 1 || p.pid == 0 {
		t.Fatalf("not started again: pid %d, restarts %d", p.pid, p.restarts)
	}
}
