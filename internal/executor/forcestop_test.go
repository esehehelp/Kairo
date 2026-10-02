package executor

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"kairo/internal/store"
)

// The test binary doubles as the processes of a test attempt (and, on
// Windows, as the CTRL_BREAK helper, like the kairo binary in production).
const (
	helperTree       = "__test-tree"        // start a sleeping child, then sleep
	helperStubborn   = "__test-stubborn"    // like helperTree, both catching CTRL_BREAK / SIGTERM and going on
	helperSleep      = "__test-sleep"       // sleep
	helperSleepStubb = "__test-sleep-stubb" // sleep ignoring interrupts
	helperOrphan     = "__test-orphan"      // start a sleeping child and exit at once
)

func TestMain(m *testing.M) {
	RunHelper(os.Args)
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case helperTree, helperStubborn, helperOrphan:
			child := helperSleep
			if os.Args[1] == helperStubborn {
				signal.Notify(make(chan os.Signal, 1), os.Interrupt, syscall.SIGTERM)
				child = helperSleepStubb
			}
			cmd := exec.Command(os.Args[0], child)
			if err := cmd.Start(); err != nil {
				os.Exit(3)
			}
			os.Stdout.WriteString(strconv.Itoa(cmd.Process.Pid) + "\n")
			if os.Args[1] == helperOrphan {
				os.Exit(0)
			}
			time.Sleep(2 * time.Minute)
			os.Exit(0)
		case helperSleep, helperSleepStubb:
			if os.Args[1] == helperSleepStubb {
				signal.Notify(make(chan os.Signal, 1), os.Interrupt, syscall.SIGTERM)
			}
			time.Sleep(2 * time.Minute)
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

// startTestTree starts a launcher and its child the way the executor
// launches an attempt, and returns the launcher, its tree handle and the
// child's pid.
func startTestTree(t *testing.T, mode string) (*exec.Cmd, *treeHandle, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], mode)
	cmd.Env = append(os.Environ(), "KAIRO_ATTEMPT_ID=att-"+mode)
	prepareProcessTree(cmd)
	hideTestConsole(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	tree, err := attachTree(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		tree.close()
	})
	var line [32]byte
	n, err := stdout.Read(line[:])
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(string(trimNewline(line[:n])))
	if err != nil {
		t.Fatalf("child pid: %q", line[:n])
	}
	go func() { _ = cmd.Wait() }()
	time.Sleep(500 * time.Millisecond) // let the child install its signal handling
	return cmd, tree, child
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func waitGone(t *testing.T, e *Local, tree processTree, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		alive, err := e.treeAlive(context.Background(), tree)
		if err != nil {
			t.Fatal(err)
		}
		if !alive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("process tree still alive")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestForceStopGracefulStopEndsTheWholeTree(t *testing.T) {
	cmd, handle, child := startTestTree(t, helperTree)
	e := &Local{ID: "e", Logger: slog.Default()}
	tree := processTree{attemptID: "att", pid: cmd.Process.Pid, handle: handle}
	if alive, err := e.treeAlive(context.Background(), tree); err != nil || !alive {
		t.Fatalf("tree not alive at start: %v %v", alive, err)
	}
	if err := e.stopTreeGracefully(context.Background(), tree); err != nil {
		t.Fatal(err)
	}
	waitGone(t, e, tree, 15*time.Second)
	if processAlive(child) {
		t.Fatalf("child %d survived the graceful stop", child)
	}
}

// fakeForceStops serves one force stop order to the executor and records its
// acknowledgements, like the daemon would.
type fakeForceStops struct {
	Coordinator
	mu    sync.Mutex
	order store.ForceStopOrder
	acks  []string
	last  json.RawMessage
}

func (f *fakeForceStops) ForceStopOrders(context.Context, string) ([]store.ForceStopOrder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.order.State == "terminated" {
		return nil, nil
	}
	return []store.ForceStopOrder{f.order}, nil
}

func (f *fakeForceStops) AckForceStop(_ context.Context, _ string, phase string, detail json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order.State = phase
	f.acks = append(f.acks, phase)
	f.last = detail
	return nil
}

func TestForceStopKillsWhatIgnoresTheGracefulStop(t *testing.T) {
	cmd, handle, child := startTestTree(t, helperStubborn)
	fake := &fakeForceStops{order: store.ForceStopOrder{AttemptID: "att", ExecutionID: "exe", State: "pending", GraceSeconds: 1, KillAfterSeconds: 1}}
	e := NewLocal("e", fake, t.TempDir(), slog.Default())
	e.running["att"] = cmd
	e.trees = map[string]*treeHandle{"att": handle}
	ctx := context.Background()
	e.carryOutForceStops(ctx)
	if len(fake.acks) != 1 || fake.acks[0] != "signalled" {
		t.Fatalf("acks after first tick: %v", fake.acks)
	}
	// within the grace period nothing is killed
	time.Sleep(1500 * time.Millisecond)
	e.carryOutForceStops(ctx)
	if !processAlive(child) {
		t.Fatal("stubborn child died during the grace period")
	}
	fake.mu.Lock()
	fake.order.KillAfterSeconds = 0
	fake.mu.Unlock()
	deadline := time.Now().Add(20 * time.Second)
	for {
		time.Sleep(forceStopCheckInterval + 100*time.Millisecond)
		e.carryOutForceStops(ctx)
		fake.mu.Lock()
		done := fake.order.State == "terminated"
		fake.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not terminated; acks %v", fake.acks)
		}
	}
	if processAlive(child) || processAlive(cmd.Process.Pid) {
		t.Fatal("process survived the kill")
	}
	var detail struct {
		Killed bool `json:"killed"`
	}
	if err := json.Unmarshal(fake.last, &detail); err != nil || !detail.Killed {
		t.Fatalf("terminated detail: %s", fake.last)
	}
}

// The launcher exits and leaves a child behind (a wrapper that outlives its
// work, or the reverse): the force stop still finds and kills the child.
func TestForceStopKillsWhatTheLauncherLeftBehind(t *testing.T) {
	cmd, handle, child := startTestTree(t, helperOrphan)
	e := &Local{ID: "e", Logger: slog.Default()}
	tree := processTree{attemptID: "att-" + helperOrphan, pid: cmd.Process.Pid, handle: handle}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if alive, _ := tree.launcherAlive(); !alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("launcher did not exit")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if alive, err := e.treeAlive(context.Background(), tree); err != nil || !alive {
		t.Fatalf("left-behind child not seen: %v %v", alive, err)
	}
	if err := e.killTree(context.Background(), tree); err != nil {
		t.Fatal(err)
	}
	waitGone(t, e, tree, 15*time.Second)
	if processAlive(child) {
		t.Fatalf("child %d survived", child)
	}
}
