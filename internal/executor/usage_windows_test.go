//go:build windows

package executor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"kairo/internal/processidentity"
)

// fakeWindows is a host with processes given by pid: parent, creation time,
// private working set and bytes written; denied processes cannot be read.
type fakeWindows struct {
	procs  map[int][4]int64
	denied map[int]bool
	job    []int
	jobIO  int64
}

func (f fakeWindows) jobProcesses(usageJob) ([]int, error) { return f.job, nil }
func (f fakeWindows) jobWritten(usageJob) (int64, error)   { return f.jobIO, nil }
func (f fakeWindows) processes() ([]winProcess, error) {
	var out []winProcess
	for pid, p := range f.procs {
		out = append(out, winProcess{pid: pid, parent: int(p[0])})
	}
	return out, nil
}
func (f fakeWindows) read(pid, field int) (int64, error) {
	if f.denied[pid] {
		return 0, fmt.Errorf("access denied")
	}
	p, ok := f.procs[pid]
	if !ok {
		return 0, errProcessGone
	}
	return p[field], nil
}
func (f fakeWindows) created(pid int) (uint64, error) {
	n, err := f.read(pid, 1)
	return uint64(n), err
}
func (f fakeWindows) memory(pid int) (int64, error)  { return f.read(pid, 2) }
func (f fakeWindows) written(pid int) (int64, error) { return f.read(pid, 3) }

func TestWindowsTreeWithoutAJobIsTheLauncherAndItsDescendants(t *testing.T) {
	host := fakeWindows{procs: map[int][4]int64{
		4:   {0, 1, 1, 1},
		100: {4, 1000, 100, 10}, // the launcher
		101: {100, 1100, 200, 20},
		102: {101, 1200, 300, 30},
		103: {100, 900, 5000, 5000}, // created before the launcher: a reused pid
		104: {103, 1300, 5000, 5000},
		200: {4, 1050, 7000, 7000}, // not of the tree
	}}
	launcher := usageTarget{attemptID: "att_1", pid: 100, identity: processidentity.WindowsIdentityFromFiletime(100, 1000)}
	s := windowsTreeUsage(host, launcher)
	if value(s.ram) != "600" || value(s.written) != "60" {
		t.Fatalf("ram %s written %s", value(s.ram), value(s.written))
	}
	// a launcher that is not the process launched any more: unknown
	replaced := launcher
	replaced.identity = processidentity.WindowsIdentityFromFiletime(100, 999)
	if s = windowsTreeUsage(host, replaced); s.ram != nil || s.written != nil {
		t.Fatalf("replaced launcher: ram %s written %s", value(s.ram), value(s.written))
	}
	// a descendant that cannot be read: unknown
	host.denied = map[int]bool{101: true}
	if s = windowsTreeUsage(host, launcher); s.ram != nil || s.written != nil {
		t.Fatalf("unreadable descendant: ram %s written %s", value(s.ram), value(s.written))
	}
}

func TestWindowsTreeWithAJobIsItsProcessesAndItsIO(t *testing.T) {
	host := fakeWindows{procs: map[int][4]int64{100: {4, 1000, 100, 10}, 101: {100, 1100, 200, 20}}, job: []int{100, 101, 102}, jobIO: 12345}
	s := windowsTreeUsage(host, usageTarget{attemptID: "att_1", pid: 100, job: 1})
	if value(s.ram) != "300" || value(s.written) != "12345" {
		t.Fatalf("ram %s written %s", value(s.ram), value(s.written))
	}
	host.denied = map[int]bool{101: true}
	if s = windowsTreeUsage(host, usageTarget{attemptID: "att_1", pid: 100, job: 1}); s.ram != nil || value(s.written) != "12345" {
		t.Fatalf("unreadable member: ram %s written %s", value(s.ram), value(s.written))
	}
}

// The real host: a child in a job object, sampled through the job and through
// its parent chain.
func TestWindowsTreeUsageOfARealChild(t *testing.T) {
	cmd := exec.Command("ping", "-n", "30", "127.0.0.1")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start ping: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	tree, err := attachTree(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.close()
	identity, err := processidentity.ForPID(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	job := duplicateJob(tree)
	defer job.close()
	if job == 0 {
		t.Fatal("job object handle not duplicated")
	}
	for name, target := range map[string]usageTarget{
		"job":          {attemptID: "att", pid: cmd.Process.Pid, identity: identity, job: job},
		"parent chain": {attemptID: "att", pid: cmd.Process.Pid, identity: identity},
	} {
		s := windowsTreeUsage(hostProcesses{}, target)
		if s.ram == nil || *s.ram <= 0 || s.written == nil || *s.written < 0 {
			t.Errorf("%s: ram %s written %s", name, value(s.ram), value(s.written))
		}
	}
}

// The WSL script over a fake /proc, where a POSIX sh is at hand (Git for
// Windows ships one).
func TestWSLUsageScriptOverAFakeProc(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	root := filepath.ToSlash(t.TempDir())
	write := func(pid int, name, body string) {
		dir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for pid, attempt := range map[int]string{10: "att_1", 11: "att_1", 12: "att_2", 13: "att_3"} {
		write(pid, "environ", "PATH=/bin\x00KAIRO_ATTEMPT_ID="+attempt+"\x00")
		write(pid, "smaps_rollup", fmt.Sprintf("Rss: %d kB\nPss: %d kB\n", 2*pid, pid))
		if pid != 13 {
			write(pid, "io", fmt.Sprintf("rchar: 1\nwrite_bytes: %d\ncancelled_write_bytes: 1\n", 100*pid))
		}
	}
	script := strings.ReplaceAll(wslUsageScript, "/proc/", root+"/")
	out, err := exec.Command(sh, "-c", script, "kairo-usage", "att_1", "att_2", "att_3", "att_4").Output()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	got := parseWSLUsage(string(out))
	for attempt, want := range map[string][2]string{"att_1": {fmt.Sprint(21 * 1024), "2098"}, "att_2": {fmt.Sprint(12 * 1024), "1199"}, "att_3": {fmt.Sprint(13 * 1024), "unknown"}, "att_4": {"0", "0"}} {
		if s := got[attempt]; value(s.ram) != want[0] || value(s.written) != want[1] {
			t.Errorf("%s: ram %s written %s, want %v (output %q)", attempt, value(s.ram), value(s.written), want, out)
		}
	}
}

func TestParseWSLUsage(t *testing.T) {
	got := parseWSLUsage("att_1 1048576 4096\natt_2 - 10\natt_3 2048 -\nnoise\n")
	for attempt, want := range map[string][2]string{"att_1": {"1048576", "4096"}, "att_2": {"unknown", "10"}, "att_3": {"2048", "unknown"}, "att_4": {"unknown", "unknown"}} {
		if s := got[attempt]; value(s.ram) != want[0] || value(s.written) != want[1] {
			t.Errorf("%s: ram %s written %s, want %v", attempt, value(s.ram), value(s.written), want)
		}
	}
}
