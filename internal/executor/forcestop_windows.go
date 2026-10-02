//go:build windows

package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"kairo/internal/processidentity"
)

// treeHandle is what a launch on Windows keeps of the attempt's process tree:
// a job object the launcher was assigned to right after it started (its
// descendants join it), and whether the launcher leads its own console
// process group (so CTRL_BREAK can be sent to that group alone).
type treeHandle struct {
	job   windows.Handle
	group bool
}

// attachTree puts a just-started launcher into a new job object. The job does
// not kill on close (the daemon may restart while attempts run) and allows
// explicit breakaway, so nothing changes for the attempt except that the
// executor can later terminate everything in it at once.
func attachTree(cmd *exec.Cmd) (*treeHandle, error) {
	h := &treeHandle{group: cmd.SysProcAttr != nil && cmd.SysProcAttr.CreationFlags&windows.CREATE_NEW_PROCESS_GROUP != 0}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return h, fmt.Errorf("create job object: %w", err)
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_BREAKAWAY_OK
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		windows.CloseHandle(job)
		return h, fmt.Errorf("configure job object: %w", err)
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return h, fmt.Errorf("open launcher: %w", err)
	}
	defer windows.CloseHandle(process)
	if err = windows.AssignProcessToJobObject(job, process); err != nil {
		windows.CloseHandle(job)
		return h, fmt.Errorf("assign launcher to job object: %w", err)
	}
	h.job = job
	return h, nil
}

func (h *treeHandle) close() {
	if h != nil && h.job != 0 {
		windows.CloseHandle(h.job)
		h.job = 0
	}
}

// jobBasicAccounting is JOBOBJECT_BASIC_ACCOUNTING_INFORMATION.
type jobBasicAccounting struct {
	TotalUserTime, TotalKernelTime, ThisPeriodTotalUserTime, ThisPeriodTotalKernelTime int64
	TotalPageFaultCount, TotalProcesses, ActiveProcesses, TotalTerminatedProcesses     uint32
}

func (h *treeHandle) activeProcesses() (int, error) {
	var info jobBasicAccounting
	if err := windows.QueryInformationJobObject(h.job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil); err != nil {
		return 0, err
	}
	return int(info.ActiveProcesses), nil
}

var errNoGracefulStop = errors.New("no graceful stop channel to this attempt (launched before force stop support, or by an earlier executor instance)")

// stopTreeGracefully asks the attempt to stop: SIGTERM to every Linux process
// of the attempt for a WSL executor, CTRL_BREAK to the launcher's console
// process group otherwise.
func (e *Local) stopTreeGracefully(ctx context.Context, t processTree) error {
	if t.wsl {
		_, err := wslAttemptProcesses(ctx, t.distro, t.attemptID, "TERM")
		return err
	}
	if t.handle == nil || !t.handle.group || t.pid == 0 {
		return errNoGracefulStop
	}
	return ctrlBreak(ctx, t.pid)
}

// killTree terminates everything of the attempt: the job object, the
// launcher's process tree (taskkill /T /F) and, for WSL, every Linux process
// carrying the attempt's KAIRO_ATTEMPT_ID.
func (e *Local) killTree(ctx context.Context, t processTree) error {
	var errs []error
	if t.handle != nil && t.handle.job != 0 {
		if err := windows.TerminateJobObject(t.handle.job, 1); err != nil {
			errs = append(errs, fmt.Errorf("terminate job object: %w", err))
		}
	}
	if t.pid != 0 {
		if alive, _ := t.launcherAlive(); alive {
			if err := killProcessTree(t.pid); err != nil {
				if alive, _ := t.launcherAlive(); alive {
					errs = append(errs, fmt.Errorf("taskkill %d: %w", t.pid, err))
				}
			}
		}
	}
	if t.wsl {
		if _, err := wslAttemptProcesses(ctx, t.distro, t.attemptID, "KILL"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// treeAlive reports whether any process of the attempt is left.
func (e *Local) treeAlive(ctx context.Context, t processTree) (bool, error) {
	if t.handle != nil && t.handle.job != 0 {
		n, err := t.handle.activeProcesses()
		if err != nil {
			return false, fmt.Errorf("query job object: %w", err)
		}
		if n > 0 {
			return true, nil
		}
	}
	alive, err := t.launcherAlive()
	if err != nil || alive {
		return alive, err
	}
	if t.wsl {
		pids, err := wslAttemptProcesses(ctx, t.distro, t.attemptID, "0")
		return len(pids) > 0, err
	}
	return false, nil
}

// wslAttemptScript finds the Linux processes of an attempt by the
// KAIRO_ATTEMPT_ID every process launched for it inherits, signals them
// unless the signal is 0, and prints their pids.
const wslAttemptScript = `pids=$(grep -lzxF -e "KAIRO_ATTEMPT_ID=$1" /proc/[0-9]*/environ 2>/dev/null | tr '\0' '\n' | cut -d/ -f3)
if [ -n "$pids" ] && [ "$2" != 0 ]; then kill -s "$2" $pids 2>/dev/null; fi
echo $pids`

func wslAttemptProcesses(ctx context.Context, distro, attemptID, signal string) ([]int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	args := []string{}
	if distro != "" {
		args = append(args, "-d", distro)
	}
	args = append(args, "--exec", "sh", "-c", wslAttemptScript, "kairo-force-stop", attemptID, signal)
	out, err := exec.CommandContext(ctx, "wsl.exe", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("find attempt processes in WSL: %w", err)
	}
	var pids []int
	for _, f := range strings.Fields(string(out)) {
		pid, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("unexpected WSL output %q", strings.TrimSpace(string(out)))
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// ctrlBreakHelperArg runs this binary as a helper that sends CTRL_BREAK to a
// console process group. Attaching to another console is process-wide state,
// so it is done in a separate short-lived process, never in the daemon.
const ctrlBreakHelperArg = "__kairo-ctrl-break"

func ctrlBreak(ctx context.Context, pid int) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, ctrlBreakHelperArg, strconv.Itoa(pid))
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("send CTRL_BREAK to %d: %w: %s", pid, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// RunHelper runs a helper subcommand of this binary when args name one and
// then exits; otherwise it returns. main calls it before anything else.
func RunHelper(args []string) {
	if len(args) != 3 || args[1] != ctrlBreakHelperArg {
		return
	}
	pid, err := strconv.Atoi(args[2])
	if err == nil {
		err = sendCtrlBreak(pid)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func sendCtrlBreak(pid int) error {
	kernel32 := windows.NewLazySystemDLL("kernel32.dll")
	_, _, _ = kernel32.NewProc("FreeConsole").Call()
	if r, _, err := kernel32.NewProc("AttachConsole").Call(uintptr(pid)); r == 0 {
		return fmt.Errorf("attach to the console of %d: %w", pid, err)
	}
	// Ignore the event here; it is meant for the attempt's group only.
	_, _, _ = kernel32.NewProc("SetConsoleCtrlHandler").Call(0, 1)
	return windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(pid))
}

// launcherRunning reports whether pid (with that creation identity, when
// known) is still running. An exited process whose object is kept open (by a
// console host, say) does not count.
func launcherRunning(pid int, identity string) (bool, error) {
	if identity != "" {
		liveness, err := processidentity.HostProcessLiveness(pid, identity)
		if err != nil || liveness == processidentity.LivenessUnknown {
			return false, errors.Join(errors.New("launcher liveness unknown"), err)
		}
		if liveness == processidentity.LivenessAbsent {
			return false, nil
		}
	}
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
			return false, nil
		}
		return false, err
	}
	defer windows.CloseHandle(h)
	var code uint32
	if err = windows.GetExitCodeProcess(h, &code); err != nil {
		return false, err
	}
	return code == 259, nil // STILL_ACTIVE
}
