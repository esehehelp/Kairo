//go:build windows

package executor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"

	"kairo/internal/extcmd"
	"kairo/internal/processidentity"
)

// usageJob is a duplicate of an attempt's job object handle, owned by one
// round of samples (the attempt's own handle may be closed meanwhile).
type usageJob windows.Handle

func duplicateJob(h *treeHandle) usageJob {
	if h == nil || h.job == 0 {
		return 0
	}
	self := windows.CurrentProcess()
	var dup windows.Handle
	if err := windows.DuplicateHandle(self, h.job, self, &dup, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return 0
	}
	return usageJob(dup)
}

func (j usageJob) close() {
	if j != 0 {
		windows.CloseHandle(windows.Handle(j))
	}
}

// sampleTrees samples the attempts' trees: for a WSL executor the Linux
// processes carrying each attempt's KAIRO_ATTEMPT_ID, in one wsl.exe run per
// round (sampleWSL); otherwise the attempt's job object, or, without one, the
// descendants of its launcher (windowsTreeUsage).
func (e *Local) sampleTrees(ctx context.Context, targets []usageTarget) map[string]usageSample {
	if e.Kind == "wsl2" || e.Attributes["environment"] == "wsl2" {
		ids := make([]string, len(targets))
		for i, t := range targets {
			ids[i] = t.attemptID
		}
		samples, err := sampleWSL(ctx, e.Attributes["distro"], ids)
		if err != nil {
			e.Logger.Warn("attempt usage unknown", "error", err)
		}
		return samples
	}
	out := map[string]usageSample{}
	for _, t := range targets {
		out[t.attemptID] = windowsTreeUsage(hostProcesses{}, t)
	}
	return out
}

// winProcess is one entry of a process snapshot.
type winProcess struct{ pid, parent int }

// winSystem is what windowsTreeUsage reads of the host (a fake in tests).
type winSystem interface {
	jobProcesses(job usageJob) ([]int, error)
	jobWritten(job usageJob) (int64, error)
	processes() ([]winProcess, error)
	// created, memory and written fail with errProcessGone for a process that
	// has exited.
	created(pid int) (uint64, error)
	memory(pid int) (int64, error)
	written(pid int) (int64, error)
}

// windowsTreeUsage samples one attempt's tree. With its job object: the
// private working sets of the job's processes, and the bytes the job has
// written (exited processes included). Without one (attachTree failed): the
// launcher and its descendants by parent pid, each created no earlier than
// its parent (Windows keeps a dead parent's pid, which a later process may
// have been given), and the bytes they have written while alive. A launcher
// that is no longer the process launched gives an unknown sample.
func windowsTreeUsage(sys winSystem, t usageTarget) usageSample {
	if t.job != 0 {
		pids, err := sys.jobProcesses(t.job)
		if err != nil {
			return usageSample{}
		}
		s := sumTree(pids, sys.memory, nil)
		if written, err := sys.jobWritten(t.job); err == nil {
			s.written = &written
		}
		return s
	}
	created, err := sys.created(t.pid)
	if err != nil || processidentity.WindowsIdentityFromFiletime(t.pid, created) != t.identity {
		return usageSample{}
	}
	snapshot, err := sys.processes()
	if err != nil {
		return usageSample{}
	}
	children := map[int][]int{}
	for _, p := range snapshot {
		if p.pid != p.parent {
			children[p.parent] = append(children[p.parent], p.pid)
		}
	}
	members := []int{t.pid}
	type node struct {
		pid     int
		created uint64
	}
	visited := map[int]bool{t.pid: true}
	for stack := []node{{t.pid, created}}; len(stack) > 0; {
		parent := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, pid := range children[parent.pid] {
			if visited[pid] {
				continue
			}
			created, err := sys.created(pid)
			if errors.Is(err, errProcessGone) {
				continue
			}
			if err != nil {
				return usageSample{}
			}
			if created < parent.created {
				continue // a later process given the dead parent's pid
			}
			visited[pid] = true
			members = append(members, pid)
			stack = append(stack, node{pid, created})
		}
	}
	return sumTree(members, sys.memory, sys.written)
}

// hostProcesses reads this host's processes.
type hostProcesses struct{}

// jobProcessIDList is JOBOBJECT_BASIC_PROCESS_ID_LIST with room for ids.
type jobProcessIDList struct {
	assigned, listed uint32
	ids              [1024]uintptr
}

func (hostProcesses) jobProcesses(job usageJob) ([]int, error) {
	var list jobProcessIDList
	if err := windows.QueryInformationJobObject(windows.Handle(job), windows.JobObjectBasicProcessIdList, uintptr(unsafe.Pointer(&list)), uint32(unsafe.Sizeof(list)), nil); err != nil {
		return nil, fmt.Errorf("list the job's processes: %w", err)
	}
	if list.listed < list.assigned {
		return nil, fmt.Errorf("the job has %d processes, more than are listed", list.assigned)
	}
	pids := make([]int, list.listed)
	for i := range pids {
		pids[i] = int(list.ids[i])
	}
	return pids, nil
}

// jobIOAccounting is JOBOBJECT_BASIC_AND_IO_ACCOUNTING_INFORMATION.
type jobIOAccounting struct {
	basic jobBasicAccounting
	io    windows.IO_COUNTERS
}

func (hostProcesses) jobWritten(job usageJob) (int64, error) {
	var info jobIOAccounting
	if err := windows.QueryInformationJobObject(windows.Handle(job), windows.JobObjectBasicAndIoAccountingInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil); err != nil {
		return 0, err
	}
	return int64(info.io.WriteTransferCount), nil
}

func (hostProcesses) processes() ([]winProcess, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(snapshot)
	var out []winProcess
	entry := windows.ProcessEntry32{Size: uint32(unsafe.Sizeof(windows.ProcessEntry32{}))}
	for err = windows.Process32First(snapshot, &entry); err == nil; err = windows.Process32Next(snapshot, &entry) {
		out = append(out, winProcess{pid: int(entry.ProcessID), parent: int(entry.ParentProcessID)})
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return nil, err
	}
	return out, nil
}

// openLive opens a running process for querying; errProcessGone when it has
// exited (even if its object is still open somewhere).
func openLive(pid int) (windows.Handle, error) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return 0, errProcessGone
	}
	if err != nil {
		return 0, err
	}
	var code uint32
	if err = windows.GetExitCodeProcess(h, &code); err != nil || code != 259 { // STILL_ACTIVE
		windows.CloseHandle(h)
		if err == nil {
			err = errProcessGone
		}
		return 0, err
	}
	return h, nil
}

func (hostProcesses) created(pid int) (uint64, error) {
	h, err := openLive(pid)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(h)
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return 0, err
	}
	return uint64(created.HighDateTime)<<32 | uint64(created.LowDateTime), nil
}

// processMemoryCountersEx2 is PROCESS_MEMORY_COUNTERS_EX2 (Windows 10 1809 and
// later): PrivateWorkingSetSize is the process's resident memory not shared
// with others, the analogue of Linux PSS that does not count shared DLLs once
// per process.
type processMemoryCountersEx2 struct {
	cb                         uint32
	pageFaultCount             uint32
	peakWorkingSetSize         uintptr
	workingSetSize             uintptr
	quotaPeakPagedPoolUsage    uintptr
	quotaPagedPoolUsage        uintptr
	quotaPeakNonPagedPoolUsage uintptr
	quotaNonPagedPoolUsage     uintptr
	pagefileUsage              uintptr
	peakPagefileUsage          uintptr
	privateUsage               uintptr
	privateWorkingSetSize      uintptr
	sharedCommitUsage          uint64
}

var (
	kernel32                 = windows.NewLazySystemDLL("kernel32.dll")
	procGetProcessMemoryInfo = kernel32.NewProc("K32GetProcessMemoryInfo")
	procGetProcessIoCounters = kernel32.NewProc("GetProcessIoCounters")
)

func (hostProcesses) memory(pid int) (int64, error) {
	h, err := openLive(pid)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(h)
	counters := processMemoryCountersEx2{cb: uint32(unsafe.Sizeof(processMemoryCountersEx2{}))}
	if r, _, err := procGetProcessMemoryInfo.Call(uintptr(h), uintptr(unsafe.Pointer(&counters)), uintptr(counters.cb)); r == 0 {
		return 0, fmt.Errorf("memory of process %d: %w", pid, err)
	}
	return int64(counters.privateWorkingSetSize), nil
}

func (hostProcesses) written(pid int) (int64, error) {
	h, err := openLive(pid)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(h)
	var counters windows.IO_COUNTERS
	if r, _, err := procGetProcessIoCounters.Call(uintptr(h), uintptr(unsafe.Pointer(&counters))); r == 0 {
		return 0, fmt.Errorf("I/O of process %d: %w", pid, err)
	}
	return int64(counters.WriteTransferCount), nil
}

// wslUsageScript prints, for each attempt id argument, "ID RAM WRITTEN" over
// the Linux processes carrying KAIRO_ATTEMPT_ID=ID (as force stop finds them):
// the sum of their PSS in bytes and of their write_bytes less
// cancelled_write_bytes, "-" when a live one cannot be read. A process that
// exits while being read is left out. It is plain sh, with no command run per
// process.
const wslUsageScript = `pss() { while read -r k v rest; do if [ "$k" = Pss: ]; then echo "$v"; return 0; fi; done < "$1/smaps_rollup"; return 1; }
written() { w= c=0; while read -r k v; do case $k in write_bytes:) w=$v;; cancelled_write_bytes:) c=$v;; esac; done < "$1/io" || return 1; [ -n "$w" ] && echo $((w-c)); }
for id in "$@"; do
  ram=0 disk=0
  for f in $(grep -lzxF -e "KAIRO_ATTEMPT_ID=$id" /proc/[0-9]*/environ 2>/dev/null | tr '\0' '\n'); do
    d=${f%/environ}
    p=$(pss "$d" 2>/dev/null) || p=
    w=$(written "$d" 2>/dev/null) || w=
    [ -d "$d" ] || continue
    if [ -n "$p" ] && [ "$ram" != - ]; then ram=$((ram+p*1024)); else ram=-; fi
    if [ -n "$w" ] && [ "$disk" != - ]; then disk=$((disk+w)); else disk=-; fi
  done
  echo "$id $ram $disk"
done`

// sampleWSL samples, in one wsl.exe run under ctx, the Linux processes of
// each attempt (wslUsageScript).
func sampleWSL(ctx context.Context, distro string, attemptIDs []string) (map[string]usageSample, error) {
	args := []string{}
	if distro != "" {
		args = append(args, "-d", distro)
	}
	args = append(args, "--exec", "sh", "-c", wslUsageScript, "kairo-usage")
	out, err := extcmd.Output(ctx, "wsl.exe", append(args, attemptIDs...)...)
	if err != nil {
		return map[string]usageSample{}, fmt.Errorf("sample attempt usage in WSL: %w", err)
	}
	return parseWSLUsage(string(out)), nil
}

// parseWSLUsage reads wslUsageScript's lines "ATTEMPT RAM WRITTEN", "-" for
// a total that is not known.
func parseWSLUsage(out string) map[string]usageSample {
	samples := map[string]usageSample{}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		var s usageSample
		if n, err := strconv.ParseInt(fields[1], 10, 64); err == nil && n >= 0 {
			s.ram = &n
		}
		if n, err := strconv.ParseInt(fields[2], 10, 64); err == nil && n >= 0 {
			s.written = &n
		}
		samples[fields[0]] = s
	}
	return samples
}
