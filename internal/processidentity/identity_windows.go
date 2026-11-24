//go:build windows

package processidentity

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

func ForPID(pid int) (string, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return "", err
	}
	return WindowsIdentityFromFiletime(pid, uint64(created.HighDateTime)<<32|uint64(created.LowDateTime)), nil
}

// LineageOf reads the lineage of process pid, named identity: its parent
// chain, each parent created no later than its child (Windows keeps a dead
// parent's pid, which a later process may have been given). The environment of
// another process is not read on Windows, so AttemptID stays empty.
func LineageOf(pid int, identity string) Lineage {
	return lineage(pid, identity, readProcess, nil)
}

func readProcess(pid int) (process, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return process{}, err
	}
	defer windows.CloseHandle(handle)
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return process{}, err
	}
	var info windows.PROCESS_BASIC_INFORMATION
	if err = windows.NtQueryInformationProcess(handle, windows.ProcessBasicInformation, unsafe.Pointer(&info), uint32(unsafe.Sizeof(info)), nil); err != nil {
		return process{}, err
	}
	var code uint32
	if err = windows.GetExitCodeProcess(handle, &code); err != nil {
		return process{}, err
	}
	start := uint64(created.HighDateTime)<<32 | uint64(created.LowDateTime)
	return process{identity: WindowsIdentityFromFiletime(pid, start), parent: int(info.InheritedFromUniqueProcessId), start: start, exited: code != 259}, nil // 259: STILL_ACTIVE
}

// HostProcessLiveness distinguishes PID reuse and keeps observation errors
// separate from positive proof of absence. A process that exited is absent
// even while another process (a parent, a console host) keeps its object
// open, which keeps its pid and creation time readable.
func HostProcessLiveness(pid int, expected string) (Liveness, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_NOT_FOUND) {
			return LivenessAbsent, nil
		}
		return LivenessUnknown, err
	}
	defer windows.CloseHandle(handle)
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return LivenessUnknown, err
	}
	if WindowsIdentityFromFiletime(pid, uint64(created.HighDateTime)<<32|uint64(created.LowDateTime)) != expected {
		return LivenessAbsent, nil
	}
	var code uint32
	if err = windows.GetExitCodeProcess(handle, &code); err != nil {
		return LivenessUnknown, err
	}
	if code != 259 { // STILL_ACTIVE
		return LivenessAbsent, nil
	}
	return LivenessAlive, nil
}
