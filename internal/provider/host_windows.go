//go:build windows

package provider

import (
	"context"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

type memoryStatusEx struct {
	Length, MemoryLoad                                                                                   uint32
	TotalPhys, AvailPhys, TotalPageFile, AvailPageFile, TotalVirtual, AvailVirtual, AvailExtendedVirtual uint64
}

var (
	procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")
	procGetSystemTimes       = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetSystemTimes")
)

func (p *Host) Observe(context.Context) (Snapshot, error) {
	m := memoryStatusEx{}
	m.Length = uint32(unsafe.Sizeof(m))
	if ok, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&m))); ok == 0 {
		return Snapshot{}, err
	}
	return p.snapshot(int64(m.TotalPhys), int64(m.AvailPhys)), nil
}

// filetimeSeconds is a FILETIME span (100 ns ticks) in seconds.
func filetimeSeconds(f windows.Filetime) float64 {
	return float64(uint64(f.HighDateTime)<<32|uint64(f.LowDateTime)) / 1e7
}

// readCPUTimes reads the node's CPU time (GetSystemTimes: kernel time
// includes idle) and Kairo's: the wslc VM's processes (vmmemwslc*), where
// Windows runs its containers, from the system's process list (their times
// are read there without opening them, which a VM's process does not
// allow).
func readCPUTimes() (cpuTimes, error) {
	var t cpuTimes
	var idle, kernel, user windows.Filetime
	if ok, _, err := procGetSystemTimes.Call(uintptr(unsafe.Pointer(&idle)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user))); ok == 0 {
		return t, err
	}
	t.total, t.idle = filetimeSeconds(kernel)+filetimeSeconds(user), filetimeSeconds(idle)
	size := uint32(1 << 20)
	var buf []byte
	for range 8 {
		buf = make([]byte, size)
		var needed uint32
		err := windows.NtQuerySystemInformation(windows.SystemProcessInformation, unsafe.Pointer(&buf[0]), size, &needed)
		if err == nil {
			break
		}
		if err != windows.STATUS_INFO_LENGTH_MISMATCH {
			return t, nil // Kairo's share unknown
		}
		size = max(size*2, needed+64<<10)
		buf = nil
	}
	if buf == nil {
		return t, nil
	}
	for offset := uint32(0); ; {
		p := (*windows.SYSTEM_PROCESS_INFORMATION)(unsafe.Pointer(&buf[offset]))
		if name := strings.ToLower(p.ImageName.String()); strings.HasPrefix(name, "vmmemwslc") {
			t.kairo += float64(p.UserTime+p.KernelTime) / 1e7
		}
		if p.NextEntryOffset == 0 {
			break
		}
		offset += p.NextEntryOffset
		if int(offset) >= len(buf) {
			break
		}
	}
	t.kairoKnown = true
	return t, nil
}
