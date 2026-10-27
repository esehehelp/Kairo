//go:build windows

package provider

import (
	"context"
	"unsafe"

	"golang.org/x/sys/windows"
)

type memoryStatusEx struct {
	Length, MemoryLoad                                                                                   uint32
	TotalPhys, AvailPhys, TotalPageFile, AvailPageFile, TotalVirtual, AvailVirtual, AvailExtendedVirtual uint64
}

var procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

func (p *Host) Observe(context.Context) (Snapshot, error) {
	m := memoryStatusEx{}
	m.Length = uint32(unsafe.Sizeof(m))
	if ok, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&m))); ok == 0 {
		return Snapshot{}, err
	}
	return p.snapshot(int64(m.TotalPhys), int64(m.AvailPhys)), nil
}
