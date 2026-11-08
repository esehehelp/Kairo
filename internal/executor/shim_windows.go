//go:build windows

package executor

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	jobObjectQuery     = 0x0004
	jobObjectAllAccess = 0x1F001F
)

var procOpenJobObject = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")

func openJob(access uint32, name string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	r, _, callErr := procOpenJobObject.Call(uintptr(access), 0, uintptr(unsafe.Pointer(p)))
	if r == 0 {
		return 0, callErr
	}
	return windows.Handle(r), nil
}

// openTree opens an adopted attempt's job object by name. Its shim leads its
// console process group (CTRL_BREAK still reaches the attempt) whether or
// not the job is found.
func openTree(name string) (*treeHandle, error) {
	h := &treeHandle{group: true}
	if name == "" {
		return h, nil
	}
	job, err := openJob(jobObjectAllAccess, name)
	if err != nil {
		return h, err
	}
	h.job = job
	return h, nil
}
