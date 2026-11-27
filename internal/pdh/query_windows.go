//go:build windows && (amd64 || arm64)

// Package pdh reads Windows performance counters through PDH (pdh.dll).
package pdh

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	dll                             = windows.NewLazySystemDLL("pdh.dll")
	procPdhOpenQueryW               = dll.NewProc("PdhOpenQueryW")
	procPdhAddEnglishCounterW       = dll.NewProc("PdhAddEnglishCounterW")
	procPdhCollectQueryData         = dll.NewProc("PdhCollectQueryData")
	procPdhGetFormattedCounterArray = dll.NewProc("PdhGetFormattedCounterArrayW")
	procPdhCloseQuery               = dll.NewProc("PdhCloseQuery")
)

const (
	pdhMoreData       = 0x800007D2
	pdhFmtDouble      = 0x00000200
	pdhFmtNoCap100    = 0x00008000
	pdhCStatusValid   = 0x00000000
	pdhCStatusNewData = 0x00000001
)

// pdhFmtCounterValueItemDouble is PDH_FMT_COUNTERVALUE_ITEM_W read as a
// double, in its 64-bit layout (the name, the status, padding, the value):
// the build constraint keeps this file to 64-bit Windows.
type pdhFmtCounterValueItemDouble struct {
	Name    *uint16
	CStatus uint32
	_       uint32
	Value   float64
}

// Query is a PDH query of one counter (a wildcard path reads all its
// instances), kept open: an average counter's value is between two
// collections (the first has none), so the query is collected once per read
// and never reopened. Path is the counter's English path (counter names are
// localized).
type Query struct {
	Path    string
	query   uintptr
	counter uintptr
}

func pdhError(call string, status uintptr) error {
	return fmt.Errorf("%s: PDH status 0x%08X", call, uint32(status))
}

// Open opens the query and adds its counter.
func (q *Query) Open() error {
	if err := dll.Load(); err != nil {
		return err
	}
	if status, _, _ := procPdhOpenQueryW.Call(0, 0, uintptr(unsafe.Pointer(&q.query))); status != 0 {
		return pdhError("PdhOpenQuery", status)
	}
	path, err := windows.UTF16PtrFromString(q.Path)
	if err != nil {
		return err
	}
	if status, _, _ := procPdhAddEnglishCounterW.Call(q.query, uintptr(unsafe.Pointer(path)), 0, uintptr(unsafe.Pointer(&q.counter))); status != 0 {
		procPdhCloseQuery.Call(q.query)
		q.query = 0
		return pdhError("PdhAddEnglishCounter "+q.Path, status)
	}
	return nil
}

// Collect collects the counter and returns each instance's value (the sum
// of the instances of one name).
func (q *Query) Collect() (map[string]float64, error) {
	if status, _, _ := procPdhCollectQueryData.Call(q.query); status != 0 {
		return nil, pdhError("PdhCollectQueryData", status)
	}
	var size, count uint32
	status, _, _ := procPdhGetFormattedCounterArray.Call(q.counter, pdhFmtDouble|pdhFmtNoCap100, uintptr(unsafe.Pointer(&size)), uintptr(unsafe.Pointer(&count)), 0)
	if status != pdhMoreData {
		return nil, pdhError("PdhGetFormattedCounterArray", status)
	}
	// The buffer holds the items, then their names; uint64s keep it aligned.
	buf := make([]uint64, (size+7)/8)
	status, _, _ = procPdhGetFormattedCounterArray.Call(q.counter, pdhFmtDouble|pdhFmtNoCap100, uintptr(unsafe.Pointer(&size)), uintptr(unsafe.Pointer(&count)), uintptr(unsafe.Pointer(&buf[0])))
	if status != 0 {
		return nil, pdhError("PdhGetFormattedCounterArray", status)
	}
	items := unsafe.Slice((*pdhFmtCounterValueItemDouble)(unsafe.Pointer(&buf[0])), count)
	values := make(map[string]float64, count)
	for _, item := range items {
		if item.CStatus == pdhCStatusValid || item.CStatus == pdhCStatusNewData {
			// PDH lists instances of one name separately (a VM's worker holds
			// GPU memory under several): they add up
			values[windows.UTF16PtrToString(item.Name)] += item.Value
		}
	}
	return values, nil
}
