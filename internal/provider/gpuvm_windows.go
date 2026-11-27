//go:build windows && (amd64 || arm64)

package provider

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"

	"kairo/internal/pdh"
)

// gpuProcessMemoryCounter is the dedicated GPU memory each process holds on
// each adapter (instances pid_<pid>_luid_0x<high>_0x<low>_phys_<n>), as Task
// Manager shows it: WDDM's own count, which also sees what a VM holds through
// GPU paravirtualization, where nvidia-smi shows no process at all.
const gpuProcessMemoryCounter = `\GPU Process Memory(*)\Dedicated Usage`

var gpuProcessInstance = regexp.MustCompile(`^pid_(\d+)_luid_0x([0-9a-fA-F]+)_0x([0-9a-fA-F]+)_phys_\d+$`)

var (
	gdi32                      = windows.NewLazySystemDLL("gdi32.dll")
	procD3DKMTEnumAdapters2    = gdi32.NewProc("D3DKMTEnumAdapters2")
	procD3DKMTQueryAdapterInfo = gdi32.NewProc("D3DKMTQueryAdapterInfo")
	procD3DKMTCloseAdapter     = gdi32.NewProc("D3DKMTCloseAdapter")
)

// d3dkmtAdapterInfo is D3DKMT_ADAPTERINFO.
type d3dkmtAdapterInfo struct {
	Adapter               uint32
	LUIDLow               uint32
	LUIDHigh              int32
	Sources               uint32
	PrecisePresentRegions int32
}

// d3dkmtEnumAdapters2 is D3DKMT_ENUMADAPTERS2.
type d3dkmtEnumAdapters2 struct {
	Count    uint32
	Adapters *d3dkmtAdapterInfo
}

// d3dkmtQueryAdapterInfo is D3DKMT_QUERYADAPTERINFO.
type d3dkmtQueryAdapterInfo struct {
	Adapter uint32
	Type    uint32
	Data    unsafe.Pointer
	Size    uint32
}

// kmtqaitypeAdapterAddress is KMTQAITYPE_ADAPTERADDRESS: the adapter's PCI
// bus, device and function (D3DKMT_ADAPTERADDRESS).
const kmtqaitypeAdapterAddress = 6

// wslcVRAM reads what the wslc VM, where Windows runs Kairo's containers,
// holds of each GPU: the dedicated memory of the VM's worker process (the
// vmwp whose child is the VM's vmmemwslc* process), by the GPU's PCI address
// (pciAddress). The query is opened once and kept.
type wslcVRAM struct {
	mu     sync.Mutex
	query  *pdh.Query
	broken bool
}

func (w *wslcVRAM) read() (map[string]int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken {
		return nil, fmt.Errorf("no %s counter", gpuProcessMemoryCounter)
	}
	if w.query == nil {
		q := &pdh.Query{Path: gpuProcessMemoryCounter}
		if err := q.Open(); err != nil {
			w.broken = true
			return nil, err
		}
		w.query = q
	}
	vm := wslcVMProcesses()
	if len(vm) == 0 {
		return nil, nil
	}
	values, err := w.query.Collect()
	if err != nil {
		return nil, err
	}
	byLUID := map[uint64]int64{}
	for instance, bytes := range values {
		m := gpuProcessInstance.FindStringSubmatch(instance)
		if m == nil {
			continue
		}
		pid, _ := strconv.ParseUint(m[1], 10, 32)
		if !vm[uint32(pid)] || bytes <= 0 {
			continue
		}
		high, _ := strconv.ParseUint(m[2], 16, 32)
		low, _ := strconv.ParseUint(m[3], 16, 32)
		byLUID[high<<32|low] += int64(bytes)
	}
	if len(byLUID) == 0 {
		return nil, nil
	}
	addresses, err := adapterAddresses()
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for luid, bytes := range byLUID {
		if address, ok := addresses[luid]; ok {
			out[address] += bytes
		}
	}
	return out, nil
}

// wslcVMProcesses are the wslc VM's processes that can hold GPU memory: its
// vmmemwslc* process and that one's parent when it is the VM's worker
// (vmwp.exe: a parent that ended may have left its pid to another process),
// from the system's process list (a VM's processes cannot be opened).
func wslcVMProcesses() map[uint32]bool {
	names := map[uint32]string{}
	parents := map[uint32]uint32{}
	eachSystemProcess(func(p *windows.SYSTEM_PROCESS_INFORMATION) {
		pid := uint32(p.UniqueProcessID)
		names[pid] = strings.ToLower(p.ImageName.String())
		parents[pid] = uint32(p.InheritedFromUniqueProcessID)
	})
	out := map[uint32]bool{}
	for pid, name := range names {
		if !strings.HasPrefix(name, "vmmemwslc") {
			continue
		}
		out[pid] = true
		if parent := parents[pid]; names[parent] == "vmwp.exe" {
			out[parent] = true
		}
	}
	return out
}

// adapterAddresses are the display adapters' PCI addresses (pciAddress) by
// LUID.
func adapterAddresses() (map[uint64]string, error) {
	if err := gdi32.Load(); err != nil {
		return nil, err
	}
	var enum d3dkmtEnumAdapters2
	if status, _, _ := procD3DKMTEnumAdapters2.Call(uintptr(unsafe.Pointer(&enum))); status != 0 {
		return nil, fmt.Errorf("D3DKMTEnumAdapters2: NTSTATUS 0x%08X", uint32(status))
	}
	if enum.Count == 0 {
		return nil, nil
	}
	adapters := make([]d3dkmtAdapterInfo, enum.Count)
	enum.Adapters = &adapters[0]
	if status, _, _ := procD3DKMTEnumAdapters2.Call(uintptr(unsafe.Pointer(&enum))); status != 0 {
		return nil, fmt.Errorf("D3DKMTEnumAdapters2: NTSTATUS 0x%08X", uint32(status))
	}
	out := map[uint64]string{}
	for _, a := range adapters[:enum.Count] {
		var address struct{ Bus, Device, Function uint32 }
		query := d3dkmtQueryAdapterInfo{Adapter: a.Adapter, Type: kmtqaitypeAdapterAddress, Data: unsafe.Pointer(&address), Size: uint32(unsafe.Sizeof(address))}
		if status, _, _ := procD3DKMTQueryAdapterInfo.Call(uintptr(unsafe.Pointer(&query))); status == 0 {
			out[uint64(uint32(a.LUIDHigh))<<32|uint64(a.LUIDLow)] = pciAddress(uint64(address.Bus), uint64(address.Device), uint64(address.Function))
		}
		closeAdapter := a.Adapter
		procD3DKMTCloseAdapter.Call(uintptr(unsafe.Pointer(&closeAdapter)))
	}
	return out, nil
}
