//go:build windows

package bench

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

var (
	kernel32                  = windows.NewLazySystemDLL("kernel32.dll")
	procGlobalMemoryStatusEx  = kernel32.NewProc("GlobalMemoryStatusEx")
	procSetThreadAffinityMask = kernel32.NewProc("SetThreadAffinityMask")
	procGetDiskFreeSpaceW     = kernel32.NewProc("GetDiskFreeSpaceW")
)

func cpuModel() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `HARDWARE\DESCRIPTION\System\CentralProcessor\0`, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer k.Close()
	name, _, err := k.GetStringValue("ProcessorNameString")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(name)
}

type memoryStatusEx struct {
	Length, MemoryLoad                                                                                   uint32
	TotalPhys, AvailPhys, TotalPageFile, AvailPageFile, TotalVirtual, AvailVirtual, AvailExtendedVirtual uint64
}

// osVersion is the Windows version and build, e.g. 10.0.26200.
func osVersion() string {
	v := windows.RtlGetVersion()
	return fmt.Sprintf("%d.%d.%d", v.MajorVersion, v.MinorVersion, v.BuildNumber)
}

// memory is the physical memory and the part of it available to new allocations.
func memory() (total, available int64, err error) {
	m := memoryStatusEx{}
	m.Length = uint32(unsafe.Sizeof(m))
	if ok, _, callErr := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&m))); ok == 0 {
		return 0, 0, callErr
	}
	return int64(m.TotalPhys), int64(m.AvailPhys), nil
}

// pinThread pins the calling OS thread to logical CPU cpu (of processor
// group 0); the caller has locked its goroutine to the thread.
func pinThread(cpu int) error {
	if cpu >= 64 {
		return errors.New("CPUs past 63 are in another processor group")
	}
	thread, _ := windows.GetCurrentThread()
	if old, _, err := procSetThreadAffinityMask.Call(uintptr(thread), uintptr(1)<<cpu); old == 0 {
		return err
	}
	return nil
}

// diskFree is the space of the volume holding dir available to this user
// (GetDiskFreeSpaceEx, which honours quotas).
func diskFree(dir string) (int64, error) {
	path, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, err
	}
	var available, total, free uint64
	if err = windows.GetDiskFreeSpaceEx(path, &available, &total, &free); err != nil {
		return 0, err
	}
	return int64(available), nil
}

// volumeInfo identifies the volume and the disk holding dir: what
// GetVolumeInformation and the storage device descriptor report.
func volumeInfo(dir string) map[string]any {
	info := map[string]any{}
	path, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return info
	}
	root := make([]uint16, windows.MAX_PATH+1)
	if windows.GetVolumePathName(path, &root[0], uint32(len(root))) != nil {
		return info
	}
	rootPath := windows.UTF16ToString(root)
	info["volume_path"] = rootPath
	label, fsName := make([]uint16, windows.MAX_PATH+1), make([]uint16, windows.MAX_PATH+1)
	var serial, maxComponent, flags uint32
	if windows.GetVolumeInformation(&root[0], &label[0], uint32(len(label)), &serial, &maxComponent, &flags, &fsName[0], uint32(len(fsName))) == nil {
		info["filesystem"] = windows.UTF16ToString(fsName)
		info["volume_label"] = windows.UTF16ToString(label)
		info["volume_serial"] = fmt.Sprintf("%04X-%04X", serial>>16, serial&0xffff)
	}
	var sectorsPerCluster, bytesPerSector, freeClusters, totalClusters uint32
	if ok, _, _ := procGetDiskFreeSpaceW.Call(uintptr(unsafe.Pointer(&root[0])), uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)), uintptr(unsafe.Pointer(&freeClusters)), uintptr(unsafe.Pointer(&totalClusters))); ok != 0 {
		info["logical_sector_bytes"] = bytesPerSector
		info["cluster_bytes"] = sectorsPerCluster * bytesPerSector
	}
	var available, total, free uint64
	if windows.GetDiskFreeSpaceEx(&root[0], &available, &total, &free) == nil {
		info["total_bytes"], info["free_bytes"] = total, available
	}
	if len(rootPath) == 3 && rootPath[1] == ':' {
		storageDescriptor(`\\.\`+rootPath[:2], info)
	}
	return info
}

const ioctlStorageQueryProperty = 0x2d1400

var busTypes = map[uint32]string{1: "scsi", 2: "atapi", 3: "ata", 4: "ieee1394", 5: "ssa", 6: "fibre", 7: "usb", 8: "raid",
	9: "iscsi", 10: "sas", 11: "sata", 12: "sd", 13: "mmc", 14: "virtual", 15: "file-backed-virtual", 16: "spaces", 17: "nvme", 18: "scm", 19: "ufs"}

// storageDescriptor adds the model, serial and bus of the disk behind
// volume (\\.\D:) and its physical sector size; a volume spanning disks
// reports none.
func storageDescriptor(volume string, info map[string]any) {
	name, err := windows.UTF16PtrFromString(volume)
	if err != nil {
		return
	}
	// No access rights: querying properties needs none (and no elevation).
	h, err := windows.CreateFile(name, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return
	}
	defer windows.CloseHandle(h)
	query := func(property uint32, out []byte) uint32 {
		in := [3]uint32{property, 0, 0} // PropertyId, PropertyStandardQuery, AdditionalParameters
		var n uint32
		if windows.DeviceIoControl(h, ioctlStorageQueryProperty, (*byte)(unsafe.Pointer(&in[0])), uint32(unsafe.Sizeof(in)), &out[0], uint32(len(out)), &n, nil) != nil {
			return 0
		}
		return n
	}
	buf := make([]byte, 4096)
	if n := query(0, buf); n >= 40 { // StorageDeviceProperty: STORAGE_DEVICE_DESCRIPTOR
		u32 := func(off int) uint32 { return *(*uint32)(unsafe.Pointer(&buf[off])) }
		str := func(off int) string {
			o := int(u32(off))
			if o == 0 || o >= int(n) {
				return ""
			}
			end := o
			for end < int(n) && buf[end] != 0 {
				end++
			}
			return strings.TrimSpace(string(buf[o:end]))
		}
		vendor, product := str(12), str(16)
		info["disk_model"] = strings.TrimSpace(vendor + " " + product)
		info["disk_revision"] = str(20)
		info["disk_serial"] = str(24)
		info["disk_bus"] = busTypes[u32(28)]
		info["removable"] = buf[10] != 0
	}
	if n := query(6, buf); n >= 28 { // StorageAccessAlignmentProperty
		info["physical_sector_bytes"] = *(*uint32)(unsafe.Pointer(&buf[20]))
	}
}

// testFile is a handle of the storage test file.
type testFile struct{ h windows.Handle }

// openTestFile opens path for unbuffered, write-through I/O
// (FILE_FLAG_NO_BUFFERING|FILE_FLAG_WRITE_THROUGH): requests go to the
// device, sector-aligned. create makes the file, which must not exist.
// Every handle carries FILE_FLAG_DELETE_ON_CLOSE: the file disappears when
// the last handle closes, even if the process is killed. When the
// filesystem refuses unbuffered I/O the file is opened buffered and direct
// is false.
func openTestFile(path string, create, wantDirect bool) (f *testFile, direct bool, err error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, false, err
	}
	disposition := uint32(windows.OPEN_EXISTING)
	if create {
		disposition = windows.CREATE_NEW
	}
	open := func(flags uint32) (windows.Handle, error) {
		return windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE|windows.DELETE,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, disposition,
			windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_DELETE_ON_CLOSE|flags, 0)
	}
	if wantDirect {
		h, err := open(windows.FILE_FLAG_NO_BUFFERING | windows.FILE_FLAG_WRITE_THROUGH)
		if err == nil {
			return &testFile{h}, true, nil
		}
		if !errors.Is(err, windows.ERROR_INVALID_PARAMETER) && !errors.Is(err, windows.ERROR_NOT_SUPPORTED) {
			return nil, false, err
		}
	}
	h, err := open(0)
	if err != nil {
		return nil, false, err
	}
	return &testFile{h}, false, nil
}

func (f *testFile) ReadAt(p []byte, off int64) (int, error) {
	var n uint32
	o := windows.Overlapped{Offset: uint32(off), OffsetHigh: uint32(off >> 32)}
	err := windows.ReadFile(f.h, p, &n, &o)
	return int(n), err
}

func (f *testFile) WriteAt(p []byte, off int64) (int, error) {
	var n uint32
	o := windows.Overlapped{Offset: uint32(off), OffsetHigh: uint32(off >> 32)}
	err := windows.WriteFile(f.h, p, &n, &o)
	return int(n), err
}

// sync flushes the file's data and the device's write cache.
func (f *testFile) sync() error { return windows.FlushFileBuffers(f.h) }

func (f *testFile) close() error { return windows.CloseHandle(f.h) }

// isDirectIOError: the filesystem refused an unbuffered request's alignment or size.
func isDirectIOError(err error) bool { return errors.Is(err, windows.ERROR_INVALID_PARAMETER) }

// dropDirect switches f to buffered I/O; unbuffered handles cannot be
// changed, so the caller reopens instead.
func (f *testFile) dropDirect() error { return errors.ErrUnsupported }

// earlyUnlink: Windows removes the file through FILE_FLAG_DELETE_ON_CLOSE.
func earlyUnlink(string) bool { return false }

// linkSpeed is what the OS reports for the interface: transmit and receive
// link speed in bits per second, the interface's description and type.
func linkSpeed(index int, name string) map[string]any {
	row := windows.MibIfRow2{InterfaceIndex: uint32(index)}
	if windows.GetIfEntry2Ex(windows.MibIfEntryNormalWithoutStatistics, &row) != nil {
		return nil
	}
	return map[string]any{
		"link_speed_tx_bps": row.TransmitLinkSpeed, "link_speed_rx_bps": row.ReceiveLinkSpeed,
		"interface_description": windows.UTF16ToString(row.Description[:]), "interface_type": row.Type,
	}
}
