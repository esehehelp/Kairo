//go:build windows

package executor

import "golang.org/x/sys/windows"

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
