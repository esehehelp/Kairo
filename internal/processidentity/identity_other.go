//go:build !windows

package processidentity

import (
	"fmt"
	"os"
)

func ForPID(pid int) (string, error) {
	info, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pid:%d:start:%d", pid, info.ModTime().UnixNano()), nil
}
