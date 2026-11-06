//go:build !linux && !windows

package executor

import (
	"fmt"
	"os/exec"
)

func confineCommand(*exec.Cmd, []string, string) (func(), error) {
	return nil, fmt.Errorf("%w: not on this platform", errConfineHere)
}

func runConfineHelper([]string) {}
