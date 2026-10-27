//go:build !windows

package main

import "os"

// privilegeName is what the controller needs to change devices here.
const privilegeName = "as root"

// privileged reports whether this process runs as root.
func privileged() bool { return os.Geteuid() == 0 }
