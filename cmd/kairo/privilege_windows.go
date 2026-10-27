package main

import "golang.org/x/sys/windows"

// privilegeName is what the controller needs to change devices here.
const privilegeName = "elevated (as administrator)"

// privileged reports whether this process runs elevated.
func privileged() bool { return windows.GetCurrentProcessToken().IsElevated() }
