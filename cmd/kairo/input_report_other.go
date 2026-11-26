//go:build !windows

package main

import "errors"

// inputReport is Windows's (input_report_windows.go): elsewhere no owner sits
// at a node's keyboard.
func inputReport([]string) error {
	return errors.New("input-report runs on Windows only")
}
