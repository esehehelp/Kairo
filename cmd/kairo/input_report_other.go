//go:build !windows

package main

import "errors"

// inputReport is Windows's (input_report_windows.go): elsewhere no owner sits
// at a node's keyboard. Its flags are read all the same (its help).
func inputReport(args []string) error {
	fs := newFlags("input-report")
	fs.String("out", "", "the file to write (the agent's [owner_use] input_file)")
	fs.Duration("every", 0, "how often")
	if _, err := parseFlags(fs, leafArgs(args)); err != nil {
		return err
	}
	return errors.New("input-report runs on Windows only")
}
