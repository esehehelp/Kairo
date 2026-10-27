//go:build linux

package provider

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func (p *Host) Observe(context.Context) (Snapshot, error) {
	total, available, err := readMeminfo("/proc/meminfo")
	if err != nil {
		return Snapshot{}, err
	}
	return p.snapshot(total, available), nil
}

// readMeminfo returns MemTotal and MemAvailable in bytes.
func readMeminfo(path string) (total, available int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	found := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, perr := strconv.ParseInt(fields[1], 10, 64)
		if perr != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total, found = kb*1024, found+1
		case "MemAvailable:":
			available, found = kb*1024, found+1
		}
	}
	if err = sc.Err(); err != nil {
		return 0, 0, err
	}
	if found != 2 {
		return 0, 0, fmt.Errorf("%s lacks MemTotal/MemAvailable", path)
	}
	return total, available, nil
}
