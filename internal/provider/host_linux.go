//go:build linux

package provider

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// readCPUTimes reads the node's CPU time (/proc/stat, in USER_HZ ticks of
// 1/100 s) and Kairo's: its Incus containers' (cgroup v2
// lxc.payload.kairo-*), in seconds. Without cgroup v2 Kairo's is unknown.
func readCPUTimes() (cpuTimes, error) {
	return readCPUTimesFrom("/proc/stat", "/sys/fs/cgroup")
}

func readCPUTimesFrom(stat, cgroups string) (cpuTimes, error) {
	var t cpuTimes
	body, err := os.ReadFile(stat)
	if err != nil {
		return t, err
	}
	line, _, _ := strings.Cut(string(body), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return t, fmt.Errorf("%s: no cpu line", stat)
	}
	for i, f := range fields[1:min(len(fields), 9)] {
		v, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return t, fmt.Errorf("%s: %w", stat, err)
		}
		t.total += v / 100
		if i == 3 || i == 4 { // idle, iowait
			t.idle += v / 100
		}
	}
	if _, err := os.Stat(filepath.Join(cgroups, "cgroup.controllers")); err != nil {
		return t, nil // not cgroup v2: Kairo's share unknown
	}
	stats, _ := filepath.Glob(filepath.Join(cgroups, "lxc.payload.kairo-*", "cpu.stat"))
	for _, path := range stats {
		body, err := os.ReadFile(path)
		if err != nil {
			continue // gone meanwhile
		}
		for _, l := range strings.Split(string(body), "\n") {
			if v, ok := strings.CutPrefix(l, "usage_usec "); ok {
				usec, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
				t.kairo += usec / 1e6
			}
		}
	}
	t.kairoKnown = true
	return t, nil
}
