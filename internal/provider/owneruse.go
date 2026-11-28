//go:build linux || windows

package provider

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"kairo/internal/store"
)

// OwnerUse is how a node's providers tell that its owner uses one of its
// resources (README: Owner use; issue #21): a GPU or the node's CPU taken
// by something that is not Kairo's work. The provider reports it
// as a claim of kind owner_use on the resource; the daemon then places
// nothing on it, suspends the checkpointable work holding it, and keeps off
// it until it has been free for a while.
//
//   - GPU: the share of its SMs used by graphics processes (pmon type G or
//     C+G; not the WSL VM's, where Windows runs Kairo's work), summed, at
//     least GPUSMPercent.
//   - CPU: the cores busy (at least ownerCPUBusy of them, or nothing waits
//     for them), and the share busy with what is not Kairo's (the whole
//     less the CPU time of Kairo's containers: on Linux the cgroups of the
//     Incus instances kairo-*, on Windows the wslc VM's vmmemwslc*
//     processes) at least CPUPercent. Not reported where Kairo's share
//     cannot be read (it would be taken for its owner's).
//
// Both are means over the last ownerWindow, not one reading: a desktop uses
// a GPU or the cores in bursts of a second or two (a page drawn, a window
// moved, a build step), and what work gives way to lasts minutes (a game, a
// render, a long build).
//   - RAM: no claim. The floor (RAMFreeBytes) is kept free: admission
//     leaves it free, and while less is available for a while the daemon
//     suspends work on the node (memory pressure, issue #45), whatever
//     took it (the owner, a kernel leak, Kairo's own work).
//
// Its owner at the keyboard (input within ownerPresence, Windows: as the
// input reporter in the owner's session writes it, InputFile) makes each
// more sensitive: the *Present thresholds apply instead. Input alone makes
// no claim.
type OwnerUse struct {
	Disabled bool
	// GPUSMPercent and GPUSMPercentPresent: zero means 50 and 25.
	GPUSMPercent, GPUSMPercentPresent float64
	// CPUPercent and CPUPercentPresent: zero means 50 and 25.
	CPUPercent, CPUPercentPresent float64
	// RAMFreeBytes and RAMFreeBytesPresent: zero means the larger of 2 GiB
	// and 5% of the RAM, and of 4 GiB and 10%.
	RAMFreeBytes, RAMFreeBytesPresent int64
	// InputFile is where the owner's last input is written (input.go); ""
	// means %LOCALAPPDATA%\kairo\input.json on Windows, none elsewhere.
	InputFile string
	// Now is the clock (nil: time.Now).
	Now func() time.Time
}

// ownerPresence: input this recent means the owner is at the keyboard.
const ownerPresence = 10 * time.Minute

// inputStale: an input file not written for this long says nothing (its
// reporter is not running).
const inputStale = 2 * time.Minute

// ownerCPUBusy: below this share of the cores busy, the owner's processes
// wait for nothing.
const ownerCPUBusy = 0.9

// ownerWindow: the owner's use of a GPU or the CPU is their mean use over
// this long.
const ownerWindow = 2 * time.Minute

// ownerWindowCover: readings over less than this tell nothing yet (an agent
// just started; a claim it made before is held by the daemon meanwhile).
const ownerWindowCover = time.Minute

// ClaimOwnerUse is the kind of the claims OwnerUse makes.
const ClaimOwnerUse = "owner_use"

func (o *OwnerUse) enabled() bool { return o != nil && !o.Disabled }

func (o *OwnerUse) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// InputRecord is what the input reporter writes: the owner's last input,
// and when it was written.
type InputRecord struct {
	LastInput string `json:"last_input"`
	Written   string `json:"written"`
}

// DefaultInputFile is where the input reporter writes on this host ("" when
// none does).
func DefaultInputFile() string {
	if runtime.GOOS != "windows" {
		return ""
	}
	base := os.Getenv("LOCALAPPDATA")
	if base == "" {
		return ""
	}
	return filepath.Join(base, "kairo", "input.json")
}

// present reports whether the owner is at the keyboard now, and their last
// input when known.
func (o *OwnerUse) present() (bool, time.Time) {
	path := o.InputFile
	if path == "" {
		path = DefaultInputFile()
	}
	if path == "" {
		return false, time.Time{}
	}
	body, err := os.ReadFile(path)
	if err != nil || len(body) > 4096 {
		return false, time.Time{}
	}
	var r InputRecord
	if json.Unmarshal(body, &r) != nil {
		return false, time.Time{}
	}
	last, err1 := time.Parse(time.RFC3339Nano, r.LastInput)
	written, err2 := time.Parse(time.RFC3339Nano, r.Written)
	now := o.now()
	if err1 != nil || err2 != nil || now.Sub(written) > inputStale {
		return false, time.Time{}
	}
	return now.Sub(last) <= ownerPresence, last
}

func (o *OwnerUse) gpuThreshold(present bool) float64 {
	if present {
		return orDefault(o.GPUSMPercentPresent, 25)
	}
	return orDefault(o.GPUSMPercent, 50)
}

func (o *OwnerUse) cpuThreshold(present bool) float64 {
	if present {
		return orDefault(o.CPUPercentPresent, 25)
	}
	return orDefault(o.CPUPercent, 50)
}

func (o *OwnerUse) ramFloor(total int64, present bool) int64 {
	if present {
		if o.RAMFreeBytesPresent > 0 {
			return o.RAMFreeBytesPresent
		}
		return max(4<<30, total/10)
	}
	if o.RAMFreeBytes > 0 {
		return o.RAMFreeBytes
	}
	return max(2<<30, total/20)
}

func orDefault(v, d float64) float64 {
	if v > 0 {
		return v
	}
	return d
}

// ownerProcess is a process using a GPU for its owner.
type ownerProcess struct {
	PID  int     `json:"pid"`
	Name string  `json:"name"`
	SM   float64 `json:"sm_percent"`
}

// ownerClaim is the owner_use claim of a resource.
func ownerClaim(resourceID string, evidence map[string]any) store.ExternalClaim {
	body, _ := json.Marshal(evidence)
	return store.ExternalClaim{ID: "owner-" + resourceID, ResourceID: resourceID, ClaimKind: ClaimOwnerUse, Evidence: body}
}

// topProcesses is the processes using the most SMs, at most five.
func topProcesses(ps []ownerProcess) []ownerProcess {
	sort.SliceStable(ps, func(i, j int) bool { return ps[i].SM > ps[j].SM })
	if len(ps) > 5 {
		ps = ps[:5]
	}
	return ps
}

// gpuReading is one reading of the graphics processes using a GPU's SMs.
type gpuReading struct {
	at        time.Time
	processes []ownerProcess
}

// gpuOwnerReadings are each GPU's readings over the last ownerWindow.
type gpuOwnerReadings struct {
	mu       sync.Mutex
	readings map[string][]gpuReading
}

// add records a reading of a GPU at now and returns the mean SM share of its
// graphics processes over the window (a reading without them counts as
// none), with each process's mean (the most first, at most five); ok is
// false while the readings cover less than ownerWindowCover.
func (r *gpuOwnerReadings) add(uuid string, now time.Time, processes []ownerProcess) (sum float64, top []ownerProcess, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readings == nil {
		r.readings = map[string][]gpuReading{}
	}
	kept := []gpuReading{}
	for _, reading := range r.readings[uuid] {
		// older than the window, or after now (the clock went back)
		if now.Sub(reading.at) < ownerWindow && !reading.at.After(now) {
			kept = append(kept, reading)
		}
	}
	kept = append(kept, gpuReading{at: now, processes: processes})
	r.readings[uuid] = kept
	if now.Sub(kept[0].at) < ownerWindowCover {
		return 0, nil, false
	}
	type key struct {
		pid  int
		name string
	}
	byProcess := map[key]float64{}
	for _, reading := range kept {
		for _, p := range reading.processes {
			byProcess[key{p.PID, p.Name}] += p.SM
		}
	}
	n := float64(len(kept))
	for k, total := range byProcess {
		top = append(top, ownerProcess{PID: k.pid, Name: k.name, SM: total / n})
		sum += total / n
	}
	sort.Slice(top, func(i, j int) bool { return top[i].PID < top[j].PID })
	return sum, topProcesses(top), true
}

// cpuTimes is a reading of the node's CPU time, all cores together: total,
// idle, and Kairo's (its containers'), in one unit; kairoKnown is false
// when Kairo's could not be read.
type cpuTimes struct {
	total, idle, kairo float64
	kairoKnown         bool
}

// cpuReading is a reading of the node's CPU time and when it was read.
type cpuReading struct {
	at    time.Time
	times cpuTimes
}

// cpuOwnerReadings are the node's readings of its CPU time over the last
// ownerWindow.
type cpuOwnerReadings struct {
	mu       sync.Mutex
	readings []cpuReading
}

// add records a reading at now and returns the readings to tell the owner's
// use by: the first in the window and this one; ok is false while they cover
// less than ownerWindowCover. A counter that went back (the wslc VM started
// again) starts the window over.
func (r *cpuOwnerReadings) add(now time.Time, cur cpuTimes) (base cpuTimes, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := []cpuReading{}
	for _, reading := range r.readings {
		if now.Sub(reading.at) < ownerWindow && !reading.at.After(now) && reading.times.kairoKnown == cur.kairoKnown {
			kept = append(kept, reading)
		}
	}
	if n := len(kept); n > 0 && (cur.total < kept[n-1].times.total || cur.idle < kept[n-1].times.idle || cur.kairo < kept[n-1].times.kairo) {
		kept = nil
	}
	r.readings = append(kept, cpuReading{at: now, times: cur})
	first := r.readings[0]
	if now.Sub(first.at) < ownerWindowCover {
		return cpuTimes{}, false
	}
	return first.times, true
}

// cpuOwnerUse is the owner's share of the cores between two readings: busy
// is the share busy, kairo Kairo's, owner the rest of busy. ok is false
// without two readings that tell it.
func cpuOwnerUse(prev, cur cpuTimes) (busy, kairo, owner float64, ok bool) {
	dt := cur.total - prev.total
	if dt <= 0 || !prev.kairoKnown || !cur.kairoKnown {
		return 0, 0, 0, false
	}
	busy = (dt - (cur.idle - prev.idle)) / dt
	kairo = max(0, cur.kairo-prev.kairo) / dt
	owner = max(0, busy-kairo)
	return busy, kairo, owner, true
}
