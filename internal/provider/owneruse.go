//go:build linux || windows

package provider

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"kairo/internal/store"
)

// OwnerUse is how a node's providers tell that its owner uses one of its
// resources (README: Owner use; issue #21): a GPU, the node's CPU or its
// RAM taken by something that is not Kairo's work. The provider reports it
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
//   - RAM: less available than RAMFreeBytes.
//
// Its owner at the keyboard (input within ownerPresence, Windows: as the
// input reporter in the owner's session writes it, InputFile) makes each
// more sensitive: the *Present thresholds apply instead. Input alone makes
// no claim.
type OwnerUse struct {
	Disabled bool
	// GPUSMPercent and GPUSMPercentPresent: zero means 30 and 10.
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
		return orDefault(o.GPUSMPercentPresent, 10)
	}
	return orDefault(o.GPUSMPercent, 30)
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

// cpuTimes is a reading of the node's CPU time, all cores together: total,
// idle, and Kairo's (its containers'), in one unit; kairoKnown is false
// when Kairo's could not be read.
type cpuTimes struct {
	total, idle, kairo float64
	kairoKnown         bool
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
