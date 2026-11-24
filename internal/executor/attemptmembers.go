//go:build !windows

package executor

import (
	"sync"
)

// Processes that left the launcher's process group are found by the
// KAIRO_ATTEMPT_ID in their environment.
// A process that exits gives up its memory, and with it its environment,
// before it closes its files (a GPU's among them) and becomes a zombie, which
// takes a while for a big one: its environment then reads empty, or its
// reading is refused (permission denied), so it is no longer found by it,
// yet it is still there. So each scan remembers the processes it found, with
// their creation identity (the stat starttime), and the next counts one of
// them that is still there with the same identity, not a zombie, while its
// environment reads empty or cannot be read.

// memberMemo holds, per attempt, the processes the last scan found, by pid.
type memberMemo struct {
	mu   sync.Mutex
	pids map[string]map[int]string
}

var attemptMembers = memberMemo{pids: map[string]map[int]string{}}

// known is what the last scan for key found (a map replaced by each scan,
// never changed in place).
func (m *memberMemo) known(key string) map[int]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pids[key]
}

// set keeps what a scan for key found; a scan that found nothing forgets key.
func (m *memberMemo) set(key string, found map[int]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(found) == 0 {
		delete(m.pids, key)
		return
	}
	m.pids[key] = found
}
