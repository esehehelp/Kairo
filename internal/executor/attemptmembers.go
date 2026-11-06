package executor

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Processes that left the launcher's process group, and every Linux process
// of a WSL attempt, are found by the KAIRO_ATTEMPT_ID in their environment.
// A process that exits gives up its memory, and with it its environment,
// before it closes its files (a GPU's among them) and becomes a zombie, which
// takes a while for a big one: its environment then reads empty, or its
// reading is refused (permission denied), so it is no longer found by it,
// yet it is still there. So each scan remembers the processes it found, with
// their creation identity (the stat starttime), and the next counts one of
// them that is still there with the same identity, not a zombie, while its
// environment reads empty or cannot be read.

// memberMemo holds, per scan key (an attempt; for WSL, its distro and
// attempt), the processes the last scan found, by pid.
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

// wslAttemptScript finds the Linux processes of an attempt ($1): those whose
// environment carries its KAIRO_ATTEMPT_ID, and those of the processes the
// last scan found (the remaining arguments, pid:starttime) that are still
// there with that starttime, not zombies, with an environment that is empty
// or cannot be read (exiting).
// It signals them unless the signal ($2) is 0, and prints them as
// pid:starttime, as read before the signal.
const wslAttemptScript = `a=$1 sig=$2
shift 2
pids=$(echo $(grep -lzxF -e "KAIRO_ATTEMPT_ID=$a" /proc/[0-9]*/environ 2>/dev/null | tr '\0' '\n' | cut -d/ -f3))
set -f
for m in "$@"; do
  p=${m%%:*}
  case " $pids " in *" $p "*) continue ;; esac
  # 0: an environment without the id (another process now); 1: empty;
  # 2: refused (exiting) or gone (then the stat below is not there)
  grep -qz . "/proc/$p/environ" 2>/dev/null && continue
  st=$(cat "/proc/$p/stat" 2>/dev/null) || continue
  set -- ${st##*) }
  case $1 in Z|X) continue ;; esac
  [ "${20}" = "${m#*:}" ] && pids="$pids $p"
done
out=
for p in $pids; do
  st=$(cat "/proc/$p/stat" 2>/dev/null) || continue
  set -- ${st##*) }
  out="$out $p:${20}"
done
if [ -n "$pids" ] && [ "$sig" != 0 ]; then kill -s "$sig" $pids 2>/dev/null; fi
echo $out`

// parseAttemptMembers reads wslAttemptScript's output: the pids, and each
// with its starttime.
func parseAttemptMembers(out string) ([]int, map[int]string, error) {
	var pids []int
	found := map[int]string{}
	for _, f := range strings.Fields(out) {
		p, start, ok := strings.Cut(f, ":")
		pid, err := strconv.Atoi(p)
		if _, serr := strconv.ParseUint(start, 10, 64); !ok || err != nil || serr != nil {
			return nil, nil, fmt.Errorf("unexpected WSL output %q", strings.TrimSpace(out))
		}
		pids = append(pids, pid)
		found[pid] = start
	}
	return pids, found, nil
}
