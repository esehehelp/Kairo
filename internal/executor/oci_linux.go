//go:build linux

package executor

import "kairo/internal/config"

// runtimeNamed is the runtime of an executor kind on this system: Incus and
// Proxmox's pct on Linux.
func runtimeNamed(kind string) (ociRuntime, bool) {
	switch kind {
	case config.KindIncus:
		return incusRuntime{}, true
	case config.KindPCT:
		return pctRuntime{}, true
	}
	return nil, false
}
