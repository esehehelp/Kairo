//go:build linux

package executor

import "kairo/internal/config"

// runtimeNamed is the runtime of an executor kind on this system: Incus on
// Linux.
func runtimeNamed(kind string) (ociRuntime, bool) {
	if kind == config.KindIncus {
		return incusRuntime{}, true
	}
	return nil, false
}
