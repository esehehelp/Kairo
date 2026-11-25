//go:build !linux

package executor

import "kairo/internal/config"

// runtimeNamed is the runtime of an executor kind on this system: wslc
// elsewhere than Linux (on Windows).
func runtimeNamed(kind string) (ociRuntime, bool) {
	if kind == config.KindWSLC {
		return wslcRuntime{}, true
	}
	return nil, false
}
