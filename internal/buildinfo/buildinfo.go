// Package buildinfo names the build a binary came from, so a CLI can tell
// when it talks to a daemon built from different source.
package buildinfo

import (
	"runtime/debug"
	"sync"
)

var version = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	revision, modified := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if revision == "" {
		return "unknown"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		revision += "-dirty"
	}
	return revision
})

// Version is the VCS revision the binary was built from (12 hex digits,
// "-dirty" when the tree had uncommitted changes), or "unknown".
func Version() string { return version() }
