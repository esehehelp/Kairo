package machine

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode"
)

// StoreRootKey is a root (or a path under one) as roots are compared: one
// spelling of one place. Separators are forward slashes; "." components and
// repeated or trailing separators go; a drive path is case folded (Windows
// compares names without case); and a WSL path to a Windows drive
// (/mnt/e/...) is that drive's path, since it reaches the same files. Two
// roots of one node are compared by their keys whatever namespace (executor)
// they are declared in. The agent also compares the places the roots really
// are (links, mounts, short names), which a declaration cannot see.
func StoreRootKey(root string) string {
	p := strings.ReplaceAll(root, `\`, "/")
	drive := ""
	switch {
	case len(p) >= 2 && p[1] == ':' && isDriveLetter(p[0]):
		drive, p = p[:1], p[2:]
	case len(p) >= 6 && strings.HasPrefix(p, "/mnt/") && isDriveLetter(p[5]) && (len(p) == 6 || p[6] == '/'):
		drive, p = p[5:6], p[6:]
	}
	p = path.Clean("/" + p)
	if drive != "" {
		return strings.ToLower(drive + ":" + p)
	}
	return p
}

func isDriveLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// PathUnder reports whether p is root or a path under it, as StoreRootKey
// spells them.
func PathUnder(p, root string) bool {
	k, r := StoreRootKey(p), StoreRootKey(root)
	return k == r || strings.HasPrefix(k, strings.TrimSuffix(r, "/")+"/")
}

// RootsOverlap reports whether two roots are one place or one holds the
// other.
func RootsOverlap(a, b string) bool { return PathUnder(a, b) || PathUnder(b, a) }

// ScratchIDValid reports whether id can name a scratch directory: a plain
// name (an attempt id: letters, digits, _ and -), never a path or a dot name.
func ScratchIDValid(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// IsScratchDir reports whether p is the scratch directory of attemptID under
// root (<root>/scratch/<attempt id>), as StoreRootKey spells them, both
// absolute (StoreRootKey reads a relative path as one from /).
func IsScratchDir(p, root, attemptID string) bool {
	return ScratchIDValid(attemptID) && StoreRootIsAbsolute(p) && StoreRootIsAbsolute(root) &&
		StoreRootKey(p) == StoreRootKey(strings.TrimRight(root, `/\`)+"/scratch/"+attemptID)
}

// IsOutputDir reports whether p is the output directory of runID under root
// (<root>/outputs/<run id>), as StoreRootKey spells them, both absolute.
func IsOutputDir(p, root, runID string) bool {
	return ScratchIDValid(runID) && StoreRootIsAbsolute(p) && StoreRootIsAbsolute(root) &&
		StoreRootKey(p) == StoreRootKey(strings.TrimRight(root, `/\`)+"/outputs/"+runID)
}

// IsPackPath reports whether p is the pack of objectID under root
// (<root>/packs/<object id>.tar.zst), as StoreRootKey spells them, both
// absolute; with suffix, the file beside it named so (the file list,
// ".files.tsv").
func IsPackPath(p, root, objectID, suffix string) bool {
	return ScratchIDValid(objectID) && StoreRootIsAbsolute(p) && StoreRootIsAbsolute(root) &&
		StoreRootKey(p) == StoreRootKey(strings.TrimRight(root, `/\`)+"/packs/"+objectID+".tar.zst"+suffix)
}

// checkStoreRoot checks that root is one spelling of one place: absolute,
// without .. and without control characters. A POSIX root has no whitespace
// or backslash (the mount table, which a deletion is checked against,
// escapes them). A drive path has no name Windows would read as another:
// one ending in a dot or a space (Windows drops them), one with ~ (as in a
// short 8.3 name), a colon (a stream) or a character names cannot hold.
func checkStoreRoot(root string) error {
	switch {
	case strings.TrimSpace(root) != root || !StoreRootIsAbsolute(root):
		return fmt.Errorf("root %q is not an absolute path", root)
	case slices.Contains(strings.Split(strings.ReplaceAll(root, `\`, "/"), "/"), ".."):
		return fmt.Errorf("root %q contains ..", root)
	case strings.ContainsFunc(root, unicode.IsControl):
		return fmt.Errorf("root %q contains a control character", root)
	}
	if strings.HasPrefix(root, "/") {
		if strings.ContainsFunc(root, func(r rune) bool { return unicode.IsSpace(r) || r == '\\' }) {
			return fmt.Errorf("root %q contains whitespace or a backslash", root)
		}
		return nil
	}
	for _, name := range strings.Split(strings.ReplaceAll(root[2:], `\`, "/"), "/") {
		switch {
		case name == "" || name == ".":
		case strings.ContainsAny(name, `:*?"<>|~`):
			return fmt.Errorf("root %q has a name Windows reads as another (%q: one of :*?\"<>|~)", root, name)
		case strings.HasSuffix(name, ".") || strings.HasSuffix(name, " "):
			return fmt.Errorf("root %q has a name ending in a dot or a space (%q), which Windows drops", root, name)
		}
	}
	return nil
}
