package executor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"kairo/internal/extcmd"
)

// wslStoreFS reaches a root inside a WSL distro (a store declared for a WSL
// executor), through one shell script run by wsl.exe --exec. Its rules are
// localStoreFS's: Kairo's directories only (the marker), links never
// followed (test -L; GNU rm -r removes a symlink itself, never what it
// points at), no other filesystem entered (rm --one-file-system, du -x).
// A root under /mnt/<drive> must be on that drive; a root on the distro's
// own disk cannot be checked against a Windows filesystem identity and is
// taken as declared.
type wslStoreFS struct{ distro string }

// wslStoreScript: $1 is the operation, the rest its arguments (list, create
// and remove take the root, then the kind's directory and marker names: a
// scratch's or an output's). Exit 3: not <root>/<dir>/<id>; 4: a link or
// non-directory; 5: the marker does not name the id; 6: the root is not on
// the declared drive; 7: a directory to remove only if empty holds more than
// its marker.
const wslStoreScript = `op=$1; shift
# realroot: the root resolves to itself (no symlink anywhere on its path),
# else exit 4.
realroot() { [ "$(realpath -e -- "$1" 2>/dev/null)" = "$1" ] || { echo "root $1 is not its own real path" >&2; exit 4; }; }
# mountpoint: $1 is a mount point (bind mounts included). mounted: one is at
# $1 or at or under $2. (mountinfo escapes a space as \040: the paths
# compared have none, and a mount point under $2 starts with "$2/" escaped
# or not.)
mountpoint() { awk -v p="$1" '$5 == p { f=1 } END { exit !f }' /proc/self/mountinfo; }
mounted() { awk -v p="$1" -v d="$2" '{ m=$5; if (m == p || m == d || index(m, d "/") == 1) f=1 } END { exit !f }' /proc/self/mountinfo; }
case "$op" in
ensure)
  root=$1; fs=$2
  # no symlink on the part of the path that exists, before anything is made
  [ "$(realpath -m -- "$root" 2>/dev/null)" = "$root" ] || { echo "root $root is not its own real path" >&2; exit 4; }
  mkdir -p -- "$root" || exit 1
  [ -d "$root" ] || { echo "root $root is not a directory" >&2; exit 1; }
  realroot "$root"
  case "$root/" in
  /mnt/[a-zA-Z]/*)
    drive=$(printf %s "$root" | cut -c6 | tr a-z A-Z)
    want=$(printf %s "$fs" | tr a-z A-Z | sed 's#[\/]*$##')
    [ "$drive:" = "$want" ] || { echo "root $root is not on filesystem $fs" >&2; exit 6; } ;;
  esac ;;
usage)
  df -B1 --output=size,avail -- "$1" | tail -n 1 ;;
size)
  d=$1
  [ -e "$d" ] || [ -L "$d" ] || { echo 0; exit 0; }
  [ -d "$d" ] && [ ! -L "$d" ] || exit 4
  du -s -b -x -P -- "$d" | cut -f1 ;;
list)
  [ -e "$1" ] || exit 0
  realroot "$1"
  p="$1/$2"; mk=$3
  [ -e "$p" ] || [ -L "$p" ] || exit 0
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  for d in "$p"/*; do
    [ -d "$d" ] && [ ! -L "$d" ] || continue
    id=${d##*/}
    m="$d/$mk"
    # a directory or marker that cannot be read: not known to be Kairo's,
    # nor to be gone
    if [ ! -x "$d" ] || { [ -e "$m" ] && [ ! -r "$m" ]; }; then printf '? %s\n' "$id"; continue; fi
    [ -f "$m" ] && [ ! -L "$m" ] || continue
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ] || continue
    printf '%s\n' "$id"
  done ;;
create)
  realroot "$1"
  p="$1/$2"; mk=$3; id=$4
  mkdir -p -- "$p" || exit 1
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  mountpoint "$p" && { echo "$p is a mount point" >&2; exit 4; }
  d="$p/$id"; m="$d/$mk"
  if [ -d "$d" ] && [ ! -L "$d" ] && [ -f "$m" ] && [ ! -L "$m" ] &&
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ]; then
    printf '%s\n' "$d"; exit 0
  fi
  mkdir -- "$d" || exit 1
  [ -d "$d" ] && [ ! -L "$d" ] || exit 4
  # created, never opened: noclobber (O_EXCL) refuses anything put there
  ( set -C; printf '%s\n' "$id" > "$m" ) || { rmdir -- "$d"; exit 1; }
  printf '%s\n' "$d" ;;
remove)
  p="$1/$2"; mk=$3; dir=$4; id=$5; mode=$6; d="$p/$id"
  [ "$dir" = "$d" ] || exit 3
  [ -e "$1" ] || exit 0
  realroot "$1"
  [ -e "$p" ] || [ -L "$p" ] || exit 0
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  [ -e "$d" ] || [ -L "$d" ] || exit 0
  [ -d "$d" ] && [ ! -L "$d" ] || exit 4
  mounted "$p" "$d" && { echo "a mount point is at $p or at or under $d" >&2; exit 4; }
  m="$d/$mk"
  if [ -f "$m" ] && [ ! -L "$m" ]; then
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ] || exit 5
  else
    rmdir -- "$d" 2>/dev/null && exit 0
    exit 5
  fi
  if [ "$mode" = empty ]; then
    [ -z "$(find "$d" -mindepth 1 -maxdepth 1 ! -name "$mk" -print | head -n 1)" ] || exit 7
  else
    find "$d" -mindepth 1 -maxdepth 1 ! -name "$mk" -exec rm -r --one-file-system -- {} + || exit 1
  fi
  rm -f -- "$m" && rmdir -- "$d" ;;
*)
  exit 2 ;;
esac`

// wslRootPath is root as the script compares it with its real path: cleaned
// (no . component, repeated or trailing slash).
func wslRootPath(root string) string { return path.Clean(root) }

func (w wslStoreFS) run(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	full := []string{}
	if w.distro != "" {
		full = append(full, "-d", w.distro)
	}
	full = append(full, "--exec", "sh", "-c", wslStoreScript, "kairo-store")
	out, err := extcmd.Output(ctx, "wsl.exe", append(full, args...)...)
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			switch exit.ExitCode() {
			case 3:
				return nil, fmt.Errorf("%w: %v", errOutsideRoot, err)
			case 4:
				return nil, fmt.Errorf("%w: %v", errScratchLink, err)
			case 5:
				return nil, fmt.Errorf("%w: %v", errScratchMarker, err)
			case 7:
				return nil, fmt.Errorf("%w: %v", errScratchNotEmpty, err)
			}
		}
		return nil, err
	}
	return out, nil
}

func (w wslStoreFS) Ensure(ctx context.Context, root, filesystem string) error {
	_, err := w.run(ctx, time.Minute, "ensure", wslRootPath(root), filesystem)
	return err
}

func (w wslStoreFS) Usage(ctx context.Context, root string) (int64, int64, error) {
	out, err := w.run(ctx, time.Minute, "usage", wslRootPath(root))
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("unexpected df output %q", strings.TrimSpace(string(out)))
	}
	total, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	free, err := strconv.ParseInt(fields[1], 10, 64)
	return total, free, err
}

// Place: inside a distro a root is compared only by its path (the distro's
// own disk holds no other store's root, and /mnt/<drive> is compared with
// the drive when stores are declared).
func (wslStoreFS) Place(context.Context, string) (string, error) { return "", nil }

func (w wslStoreFS) List(ctx context.Context, root string) ([]string, []string, error) {
	return w.ListKind(ctx, root, kindScratch)
}

func (w wslStoreFS) ListKind(ctx context.Context, root string, kind dirKind) ([]string, []string, error) {
	out, err := w.run(ctx, 5*time.Minute, "list", wslRootPath(root), kind.dir, kind.marker)
	if err != nil {
		return nil, nil, err
	}
	var ids, unread []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if id, ok := strings.CutPrefix(line, "? "); ok {
			if validScratchID(id) {
				unread = append(unread, id)
			}
		} else if validScratchID(line) {
			ids = append(ids, line)
		}
	}
	slices.Sort(ids)
	slices.Sort(unread)
	return ids, unread, nil
}

func (w wslStoreFS) Create(ctx context.Context, root, attemptID string) (string, error) {
	return w.CreateKind(ctx, root, kindScratch, attemptID)
}

func (w wslStoreFS) CreateKind(ctx context.Context, root string, kind dirKind, attemptID string) (string, error) {
	if !validScratchID(attemptID) {
		return "", fmt.Errorf("%w: id %q", errOutsideRoot, attemptID)
	}
	out, err := w.run(ctx, time.Minute, "create", wslRootPath(root), kind.dir, kind.marker, attemptID)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (w wslStoreFS) Remove(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error {
	return w.RemoveKind(ctx, root, kindScratch, dir, attemptID, onlyEmpty)
}

func (w wslStoreFS) RemoveKind(ctx context.Context, root string, kind dirKind, dir, attemptID string, onlyEmpty bool) error {
	if !validScratchID(attemptID) {
		return fmt.Errorf("%w: id %q", errOutsideRoot, attemptID)
	}
	if !strings.HasPrefix(root, "/") {
		return fmt.Errorf("%w: root %s is not absolute", errOutsideRoot, root)
	}
	mode := "all"
	if onlyEmpty {
		mode = "empty"
	}
	_, err := w.run(ctx, 30*time.Minute, "remove", wslRootPath(root), kind.dir, kind.marker, dir, attemptID, mode)
	return err
}

func (w wslStoreFS) Size(ctx context.Context, dir string) (int64, error) {
	out, err := w.run(ctx, 15*time.Minute, "size", dir)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
}
