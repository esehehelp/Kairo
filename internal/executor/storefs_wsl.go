package executor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
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

// wslStoreScript: $1 is the operation, the rest its arguments. Exit 3: not
// <root>/scratch/<id>; 4: a link or non-directory; 5: the marker does not
// name the attempt; 6: the root is not on the declared drive; 7: a scratch
// to remove only if empty holds more than its marker.
const wslStoreScript = `op=$1; shift
case "$op" in
ensure)
  root=$1; fs=$2
  mkdir -p -- "$root" || exit 1
  [ -d "$root" ] || { echo "root $root is not a directory" >&2; exit 1; }
  case "$root/" in
  /mnt/[a-zA-Z]/*)
    drive=$(printf %s "$root" | cut -c6 | tr a-z A-Z)
    want=$(printf %s "$fs" | tr a-z A-Z | sed 's#[\\/]*$##')
    [ "$drive:" = "$want" ] || { echo "root $root is not on filesystem $fs" >&2; exit 6; } ;;
  esac ;;
usage)
  df -B1 --output=size,avail -- "$1" | tail -n 1 ;;
size)
  d=$1
  [ -e "$d" ] || [ -L "$d" ] || { echo 0; exit 0; }
  [ -d "$d" ] && [ ! -L "$d" ] || exit 4
  du -s -b -x -P -- "$d" | cut -f1 ;;
scan)
  p="$1/scratch"
  [ -e "$p" ] || [ -L "$p" ] || exit 0
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  for d in "$p"/*; do
    [ -d "$d" ] && [ ! -L "$d" ] || continue
    id=${d##*/}
    m="$d/.kairo-scratch"
    [ -f "$m" ] && [ ! -L "$m" ] || continue
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ] || continue
    printf '%s %s\n' "$id" "$(du -s -b -x -P -- "$d" | cut -f1)"
  done ;;
create)
  p="$1/scratch"; id=$2
  mkdir -p -- "$p" || exit 1
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  d="$p/$id"; m="$d/.kairo-scratch"
  if [ -d "$d" ] && [ ! -L "$d" ] && [ -f "$m" ] && [ ! -L "$m" ] &&
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ]; then
    printf '%s\n' "$d"; exit 0
  fi
  mkdir -- "$d" || exit 1
  printf '%s\n' "$id" > "$m" || { rmdir -- "$d"; exit 1; }
  printf '%s\n' "$d" ;;
remove)
  p="$1/scratch"; dir=$2; id=$3; mode=$4; d="$p/$id"
  [ "$dir" = "$d" ] || exit 3
  [ -e "$p" ] || [ -L "$p" ] || exit 0
  [ -d "$p" ] && [ ! -L "$p" ] || exit 4
  [ -e "$d" ] || [ -L "$d" ] || exit 0
  [ -d "$d" ] && [ ! -L "$d" ] || exit 4
  m="$d/.kairo-scratch"
  if [ -f "$m" ] && [ ! -L "$m" ]; then
    [ "$(head -c 1024 -- "$m" | tr -d '[:space:]')" = "$id" ] || exit 5
  else
    rmdir -- "$d" 2>/dev/null && exit 0
    exit 5
  fi
  if [ "$mode" = empty ]; then
    [ -z "$(find "$d" -mindepth 1 -maxdepth 1 ! -name .kairo-scratch -print | head -n 1)" ] || exit 7
  else
    find "$d" -mindepth 1 -maxdepth 1 ! -name .kairo-scratch -exec rm -r --one-file-system -- {} + || exit 1
  fi
  rm -f -- "$m" && rmdir -- "$d" ;;
*)
  exit 2 ;;
esac`

// wslRootPath is root without a trailing slash (but / itself).
func wslRootPath(root string) string {
	if trimmed := strings.TrimRight(root, "/"); trimmed != "" {
		return trimmed
	}
	return root
}

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

func (w wslStoreFS) Scan(ctx context.Context, root string) (map[string]int64, error) {
	out, err := w.run(ctx, 15*time.Minute, "scan", wslRootPath(root))
	if err != nil {
		return nil, err
	}
	sizes := map[string]int64{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !validScratchID(fields[0]) {
			continue
		}
		size, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unexpected scan output %q", line)
		}
		sizes[fields[0]] = size
	}
	return sizes, nil
}

func (w wslStoreFS) Create(ctx context.Context, root, attemptID string) (string, error) {
	if !validScratchID(attemptID) {
		return "", fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	out, err := w.run(ctx, time.Minute, "create", wslRootPath(root), attemptID)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (w wslStoreFS) Remove(ctx context.Context, root, dir, attemptID string, onlyEmpty bool) error {
	if !validScratchID(attemptID) {
		return fmt.Errorf("%w: attempt id %q", errOutsideRoot, attemptID)
	}
	mode := "all"
	if onlyEmpty {
		mode = "empty"
	}
	_, err := w.run(ctx, 30*time.Minute, "remove", wslRootPath(root), dir, attemptID, mode)
	return err
}

func (w wslStoreFS) Size(ctx context.Context, dir string) (int64, error) {
	out, err := w.run(ctx, 15*time.Minute, "size", dir)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
}
