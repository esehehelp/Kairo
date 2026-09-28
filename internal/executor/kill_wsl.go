package executor

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// wslSignalScript signals ($1: 0 probes only, TERM, KILL) every process in the
// distro whose environment carries KAIRO_ATTEMPT_ID=$2, which every process
// of the attempt inherits from the launch. It exits 0 when it found one and 1
// when none is left.
const wslSignalScript = `sig=$1 id=$2 found=1
for d in /proc/[0-9]*; do
  pid=${d#/proc/}
  [ "$pid" = "$$" ] && continue
  if tr '\0' '\n' <"$d/environ" 2>/dev/null | grep -qxF "KAIRO_ATTEMPT_ID=$id"; then
    found=0
    [ "$sig" = 0 ] || kill -s "$sig" "$pid" 2>/dev/null
  fi
done
exit $found`

// wslRun runs argv (wsl.exe and its arguments) and returns its exit code; a
// variable so tests can run the script without WSL.
var wslRun = func(ctx context.Context, argv []string) (int, error) {
	err := exec.CommandContext(ctx, argv[0], argv[1:]...).Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), nil
	}
	return 0, err
}

// wslSignalAttempt signals the Linux side of a WSL2 attempt, which killing the
// Windows-side wsl.exe launcher does not stop, and reports whether any of it
// was still running.
func wslSignalAttempt(ctx context.Context, distro, attemptID, sig string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	argv := []string{"wsl.exe"}
	if distro != "" {
		argv = append(argv, "-d", distro)
	}
	// --exec preserves argv boundaries, so the script needs no quoting.
	argv = append(argv, "--exec", "sh", "-c", wslSignalScript, "sh", sig, attemptID)
	code, err := wslRun(ctx, argv)
	switch {
	case err != nil:
		return false, err
	case code == 0:
		return true, nil
	case code == 1:
		return false, nil
	default:
		return false, fmt.Errorf("signal WSL processes of %s: %s exited %d", attemptID, strings.Join(argv[:len(argv)-5], " "), code)
	}
}
