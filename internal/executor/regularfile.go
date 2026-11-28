package executor

import (
	"fmt"
	"io"
	"os"
)

// readRegular reads the regular file at path, of at most limit bytes. Not a
// link nor anything else: what an attempt can write (its container's
// /kairo/run, a stream's directory) it could point anywhere on the node,
// /dev/zero or another's file, for the agent to read. The file opened is
// the one looked at (a link swapped in between is refused).
func readRegular(path string, limit int64) ([]byte, error) {
	seen, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !seen.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file (%s)", path, seen.Mode().Type())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(seen, opened) {
		return nil, fmt.Errorf("%s changed while it was opened", path)
	}
	body, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%s is over %d bytes", path, limit)
	}
	return body, nil
}
