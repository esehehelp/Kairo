// Package sourcesnap names code snapshots (kairo source): a snapshot is
// the files of declared paths of a project's directory at apply time,
// packed as a data object is (a tar.zst and its file list: path, bytes,
// sha256 and mtime of each regular file). Its id is the sha256 of its file
// list without the mtimes, so the same files make the same snapshot
// whenever and wherever they are packed.
package sourcesnap

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ListHeader is a file list's first line.
const ListHeader = "path\tbytes\tsha256\tmtime_ns"

// File is a file a list names.
type File struct {
	Path   string // escaped as in the list
	Bytes  string
	SHA256 string
}

var idPattern = regexp.MustCompile(`^src_[0-9a-f]{32}$`)

// ValidID reports whether id is a snapshot id.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// Parse reads a file list into its files, in its order.
func Parse(list string) ([]File, error) {
	lines := strings.Split(strings.TrimSuffix(list, "\n"), "\n")
	if len(lines) == 0 || lines[0] != ListHeader {
		return nil, errors.New("not a file list Kairo wrote")
	}
	files := make([]File, 0, len(lines)-1)
	for _, line := range lines[1:] {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 || fields[0] == "" {
			return nil, fmt.Errorf("file list line %q", line)
		}
		files = append(files, File{Path: fields[0], Bytes: fields[1], SHA256: fields[2]})
	}
	return files, nil
}

// ID is the id of the snapshot whose file list is list.
func ID(list string) (string, error) {
	files, err := Parse(list)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, f := range files {
		fmt.Fprintf(h, "%s\t%s\t%s\n", f.Path, f.Bytes, f.SHA256)
	}
	return "src_" + hex.EncodeToString(h.Sum(nil))[:32], nil
}

// Diff is how snapshot b differs from a: paths added, removed and changed
// (another size or sha256), each sorted as the lists are.
type Diff struct {
	Added, Removed, Changed []string
}

// Compare lists how the files of list b differ from those of list a.
func Compare(a, b string) (Diff, error) {
	fa, err := Parse(a)
	if err != nil {
		return Diff{}, err
	}
	fb, err := Parse(b)
	if err != nil {
		return Diff{}, err
	}
	old := make(map[string]File, len(fa))
	for _, f := range fa {
		old[f.Path] = f
	}
	var d Diff
	seen := map[string]bool{}
	for _, f := range fb {
		seen[f.Path] = true
		o, ok := old[f.Path]
		switch {
		case !ok:
			d.Added = append(d.Added, f.Path)
		case o.Bytes != f.Bytes || o.SHA256 != f.SHA256:
			d.Changed = append(d.Changed, f.Path)
		}
	}
	for _, f := range fa {
		if !seen[f.Path] {
			d.Removed = append(d.Removed, f.Path)
		}
	}
	return d, nil
}

// Unescape is a list's path as the file's: a list writes a backslash, tab,
// CR or LF in a path as a backslash and one of '\\', 't', 'r', 'n'.
func Unescape(p string) string {
	if !strings.Contains(p, `\`) {
		return p
	}
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		if p[i] != '\\' || i+1 == len(p) {
			b.WriteByte(p[i])
			continue
		}
		i++
		switch p[i] {
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		default:
			b.WriteByte(p[i])
		}
	}
	return b.String()
}
