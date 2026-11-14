package sourcesnap

import (
	"strings"
	"testing"
)

// A snapshot's id is its files' (path, size, sha256): the same files packed
// at another time (other mtimes) make the same snapshot, any other file
// another one.
func TestASnapshotIsNamedByItsFiles(t *testing.T) {
	a := ListHeader + "\nsrc/a.py\t3\t" + strings.Repeat("ab", 32) + "\t1\nsrc/b.py\t4\t" + strings.Repeat("cd", 32) + "\t1\n"
	later := strings.ReplaceAll(a, "\t1\n", "\t99\n")
	changed := strings.Replace(a, "\t4\t", "\t5\t", 1)
	ida, err := ID(a)
	if err != nil || !ValidID(ida) {
		t.Fatalf("id %q %v", ida, err)
	}
	if idl, _ := ID(later); idl != ida {
		t.Fatalf("mtimes changed the id: %s %s", idl, ida)
	}
	if idc, _ := ID(changed); idc == ida {
		t.Fatal("another file, the same id")
	}
	if _, err := ID("not a list"); err == nil {
		t.Fatal("a list without its header")
	}
	b := ListHeader + "\nsrc/b.py\t5\t" + strings.Repeat("cd", 32) + "\t1\nsrc/c.py\t1\t" + strings.Repeat("ef", 32) + "\t1\n"
	d, err := Compare(a, b)
	if err != nil || strings.Join(d.Added, ",") != "src/c.py" || strings.Join(d.Removed, ",") != "src/a.py" || strings.Join(d.Changed, ",") != "src/b.py" {
		t.Fatalf("diff %+v %v", d, err)
	}
	if got := Unescape(`a\tb\\c`); got != "a\tb\\c" {
		t.Fatalf("unescape %q", got)
	}
}
