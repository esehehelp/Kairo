package machine

import (
	"strings"
	"testing"
)

const stores = `
[[stores]]
id = "e-durable"
class = "durable"
node = "win"
root = "E:/kairo"
filesystem = "E:"
backend = "local-fs"
reserve_free_bytes = 214748364800

[[stores]]
id = "wsl-scratch"
class = "volatile"
node = "win"
executor = "ubuntu-wsl"
root = "/home/kairo/scratch-root"
filesystem = "F:"

[[stores]]
id = "pve0-pool"
class = "volatile"
node = "pve0-train"
root = "/srv/kairo"
filesystem = "/"
`

func TestStoresParse(t *testing.T) {
	spec, err := Parse(nodes + stores)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Stores) != 3 {
		t.Fatalf("stores: %+v", spec.Stores)
	}
	e := spec.Stores[0]
	if e.ID != "e-durable" || e.Class != StoreDurable || e.Node != "win" || e.Root != "E:/kairo" || e.Filesystem != "E:" || e.Backend != StoreBackendLocalFS || e.ReserveFreeBytes != 200<<30 {
		t.Fatalf("e-durable: %+v", e)
	}
	if w := spec.Stores[1]; w.Executor != "ubuntu-wsl" || w.Backend != "" || w.Normalized().Backend != StoreBackendLocalFS {
		t.Fatalf("wsl-scratch: %+v", w)
	}
	ids := spec.NodeIDs()
	if strings.Join(ids, ",") != "pve0-glm,pve0-train,win" {
		t.Fatalf("node ids %v", ids)
	}
}

func TestStoreValidation(t *testing.T) {
	store := func(fields string) string { return "[[stores]]\n" + fields }
	base := "id = \"s\"\nclass = \"volatile\"\nnode = \"win\"\nfilesystem = \"D:\"\n"
	for _, c := range []struct{ name, stores, want string }{
		{"no id", store("class = \"volatile\"\nnode = \"win\"\nroot = \"D:/k\"\nfilesystem = \"D:\"\n"), "needs an id"},
		{"twice", store(base+"root = \"D:/a\"\n") + store(base+"root = \"D:/b\"\n"), "store s is declared twice"},
		{"class", store("id = \"s\"\nclass = \"fast\"\nnode = \"win\"\nroot = \"D:/k\"\nfilesystem = \"D:\"\n"), `class "fast" is neither`},
		{"backend", store(base + "root = \"D:/k\"\nbackend = \"s3\"\n"), `backend "s3" is not supported`},
		{"no node", store("id = \"s\"\nclass = \"volatile\"\nroot = \"D:/k\"\nfilesystem = \"D:\"\n"), "needs a node"},
		{"relative root", store(base + "root = \"kairo\"\n"), "is not an absolute path"},
		{"drive without slash", store(base + "root = \"D:kairo\"\n"), "is not an absolute path"},
		{"dot dot", store(base + "root = \"D:/k/../x\"\n"), "contains .."},
		{"no filesystem", store("id = \"s\"\nclass = \"volatile\"\nnode = \"win\"\nroot = \"D:/k\"\n"), "needs a filesystem"},
		{"negative reserve", store(base + "root = \"D:/k\"\nreserve_free_bytes = -1\n"), "reserve_free_bytes is negative"},
		{"nested", store(base+"root = \"D:/k\"\n") + store("id = \"t\"\nclass = \"durable\"\nnode = \"win\"\nfilesystem = \"D:\"\nroot = \"d:\\\\K\\\\inner\"\n"), "nested roots"},
		{"unknown field", store(base + "root = \"D:/k\"\nsize = 3\n"), "unknown machine field"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(nodes + c.stores)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}
	// The same root in two namespaces (the node's and a WSL executor's) is
	// two places.
	if _, err := Parse(nodes + store(base+"root = \"/k\"\n") + store("id = \"t\"\nclass = \"volatile\"\nnode = \"win\"\nexecutor = \"ubuntu-wsl\"\nfilesystem = \"F:\"\nroot = \"/k\"\n")); err != nil {
		t.Fatalf("one root in two namespaces: %v", err)
	}
}

func TestStoreRootIsAbsolute(t *testing.T) {
	for root, want := range map[string]bool{
		"E:/kairo": true, `E:\kairo`: true, "/srv/kairo": true, "e:/": true,
		"kairo": false, "E:kairo": false, "": false, "1:/x": false, `\\server\share`: false,
	} {
		if got := StoreRootIsAbsolute(root); got != want {
			t.Errorf("%q: %v, want %v", root, got, want)
		}
	}
}
