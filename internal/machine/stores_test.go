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
		// One place spelled twice is refused, whatever executor either names.
		{"dot spelling", store(base+"root = \"D:/k\"\n") + durable("D:/./k//"), "nested roots"},
		{"case", store(base+"root = \"D:/k\"\n") + durable("d:/K"), "nested roots"},
		{"with and without executor", store(base+"root = \"/k\"\n") + wsl("/k/inner"), "nested roots"},
		{"WSL path to the drive", store(base+"root = \"D:/k\"\n") + wsl("/mnt/d/K/inner"), "nested roots"},
		{"drive holding a WSL path", wsl("/mnt/d/k/inner") + durable("D:/"), "nested roots"},
		{"trailing dot", store(base + "root = \"D:/k.\"\n"), "ending in a dot or a space"},
		{"trailing space", store(base + "root = \"D:/k /x\"\n"), "ending in a dot or a space"},
		{"short name", store(base + "root = \"D:/PROGRA~1/k\"\n"), "reads as another"},
		{"stream", store(base + "root = \"D:/k:stream\"\n"), "reads as another"},
		{"POSIX whitespace", store(base + "root = \"/srv/my scratch\"\n"), "whitespace or a backslash"},
		{"POSIX backslash", store(base + "root = \"/srv/a\\\\b\"\n"), "whitespace or a backslash"},
		{"control character", store(base + "root = \"D:/k\\u0001\"\n"), "control character"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(nodes + c.stores)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}
	// One root on two nodes is two places; two roots side by side are too.
	other := store("id = \"t\"\nclass = \"durable\"\nnode = \"pve0-train\"\nfilesystem = \"/\"\nroot = \"/k\"\n")
	if _, err := Parse(nodes + store(base+"root = \"/k\"\n") + other); err != nil {
		t.Fatalf("one root on two nodes: %v", err)
	}
	if _, err := Parse(nodes + store(base+"root = \"D:/k\"\n") + durable("D:/kept")); err != nil {
		t.Fatalf("roots side by side: %v", err)
	}
}

// durable is a second store, t, durable on node win at root; wsl one of
// executor ubuntu-wsl.
func durable(root string) string {
	return "[[stores]]\nid = \"t\"\nclass = \"durable\"\nnode = \"win\"\nfilesystem = \"D:\"\nroot = \"" + root + "\"\n"
}

func wsl(root string) string {
	return "[[stores]]\nid = \"w\"\nclass = \"volatile\"\nnode = \"win\"\nexecutor = \"ubuntu-wsl\"\nfilesystem = \"D:\"\nroot = \"" + root + "\"\n"
}

func TestStoreRootKey(t *testing.T) {
	for root, want := range map[string]string{
		"E:/data": "e:/data", `E:\Data\`: "e:/data", "e:/./data//x/": "e:/data/x", "E:/": "e:/", "E:": "e:/",
		"/mnt/e/Data": "e:/data", "/mnt/e": "e:/", "/mnt/ee/x": "/mnt/ee/x", "/srv//Kairo/./x/": "/srv/Kairo/x", "/": "/",
	} {
		if got := StoreRootKey(root); got != want {
			t.Errorf("%q: %q, want %q", root, got, want)
		}
	}
	for _, c := range []struct {
		p, root string
		want    bool
	}{
		{"E:/k", "E:/k", true}, {`e:\K\scratch\x`, "E:/k", true}, {"E:/kept", "E:/k", false}, {"E:/k", "E:/", true}, {"/srv/x", "/", true}, {"/mnt/e/k/a", "E:/k", true},
	} {
		if got := PathUnder(c.p, c.root); got != c.want {
			t.Errorf("PathUnder(%q, %q) = %v", c.p, c.root, got)
		}
	}
	for _, c := range []struct {
		p, root, id string
		want        bool
	}{
		{`E:\k\scratch\att_1`, "E:/k/", "att_1", true},
		{"/srv/k/scratch/att_1", "/srv/k", "att_1", true},
		{"/srv/k/scratch/att_1/x", "/srv/k", "att_1", false},
		{"/srv/k/scratch/../../durable", "/srv/k", "att_1", false},
		{"/srv/k/scratch", "/srv/k", "", false},
		{"/srv/k/scratch/..", "/srv/k", "..", false},
		{"/srv/durable/scratch/att_1", "/srv/k", "att_1", false},
	} {
		if got := IsScratchDir(c.p, c.root, c.id); got != c.want {
			t.Errorf("IsScratchDir(%q, %q, %q) = %v", c.p, c.root, c.id, got)
		}
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
