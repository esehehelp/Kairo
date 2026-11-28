package orchestration

import (
	"strings"
	"testing"
)

const streamManifest = `schema_version = 1
[project]
name = "neo"
[images.py]
ref = "python:3.12-slim"
[raws.wiki]
urls = ["https://dumps.example.org/jawiki/20261101/jawiki-pages.xml.bz2", "https://dumps.example.org/jawiki/20261101/abstract.xml.gz"]
[raws.web]
hf = { repo = "someone/web-ja", revision = "0123456789abcdef0123456789abcdef01234567", files = ["data/*.parquet"] }
[streams.mix]
sources = [ { raw = "wiki", weight = 3, format = "text.bz2" }, { raw = "web", format = "jsonl", field = "text" } ]
shuffle = { seed = 7, block = 1024, files = true }
stages = [ { argv = ["bin/norm"], workers = 4 } ]
memory_bytes = 1073741824
[[tasks]]
name = "train"
queue = "q"
depends_on = []
policy = "RunToCompletion@v1"
[tasks.execution]
schema_version = 2
argv = ["python", "-m", "train"]
cwd = "/work"
image = "py"
streams = ["mix"]
`

func webListing() map[string][]RawFile {
	h := HFRaw{Repo: "someone/web-ja", Revision: "0123456789abcdef0123456789abcdef01234567", Files: []string{"data/*.parquet"}}
	return map[string][]RawFile{"web": {
		{Name: "data/a.parquet", URL: HFFileURL(h, "data/a.parquet"), Size: 10, SHA256: strings.Repeat("a", 64), SizeKnown: true},
		{Name: "data/b.parquet", URL: HFFileURL(h, "data/b.parquet"), Size: 20, SizeKnown: true},
	}}
}

// A task reads declared streams: each with its raws' files (URLs, or the
// Hugging Face listing the apply sent) in the task, and so in its digest;
// what is not
// declared, not pinned to a commit or not in a container is refused.
func TestATaskReadsDeclaredStreams(t *testing.T) {
	if _, err := Parse([]byte(streamManifest)); err == nil || !strings.Contains(err.Error(), "sent no listing") {
		t.Fatalf("a Hugging Face raw without its listing: %v", err)
	}
	parsed, err := parseWithListing(streamManifest, webListing())
	if err != nil {
		t.Fatal(err)
	}
	ex := parsed.Manifest.Tasks[0].Execution
	if len(ex.StreamDecls) != 1 {
		t.Fatalf("decls %+v", ex.StreamDecls)
	}
	d := ex.StreamDecls[0]
	if d.Name != "mix" || *d.Stream.Epochs != 1 || d.Stream.Sources[1].Weight != 1 || len(d.Files["wiki"]) != 2 || d.Files["wiki"][0].Name != "jawiki-pages.xml.bz2" ||
		len(d.Files["web"]) != 2 || d.Files["web"][0].URL != "https://huggingface.co/datasets/someone/web-ja/resolve/0123456789abcdef0123456789abcdef01234567/data/a.parquet" {
		t.Fatalf("decl %+v", d)
	}
	other := webListing()
	other["web"] = other["web"][:1]
	again, err := parseWithListing(streamManifest, other)
	if err != nil || again.TaskDigests["train"] == parsed.TaskDigests["train"] {
		t.Fatalf("another listing, the same task digest %s: %v", again.TaskDigests["train"], err)
	}
	for _, bad := range []struct{ from, to, want string }{
		{`streams = ["mix"]`, `streams = ["other"]`, "which is no [streams.other]"},
		{`{ raw = "wiki"`, `{ raw = "none"`, "which is no [raws.none]"},
		{`revision = "0123456789abcdef0123456789abcdef01234567"`, `revision = "main"`, "40-hex commit"},
		{`format = "text.bz2"`, `format = "xml"`, "the framing is one of"},
		{`format = "text.bz2"`, `format = "parquet.zst"`, "no compression around them"},
		{`format = "text.bz2"`, `format = "text", field = "x"`, "a field is of jsonl, csv"},
		{`image = "py"`, `source = "x"`, "which is no [sources.x]"},
		{"image = \"py\"\n", "", "runs in the task's container"},
		{`streams = ["mix"]`, "streams = [\"mix\"]\n[tasks.execution.capacity]\nram_bytes = 9223372036854000000", "add up past what is counted"},
		{`"https://dumps.example.org/jawiki/20261101/abstract.xml.gz"`, `"https://dumps.example.org/other/jawiki-pages.xml.bz2"`, "two files named"},
		{`urls = [`, `hf = { repo = "a/b", revision = "0123456789abcdef0123456789abcdef01234567", files = ["*"] }
urls = [`, "urls or hf"},
	} {
		text := strings.Replace(streamManifest, bad.from, bad.to, 1)
		if text == streamManifest {
			t.Fatalf("%q not in the manifest", bad.from)
		}
		if _, err := parseWithListing(text, webListing()); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Fatalf("%s -> %s: %v, want %q", bad.from, bad.to, err, bad.want)
		}
	}
	forged := webListing()
	forged["web"][0].URL = "https://elsewhere.example.org/a.parquet"
	if _, err := parseWithListing(streamManifest, forged); err == nil || !strings.Contains(err.Error(), "is not one of") {
		t.Fatalf("a listing of another place: %v", err)
	}
}

func parseWithListing(text string, listing map[string][]RawFile) (Validated, error) {
	v, err := Parse([]byte(text))
	if err == nil || !strings.Contains(err.Error(), "sent no listing") {
		return v, err
	}
	m, err := Decode([]byte(text))
	if err != nil {
		return Validated{}, err
	}
	m.RawFiles = listing
	return Normalize(m)
}
