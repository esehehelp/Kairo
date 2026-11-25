package orchestration

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
)

// Raw is [raws.NAME]: raw data a stream reads (issue #43), fetched from where
// it is published while a task reads it; Kairo keeps its identity only (each
// file's sha256 and chunk hashes, made the first time it is read). Either
// URLs, one file each, or the files of a Hugging Face repository at a commit.
type Raw struct {
	URLs []string `toml:"urls" json:"urls,omitempty"`
	HF   *HFRaw   `toml:"hf" json:"hf,omitempty"`
}

// HFRaw is the files of a Hugging Face repository at a commit whose paths
// match Files (path.Match patterns, "**" not supported: one per directory
// level), resolved when the project is applied.
type HFRaw struct {
	Repo     string   `toml:"repo" json:"repo"`
	Revision string   `toml:"revision" json:"revision"`
	Files    []string `toml:"files" json:"files"`
	Type     string   `toml:"type" json:"type,omitempty"`
}

// RawFile is a file of a raw as an apply resolved it: its name (its path in
// the raw), where it is fetched from and, when published, its size and
// sha256 (a Hugging Face LFS file's).
type RawFile struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	// SizeKnown: Size was published (a listing's; 0 then means empty).
	SizeKnown bool `json:"size_known,omitempty"`
}

// Stream is [streams.NAME]: the records of raws in a fixed order for the
// ranks of a task's attempts (internal/loader).
type Stream struct {
	Sources []StreamSource `toml:"sources" json:"sources"`
	Shuffle StreamShuffle  `toml:"shuffle" json:"shuffle"`
	Stages  []StreamStage  `toml:"stages" json:"stages,omitempty"`
	// Epochs of the first source the stream lasts: default 1, 0 no end.
	Epochs *int64 `toml:"epochs" json:"epochs"`
	// MemoryBytes is added to the task's RAM: what the loader holds.
	MemoryBytes int64 `toml:"memory_bytes" json:"memory_bytes,omitempty"`
}

type StreamSource struct {
	Raw    string `toml:"raw" json:"raw"`
	Weight int    `toml:"weight" json:"weight"`
	Format string `toml:"format" json:"format"`
	Field  string `toml:"field" json:"field,omitempty"`
}

type StreamShuffle struct {
	Seed  uint64 `toml:"seed" json:"seed"`
	Block int    `toml:"block" json:"block,omitempty"`
	Files bool   `toml:"files" json:"files,omitempty"`
}

// StreamStage is a project process records go through, run in the task's
// image and source, from its cwd.
type StreamStage struct {
	Argv    []string `toml:"argv" json:"argv"`
	Workers int      `toml:"workers" json:"workers,omitempty"`
}

// StreamDecl is a stream a task reads, with the files of its raws as the
// apply resolved them: part of the task's digest, so a task reads what it
// was declared with, for good.
type StreamDecl struct {
	Name   string               `json:"name"`
	Stream Stream               `json:"stream"`
	Files  map[string][]RawFile `json:"files"`
}

var (
	streamName  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	hfRepo      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
	commitHash  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Hex   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	knownFormat = map[string]bool{"text": true, "jsonl": true, "lenbin": true, "file": true}
)

// StreamEnv is the environment variable of stream name: KAIRO_STREAM_ and
// the name upper-cased, anything not a letter or digit an underscore.
func StreamEnv(name string) string {
	return "KAIRO_STREAM_" + strings.ToUpper(notEnvChar.ReplaceAllString(name, "_"))
}

var notEnvChar = regexp.MustCompile(`[^A-Za-z0-9]`)

// HFFileURL is where a file of a Hugging Face repository is fetched from.
func HFFileURL(h HFRaw, name string) string {
	prefix := "https://huggingface.co/"
	if h.Type != "model" {
		prefix += "datasets/"
	}
	return prefix + h.Repo + "/resolve/" + h.Revision + "/" + (&url.URL{Path: name}).EscapedPath()
}

// normalizeStreams checks [raws] and [streams], resolves each raw's files
// (URLs, or the Hugging Face listing the apply sent in RawFiles) and puts in
// each task the streams it reads (StreamDecls).
func normalizeStreams(m *Manifest) error {
	files := map[string][]RawFile{}
	for name, raw := range m.Raws {
		if !streamName.MatchString(name) {
			return fmt.Errorf("raw name %q: a letter, then letters, digits, '_' or '-' (64 at most)", name)
		}
		switch {
		case len(raw.URLs) > 0 && raw.HF != nil, len(raw.URLs) == 0 && raw.HF == nil:
			return fmt.Errorf("raw %q: urls or hf, one of them", name)
		case raw.HF != nil:
			h := raw.HF
			if !hfRepo.MatchString(h.Repo) || !commitHash.MatchString(h.Revision) || len(h.Files) == 0 || h.Type != "" && h.Type != "dataset" && h.Type != "model" {
				return fmt.Errorf("raw %q: hf needs repo (owner/name), revision (a 40-hex commit: the same files for good), files (patterns) and type dataset or model", name)
			}
			for _, p := range h.Files {
				if _, err := path.Match(p, ""); err != nil {
					return fmt.Errorf("raw %q: file pattern %q: %w", name, p, err)
				}
			}
			resolved, ok := m.RawFiles[name]
			if !ok {
				return fmt.Errorf("raw %q: this apply sent no listing of %s at %s (apply with a kairo that resolves them)", name, h.Repo, h.Revision)
			}
			if len(resolved) == 0 {
				return fmt.Errorf("raw %q: no file of %s at %s matches %v", name, h.Repo, h.Revision, h.Files)
			}
			for i, f := range resolved {
				matches := false
				for _, p := range h.Files {
					ok, _ := path.Match(p, f.Name)
					matches = matches || ok
				}
				if !matches || f.URL != HFFileURL(*h, f.Name) || f.Size < 0 || !f.SizeKnown || f.SHA256 != "" && !sha256Hex.MatchString(f.SHA256) {
					return fmt.Errorf("raw %q: the listing's file %q is not one of %s at %s matching %v", name, f.Name, h.Repo, h.Revision, h.Files)
				}
				if i > 0 && resolved[i-1].Name >= f.Name {
					return fmt.Errorf("raw %q: the listing is not in order by name, each once (%q after %q)", name, f.Name, resolved[i-1].Name)
				}
			}
			files[name] = resolved
		default:
			if _, sent := m.RawFiles[name]; sent {
				return fmt.Errorf("raw %q: a listing sent for urls", name)
			}
			seen := map[string]bool{}
			for _, u := range raw.URLs {
				parsed, err := url.Parse(u)
				if err != nil {
					return fmt.Errorf("raw %q: %q is not an http(s) URL of a file", name, u)
				}
				base := path.Base(parsed.Path)
				if parsed.Scheme != "https" && parsed.Scheme != "http" || parsed.Host == "" || base == "/" || base == "." {
					return fmt.Errorf("raw %q: %q is not an http(s) URL of a file", name, u)
				}
				if seen[base] {
					return fmt.Errorf("raw %q: two files named %q (each URL's last path element names its file)", name, base)
				}
				seen[base] = true
				files[name] = append(files[name], RawFile{Name: base, URL: u})
			}
		}
	}
	for name := range m.RawFiles {
		if _, ok := m.Raws[name]; !ok {
			return fmt.Errorf("a listing of %q, which is no [raws.%s]", name, name)
		}
	}
	byURL := map[string]RawFile{}
	for name, fs := range files {
		for _, f := range fs {
			if other, ok := byURL[f.URL]; ok && (other.SHA256 != f.SHA256 || other.Size != f.Size || other.SizeKnown != f.SizeKnown) {
				return fmt.Errorf("raw %q: %s is published otherwise in another raw (one URL is one file)", name, f.URL)
			}
			byURL[f.URL] = f
		}
	}
	for name, s := range m.Streams {
		if !streamName.MatchString(name) {
			return fmt.Errorf("stream name %q: a letter, then letters, digits, '_' or '-' (64 at most)", name)
		}
		if len(s.Sources) == 0 {
			return fmt.Errorf("stream %q has no sources", name)
		}
		for i, src := range s.Sources {
			if _, ok := m.Raws[src.Raw]; !ok {
				return fmt.Errorf("stream %q source %d reads raw %q, which is no [raws.%s]", name, i, src.Raw, src.Raw)
			}
			framing, compression, _ := strings.Cut(src.Format, ".")
			if !knownFormat[framing] || compression != "" && compression != "zst" && compression != "gz" && compression != "bz2" || src.Field != "" && framing != "jsonl" {
				return fmt.Errorf("stream %q source %d: format %q (field %q): text, jsonl, lenbin or file, then .zst, .gz or .bz2 or nothing; a field for jsonl", name, i, src.Format, src.Field)
			}
			if src.Weight == 0 {
				s.Sources[i].Weight = 1
			}
			if s.Sources[i].Weight < 1 {
				return fmt.Errorf("stream %q source %d: weight %d", name, i, src.Weight)
			}
		}
		if s.Shuffle.Block < 0 || s.Shuffle.Block > 1<<24 {
			return fmt.Errorf("stream %q: shuffle block %d (0 to %d)", name, s.Shuffle.Block, 1<<24)
		}
		for i, st := range s.Stages {
			if len(st.Argv) == 0 || st.Workers < 0 || st.Workers > 256 {
				return fmt.Errorf("stream %q stage %d: an argv and 1 to 256 workers", name, i)
			}
		}
		if s.Epochs == nil {
			one := int64(1)
			s.Epochs = &one
		} else if *s.Epochs < 0 {
			return fmt.Errorf("stream %q: epochs %d", name, *s.Epochs)
		}
		if s.MemoryBytes < 0 {
			return fmt.Errorf("stream %q: memory_bytes %d", name, s.MemoryBytes)
		}
		m.Streams[name] = s
	}
	for i := range m.Tasks {
		ex := &m.Tasks[i].Execution
		ex.StreamDecls = nil
		names := append([]string(nil), ex.Streams...)
		sort.Strings(names)
		envs := map[string]string{}
		for j, name := range names {
			if other, taken := envs[StreamEnv(name)]; taken && other != name {
				return fmt.Errorf("task %q reads streams %q and %q, both found at %s: rename one", m.Tasks[i].Name, other, name, StreamEnv(name))
			}
			envs[StreamEnv(name)] = name
			s, ok := m.Streams[name]
			if !ok {
				return fmt.Errorf("task %q reads stream %q, which is no [streams.%s]", m.Tasks[i].Name, name, name)
			}
			if j > 0 && names[j-1] == name {
				return fmt.Errorf("task %q names stream %q twice", m.Tasks[i].Name, name)
			}
			if ex.Image == "" {
				return fmt.Errorf("task %q reads stream %q: a stream's loader runs in the task's container (an image)", m.Tasks[i].Name, name)
			}
			if ex.Gang != nil {
				return fmt.Errorf("task %q: a gang does not read streams yet", m.Tasks[i].Name)
			}
			decl := StreamDecl{Name: name, Stream: s, Files: map[string][]RawFile{}}
			for _, src := range s.Sources {
				decl.Files[src.Raw] = files[src.Raw]
			}
			ex.StreamDecls = append(ex.StreamDecls, decl)
		}
		ex.Streams = names
	}
	return nil
}
