package api

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"kairo/internal/sourcesnap"
	"kairo/internal/store"
)

// testSourcePack is a snapshot of files (path to content): its pack and meta.
func testSourcePack(t *testing.T, files map[string]string, paths ...string) ([]byte, SourceUpload) {
	t.Helper()
	var pack bytes.Buffer
	zw, err := zstd.NewWriter(&pack)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	list := sourcesnap.ListHeader + "\n"
	var content int64
	for _, p := range paths {
		body := files[p]
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: p, Size: int64(len(body)), Mode: 0o644, Format: tar.FormatPAX}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(body))
		list += fmt.Sprintf("%s\t%d\t%s\t1\n", p, len(body), hex.EncodeToString(sum[:]))
		content += int64(len(body))
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := sourcesnap.ID(list)
	if err != nil {
		t.Fatal(err)
	}
	packSum, listSum := sha256.Sum256(pack.Bytes()), sha256.Sum256([]byte(list))
	return pack.Bytes(), SourceUpload{ID: id, List: list, ListSHA256: hex.EncodeToString(listSum[:]), PackBytes: int64(pack.Len()), PackSHA256: hex.EncodeToString(packSum[:]),
		Files: int64(len(paths)), ContentBytes: content, Project: "neo", SourceName: "code"}
}

func (d *testDaemon) sendSource(t *testing.T, token string, meta SourceUpload, pack []byte) (int, []byte) {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, _ := form.CreateFormField("meta")
	_ = json.NewEncoder(part).Encode(meta)
	part, _ = form.CreateFormFile("pack", meta.ID+".tar.zst")
	_, _ = part.Write(pack)
	_ = form.Close()
	req, err := http.NewRequest(http.MethodPost, d.server.URL+"/api/sources", &body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// A snapshot sent is kept once its pack holds just what its file list says
// (sent again: not read again); one whose pack does not is refused; it is
// listed and shown with its file list, and an agent fetches its pack and
// list.
func TestCodeSnapshotsAreSentCheckedAndFetched(t *testing.T) {
	d := newTestDaemon(t, false)
	files := map[string]string{"src/a.py": "print('a')\n", "pyproject.toml": "[project]\n"}
	pack, meta := testSourcePack(t, files, "pyproject.toml", "src/a.py")
	// a pack that holds another file than listed
	otherPack, _ := testSourcePack(t, map[string]string{"pyproject.toml": "[project]\n", "src/a.py": "print('b')\n"}, "pyproject.toml", "src/a.py")
	bad := meta
	sum := sha256.Sum256(otherPack)
	bad.PackBytes, bad.PackSHA256 = int64(len(otherPack)), hex.EncodeToString(sum[:])
	status, raw := d.sendSource(t, d.admin, bad, otherPack)
	expectError(t, "a pack not as listed", status, raw, http.StatusBadRequest, "bad_request")
	if status, raw = d.sendSource(t, d.admin, meta, pack); status != http.StatusCreated {
		t.Fatalf("send: %d %s", status, raw)
	}
	if status, raw = d.sendSource(t, d.admin, meta, pack); status != http.StatusOK || !strings.Contains(string(raw), `"stored":false`) {
		t.Fatalf("sent again: %d %s", status, raw)
	}
	var shown struct {
		Source store.SourceSnapshot `json:"source"`
	}
	status, raw = d.call(t, http.MethodGet, "/api/sources/"+meta.ID, d.admin, nil)
	if status != http.StatusOK || json.Unmarshal(raw, &shown) != nil || shown.Source.List != meta.List || shown.Source.Project != "" && shown.Source.Project != "neo" {
		t.Fatalf("show: %d %s", status, raw)
	}
	if status, raw = d.call(t, http.MethodGet, "/api/sources", d.admin, nil); status != http.StatusOK || !strings.Contains(string(raw), meta.ID) {
		t.Fatalf("list: %d %s", status, raw)
	}
	node := d.token(t, "agent", store.RoleNode, "node")
	for file, want := range map[string][]byte{"pack": pack, "list": []byte(meta.List)} {
		req, _ := http.NewRequest(http.MethodGet, d.server.URL+"/api/agent/sources/"+meta.ID+"/"+file, nil)
		req.Header.Set("Authorization", "Bearer "+node)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !bytes.Equal(got, want) {
			t.Fatalf("agent fetch of the %s: %d, %d bytes", file, resp.StatusCode, len(got))
		}
	}
	if status, _ := d.call(t, http.MethodGet, "/api/agent/sources/"+meta.ID+"/pack", d.admin, nil); status != http.StatusForbidden {
		t.Fatalf("an operator token fetched as an agent: %d", status)
	}
}
