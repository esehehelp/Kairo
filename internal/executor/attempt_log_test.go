package executor

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestOpenAttemptLogKeepsContentAndWritesAfterIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.stdout.log")
	if err := os.WriteFile(path, []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := openAttemptLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("after\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if b, _ := os.ReadFile(path); string(b) != "before\nafter\n" {
		t.Fatalf("log holds %q", b)
	}
}

// Git Bash (MSYS) cannot write to a handle opened with append-only access,
// which is what O_APPEND gives on Windows.
func TestOpenAttemptLogTakesGitBashOutput(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows only")
	}
	bash := `C:\Program Files\Git\bin\bash.exe`
	if _, err := os.Stat(bash); err != nil {
		t.Skip("Git Bash not installed")
	}
	path := filepath.Join(t.TempDir(), "a.stdout.log")
	f, err := openAttemptLog(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bash, "-c", "echo from-bash")
	cmd.Stdout, cmd.Stderr = f, f
	err = cmd.Run()
	f.Close()
	if b, _ := os.ReadFile(path); err != nil || string(b) != "from-bash\n" {
		t.Fatalf("bash: %v, log holds %q", err, b)
	}
}
