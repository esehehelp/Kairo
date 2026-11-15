package main

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"kairo/internal/tlsutil"
)

// controlSnapshot: kairo control snapshot [--out DIR]. Inside an attempt of
// project kairo, queue control-snapshot (the daemon declares one every
// control_snapshot_minutes), it takes the snapshot with the attempt's token
// and writes it to the run's output directory, which Kairo packs and copies
// to every durable store; an operator takes one with an admin token. The tar
// written (kairo-control-<time>.tar) must hold the database's dump, or the
// command fails.
func controlSnapshot(args []string) error {
	fs := newFlags("control snapshot")
	api := apiFlag(fs)
	out := fs.String("out", "", "directory to write the snapshot to (default: $KAIRO_OUTPUT_DIR, else here)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("control snapshot takes no arguments")
	}
	attemptToken := os.Getenv("KAIRO_ATTEMPT_TOKEN")
	inAttempt := attemptToken != "" && os.Getenv("KAIRO_API_URL") != "" && *api == ""
	dir := *out
	if dir == "" {
		dir = os.Getenv("KAIRO_OUTPUT_DIR")
	}
	if dir == "" {
		if inAttempt {
			// written anywhere else, nothing would keep it
			return errors.New("this attempt has no output directory (KAIRO_OUTPUT_DIR): its executor serves no volatile store")
		}
		dir = "."
	}
	var base, token, path string
	var client *http.Client
	if inAttempt {
		var caPEM []byte
		if caEnv := os.Getenv("KAIRO_API_CA"); strings.TrimSpace(caEnv) != "" {
			if caPEM, err = tlsutil.CAFromEnv(caEnv); err != nil {
				return fmt.Errorf("KAIRO_API_CA: %w", err)
			}
		}
		tlsConfig, err := tlsutil.ClientConfig(caPEM)
		if err != nil {
			return fmt.Errorf("Kairo CA: %w", err)
		}
		base, token, path = strings.TrimRight(os.Getenv("KAIRO_API_URL"), "/"), attemptToken, "/api/worker/control-snapshot"
		client = &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig}}
	} else {
		c, err := newAPIClient(*api)
		if err != nil {
			return err
		}
		base, token, path = c.baseURL, c.token, "/api/control/snapshot"
		transport := c.http.Transport
		client = &http.Client{Transport: transport}
	}
	client.Timeout = time.Hour
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	target, err := url.JoinPath(base, path)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("control snapshot: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := filepath.Join(dir, "kairo-control-"+time.Now().UTC().Format("20060102T150405Z")+".tar")
	f, err := os.Create(name + ".partial")
	if err != nil {
		return err
	}
	n, err := io.Copy(f, resp.Body)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = checkControlSnapshot(name + ".partial")
	}
	if err != nil {
		_ = os.Remove(name + ".partial")
		return err
	}
	if err = os.Rename(name+".partial", name); err != nil {
		return err
	}
	fmt.Printf("%s (%d bytes)\n", name, n)
	return nil
}

// checkControlSnapshot reads the tar at p through: it must be whole and hold
// the database's dump.
func checkControlSnapshot(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	dump := int64(-1)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("the snapshot is cut short or damaged: %w", err)
		}
		if h.Name == "kairo.dump" {
			dump = h.Size
		}
		if _, err = io.Copy(io.Discard, tr); err != nil {
			return fmt.Errorf("the snapshot is cut short: %w", err)
		}
	}
	if dump <= 0 {
		return errors.New("the snapshot holds no database dump")
	}
	return nil
}

// pgDumpTo writes the dump of the database at dsn (pg_dump -Fc, the binary
// pgDump) to w; the password goes in the environment, not the arguments.
func pgDumpTo(ctx context.Context, pgDump, dsn string, w io.Writer) error {
	env := os.Environ()
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") && u.User != nil {
		if password, ok := u.User.Password(); ok {
			env = append(env, "PGPASSWORD="+password)
			u.User = url.User(u.User.Username())
			dsn = u.String()
		}
	}
	cmd := exec.CommandContext(ctx, pgDump, "-Fc", "--dbname="+dsn)
	cmd.Env, cmd.Stdout = env, w
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w: %s", pgDump, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
