package api

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"kairo/internal/buildinfo"
	"kairo/internal/store"
)

// The control plane snapshot (store/control_snapshot.go): a tar of
//   SNAPSHOT.txt   when, which daemon
//   kairo.dump     the database (DumpDatabase: pg_dump -Fc)
//   sources/...    the code snapshots (SourceDirectory)
//   config/...     ControlFiles (the daemon's configuration and TLS
//                  certificates; never its key or the database password), by base name
// handed to an attempt of project kairo, queue control-snapshot (`kairo
// control snapshot`, GET /api/worker/control-snapshot) or an admin (GET
// /api/control/snapshot).

// workerControlSnapshot: an attempt's, only one of a control-snapshot task.
func (s *Server) workerControlSnapshot(w http.ResponseWriter, r *http.Request) {
	p := principalOf(r)
	if p.Worker == nil {
		writeError(w, ErrForbidden)
		return
	}
	project, queue, err := s.Store.AttemptQueue(r.Context(), p.Worker.AttemptID)
	if err != nil {
		writeError(w, err)
		return
	}
	if project != store.ControlProject || queue != store.ControlQueue {
		// the token is a worker's; what it asks for is not there for it
		writeError(w, fmt.Errorf("%w: only an attempt of %s/%s takes the control snapshot", store.ErrNotFound, store.ControlProject, store.ControlQueue))
		return
	}
	s.controlSnapshot(w, r)
}

func (s *Server) controlSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.DumpDatabase == nil {
		writeError(w, errors.New("this daemon cannot dump its database (no pg_dump configured)"))
		return
	}
	dump, err := os.CreateTemp("", "kairo-control-*.dump")
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() {
		dump.Close()
		_ = os.Remove(dump.Name())
	}()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	if err = s.DumpDatabase(ctx, dump); err != nil {
		writeError(w, fmt.Errorf("dump the database: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	tw := tar.NewWriter(w)
	now := time.Now().UTC()
	note := fmt.Sprintf("kairo control snapshot 1\ntaken %s\ndaemon %s\n", now.Format(time.RFC3339), buildinfo.Version())
	if err = tw.WriteHeader(&tar.Header{Name: "SNAPSHOT.txt", Mode: 0o644, Size: int64(len(note)), ModTime: now, Format: tar.FormatPAX}); err == nil {
		_, err = io.WriteString(tw, note)
	}
	if err == nil {
		err = addTarFile(tw, dump.Name(), "kairo.dump")
	}
	if err == nil && s.SourceDirectory != "" {
		err = filepath.WalkDir(s.SourceDirectory, func(p string, d fs.DirEntry, err error) error {
			if err != nil || !d.Type().IsRegular() {
				return err
			}
			rel, err := filepath.Rel(s.SourceDirectory, p)
			if err != nil {
				return err
			}
			return addTarFile(tw, p, "sources/"+filepath.ToSlash(rel))
		})
	}
	for _, f := range s.ControlFiles {
		if err != nil {
			break
		}
		if f != "" {
			err = addTarFile(tw, f, "config/"+filepath.Base(f))
		}
	}
	if err == nil {
		err = tw.Close()
	}
	if err != nil {
		// the status is sent: the client finds the tar cut short
		logger := s.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("control snapshot not sent whole", "error", err)
	}
}

// addTarFile writes the regular file p into tw as name.
func addTarFile(tw *tar.Writer, p, name string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if err = tw.WriteHeader(&tar.Header{Name: name, Mode: int64(fi.Mode().Perm()), Size: fi.Size(), ModTime: fi.ModTime(), Format: tar.FormatPAX}); err != nil {
		return err
	}
	_, err = io.CopyN(tw, f, fi.Size())
	return err
}
