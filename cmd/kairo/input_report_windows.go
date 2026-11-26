//go:build windows

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"kairo/internal/provider"
)

var (
	user32               = windows.NewLazySystemDLL("user32.dll")
	procGetLastInputInfo = user32.NewProc("GetLastInputInfo")
	procGetTickCount64   = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetTickCount64")
)

type lastInputInfo struct {
	Size uint32
	Time uint32
}

// inputReport writes, every few seconds, when the owner last used the
// keyboard or mouse in this session (GetLastInputInfo) to the file this
// host's agent reads (README: Owner use): the agent runs in session 0, where
// the owner's input is not seen. It runs in the owner's session, started at
// logon (ops/windows/install-input-report.ps1).
func inputReport(args []string) error {
	fs := newFlags("input-report")
	out := fs.String("out", provider.DefaultInputFile(), "the file to write (the agent's [owner_use] input_file)")
	every := fs.Duration("every", 5*time.Second, "how often")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("no --out and no LOCALAPPDATA")
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(*every)
	defer ticker.Stop()
	for {
		if last, ok := lastInput(); ok {
			_ = writeInputRecord(*out, last)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// lastInput is when this session last had keyboard or mouse input.
func lastInput() (time.Time, bool) {
	info := lastInputInfo{Size: uint32(unsafe.Sizeof(lastInputInfo{}))}
	if ok, _, _ := procGetLastInputInfo.Call(uintptr(unsafe.Pointer(&info))); ok == 0 {
		return time.Time{}, false
	}
	now, _, _ := procGetTickCount64.Call()
	// the input's tick is the low 32 bits of the tick count (it wraps every
	// 49.7 days): its age is the difference in those bits
	age := time.Duration(uint32(now)-info.Time) * time.Millisecond
	return time.Now().Add(-age), true
}

// writeInputRecord replaces the file with the record, whole.
func writeInputRecord(path string, last time.Time) error {
	body, err := json.Marshal(provider.InputRecord{LastInput: last.UTC().Format(time.RFC3339Nano), Written: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
