package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kairo/internal/tlsutil"
)

func TestTLSInitAndIssue(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	if err := tlsCommand([]string{"init", "--dir", dir, "--host", "127.0.0.1", "--host", "localhost"}); err != nil {
		t.Fatalf("tls init: %v", err)
	}
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, name := range []string{tlsutil.CACertFile, tlsutil.CAKeyFile, tlsutil.ServerCertFile, tlsutil.ServerKeyFile} {
		read(name)
	}
	ca, server := read(tlsutil.CACertFile), read(tlsutil.ServerCertFile)

	if err := tlsCommand([]string{"init", "--dir", dir, "--host", "127.0.0.1"}); err == nil {
		t.Fatal("tls init overwrote an existing CA")
	}
	if err := tlsCommand([]string{"issue", "--dir", dir, "--host", "127.0.0.1"}); err != nil {
		t.Fatalf("tls issue: %v", err)
	}
	if !bytes.Equal(ca, read(tlsutil.CACertFile)) {
		t.Fatal("tls issue changed ca.pem")
	}
	if bytes.Equal(server, read(tlsutil.ServerCertFile)) {
		t.Fatal("tls issue did not replace server.pem")
	}
}

func TestTLSCommandValidatesArguments(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		nil,
		{"rotate", "--dir", dir, "--host", "x"},
		{"init", "--host", "127.0.0.1"},
		{"init", "--dir", dir},
		{"issue", "--dir", dir, "--host", "127.0.0.1", "extra"},
	} {
		if err := tlsCommand(args); err == nil {
			t.Fatalf("tls %v was accepted", args)
		}
	}
	if err := run([]string{"tls"}); err == nil || err.Error() == usage().Error() {
		t.Fatalf("tls was not dispatched: %v", err)
	}
}
