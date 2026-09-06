package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"kairo/internal/api"
	"kairo/internal/config"
	"kairo/internal/executor"
	"kairo/internal/provider"
	"kairo/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "kairo:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "execution":
		return executionCommand(args[1:])
	case "project":
		return projectCommand(args[1:])
	case "queue":
		return queueCommand(args[1:])
	case "task":
		return taskCommand(args[1:])
	case "resource":
		return resourceCommand(args[1:])
	case "doctor":
		return doctorCommand(args[1:])
	default:
		return usage()
	}
}

func usage() error {
	return errors.New("usage: kairo <serve|execution|project|queue|task|resource|doctor>")
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "", "daemon TOML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("serve requires --config")
	}
	daemonConfig, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if dir := filepath.Dir(daemonConfig.DatabasePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	st, err := store.Open(daemonConfig.DatabasePath)
	if err != nil {
		return err
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	providerSet := map[string]provider.Provider{}
	attributes, _ := json.Marshal(daemonConfig.Node.Labels)
	if err := st.UpsertNode(context.Background(), store.Node{ID: daemonConfig.Node.ID, Name: daemonConfig.Node.Name, OS: daemonConfig.Node.OS, Architecture: daemonConfig.Node.Architecture, Attributes: attributes, Enabled: true}); err != nil {
		return err
	}
	for _, p := range daemonConfig.Providers {
		if err := st.UpsertProvider(context.Background(), p.ID, daemonConfig.Node.ID, p.Kind, nil); err != nil {
			return err
		}
		ttl := time.Duration(p.ObservationTTLSeconds) * time.Second
		switch p.Kind {
		case "nvidia":
			providerSet[p.ID] = &provider.NVIDIA{ProviderID: p.ID, NodeID: daemonConfig.Node.ID, TTL: ttl}
		case "host":
			providerSet[p.ID] = &provider.Host{ProviderID: p.ID, NodeID: daemonConfig.Node.ID, TTL: ttl}
		case "filesystem":
			providerSet[p.ID] = &provider.Filesystem{ProviderID: p.ID, NodeID: daemonConfig.Node.ID, Filesystems: p.Filesystems, TTL: ttl}
		default:
			return fmt.Errorf("unsupported provider kind %q", p.Kind)
		}
	}
	for _, e := range daemonConfig.Executors {
		enabled := e.Enabled == nil || *e.Enabled
		attrs, _ := json.Marshal(e.Labels)
		if err := st.UpsertExecutor(context.Background(), store.Executor{ID: e.ID, NodeID: daemonConfig.Node.ID, Kind: e.Kind, Attributes: attrs, Enabled: enabled}); err != nil {
			return err
		}
		if enabled {
			stale, err := st.MarkExecutorUnknown(context.Background(), e.ID)
			if err != nil {
				return fmt.Errorf("startup reconciliation: %w", err)
			}
			if stale > 0 {
				logger.Warn("executor has stale leases pending reconciliation", "executor_id", e.ID, "count", stale)
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr:              daemonConfig.Listen,
		Handler:           (&api.Server{Store: st, Logger: logger, ObserveOnly: daemonConfig.ObserveOnly}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1+len(daemonConfig.Executors))
	go func() {
		logger.Info("control plane listening", "address", daemonConfig.Listen, "database", daemonConfig.DatabasePath)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	// Scope control is reconciled independently from process execution. This
	// keeps a durable pause progressing even when no executor is currently able
	// to reserve work; observe-only mode records explicit actuation blockers.
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := st.ReconcilePauseOperations(ctx, !daemonConfig.ObserveOnly); err != nil && ctx.Err() == nil {
				logger.Error("pause reconciliation failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	for _, configured := range daemonConfig.Executors {
		if configured.Enabled != nil && !*configured.Enabled {
			continue
		}
		configured := configured
		exec := executor.NewLocal(configured.ID, st, daemonConfig.LogDirectory, logger)
		exec.APIURL = strings.TrimRight(daemonConfig.AdvertiseURL, "/")
		exec.NodeID = daemonConfig.Node.ID
		exec.Kind = configured.Kind
		exec.Attributes = configured.Labels
		exec.ObserveOnly = daemonConfig.ObserveOnly
		exec.ObservationInterval = time.Duration(daemonConfig.ObservationIntervalSeconds) * time.Second
		exec.Providers = providerSet
		go func() {
			if err := exec.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func apiFlag(fs *flag.FlagSet) *string {
	return fs.String("api", "http://127.0.0.1:7474", "Kairo API URL")
}

func request(apiURL, method, path string, body any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, strings.TrimRight(apiURL, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	_, _ = os.Stdout.Write(payload)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	return nil
}
