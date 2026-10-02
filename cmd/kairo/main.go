package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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
	"kairo/internal/secfile"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

func main() {
	// A Windows force stop runs this binary as a short-lived CTRL_BREAK helper.
	executor.RunHelper(os.Args)
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
	case "agent":
		return agentCommand(args[1:])
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
	case "node":
		return nodeCommand(args[1:])
	case "token":
		return tokenCommand(args[1:])
	case "tls":
		return tlsCommand(args[1:])
	case "doctor":
		return doctorCommand(args[1:])
	default:
		return usage()
	}
}

func usage() error {
	return errors.New("usage: kairo <serve|agent|execution|project|queue|task|resource|node|token|tls|doctor>")
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
	// Anyone who can write the database can mint tokens: keep it (and its
	// WAL, which holds recent pages) to this user.
	if err := secfile.RestrictExisting(daemonConfig.DatabasePath, daemonConfig.DatabasePath+"-wal", daemonConfig.DatabasePath+"-shm"); err != nil {
		return fmt.Errorf("restrict database to its owner: %w", err)
	}
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
	handler := (&api.Server{Store: st, ObserveOnly: daemonConfig.ObserveOnly, LogDirectory: daemonConfig.LogDirectory}).Handler()
	server := &http.Server{
		Addr:              daemonConfig.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	// With TLS the daemon's CA goes to every attempt (KAIRO_API_CA) so its
	// processes can verify the daemon they report to.
	apiCA := ""
	if daemonConfig.TLS() {
		cert, err := tls.LoadX509KeyPair(daemonConfig.TLSCertFile, daemonConfig.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		server.TLSConfig = tlsutil.ServerConfig()
		server.TLSConfig.Certificates = []tls.Certificate{cert}
		caPEM, err := os.ReadFile(daemonConfig.TLSCAFile)
		if err != nil {
			return fmt.Errorf("read TLS CA: %w", err)
		}
		if apiCA, err = tlsutil.CAEnv(caPEM); err != nil {
			return fmt.Errorf("TLS CA %s: %w", daemonConfig.TLSCAFile, err)
		}
		// Attempts reach the daemon at advertise_url and trust only the CA:
		// a certificate that does not chain to it or cover that host would
		// leave the daemon healthy while every attempt fails.
		advertised, err := url.Parse(daemonConfig.AdvertiseURL)
		if err != nil {
			return err
		}
		if err := tlsutil.VerifyServer(cert, caPEM, advertised.Hostname()); err != nil {
			return fmt.Errorf("TLS certificate %s does not serve advertise_url %s with CA %s (kairo tls issue --host %s): %w", daemonConfig.TLSCertFile, daemonConfig.AdvertiseURL, daemonConfig.TLSCAFile, advertised.Hostname(), err)
		}
	}
	errCh := make(chan error, 1+len(daemonConfig.Executors))
	go func() {
		logger.Info("control plane listening", "address", daemonConfig.Listen, "tls", daemonConfig.TLS(), "database", daemonConfig.DatabasePath)
		serve := server.ListenAndServe
		if daemonConfig.TLS() {
			serve = func() error { return server.ListenAndServeTLS("", "") }
		}
		if err := serve(); !errors.Is(err, http.ErrServerClosed) {
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
	if !daemonConfig.ObserveOnly {
		// Node quarantine: suspend or terminate what runs on quarantined nodes.
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				if err := st.ReconcileNodeQuarantines(ctx); err != nil && ctx.Err() == nil {
					logger.Error("node quarantine reconciliation failed", "error", err)
				}
				// Gangs: abort placements not prepared in time, keep launched ranks to one fate.
				if err := st.ReconcileGangs(ctx); err != nil && ctx.Err() == nil {
					logger.Error("gang reconciliation failed", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if !daemonConfig.ObserveOnly {
		// Project declarations are reconciled above the V3 execution layer. The
		// reconciler only emits immutable requests; executors and leases remain
		// owned by the existing coordination machinery below it.
		go func() {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				if err := st.ReconcileProjectTasks(ctx); err != nil && ctx.Err() == nil {
					logger.Error("project orchestration reconciliation failed", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	for _, configured := range daemonConfig.Executors {
		if configured.Enabled != nil && !*configured.Enabled {
			continue
		}
		configured := configured
		exec := executor.NewLocal(configured.ID, st, daemonConfig.LogDirectory, logger)
		exec.APIURL = strings.TrimRight(daemonConfig.AdvertiseURL, "/")
		exec.APICA = apiCA
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
