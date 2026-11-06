package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/config"
	"kairo/internal/executor"
	"kairo/internal/provider"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

// agentCommand runs this host as a remote node: its providers observe local
// GPUs/CPU/RAM/disk and its executors launch attempts here, while every
// coordination decision (reservation, fencing, quiescence, release) is made by
// the daemon's store through its /api/agent operations. Attempt logs are written
// locally and shipped into the daemon's log directory.
func agentCommand(args []string) error {
	fs := newFlags("agent")
	configPath := fs.String("config", "", "agent TOML configuration")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if *configPath == "" || len(positional) != 0 {
		return usageError("agent requires --config AGENT.toml and no arguments")
	}
	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		return fmt.Errorf("load agent config: %w", err)
	}
	if err = os.MkdirAll(cfg.LogDirectory, 0o755); err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// The external-command helper (internal/extcmd) logs through the default.
	slog.SetDefault(logger)
	var caPEM []byte
	apiCA := ""
	if cfg.CAFile != "" {
		if caPEM, err = os.ReadFile(cfg.CAFile); err != nil {
			return fmt.Errorf("read ca_file: %w", err)
		}
		if apiCA, err = tlsutil.CAEnv(caPEM); err != nil {
			return fmt.Errorf("ca_file %s: %w", cfg.CAFile, err)
		}
	}
	tlsConfig, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		return err
	}
	client := agentclient.New(cfg.ServerURL, cfg.Token, tlsConfig)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	providerSet := map[string]provider.Provider{}
	registered := make([]agentclient.Provider, 0, len(cfg.Providers))
	for _, p := range cfg.Providers {
		ttl := time.Duration(p.ObservationTTLSeconds) * time.Second
		switch p.Kind {
		case "nvidia":
			providerSet[p.ID] = &provider.NVIDIA{ProviderID: p.ID, NodeID: cfg.Node.ID, TTL: ttl}
		case "host":
			providerSet[p.ID] = &provider.Host{ProviderID: p.ID, NodeID: cfg.Node.ID, TTL: ttl}
		case "filesystem":
			providerSet[p.ID] = &provider.Filesystem{ProviderID: p.ID, NodeID: cfg.Node.ID, Filesystems: p.Filesystems, TTL: ttl}
		default:
			return fmt.Errorf("unsupported provider kind %q", p.Kind)
		}
		registered = append(registered, agentclient.Provider{ID: p.ID, Kind: p.Kind})
	}
	executors := make([]store.Executor, 0, len(cfg.Executors))
	for _, e := range cfg.Executors {
		labels := e.Labels
		// Executors outside a WSL distro advertise this agent's pack server:
		// other agents fetch from it the packs they make.
		if cfg.Packs.Addr != "" && e.Kind != "wsl2" && e.Labels["environment"] != "wsl2" {
			labels = map[string]string{"pack_addr": cfg.Packs.Addr}
			for k, v := range e.Labels {
				labels[k] = v
			}
		}
		attrs, _ := json.Marshal(labels)
		executors = append(executors, store.Executor{ID: e.ID, NodeID: cfg.Node.ID, Kind: e.Kind, Attributes: attrs, Enabled: e.Enabled == nil || *e.Enabled})
	}
	nodeAttrs, _ := json.Marshal(cfg.Bench.Attributes(cfg.Node.Labels))
	node := store.Node{ID: cfg.Node.ID, Name: cfg.Node.Name, OS: cfg.Node.OS, Architecture: cfg.Node.Architecture, Attributes: nodeAttrs, Enabled: true}

	// Registration is retried: the daemon (or the tunnel to it) may come up
	// after the agent.
	for delay := time.Second; ; delay = min(2*delay, 30*time.Second) {
		stale, regErr := client.Register(ctx, node, executors, registered)
		if regErr == nil {
			for id, n := range stale {
				if n > 0 {
					logger.Warn("executor has stale leases pending reconciliation", "executor_id", id, "count", n)
				}
			}
			break
		}
		logger.Warn("agent registration failed; retrying", "server", cfg.ServerURL, "error", regErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	logger.Info("agent registered", "node_id", cfg.Node.ID, "server", cfg.ServerURL, "executors", len(executors))

	errCh := make(chan error, 2+len(cfg.Executors))
	shipper := &agentclient.LogShipper{Client: client, Dir: cfg.LogDirectory}
	go func() {
		if err := shipper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	// Packs are fetched from other agents over HTTPS, trusting the daemon's CA.
	packClient := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig.Clone(), ResponseHeaderTimeout: time.Minute}}
	var running []*executor.Local
	for _, configured := range cfg.Executors {
		if configured.Enabled != nil && !*configured.Enabled {
			continue
		}
		exec := executor.NewLocal(configured.ID, client, cfg.LogDirectory, logger)
		exec.PackClient = packClient
		running = append(running, exec)
		exec.APIURL = cfg.AttemptAPIURL
		exec.APICA = apiCA
		exec.NodeID = cfg.Node.ID
		exec.Kind = configured.Kind
		exec.Attributes = configured.Labels
		exec.ObserveOnly = cfg.ObserveOnly
		exec.ObservationInterval = time.Duration(cfg.ObservationIntervalSeconds) * time.Second
		exec.MinLogFreeBytes = int64(cfg.LogMinFreeMiB) << 20
		exec.FlushLogs = shipper.ShipAttempt
		exec.Providers = providerSet
		go func() {
			if err := exec.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	if cfg.Packs.Listen != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Packs.CertFile, cfg.Packs.KeyFile)
		if err != nil {
			return fmt.Errorf("packs certificate: %w", err)
		}
		serverTLS := tlsutil.ServerConfig()
		serverTLS.Certificates = []tls.Certificate{cert}
		server := &http.Server{Addr: cfg.Packs.Listen, TLSConfig: serverTLS, ReadHeaderTimeout: 30 * time.Second,
			Handler: &executor.PackServer{Store: client, NodeID: cfg.Node.ID, Executors: running, Logger: logger}}
		go func() {
			if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("pack server: %w", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		logger.Info("pack server listening", "listen", cfg.Packs.Listen, "addr", cfg.Packs.Addr)
	}
	select {
	case <-ctx.Done():
		shipper.Sweep(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}
