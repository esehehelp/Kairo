package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/config"
	"kairo/internal/executor"
	"kairo/internal/provider"
	"kairo/internal/store"
)

// agentCommand runs this host as a remote node: its providers observe local
// GPUs/CPU/RAM/disk and its executors launch attempts here, while every
// coordination decision (reservation, fencing, quiescence, release) is made by
// the daemon's store through the /v3/agent API. Attempt logs are written
// locally and shipped into the daemon's log directory.
func agentCommand(args []string) error {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	configPath := fs.String("config", "", "agent TOML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("agent requires --config")
	}
	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		return fmt.Errorf("load agent config: %w", err)
	}
	if err = os.MkdirAll(cfg.LogDirectory, 0o755); err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	client := agentclient.New(cfg.ServerURL, cfg.Token)
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
		attrs, _ := json.Marshal(e.Labels)
		executors = append(executors, store.Executor{ID: e.ID, NodeID: cfg.Node.ID, Kind: e.Kind, Attributes: attrs, Enabled: e.Enabled == nil || *e.Enabled})
	}
	nodeAttrs, _ := json.Marshal(cfg.Node.Labels)
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

	errCh := make(chan error, 1+len(cfg.Executors))
	shipper := &agentclient.LogShipper{Client: client, Dir: cfg.LogDirectory}
	go func() {
		if err := shipper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	for _, configured := range cfg.Executors {
		if configured.Enabled != nil && !*configured.Enabled {
			continue
		}
		exec := executor.NewLocal(configured.ID, client, cfg.LogDirectory, logger)
		exec.APIURL = cfg.AttemptAPIURL
		exec.NodeID = cfg.Node.ID
		exec.Kind = configured.Kind
		exec.Attributes = configured.Labels
		exec.ObserveOnly = cfg.ObserveOnly
		exec.ObservationInterval = time.Duration(cfg.ObservationIntervalSeconds) * time.Second
		exec.Providers = providerSet
		go func() {
			if err := exec.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	select {
	case <-ctx.Done():
		shipper.Sweep(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}
