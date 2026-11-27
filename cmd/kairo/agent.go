package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/config"
	"kairo/internal/executor"
	"kairo/internal/fabric"
	"kairo/internal/provider"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

// An agent's executors bring code snapshots and clean them up through the
// daemon (executor/source.go).
var _ interface {
	executor.SourceFetcher
	executor.SourceKeeper
} = (*agentclient.Client)(nil)

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
	// the owner's use of this node's resources (README: Owner use)
	o := cfg.OwnerUse
	owner := &provider.OwnerUse{Disabled: o.Disabled, GPUSMPercent: o.GPUSMPercent, GPUSMPercentPresent: o.GPUSMPercentPresent, CPUPercent: o.CPUPercent,
		CPUPercentPresent: o.CPUPercentPresent, RAMFreeBytes: o.RAMFreeMiB << 20, RAMFreeBytesPresent: o.RAMFreeMiBPresent << 20, InputFile: o.InputFile}
	for _, p := range cfg.Providers {
		ttl := time.Duration(p.ObservationTTLSeconds) * time.Second
		switch p.Kind {
		case "nvidia":
			providerSet[p.ID] = &provider.NVIDIA{ProviderID: p.ID, NodeID: cfg.Node.ID, TTL: ttl, Owner: owner}
		case "host":
			providerSet[p.ID] = &provider.Host{ProviderID: p.ID, NodeID: cfg.Node.ID, TTL: ttl, Owner: owner}
		case "filesystem":
			providerSet[p.ID] = &provider.Filesystem{ProviderID: p.ID, NodeID: cfg.Node.ID, Filesystems: p.Filesystems, TTL: ttl}
		default:
			return fmt.Errorf("unsupported provider kind %q", p.Kind)
		}
		registered = append(registered, agentclient.Provider{ID: p.ID, Kind: p.Kind})
	}
	// The fabric (issue #47): the node's fixed address there, known from a
	// run before (or from the daemon now, once the node is known to it),
	// names its transfer endpoint unless [packs] addr says otherwise.
	var fab *fabric.Manager
	packAddr := cfg.Packs.Addr
	if cfg.Fabric.Enabled {
		// off the fabric (first start, every peer moved): the daemon at its
		// bootstrap address
		var bootstrap *agentclient.Client
		if cfg.Fabric.BootstrapURL != "" {
			bootstrap = agentclient.New(cfg.Fabric.BootstrapURL, cfg.Token, tlsConfig)
		}
		fab = &fabric.Manager{Interface: cfg.Fabric.Interface, ListenPort: cfg.Fabric.ListenPort, KeyFile: cfg.Fabric.KeyFile, Endpoints: cfg.Fabric.Endpoints, Logger: logger,
			Sync: func(ctx context.Context, self store.FabricSelf) (store.FabricView, error) {
				view, err := client.SyncFabric(ctx, cfg.Node.ID, self)
				if err != nil && bootstrap != nil {
					if v, berr := bootstrap.SyncFabric(ctx, cfg.Node.ID, self); berr == nil {
						return v, nil
					}
				}
				return view, err
			},
			Report: func(ctx context.Context, report store.FabricLinksReport) error {
				return client.ReportFabricLinks(ctx, cfg.Node.ID, report)
			}}
		// peers time a connect to the transfer endpoint at the fabric address
		// (not on Windows: the endpoint is off the fabric there)
		if _, port, err := net.SplitHostPort(cfg.Packs.Listen); err == nil && fabric.HostOnFabric() {
			fab.ProbePort, _ = strconv.Atoi(port)
		}
		if fab.RelayFrom, fab.RelayTo, err = config.ParseRelayPorts(cfg.Fabric.RelayPorts); err != nil {
			return err
		}
		// FRR and the segments (issue #47 phase 2), FRR's container named as
		// this installation's containers are
		prefix := cfg.ContainerPrefix
		if prefix == "" {
			prefix = executor.DefaultContainerPrefix
		}
		fab.Routing, fab.NodeID, fab.Names, fab.RouterName = cfg.Fabric.FabricRouting(), cfg.Node.ID, cfg.Fabric.Names, prefix+"frr"
		// what fetches from other agents took measures the links
		executor.TransferObserved = fab.Received
		// back on the fabric as it was last, before the daemon is asked
		if err := fab.Restore(ctx); err != nil {
			logger.Warn("fabric not restored", "error", err)
		}
		address := fabric.KnownAddress(cfg.Fabric.KeyFile)
		if err := fab.Once(ctx); err != nil {
			logger.Warn("fabric not set up before registration (set up after it)", "error", err)
		} else {
			address = fab.Address()
		}
		// on Windows the fabric is in the wslc VM: the agent's transfer
		// endpoint is off it ([packs] addr)
		if packAddr == "" && address != "" && cfg.Packs.Listen != "" && fabric.HostOnFabric() {
			if _, port, err := net.SplitHostPort(cfg.Packs.Listen); err == nil {
				packAddr = net.JoinHostPort(address, port)
			}
		}
	}
	executors := make([]store.Executor, 0, len(cfg.Executors))
	for _, e := range cfg.Executors {
		labels, err := e.Attributes()
		if err != nil {
			return err
		}
		// Executors advertise this agent's pack server: other agents fetch
		// from it the packs they make.
		if packAddr != "" {
			if _, ok := labels["pack_addr"]; !ok {
				labels["pack_addr"] = packAddr
			}
		}
		attrs, _ := json.Marshal(labels)
		executors = append(executors, store.Executor{ID: e.ID, NodeID: cfg.Node.ID, Kind: e.Kind, Attributes: attrs, Enabled: e.Enabled == nil || *e.Enabled})
	}
	nodeAttrs, _ := json.Marshal(config.NodeAttributes(cfg.Node.Labels))
	node := store.Node{ID: cfg.Node.ID, Name: cfg.Node.Name, OS: cfg.Node.OS, Architecture: cfg.Node.Architecture, Attributes: nodeAttrs, Enabled: true}

	// The attempts an earlier run of this agent launched that still run (or
	// exited since, their exit written) are adopted, not lost.
	ours := map[string]bool{}
	for _, e := range executors {
		ours[e.ID] = e.Enabled
	}
	records := executor.AdoptableAttempts(cfg.LogDirectory, ours, logger)
	var adopted []string
	// Registration is retried: the daemon (or the tunnel to it) may come up
	// after the agent.
	for delay := time.Second; ; delay = min(2*delay, 30*time.Second) {
		stale, kept, regErr := client.RegisterAdopting(ctx, node, executors, registered, executor.Adopted(records))
		adopted = kept
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
	if fab != nil {
		// kept on the fabric from now on (its address, if given only now,
		// names the transfer endpoint from the next start)
		go fab.Run(ctx)
	}

	errCh := make(chan error, 2+len(cfg.Executors))
	shipper := &agentclient.LogShipper{Client: client, Dir: cfg.LogDirectory}
	go func() {
		if err := shipper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	// Packs are fetched from other agents over HTTPS (HTTP/1.1), trusting the
	// daemon's CA (executor.PackHTTPClient).
	packClient := executor.PackHTTPClient(tlsConfig)
	// this installation's containers (another's on this host are none of its)
	if cfg.ContainerPrefix != "" {
		if err := executor.ValidContainerPrefix(cfg.ContainerPrefix); err != nil {
			return err
		}
		executor.ContainerPrefix = cfg.ContainerPrefix
	}
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
		if fab != nil && fab.Routing {
			// a gang member's container is on its generation's segment
			exec.Segments = fab.EnsureSegment
		}
		exec.Adopt(records, adopted)
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
		server := executor.PackHTTPServer(cfg.Packs.Listen, cert, &executor.PackServer{Store: client, NodeID: cfg.Node.ID, Executors: running, Logger: logger, NoBench: cfg.ObserveOnly})
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
	// network benches this node measures, against other nodes' transfer
	// endpoints (trusted as transfers trust them: the CA file, or the
	// system's roots); an observe-only agent runs none
	if !cfg.ObserveOnly {
		go runNetBenches(ctx, client, cfg.Node.ID, cfg.CAFile, logger)
	}
	select {
	case <-ctx.Done():
		shipper.Sweep(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}
