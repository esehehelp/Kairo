package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"kairo/internal/api"
	"kairo/internal/config"
	"kairo/internal/executor"
	"kairo/internal/fabric"
	"kairo/internal/hwpolicy"
	"kairo/internal/secfile"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

func main() {
	// A Windows force stop runs this binary as a short-lived CTRL_BREAK helper.
	executor.RunHelper(os.Args)
	// The Windows agent's fabric work in its wslc VM (fabric_windows.go).
	if len(os.Args) > 1 && os.Args[1] == fabric.LinkArg {
		if err := fabric.LinkCommand(context.Background(), os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "kairo:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if err := run(os.Args[1:]); err != nil {
		reportError(os.Stderr, err)
		os.Exit(exitCode(err))
	}
}

// run runs one command; a command that printed its help returns nil.
func run(args []string) error {
	err := runCommand(args)
	if errors.Is(err, errHelpShown) {
		return nil
	}
	return err
}

func runCommand(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	if isHelpArg(args[0]) {
		return helpCommand(args[1:])
	}
	if c := findCommand(args[0]); c != nil {
		return c.run(args[1:])
	}
	return unknownCommand(nil, args[0])
}

// usage is the error for kairo without a command.
func usage() error {
	return &cliError{code: exitUsage, msg: topUsage(), raw: true}
}

// leafArgs handles "kairo CMD help" for a command without subcommands.
func leafArgs(args []string) []string {
	if len(args) > 0 && args[0] == "help" {
		return []string{"--help"}
	}
	return args
}

func serve(args []string) error {
	fs := newFlags("serve")
	configPath := fs.String("config", "", "daemon TOML configuration")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if *configPath == "" || len(positional) != 0 {
		return usageError("serve requires --config DAEMON.toml and no arguments")
	}
	daemonConfig, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	// Anyone who can write the database can mint tokens: keep its password
	// to this user.
	if err := secfile.RestrictExisting(daemonConfig.DatabasePasswordFile); err != nil {
		return fmt.Errorf("restrict the database password file to its owner: %w", err)
	}
	dsn, err := daemonConfig.Database()
	if err != nil {
		return err
	}
	st, err := store.Open(dsn)
	if err != nil {
		return fmt.Errorf("open the database: %w", err)
	}
	defer st.Close()
	if daemonConfig.Scheduler.DiskMinFreeBytes != nil {
		st.SetSchedulingPolicy(store.SchedulingPolicy{DiskMinFreeBytes: *daemonConfig.Scheduler.DiskMinFreeBytes})
	}
	scrubEvery, err := daemonConfig.Data.ScrubEvery()
	if err != nil {
		return err
	}
	confineFrom, err := daemonConfig.Data.ConfineTime()
	if err != nil {
		return err
	}
	runSnapshotEvery, err := daemonConfig.Data.RunSnapshotEvery()
	if err != nil {
		return err
	}
	st.SetDataPolicy(store.DataPolicy{ScrubInterval: scrubEvery, ScrubBytesPerDay: daemonConfig.Data.ScrubBytesPerDay, ConfineFrom: confineFrom, RunSnapshotInterval: runSnapshotEvery})
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// The external-command helper (internal/extcmd) and the gang reconciler log through the default.
	slog.SetDefault(logger)
	attributes, _ := json.Marshal(config.NodeAttributes(daemonConfig.Node.Labels))
	if err := st.UpsertNode(context.Background(), store.Node{ID: daemonConfig.Node.ID, Name: daemonConfig.Node.Name, OS: daemonConfig.Node.OS, Architecture: daemonConfig.Node.Architecture, Attributes: attributes, Enabled: true}); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// the daemon's own node on the fabric (issue #47), synced with the
	// store itself
	if f := daemonConfig.Fabric; f.Prefix != "" && f.Join {
		keyFile, iface, port := f.KeyFile, f.Interface, f.ListenPort
		if keyFile == "" {
			keyFile = filepath.Join(filepath.Dir(*configPath), "fabric.key")
		}
		if iface == "" {
			iface = fabric.DefaultInterface
		}
		if port == 0 {
			port = fabric.DefaultListenPort
		}
		fab := &fabric.Manager{Interface: iface, ListenPort: port, KeyFile: keyFile, Endpoints: f.Endpoints, Logger: logger,
			Sync: func(ctx context.Context, self store.FabricSelf) (store.FabricView, error) {
				return st.SyncFabric(ctx, daemonConfig.Node.ID, store.FabricPlan{Prefix: f.Prefix, ASN: f.ASN, VXLANPort: f.VXLANPort}, self)
			},
			Report: func(ctx context.Context, report store.FabricLinksReport) error {
				return st.RecordFabricLinks(ctx, daemonConfig.Node.ID, report)
			}}
		if _, port, err := net.SplitHostPort(daemonConfig.Listen); err == nil {
			fab.ProbePort, _ = strconv.Atoi(port)
		}
		if fab.RelayFrom, fab.RelayTo, err = config.ParseRelayPorts(f.RelayPorts); err != nil {
			return err
		}
		go fab.Run(ctx)
	}
	defer stop()
	pgDump := daemonConfig.PGDump
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	// what a control snapshot carries beside the dump (README: Kairo keeps
	// itself): never a secret. The TLS key and database_password_file are
	// left out, and so is the configuration when its database_url holds a
	// password (issue #11: the snapshot is copied to every durable store).
	controlFiles := []string{daemonConfig.TLSCertFile, daemonConfig.TLSCAFile}
	if config.HasPassword(daemonConfig.DatabaseURL) {
		logger.Warn("database_url holds a password: the configuration is left out of control snapshots (keep the password in database_password_file)")
	} else {
		controlFiles = append([]string{*configPath}, controlFiles...)
	}
	if daemonConfig.Fabric.Prefix != "" {
		if _, err := store.ParseFabricPrefix(daemonConfig.Fabric.Prefix); err != nil {
			return err
		}
	}
	handler := (&api.Server{Store: st, ObserveOnly: daemonConfig.ObserveOnly, LogDirectory: daemonConfig.LogDirectory, SourceDirectory: daemonConfig.SourceDirectory, Logger: logger,
		FabricPrefix: daemonConfig.Fabric.Prefix, FabricASN: daemonConfig.Fabric.ASN, FabricVXLANPort: daemonConfig.Fabric.VXLANPort,
		FabricSegmentPrefix: daemonConfig.Fabric.SegmentPrefix,
		DumpDatabase:        func(ctx context.Context, w io.Writer) error { return pgDumpTo(ctx, pgDump, dsn, w) }, ControlFiles: controlFiles}).Handler()
	server := &http.Server{
		Addr:              daemonConfig.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          tlsutil.ServerErrorLog(),
	}
	// With TLS the CA must be one attempts can be given (KAIRO_API_CA, by
	// their agents) to verify the daemon they report to.
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
		if _, err = tlsutil.CAEnv(caPEM); err != nil {
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
	errCh := make(chan error, 1)
	go func() {
		logger.Info("control plane listening", "address", daemonConfig.Listen, "tls", daemonConfig.TLS(), "database", config.RedactDatabase(dsn))
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
	// Resource observations describe the present; only each resource's
	// latest one is ever read. Device samples are kept for a few days. Prune
	// the rest periodically (observe-only daemons record both too).
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			removed, err := st.PruneObservations(ctx, store.ObservationRetention)
			if err != nil && ctx.Err() == nil {
				logger.Error("observation pruning failed", "error", err)
			} else if removed > 0 {
				logger.Info("pruned resource observations", "rows", removed)
			}
			removed, err = st.PruneDeviceSamples(ctx, store.DeviceSampleRetention)
			if err != nil && ctx.Err() == nil {
				logger.Error("device sample pruning failed", "error", err)
			} else if removed > 0 {
				logger.Info("pruned device samples", "rows", removed)
			}
			removed, err = st.PruneHistory(ctx, store.HistoryRetention)
			if err != nil && ctx.Err() == nil {
				logger.Error("resource history pruning failed", "error", err)
			} else if removed > 0 {
				logger.Info("pruned resource history", "rows", removed)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	checkpointEvery, err := daemonConfig.Data.CheckpointEvery()
	if err != nil {
		return err
	}
	if checkpointEvery > 0 && !daemonConfig.ObserveOnly {
		// Checkpoints of running work: a running checkpointable attempt is
		// asked for one now and then; what it publishes is kept
		// (api.protectCheckpoint).
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				commands, err := st.RequestDueCheckpoints(ctx, checkpointEvery)
				if err != nil && ctx.Err() == nil {
					logger.Error("checkpoints not requested", "error", err)
				} else if len(commands) > 0 {
					logger.Info("checkpoints requested to keep running work", "commands", len(commands), "every", checkpointEvery)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if every := daemonConfig.ControlSnapshotEvery(); every > 0 && !daemonConfig.ObserveOnly {
		// Kairo keeps itself: a control-snapshot task every interval, its
		// output copied to every durable store like any other.
		go func() {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				name, err := st.ControlSnapshotTask(ctx, time.Now(), every)
				if err != nil && ctx.Err() == nil {
					logger.Error("control snapshot not declared", "error", err)
				} else if name != "" {
					logger.Info("control snapshot declared", "project", store.ControlProject, "task", name)
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
		// Node quarantine: suspend or terminate what runs on quarantined nodes.
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				if err := st.ReconcileNodeQuarantines(ctx); err != nil && ctx.Err() == nil {
					logger.Error("node quarantine reconciliation failed", "error", err)
				}
				// its owner's use of a resource: the work there that can
				// checkpoint gives way (README: Owner use)
				if n, err := st.ReconcileOwnerUse(ctx); err != nil && ctx.Err() == nil {
					logger.Error("owner use reconciliation failed", "error", err)
				} else if n > 0 {
					logger.Info("work suspended for its owner's use of a resource", "suspends", n)
				}
				// a node short of RAM for a while: its work that can
				// checkpoint gives it back, one attempt at a time
				if n, err := st.ReconcileMemoryPressure(ctx); err != nil && ctx.Err() == nil {
					logger.Error("memory pressure reconciliation failed", "error", err)
				} else if n > 0 {
					logger.Info("work suspended for its node's memory pressure", "suspends", n)
				}
				// gangs: a placement not prepared in time is given up, the
				// members of a launched one share one fate, and the segment
				// of a generation that ended is taken away
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
		// Release stale leases that cannot still have processes once their
		// resources are observed again.
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				if n, err := st.ReconcileSafeStaleLeases(ctx); err != nil && ctx.Err() == nil {
					logger.Error("stale lease reconciliation failed", "error", err)
				} else if n > 0 {
					logger.Info("stale leases released without confirmation", "count", n)
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
		// The operating-point policy asks for device settings within each
		// device's max_slowdown, and ends bench runs whose execution ended.
		go (&hwpolicy.Policy{Store: st, Logger: logger}).Run(ctx)
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
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
