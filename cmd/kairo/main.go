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
	case "plan":
		return planCommand(args[1:])
	case "queue":
		return queueCommand(args[1:])
	case "task":
		return taskCommand(args[1:])
	case "resource":
		return resourceCommand(args[1:])
	case "doctor":
		return doctorCommand(args[1:])
	case "resource-add":
		return resourceAdd(args[1:])
	case "resource-ready":
		return resourceReady(args[1:])
	case "submit":
		return submit(args[1:])
	case "status":
		return simpleRequest(args[1:], http.MethodGet, "/v1/state")
	case "pause":
		return workloadAction(args[1:], "pause")
	case "resume":
		return workloadAction(args[1:], "resume")
	default:
		return usage()
	}
}

func usage() error {
	return errors.New("usage: kairo <serve|plan|queue|task|resource|doctor>")
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "", "daemon TOML configuration")
	dbPath := fs.String("db", "local/kairo.db", "SQLite database path")
	listen := fs.String("listen", "127.0.0.1:7474", "HTTP listen address")
	advertise := fs.String("advertise", "http://127.0.0.1:7474", "URL passed to workers")
	logDir := fs.String("log-dir", "local/attempts", "attempt log directory")
	executorID := fs.String("executor-id", "local", "stable local executor identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var daemonConfig *config.Config
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		daemonConfig = &loaded
		*dbPath, *listen, *advertise, *logDir = loaded.DatabasePath, loaded.Listen, loaded.AdvertiseURL, loaded.LogDirectory
	}
	if dir := filepath.Dir(*dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	providerSet := map[string]provider.Provider{}
	if daemonConfig != nil {
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
	} else {
		quarantined, err := st.QuarantineUnreconciled(context.Background(), *executorID)
		if err != nil {
			return fmt.Errorf("startup reconciliation: %w", err)
		}
		if quarantined > 0 {
			logger.Warn("quarantined resources from unreconciled attempts", "count", quarantined)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr:              *listen,
		Handler:           (&api.Server{Store: st, Logger: logger}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 2+len(providerSet))
	go func() {
		logger.Info("control plane listening", "address", *listen, "database", *dbPath)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	if daemonConfig == nil {
		exec := executor.NewLocal(*executorID, st, *logDir, logger)
		exec.APIURL = strings.TrimRight(*advertise, "/")
		go func() {
			if err := exec.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		for _, configured := range daemonConfig.Executors {
			if configured.Enabled != nil && !*configured.Enabled {
				continue
			}
			configured := configured
			exec := executor.NewLocal(configured.ID, st, *logDir, logger)
			exec.APIURL = strings.TrimRight(*advertise, "/")
			exec.NodeID = daemonConfig.Node.ID
			exec.Kind = configured.Kind
			exec.Attributes = configured.Labels
			exec.V1 = true
			exec.ObserveOnly = daemonConfig.ObserveOnly
			exec.ObservationInterval = time.Duration(daemonConfig.ObservationIntervalSeconds) * time.Second
			exec.Providers = providerSet
			go func() {
				if err := exec.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					errCh <- err
				}
			}()
		}
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

func resourceAdd(args []string) error {
	fs := flag.NewFlagSet("resource-add", flag.ContinueOnError)
	apiURL := apiFlag(fs)
	id := fs.String("id", "", "resource identifier")
	kind := fs.String("kind", "gpu", "resource kind")
	binding := fs.String("binding", "", "host binding, e.g. CUDA index or UUID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return request(*apiURL, http.MethodPost, "/v1/resources", map[string]any{
		"id": *id, "kind": *kind, "binding": *binding,
	})
}

func resourceReady(args []string) error {
	fs := flag.NewFlagSet("resource-ready", flag.ContinueOnError)
	apiURL := apiFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("resource-ready requires one resource ID")
	}
	return request(*apiURL, http.MethodPost, "/v1/resources/"+fs.Arg(0)+"/ready", nil)
}

func submit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	apiURL := apiFlag(fs)
	project := fs.String("project", "", "project identity")
	key := fs.String("key", "", "project-owned logical workload identity")
	priority := fs.Int("priority", 0, "scheduling priority")
	cwd := fs.String("cwd", "", "working directory")
	resourceKind := fs.String("resource-kind", "gpu", "resource kind")
	resourceCount := fs.Int("resource-count", 1, "number of resources")
	preemptible := fs.Bool("cooperative-suspend", false, "workload supports cooperative suspension")
	if err := fs.Parse(args); err != nil {
		return err
	}
	argv := fs.Args()
	if len(argv) > 0 && argv[0] == "--" {
		argv = argv[1:]
	}
	if *cwd == "" {
		resolved, err := os.Getwd()
		if err != nil {
			return err
		}
		*cwd = resolved
	}
	return request(*apiURL, http.MethodPost, "/v1/workloads", map[string]any{
		"project": *project, "external_key": *key, "priority": *priority,
		"argv": argv, "cwd": *cwd, "resource_kind": *resourceKind,
		"resource_count": *resourceCount, "cooperative_suspend": *preemptible,
	})
}

func simpleRequest(args []string, method, path string) error {
	fs := flag.NewFlagSet("request", flag.ContinueOnError)
	apiURL := apiFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return request(*apiURL, method, path, nil)
}

func workloadAction(args []string, action string) error {
	fs := flag.NewFlagSet(action, flag.ContinueOnError)
	apiURL := apiFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%s requires one workload ID", action)
	}
	return request(*apiURL, http.MethodPost,
		"/v1/workloads/"+fs.Arg(0)+"/"+action, nil)
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
