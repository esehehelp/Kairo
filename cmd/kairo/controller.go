package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/buildinfo"
	"kairo/internal/config"
	"kairo/internal/cpupower"
	"kairo/internal/devicecontrol"
	"kairo/internal/store"
	"kairo/internal/tlsutil"
)

// controllerCommand runs the device controller of this physical host: it
// keeps the settings of the devices its node controls (the host's
// `controller` in the machine declaration) as the daemon's desired state
// says, and returns them to their defaults when it stops or loses the daemon.
func controllerCommand(args []string) error {
	fs := newFlags("controller")
	configPath := fs.String("config", "", "controller TOML configuration")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if *configPath == "" || len(positional) != 0 {
		return usageError("controller requires --config CONTROLLER.toml and no arguments")
	}
	cfg, err := config.LoadController(*configPath)
	if err != nil {
		return fmt.Errorf("load controller config: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// The external-command helper (internal/extcmd) logs through the default.
	slog.SetDefault(logger)
	if !privileged() {
		logger.Warn("not running " + privilegeName + ": device changes will fail and be reported as such")
	}
	var caPEM []byte
	if cfg.CAFile != "" {
		if caPEM, err = os.ReadFile(cfg.CAFile); err != nil {
			return fmt.Errorf("read ca_file: %w", err)
		}
	}
	tlsConfig, err := tlsutil.ClientConfig(caPEM)
	if err != nil {
		return err
	}
	state, err := devicecontrol.OpenState(cfg.StateFile)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	controller := &devicecontrol.Controller{
		Daemon:       agentclient.New(cfg.ServerURL, cfg.Token, tlsConfig),
		Node:         store.Node{ID: cfg.Node.ID, Name: cfg.Node.Name, OS: cfg.Node.OS, Architecture: cfg.Node.Architecture, Enabled: true},
		Version:      buildinfo.Version(),
		Actuators:    devicecontrol.HostActuators(state),
		State:        state,
		PollInterval: time.Duration(cfg.PollIntervalSeconds) * time.Second,
		ResetAfter:   time.Duration(cfg.ResetAfterSeconds) * time.Second,
		CPUMeter:     cpupower.New(),
		Logger:       logger,
	}
	logger.Info("controller starting", "node_id", cfg.Node.ID, "server", cfg.ServerURL, "state_file", cfg.StateFile)
	return controller.Run(ctx)
}
