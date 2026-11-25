package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"kairo/internal/agentclient"
	"kairo/internal/bench"
	"kairo/internal/hwbench"
	"kairo/internal/store"
)

// netBenchPoll is how often an agent asks for the network benches it is to
// measure.
const netBenchPoll = 15 * time.Second

// runNetBenches measures the network benches this node's agent is asked to
// (store.NetBenches), each with a driver of its own as a bench task's runs
// (hwbench.Driver): the workloads run as this binary, against the transfer
// endpoint of the node measured, with the run's token; the results go
// through the agent API. A run whose driver ended is asked for again while
// it is live (the daemon fails it after its deadline).
func runNetBenches(ctx context.Context, client *agentclient.Client, nodeID, caFile string, logger *slog.Logger) {
	self, err := os.Executable()
	if err != nil {
		logger.Warn("network benches: this binary is not found", "error", err)
		return
	}
	var mu sync.Mutex
	running := map[string]bool{}
	tick := time.NewTicker(netBenchPoll)
	defer tick.Stop()
	for {
		benches, err := client.NetBenches(ctx, nodeID)
		if err != nil && ctx.Err() == nil {
			logger.Debug("network benches not listed", "error", err)
		}
		for _, b := range benches {
			mu.Lock()
			busy := running[b.RunID]
			running[b.RunID] = true
			mu.Unlock()
			if busy {
				continue
			}
			go func(b store.NetBench) {
				defer func() {
					mu.Lock()
					delete(running, b.RunID)
					mu.Unlock()
				}()
				log := logger.With("network_bench", b.RunID, "endpoint", b.Addr)
				log.Info("measuring a network bench")
				d := &hwbench.Driver{Daemon: netBenchDaemon{client: client, node: nodeID, run: b.RunID},
					Runner: hwbench.ExecRunner{Stderr: os.Stderr, Env: []string{bench.TokenEnv + "=" + b.Token}},
					Logger: log, Env: os.Getenv, Timing: hwbench.DefaultTiming(), Kairo: self, PackCA: caFile}
				if err := d.Run(ctx, b.RunID); err != nil && ctx.Err() == nil {
					log.Warn("network bench driver ended", "error", err)
				}
			}(b)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// netBenchDaemon is hwbench.Daemon for a network bench's agent: the agent
// API's network bench calls. A network bench sets no device and takes no
// commands.
type netBenchDaemon struct {
	client    *agentclient.Client
	node, run string
}

func (d netBenchDaemon) Status(ctx context.Context) (store.BenchStatus, error) {
	return d.client.NetBenchStatus(ctx, d.node, d.run)
}

func (netBenchDaemon) SetSetting(context.Context, store.DeviceSettings) (store.DeviceSettingResult, error) {
	return store.DeviceSettingResult{}, errors.New("a network bench sets no device")
}

func (netBenchDaemon) CPUEnergy(context.Context, time.Time, time.Time) (store.CPUEnergy, error) {
	return store.CPUEnergy{}, nil
}

func (d netBenchDaemon) Start(ctx context.Context, start store.BenchStart) error {
	return d.client.NetBenchStart(ctx, d.node, d.run, start)
}

func (d netBenchDaemon) AddPoints(ctx context.Context, points []store.BenchPoint) error {
	return d.client.NetBenchPoints(ctx, d.node, d.run, points)
}

func (d netBenchDaemon) RecordKind(ctx context.Context, kind string, ref store.BenchReference) error {
	return d.client.NetBenchKind(ctx, d.node, d.run, kind, ref)
}

func (d netBenchDaemon) Finish(ctx context.Context, finish store.BenchFinish) error {
	return d.client.NetBenchFinish(ctx, d.node, d.run, finish)
}

func (netBenchDaemon) Commands(context.Context) ([]store.Command, error) { return nil, nil }

func (netBenchDaemon) Ack(context.Context, string, string, map[string]any) error { return nil }
