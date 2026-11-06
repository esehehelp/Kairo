package devicecontrol

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/internal/extcmd"
	"kairo/internal/store"
)

// What Kairo deletes inside a Proxmox container goes back to the host's thin
// pool only once the container is trimmed. A controller whose Daemon is a
// TrimDaemon asks, every trimPoll, which of its host's containers are to be
// trimmed (store.TrimsToMake), trims them one at a time in the background
// (pct fstrim <vmid>, up to trimTimeout each) and reports each (Problem when
// it failed). Device settings are kept meanwhile: a trim never holds a poll.

// TrimDaemon is a Daemon that orders trims (agentclient.Client is one).
type TrimDaemon interface {
	TrimsToMake(ctx context.Context, nodeID string) ([]store.TrimOrder, error)
	RecordTrim(ctx context.Context, nodeID string, done store.TrimDone) error
}

const (
	trimPoll    = 5 * time.Minute
	trimTimeout = time.Hour
)

// PctFstrim trims a Proxmox container: pct fstrim <vmid>.
func PctFstrim(ctx context.Context, vmid int) (string, error) {
	out, err := extcmd.Output(ctx, "pct", "fstrim", strconv.Itoa(vmid))
	return strings.TrimSpace(string(out)), err
}

// trimState is the controller's trimming: when it last asked, whether a
// round runs.
type trimState struct {
	mu      sync.Mutex
	at      time.Time
	running bool
}

// trim starts a trimming round in the background when one is due.
func (c *Controller) trim(ctx context.Context) {
	daemon, ok := c.Daemon.(TrimDaemon)
	if !ok {
		return
	}
	c.trims.mu.Lock()
	due := !c.trims.running && c.now().Sub(c.trims.at) >= trimPoll
	if due {
		c.trims.running, c.trims.at = true, c.now()
	}
	c.trims.mu.Unlock()
	if !due {
		return
	}
	run := func() {
		defer func() {
			c.trims.mu.Lock()
			c.trims.running = false
			c.trims.mu.Unlock()
		}()
		c.trimDue(context.WithoutCancel(ctx), daemon)
	}
	if c.trimSync {
		run()
		return
	}
	go run()
}

// trimDue trims every container the daemon says is due, and reports each.
func (c *Controller) trimDue(ctx context.Context, daemon TrimDaemon) {
	orders, err := daemon.TrimsToMake(ctx, c.Node.ID)
	if err != nil {
		c.logger().Debug("cannot ask for trims", "error", err)
		return
	}
	trimmer := c.Trimmer
	if trimmer == nil {
		trimmer = PctFstrim
	}
	for _, o := range orders {
		started := c.now()
		runCtx, cancel := context.WithTimeout(ctx, trimTimeout)
		out, err := trimmer(runCtx, o.VMID)
		cancel()
		done := store.TrimDone{NodeID: o.NodeID, Output: out}
		if err != nil {
			done.Problem = err.Error()
			c.logger().Warn("container not trimmed", "node_id", o.NodeID, "vmid", o.VMID, "error", err, "output", out)
		} else {
			c.logger().Info("container trimmed", "node_id", o.NodeID, "vmid", o.VMID, "deleted_bytes", o.RequestedBytes,
				"seconds", c.now().Sub(started).Seconds(), "output", out)
		}
		if err := daemon.RecordTrim(ctx, c.Node.ID, done); err != nil {
			c.logger().Warn("trim not recorded", "node_id", o.NodeID, "error", err)
		}
	}
}
