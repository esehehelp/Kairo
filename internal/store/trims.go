package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"kairo/internal/machine"
)

// What Kairo deletes inside a Proxmox container goes back to the host's thin
// pool only once the container is trimmed (pct fstrim). A deletion on a node
// records that it is to be trimmed (node_trims); the controller of the
// node's host trims each of its containers ([[hosts]] containers) that is,
// at most once per trimInterval, and reports how it went.

// trimInterval is how often one container is trimmed at most.
const trimInterval = time.Hour

// TrimOrder is a container a host's controller is to trim now: the node, its
// vmid, and the bytes deleted there since its last trim.
type TrimOrder struct {
	NodeID         string `json:"node_id"`
	VMID           int    `json:"vmid"`
	RequestedBytes int64  `json:"requested_bytes"`
}

// TrimDone is how a trim went: Problem when it failed, and what the command
// printed (cut to 4 KiB).
type TrimDone struct {
	NodeID  string `json:"node_id"`
	Problem string `json:"problem,omitempty"`
	Output  string `json:"output,omitempty"`
}

// controlledContainers is the containers of the host controllerNodeID
// controls, by node.
func controlledContainers(spec machine.Spec, controllerNodeID string) map[string]int {
	for _, h := range spec.Hosts {
		if h.Controller == controllerNodeID {
			return h.Containers
		}
	}
	return nil
}

// TrimsToMake is what the controller on node controllerNodeID is to trim
// now: its host's containers with a deletion since their last trim, each at
// most once per trimInterval, none while one ordered within trimInterval is
// still to report, and one whose last trim failed again only after
// trimInterval. Each is ordered now: the trim covers what was deleted before.
func (s *Store) TrimsToMake(ctx context.Context, controllerNodeID string) ([]TrimOrder, error) {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return nil, err
	}
	containers := controlledContainers(spec, controllerNodeID)
	out := []TrimOrder{}
	if len(containers) == 0 {
		return out, nil
	}
	nodes := make([]string, 0, len(containers))
	for node := range containers {
		nodes = append(nodes, node)
	}
	slices.Sort(nodes)
	since := time.Now().UTC().Add(-trimInterval).Format(time.RFC3339Nano)
	err = eachRow(ctx, tx, `SELECT node_id,requested_bytes FROM node_trims WHERE node_id=ANY($1::text[]) AND requested_at IS NOT NULL
		AND (trimmed_at IS NULL OR ((requested_at)::timestamptz>(trimmed_at)::timestamptz AND (trimmed_at)::timestamptz<($2)::timestamptz))
		AND (ordered_at IS NULL OR (ordered_at)::timestamptz<($2)::timestamptz)
		AND (failed_at IS NULL OR (failed_at)::timestamptz<($2)::timestamptz) ORDER BY node_id`,
		[]any{nodes, since}, func(rows *sql.Rows) error {
			var o TrimOrder
			if err := rows.Scan(&o.NodeID, &o.RequestedBytes); err != nil {
				return err
			}
			o.VMID = containers[o.NodeID]
			out = append(out, o)
			return nil
		})
	if err != nil {
		return nil, err
	}
	t := now()
	for _, o := range out {
		if _, err = tx.ExecContext(ctx, `UPDATE node_trims SET ordered_at=$1,updated_at=$2 WHERE node_id=$3`, t, t, o.NodeID); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit()
}

// RecordTrim records a trim the controller on node controllerNodeID made of
// one of its host's containers.
func (s *Store) RecordTrim(ctx context.Context, controllerNodeID string, done TrimDone) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	spec, err := declaredSpec(ctx, tx)
	if err != nil {
		return err
	}
	if _, ok := controlledContainers(spec, controllerNodeID)[done.NodeID]; !ok {
		return fmt.Errorf("node %s is no container of the host node %s controls", done.NodeID, controllerNodeID)
	}
	if len(done.Output) > 4096 {
		done.Output = done.Output[:4096]
	}
	t := now()
	if done.Problem == "" {
		// Trimmed as of its order (now, when it was never ordered): what was
		// deleted since is still to be trimmed, and its bytes stay counted.
		_, err = tx.ExecContext(ctx, `UPDATE node_trims SET trimmed_at=COALESCE(ordered_at,$1),
			requested_bytes=CASE WHEN ordered_at IS NOT NULL AND (requested_at)::timestamptz>(ordered_at)::timestamptz THEN requested_bytes ELSE 0 END,
			ordered_at=NULL,failed_at=NULL,problem=NULL,output=$2,updated_at=$3 WHERE node_id=$4`, t, nullable(done.Output), t, done.NodeID)
	} else {
		// Failed: still to be trimmed, again after trimInterval.
		_, err = tx.ExecContext(ctx, `UPDATE node_trims SET ordered_at=NULL,failed_at=$1,problem=$2,output=$3,updated_at=$4 WHERE node_id=$5`,
			t, done.Problem, nullable(done.Output), t, done.NodeID)
	}
	if err != nil {
		return err
	}
	event := "node_trimmed"
	if done.Problem != "" {
		event = "node_trim_failed"
	}
	if err = appendCoordinationEventTx(ctx, tx, event, nil, "node", done.NodeID, nil, map[string]any{"controller": controllerNodeID, "problem": done.Problem, "output": done.Output}); err != nil {
		return err
	}
	return tx.Commit()
}
