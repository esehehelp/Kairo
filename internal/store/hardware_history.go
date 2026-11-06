package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"
)

// Hardware history: what each resource was observed to have, and what its
// leases reserved of it, over the last days, for dashboards.
//
// Resource observations are kept only an hour (only each resource's latest
// is read for admission), so recordHistoryTx keeps at most one row per
// resource every HistoryInterval in resource_history, with the sum of what
// its live leases reserved then, for HistoryRetention; recordDeviceHistoryTx
// does the same with device samples (power, clocks) in device_history.
// HardwareSamples averages both into buckets; LeaseIntervals lists the
// leases that held a resource within a window.
//
// The interval keeps a day's history small enough to average on each
// request: on pve1's PostgreSQL (2 cores) a day of device_samples (a row per
// provider poll, about 150,000) took 2.3 s; a day of both histories is about
// 80,000 narrow rows.

// HistoryInterval is the least time between two history rows of a resource
// or device.
const HistoryInterval = 30 * time.Second

// HistoryRetention is how long resource history is kept (as device samples).
var HistoryRetention = 72 * time.Hour

// recordHistoryTx keeps observation o of a resource in resource_history,
// unless the resource has a row less than HistoryInterval from it.
func recordHistoryTx(ctx context.Context, tx *sql.Tx, o Observation) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO resource_history(resource_id,sampled_at,total_bytes,free_bytes,utilization,reserved)
		SELECT $1,$2::timestamptz,$3,$4,$5,
			(SELECT COALESCE(SUM(li.quantity),0) FROM lease_items li JOIN leases l ON l.id=li.lease_id WHERE li.resource_id=$1 AND l.state IN `+liveLeaseStates+`)
		WHERE NOT EXISTS(SELECT 1 FROM resource_history WHERE resource_id=$1 AND sampled_at>$2::timestamptz-$6*interval '1 second' AND sampled_at<$2::timestamptz+$6*interval '1 second')
		ON CONFLICT DO NOTHING`,
		o.ResourceID, o.ObservedAt, o.TotalBytes, o.FreeBytes, o.Utilization, HistoryInterval.Seconds())
	return err
}

// recordDeviceHistoryTx keeps device sample s of device, read at at, in
// device_history, unless the device has a row less than HistoryInterval from
// it.
func recordDeviceHistoryTx(ctx context.Context, tx *sql.Tx, device string, at time.Time, s DeviceSample) error {
	var clock *float64
	if s.SMClockMHz != nil {
		v := float64(*s.SMClockMHz)
		clock = &v
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO device_history(device,sampled_at,power_w,power_limit_w,sm_clock_mhz,mem_util)
		SELECT $1,$2::timestamptz,$3,$4,$5,$6
		WHERE NOT EXISTS(SELECT 1 FROM device_history WHERE device=$1 AND sampled_at>$2::timestamptz-$7*interval '1 second' AND sampled_at<$2::timestamptz+$7*interval '1 second')
		ON CONFLICT DO NOTHING`,
		device, at.UTC().Format(time.RFC3339Nano), s.PowerW, s.PowerLimitW, clock, s.MemUtilization, HistoryInterval.Seconds())
	return err
}

// PruneHistory deletes resource and device history older than olderThan and
// returns how many rows it removed.
func (s *Store) PruneHistory(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan).UTC().Format(time.RFC3339Nano)
	removed := 0
	for _, table := range []string{"resource_history", "device_history"} {
		result, err := s.exec(ctx, `DELETE FROM `+table+` WHERE sampled_at<$1::timestamptz`, cutoff)
		if err != nil {
			return removed, err
		}
		n, _ := result.RowsAffected()
		removed += int(n)
	}
	return removed, nil
}

// HardwareSeries is one device's or resource's history, bucketed: T holds
// the start of each bucket that has a sample (Unix milliseconds), and each
// metric one value per bucket (nil: no sample of it there).
//
// Key is a GPU's UUID (one series for every node it is declared on), any
// other resource's id, or a host's CPU package series as its controller
// reads it (host/<host>/cpu, kind host_cpu). Metrics by kind:
//   - gpu: util (percent), mem_used, mem_total (bytes), power_w,
//     power_limit_w, sm_clock_mhz, mem_util (percent)
//   - cpu: total, reserved (millicores), power_w (package power, as the node
//     reads it)
//   - ram, disk: used, total, reserved (bytes)
//   - host_cpu: power_w
type HardwareSeries struct {
	Key       string                `json:"key"`
	Kind      string                `json:"kind"`
	Nodes     []string              `json:"nodes"`
	Resources []string              `json:"resources,omitempty"`
	T         []int64               `json:"t"`
	Metrics   map[string][]*float64 `json:"metrics"`
}

// HardwareSamples is GET /api/hardware/samples.
type HardwareSamples struct {
	Since       string           `json:"since"`
	Until       string           `json:"until"`
	StepSeconds int64            `json:"step_seconds"`
	Series      []HardwareSeries `json:"series"`
}

// metricsOf are the metrics of a series kind, in the order they are listed.
var metricsOf = map[string][]string{
	"gpu":      {"util", "mem_used", "mem_total", "power_w", "power_limit_w", "sm_clock_mhz", "mem_util"},
	"cpu":      {"total", "reserved", "power_w"},
	"ram":      {"used", "total", "reserved"},
	"disk":     {"used", "total", "reserved"},
	"host_cpu": {"power_w"},
}

// HardwareSamples averages the history of [since, until) into buckets of
// step (aligned on since). keys limits it to those series (none: all).
func (s *Store) HardwareSamples(ctx context.Context, since, until time.Time, step time.Duration, keys []string) (HardwareSamples, error) {
	out := HardwareSamples{Since: since.UTC().Format(time.RFC3339), Until: until.UTC().Format(time.RFC3339), StepSeconds: int64(step / time.Second), Series: []HardwareSeries{}}
	want := func(key string) bool { return len(keys) == 0 || slices.Contains(keys, key) }
	type builder struct {
		series  *HardwareSeries
		buckets map[int64]map[string]float64
	}
	builders := map[string]*builder{}
	get := func(key, kind string) *builder {
		b := builders[key]
		if b == nil {
			b = &builder{series: &HardwareSeries{Key: key, Kind: kind, Nodes: []string{}}, buckets: map[int64]map[string]float64{}}
			builders[key] = b
		}
		return b
	}
	set := func(b *builder, bucket time.Time, metric string, value sql.NullFloat64) {
		if !value.Valid || !slices.Contains(metricsOf[b.series.Kind], metric) {
			return
		}
		ms := bucket.UnixMilli()
		if b.buckets[ms] == nil {
			b.buckets[ms] = map[string]float64{}
		}
		b.buckets[ms][metric] = value.Float64
	}
	// Every resource's series key, kind, node and row.
	kindOf := map[string]string{}
	err := eachRow(ctx, s.db, `SELECT `+deviceKey+`,r.kind,r.node_id,r.id FROM resource_instances r WHERE r.kind IN('gpu','cpu','ram','disk') ORDER BY r.node_id,r.id`, nil, func(rows *sql.Rows) error {
		var key, kind, node, resourceID string
		if err := rows.Scan(&key, &kind, &node, &resourceID); err != nil {
			return err
		}
		if !want(key) {
			return nil
		}
		kindOf[key] = kind
		series := get(key, kind).series
		if !slices.Contains(series.Nodes, node) {
			series.Nodes = append(series.Nodes, node)
		}
		series.Resources = append(series.Resources, resourceID)
		return nil
	})
	if err != nil {
		return out, err
	}
	interval := fmt.Sprintf("%d seconds", int64(step/time.Second))
	err = eachRow(ctx, s.db, `SELECT `+deviceKey+`,date_bin($3::interval,h.sampled_at,$1::timestamptz),
			AVG(h.utilization),AVG(h.total_bytes-h.free_bytes),AVG(h.total_bytes),AVG(h.reserved)
		FROM resource_history h JOIN resource_instances r ON r.id=h.resource_id
		WHERE h.sampled_at>=$1::timestamptz AND h.sampled_at<$2::timestamptz AND r.kind IN('gpu','cpu','ram','disk')
		GROUP BY 1,2`, []any{out.Since, out.Until, interval}, func(rows *sql.Rows) error {
		var key string
		var bucket time.Time
		var util, used, total, reserved sql.NullFloat64
		if err := rows.Scan(&key, &bucket, &util, &used, &total, &reserved); err != nil {
			return err
		}
		b := builders[key]
		if b == nil {
			return nil
		}
		switch b.series.Kind {
		case "gpu":
			set(b, bucket, "util", util)
			set(b, bucket, "mem_used", used)
			set(b, bucket, "mem_total", total)
		default:
			set(b, bucket, "used", used)
			set(b, bucket, "total", total)
			set(b, bucket, "reserved", reserved)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	// The devices' power and clocks: a GPU's by its UUID, a node's CPU
	// package by its cpu resource, a host's by its controller's series.
	hostNodes := map[string][]string{}
	if spec, err := declaredSpec(ctx, s.db); err == nil {
		for _, host := range spec.Hosts {
			if device := HostCPUDevice(host.ID); want(device) {
				hostNodes[device] = host.Nodes
			}
		}
	}
	devices := make([]string, 0, len(builders)+len(hostNodes))
	for key, b := range builders {
		if b.series.Kind == "gpu" || b.series.Kind == "cpu" {
			devices = append(devices, key)
		}
	}
	for device := range hostNodes {
		devices = append(devices, device)
	}
	err = eachRow(ctx, s.db, `SELECT device,date_bin($3::interval,sampled_at,$1::timestamptz),
			AVG(power_w),AVG(power_limit_w),AVG(sm_clock_mhz),AVG(mem_util)
		FROM device_history WHERE device=ANY($4::text[]) AND sampled_at>=$1::timestamptz AND sampled_at<$2::timestamptz
		GROUP BY 1,2`, []any{out.Since, out.Until, interval, devices}, func(rows *sql.Rows) error {
		var device string
		var bucket time.Time
		var power, limit, clock, memUtil sql.NullFloat64
		if err := rows.Scan(&device, &bucket, &power, &limit, &clock, &memUtil); err != nil {
			return err
		}
		b := builders[device]
		if b == nil {
			nodes, ok := hostNodes[device]
			if !ok {
				return nil
			}
			b = get(device, "host_cpu")
			b.series.Nodes = append(b.series.Nodes, nodes...)
		}
		set(b, bucket, "power_w", power)
		set(b, bucket, "power_limit_w", limit)
		set(b, bucket, "sm_clock_mhz", clock)
		set(b, bucket, "mem_util", memUtil)
		return nil
	})
	if err != nil {
		return out, err
	}
	for _, b := range builders {
		times := make([]int64, 0, len(b.buckets))
		for ms := range b.buckets {
			times = append(times, ms)
		}
		slices.Sort(times)
		b.series.T = times
		b.series.Metrics = map[string][]*float64{}
		for _, metric := range metricsOf[b.series.Kind] {
			values := make([]*float64, len(times))
			for i, ms := range times {
				if v, ok := b.buckets[ms][metric]; ok && !math.IsNaN(v) {
					values[i] = &v
				}
			}
			b.series.Metrics[metric] = values
		}
		out.Series = append(out.Series, *b.series)
	}
	sort.Slice(out.Series, func(i, j int) bool {
		a, b := out.Series[i], out.Series[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Key < b.Key
	})
	return out, nil
}

// LeaseInterval is a lease holding a resource: from when it was made
// (reserved: admission counts it from then) to when it was released (nil
// while it holds the resource, a stale lease too). Key is the resource's
// series key (HardwareSeries.Key).
type LeaseInterval struct {
	Key         string  `json:"key"`
	ResourceID  string  `json:"resource_id"`
	Kind        string  `json:"kind"`
	Node        string  `json:"node"`
	LeaseID     string  `json:"lease_id"`
	State       string  `json:"state"`
	ExecutionID string  `json:"execution_id"`
	Project     string  `json:"project"`
	Queue       string  `json:"queue"`
	Task        string  `json:"task"`
	Quantity    int64   `json:"quantity"`
	Start       string  `json:"start"`
	End         *string `json:"end"`
}

// LeaseIntervals lists the leases that held a resource at some time in
// [since, until), by resource and start. keys limits it to those series
// keys (none: all).
func (s *Store) LeaseIntervals(ctx context.Context, since, until time.Time, keys []string) ([]LeaseInterval, error) {
	out := []LeaseInterval{}
	// A released lease without its release time (rows from before it was
	// recorded) ends at the last time it was seen changing.
	err := eachRow(ctx, s.db, `SELECT `+deviceKey+`,r.id,r.kind,r.node_id,l.id,l.state,e.id,p.external_key,COALESCE(q.external_key,''),COALESCE(t.external_key,''),li.quantity,l.created_at,
			CASE WHEN l.state='released' THEN COALESCE(l.released_at,l.releasing_at,l.stale_at,l.activated_at,l.prepared_at,l.created_at) END
		FROM lease_items li JOIN leases l ON l.id=li.lease_id JOIN resource_instances r ON r.id=li.resource_id
		JOIN execution_requests e ON e.id=l.execution_id JOIN coordination_scopes p ON p.id=e.project_scope_id
		LEFT JOIN coordination_scopes q ON q.id=e.queue_scope_id LEFT JOIN coordination_scopes t ON t.id=e.task_scope_id
		WHERE l.created_at::timestamptz<$2::timestamptz
		AND (l.state<>'released' OR COALESCE(l.released_at,l.releasing_at,l.stale_at,l.activated_at,l.prepared_at,l.created_at)::timestamptz>$1::timestamptz)
		ORDER BY 1,l.created_at::timestamptz,l.id`,
		[]any{since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano)}, func(rows *sql.Rows) error {
			var v LeaseInterval
			var end sql.NullString
			if err := rows.Scan(&v.Key, &v.ResourceID, &v.Kind, &v.Node, &v.LeaseID, &v.State, &v.ExecutionID, &v.Project, &v.Queue, &v.Task, &v.Quantity, &v.Start, &end); err != nil {
				return err
			}
			if len(keys) != 0 && !slices.Contains(keys, v.Key) {
				return nil
			}
			if end.Valid {
				v.End = &end.String
			}
			out = append(out, v)
			return nil
		})
	return out, err
}
