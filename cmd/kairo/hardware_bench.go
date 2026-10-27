package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"

	"kairo/internal/hwbench"
	"kairo/internal/store"
)

// hardwareBench: kairo hardware bench HOST DEVICE [--kinds K,...] [--node
// NODE] [--dry-run] [--json].
func hardwareBench(args []string) error {
	fs := newFlags("hardware bench")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	kinds := fs.String("kinds", "", "comma-separated kinds or groups to measure (default every kind of the device)")
	node := fs.String("node", "", "the host's node to run on (default the first that can)")
	dryRun := fs.Bool("dry-run", false, "print the bench and its execution without submitting it")
	actor := fs.String("actor", defaultActor(), "audit actor")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return usageError("hardware bench requires HOST and DEVICE (a GPU UUID, cpu, disk:FILESYSTEM or net:NODE)")
	}
	body := map[string]any{"node": *node, "dry_run": *dryRun, "actor": *actor}
	if *kinds != "" {
		body["kinds"] = strings.Split(*kinds, ",")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	path := "/api/hardware/" + url.PathEscape(positional[0]) + "/" + url.PathEscape(positional[1]) + "/bench"
	payload, err := client.call(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var plan store.BenchPlan
	if err := json.Unmarshal(payload, &plan); err != nil {
		return fmt.Errorf("decode bench: %w", err)
	}
	return renderBenchPlan(os.Stdout, plan)
}

// renderBenchPlan says what a bench measures, where, and what it holds.
func renderBenchPlan(w io.Writer, plan store.BenchPlan) error {
	run, e := plan.Run, plan.Execution
	where := run.NodeID
	if run.PeerNodeID != nil {
		where += " and " + *run.PeerNodeID
	}
	if plan.DryRun {
		fmt.Fprintf(w, "would bench %s/%s (%s: %s) on %s\n", run.HostID, run.DeviceID, run.Class, strings.Join(run.Kinds, ", "), where)
	} else {
		fmt.Fprintf(w, "bench %s of %s/%s (%s: %s) on %s: execution %s\n", run.ID, run.HostID, run.DeviceID, run.Class, strings.Join(run.Kinds, ", "), where, run.ExecutionID)
	}
	var holds []string
	for _, x := range e.Exclusive {
		holds = append(holds, "GPU "+x.Device)
	}
	if e.Capacity.CPUMillis > 0 {
		holds = append(holds, formatCPU(e.Capacity.CPUMillis)+" CPU")
	}
	if e.Capacity.RAMBytes > 0 {
		holds = append(holds, formatSize(e.Capacity.RAMBytes)+" RAM")
	}
	for _, d := range e.Capacity.Disks {
		holds = append(holds, formatSize(d.ReserveBytes)+" of "+d.Filesystem)
	}
	if e.Gang != nil {
		holds = append(holds, fmt.Sprintf("a %d-rank gang", e.Gang.Size))
	}
	fmt.Fprintf(w, "  holds %s, at priority %d in %s/%s (never preempted; waits for every other execution)\n", strings.Join(holds, ", "), e.Priority, e.Scope.Project, e.Scope.Queue)
	fmt.Fprintf(w, "  argv %s (cwd %s)\n", strings.Join(e.Argv, " "), e.CWD)
	if !plan.DryRun {
		fmt.Fprintf(w, "  follow it with kairo hardware profile %s %s\n", run.HostID, run.DeviceID)
	}
	return nil
}

func formatCPU(millis int64) string { return number(float64(millis) / 1000) }

func formatSize(bytes int64) string {
	switch {
	case bytes >= 1<<30:
		return fmt.Sprintf("%s GiB", number(math.Round(float64(bytes)/(1<<30)*10)/10))
	case bytes >= 1<<20:
		return fmt.Sprintf("%s MiB", number(math.Round(float64(bytes)/(1<<20)*10)/10))
	}
	return fmt.Sprintf("%d B", bytes)
}

// hardwareBenchRun: kairo hardware bench-run --run RUN_ID, the argv of a
// bench execution.
func hardwareBenchRun(args []string) error {
	fs := newFlags("hardware bench-run")
	runID := fs.String("run", "", "the bench run this attempt carries out")
	positional, err := parseFlags(fs, leafArgs(args))
	if err != nil {
		return err
	}
	if *runID == "" || len(positional) != 0 {
		return usageError("hardware bench-run requires --run RUN_ID and no arguments (it runs inside a bench's attempt)")
	}
	ctx, stop := signalContext()
	defer stop()
	return hwbench.Main(ctx, *runID)
}

// hardwareProfile: kairo hardware profile HOST [DEVICE] [--json].
func hardwareProfile(args []string) error {
	fs := newFlags("hardware profile")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) < 1 || len(positional) > 2 {
		return usageError("hardware profile requires HOST and at most one DEVICE")
	}
	path := "/api/hardware/" + url.PathEscape(positional[0]) + "/profile"
	if len(positional) == 2 {
		path += "?device=" + url.QueryEscape(positional[1])
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Host store.HardwareHost `json:"host"`
		Runs []store.BenchRun   `json:"runs"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode profile: %w", err)
	}
	device := ""
	if len(positional) == 2 {
		device = positional[1]
	}
	return renderProfile(os.Stdout, body.Host, body.Runs, device)
}

// renderProfile is a block per device: its runs, then per kind the curve of
// its latest valid run (the point for its max_slowdown marked *), and the
// drift from the device's previous run of that kind.
func renderProfile(w io.Writer, host store.HardwareHost, runs []store.BenchRun, only string) error {
	slowdown := map[string]float64{}
	var devices []string
	for _, d := range host.Devices {
		slowdown[d.DeviceID] = d.MaxSlowdown
		devices = append(devices, d.DeviceID)
	}
	byDevice := map[string][]store.BenchRun{}
	for _, run := range runs {
		byDevice[run.DeviceID] = append(byDevice[run.DeviceID], run)
		if !slices.Contains(devices, run.DeviceID) {
			devices = append(devices, run.DeviceID)
		}
	}
	shown := 0
	for _, device := range devices {
		if only != "" && device != only {
			continue
		}
		if shown > 0 {
			fmt.Fprintln(w)
		}
		shown++
		deviceRuns := byDevice[device]
		s, swept := slowdown[device]
		header := fmt.Sprintf("%s/%s", host.ID, device)
		if swept {
			header += "  max_slowdown " + number(s)
		}
		fmt.Fprintln(w, header)
		if len(deviceRuns) == 0 {
			fmt.Fprintf(w, "  not benched (kairo hardware bench %s %s)\n", host.ID, device)
			continue
		}
		// the latest three runs, newest first
		for i := len(deviceRuns) - 1; i >= max(0, len(deviceRuns)-3); i-- {
			fmt.Fprintf(w, "  %s\n", benchRunText(deviceRuns[i]))
		}
		curves := hwbench.Curves(deviceRuns)
		latest := deviceRuns[len(deviceRuns)-1]
		for _, kind := range latest.Kinds {
			if _, ok := curves[kind]; !ok {
				reason := "no valid run yet"
				if ref, ok := latest.References[kind]; ok && ref.Reason != "" {
					reason = ref.Reason
				}
				fmt.Fprintf(w, "  %s: %s\n", kind, reason)
			}
		}
		kinds := make([]string, 0, len(curves))
		for kind := range curves {
			kinds = append(kinds, kind)
		}
		sort.Slice(kinds, func(i, j int) bool { return benchKindOrder(kinds[i]) < benchKindOrder(kinds[j]) })
		for _, kind := range kinds {
			if err := renderCurve(w, curves[kind], deviceRuns, s, swept); err != nil {
				return err
			}
		}
	}
	if shown == 0 {
		fmt.Fprintf(w, "host %s has no device %s and no bench of one\n", host.ID, only)
	}
	return nil
}

func benchKindOrder(kind string) int {
	for i, k := range store.BenchKinds("") {
		if k.Name == kind {
			return i
		}
	}
	return math.MaxInt
}

// benchRunText is "bnc_1 finished 10-27 10:01-12:10, valid; GPU driver
// 560.35, idle 34 °C".
func benchRunText(run store.BenchRun) string {
	text := run.ID + " " + run.State
	switch {
	case run.StartedAt != nil && run.EndedAt != nil:
		text += " " + shortStamp(run.StartedAt) + " to " + shortStamp(run.EndedAt)
	case run.StartedAt != nil:
		text += " since " + shortStamp(run.StartedAt)
	default:
		text += " (requested " + shortStamp(&run.CreatedAt) + ")"
	}
	if run.Valid != nil {
		if *run.Valid {
			text += ", valid"
		} else {
			text += ", invalid"
		}
	}
	if run.Reason != nil {
		text += ": " + *run.Reason
	}
	var facts []string
	if run.DriverVersion != nil {
		facts = append(facts, "GPU driver "+*run.DriverVersion)
	}
	if run.IdleTemperatureC != nil {
		facts = append(facts, fmt.Sprintf("idle %.0f °C", *run.IdleTemperatureC))
	}
	if run.DriverBuild != nil {
		facts = append(facts, "kairo "+*run.DriverBuild)
	}
	if len(facts) != 0 {
		text += "; " + strings.Join(facts, ", ")
	}
	return text
}

// renderCurve is one kind: its reference, its points, and its drift.
func renderCurve(w io.Writer, curve hwbench.Curve, runs []store.BenchRun, s float64, swept bool) error {
	ref := curve.Reference
	line := fmt.Sprintf("  %s (%s): reference %s", curve.Kind, curve.RunID, rateText(ref.Mean, ref.Unit))
	if len(ref.WorkPerSecond) > 1 {
		line += fmt.Sprintf(" (spread %.1f%%)", ref.Spread*100)
	}
	if ref.PowerW != nil {
		line += fmt.Sprintf(", %.0f W", *ref.PowerW)
	}
	if ref.JoulesPerWork != nil {
		line += ", " + perWorkText(*ref.JoulesPerWork, ref.Unit)
	}
	fmt.Fprintln(w, line)
	var chosen *store.BenchPoint
	if swept {
		chosen = hwbench.Select(curve.Points, s)
	}
	var run store.BenchRun
	for _, r := range runs {
		if r.ID == curve.RunID {
			run = r
		}
	}
	rows := []string{"    \tSETTING\tREL\tW\tENERGY\tCLOCK\tTEMP\tVALID"}
	for _, p := range run.Points {
		if p.Kind != curve.Kind {
			continue
		}
		mark := ""
		if chosen != nil && p.Seq == chosen.Seq {
			mark = "*"
		}
		setting := p.Settings.String()
		if p.Phase == store.PhaseReference {
			setting += " (reference)"
		}
		valid := "yes"
		if !p.Valid {
			valid = "no"
		}
		if len(p.Flags) != 0 {
			valid += ": " + strings.Join(p.Flags, ", ")
		}
		m := p.Metrics
		rows = append(rows, strings.Join([]string{"    " + mark, setting, optional(m.RelPerf, "%.3f"), optional(m.PowerW, "%.0f"),
			perWorkOptional(m.JoulesPerWork, m.Unit), optional(m.SMClockMHz, "%.0f MHz"), optional(m.TemperatureC, "%.0f °C"), valid}, "\t"))
	}
	if err := writeTable(w, rows); err != nil {
		return err
	}
	if swept && chosen == nil {
		fmt.Fprintf(w, "    no point within max_slowdown %s: the defaults\n", number(s))
	}
	if drift := driftText(curve, runs); drift != "" {
		fmt.Fprintf(w, "    %s\n", drift)
	}
	return nil
}

// driftText compares a curve's reference with the device's previous run
// with a valid reference of the same kind: "drift from bnc_0 (10-20 09:00):
// work/s -0.8%, energy/work +1.2%".
func driftText(curve hwbench.Curve, runs []store.BenchRun) string {
	var previous *store.BenchRun
	for i := range runs {
		if runs[i].ID == curve.RunID {
			break
		}
		if ref, ok := runs[i].References[curve.Kind]; ok && ref.Valid {
			previous = &runs[i]
		}
	}
	if previous == nil {
		return ""
	}
	before := previous.References[curve.Kind]
	parts := []string{}
	if before.Mean > 0 {
		parts = append(parts, fmt.Sprintf("work/s %+.1f%%", (curve.Reference.Mean/before.Mean-1)*100))
	}
	if before.JoulesPerWork != nil && curve.Reference.JoulesPerWork != nil && *before.JoulesPerWork > 0 {
		parts = append(parts, fmt.Sprintf("energy/work %+.1f%%", (*curve.Reference.JoulesPerWork / *before.JoulesPerWork - 1)*100))
	}
	if len(parts) == 0 {
		return ""
	}
	when := previous.CreatedAt
	if previous.EndedAt != nil {
		when = *previous.EndedAt
	}
	return fmt.Sprintf("drift from %s (%s): %s", previous.ID, shortStamp(&when), strings.Join(parts, ", "))
}

func optional(v *float64, format string) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf(format, *v)
}

// siPrefixes scale a positive value into [1, 1000).
var siPrefixes = []struct {
	factor float64
	prefix string
}{{1e15, "P"}, {1e12, "T"}, {1e9, "G"}, {1e6, "M"}, {1e3, "k"}, {1, ""}, {1e-3, "m"}, {1e-6, "µ"}, {1e-9, "n"}, {1e-12, "p"}, {1e-15, "f"}}

func si(v float64) (float64, string) {
	for _, p := range siPrefixes {
		if math.Abs(v) >= p.factor {
			return v / p.factor, p.prefix
		}
	}
	return v / 1e-15, "f"
}

// unitSymbol is a bench unit as printed: byte -> B.
func unitSymbol(unit string) string {
	switch unit {
	case "byte":
		return "B"
	case "request":
		return "req"
	}
	return unit
}

// rateText is "61.2 Tflop/s".
func rateText(v float64, unit string) string {
	scaled, prefix := si(v)
	return fmt.Sprintf("%.3g %s%s/s", scaled, prefix, unitSymbol(unit))
}

// perWorkText is "4.90 pJ/flop".
func perWorkText(joules float64, unit string) string {
	scaled, prefix := si(joules)
	return fmt.Sprintf("%.3g %sJ/%s", scaled, prefix, unitSymbol(unit))
}

func perWorkOptional(joules *float64, unit string) string {
	if joules == nil {
		return "-"
	}
	return perWorkText(*joules, unit)
}
