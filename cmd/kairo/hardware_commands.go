package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"kairo/internal/store"
)

// controllerSilentAfter is how long a controller may go without polling
// before `hardware show` says it is not polling.
const controllerSilentAfter = time.Minute

// hardwareShow: kairo hardware show [HOST] [--json].
func hardwareShow(args []string) error {
	fs := newFlags("hardware show")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) > 1 {
		return usageError("hardware show accepts at most one HOST")
	}
	path := "/api/hardware"
	if len(positional) == 1 {
		path += "?host=" + url.QueryEscape(positional[0])
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
		Hosts []store.HardwareHost `json:"hosts"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode hardware: %w", err)
	}
	return renderHardware(os.Stdout, body.Hosts, time.Now())
}

// renderHardware is a block per host: its nodes and controller, then a row
// per device (policy, desired setting and owner, what is applied, and
// whether the two agree), with its latest error and what it supports.
func renderHardware(w io.Writer, hosts []store.HardwareHost, now time.Time) error {
	if len(hosts) == 0 {
		fmt.Fprintln(w, "no hosts declared ([[hosts]] in kairo machine apply)")
		return nil
	}
	for i, host := range hosts {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintf(w, "%s  nodes %s  controller %s  max_slowdown %s\n", host.ID, strings.Join(host.Nodes, ","), controllerText(host, now), number(host.MaxSlowdown))
		rows := []string{"DEVICE\tKIND\tSLOWDOWN\tCONTROL\tDESIRED\tOWNER\tAPPLIED\tSTATE"}
		var notes []string
		for _, d := range host.Devices {
			desired, owner := "defaults", "-"
			if d.Effective != nil {
				desired, owner = d.Effective.Settings.String(), d.Effective.Owner.String()
			}
			applied := "-"
			if d.Applied != nil {
				applied = d.Applied.String()
				if d.AppliedNote != nil {
					applied += " (" + *d.AppliedNote + ")"
				}
			}
			control := "yes"
			if !d.Control {
				control = "no"
			}
			rows = append(rows, strings.Join([]string{d.DeviceID, d.Kind, number(d.MaxSlowdown), control, desired, owner, applied, deviceState(d)}, "\t"))
			if d.LastError != nil {
				notes = append(notes, fmt.Sprintf("  %s: error %s: %s", d.DeviceID, shortStamp(d.LastErrorAt), *d.LastError))
			}
			if len(d.Settings) > 1 {
				var below []string
				for _, s := range d.Settings[1:] {
					below = append(below, s.Owner.String()+" "+s.Settings.String())
				}
				notes = append(notes, fmt.Sprintf("  %s: overridden: %s", d.DeviceID, strings.Join(below, "; ")))
			}
			if d.Capabilities != nil {
				notes = append(notes, fmt.Sprintf("  %s: supports %s", d.DeviceID, capabilitiesText(*d.Capabilities)))
			}
			if d.Policy != nil {
				notes = append(notes, fmt.Sprintf("  %s: policy %s", d.DeviceID, policyText(*d.Policy)))
			}
			if d.Bench != nil {
				notes = append(notes, fmt.Sprintf("  %s: bench %s", d.DeviceID, benchRunText(*d.Bench)))
			}
		}
		if err := writeTable(w, rows); err != nil {
			return err
		}
		for _, note := range notes {
			fmt.Fprintln(w, note)
		}
	}
	return nil
}

func controllerText(host store.HardwareHost, now time.Time) string {
	if host.Controller == nil {
		return "none (observe only)"
	}
	text := *host.Controller
	seen, ok := parseStamp(host.ControllerLastSeenAt)
	switch {
	case !ok:
		text += " (never seen)"
	case now.Sub(seen) > controllerSilentAfter:
		text += " (last seen " + shortTime(seen) + ", not polling)"
	default:
		text += " (seen " + clock(seen) + ")"
	}
	return text
}

// deviceState is whether a device holds what is desired of it.
func deviceState(d store.HardwareDevice) string {
	switch {
	case !d.Control:
		return "not controlled"
	case d.Controller == nil:
		return "observe only"
	case d.ReportedAt == nil:
		return "not reported"
	}
	desired := store.DeviceSettings{}
	if d.Effective != nil {
		desired = d.Effective.Settings
	}
	inSync := d.Applied != nil && d.Applied.Equal(desired) && d.AppliedGeneration != nil && *d.AppliedGeneration == d.Generation
	switch {
	case inSync:
		return "in sync"
	case d.LastError != nil:
		return "failed"
	case d.Applied != nil && d.AppliedNote != nil:
		return "reset"
	}
	return "pending"
}

// policyText is the policy's latest decision: "graphics_clock_max_mhz=1800MHz
// (profile, for gemm: perf 0.97, 250 W vs 300 W) at 10-27 10:01".
func policyText(d store.PolicyDecision) string {
	text := d.Settings.String() + " (" + d.Basis
	var expected []string
	for _, e := range d.Expected {
		part := fmt.Sprintf("%s perf %.3f", e.Kind, e.RelPerf)
		if e.PowerW != nil && e.DefaultPowerW != nil {
			part += fmt.Sprintf(", %.0f W vs %.0f W", *e.PowerW, *e.DefaultPowerW)
		}
		expected = append(expected, part)
	}
	switch {
	case len(expected) != 0:
		text += ", for " + strings.Join(expected, "; ")
	case len(d.Candidates) != 0:
		text += ", for " + strings.Join(d.Candidates, ", ")
	}
	if d.Reason != "" {
		text += ": " + d.Reason
	}
	text += ")"
	if d.LeaseID != "" {
		text += " for lease " + d.LeaseID
	}
	return text + " at " + shortStamp(&d.DecidedAt)
}

// capabilitiesText is "graphics_clock_max_mhz 210-2130MHz, power_limit_w
// 100-350W (default 320W)".
func capabilitiesText(c store.DeviceCapabilities) string {
	span := func(values []int, unit string) string {
		if len(values) == 0 {
			return ""
		}
		return fmt.Sprintf(" %d-%d%s", slices.Min(values), slices.Max(values), unit)
	}
	var parts []string
	for _, name := range c.Settings {
		text := name
		switch name {
		case store.SettingGraphicsClockMax:
			text += span(c.GraphicsClocksMHz, "MHz")
		case store.SettingMemoryClock:
			text += span(c.MemoryClocksMHz, "MHz")
		case store.SettingPowerLimit:
			if c.PowerLimitMinW != nil && c.PowerLimitMaxW != nil {
				text += fmt.Sprintf(" %s-%sW", number(*c.PowerLimitMinW), number(*c.PowerLimitMaxW))
			}
			if c.PowerLimitDefaultW != nil {
				text += fmt.Sprintf(" (default %sW)", number(*c.PowerLimitDefaultW))
			}
		case store.SettingMaxFreq:
			if c.MaxFreqMinMHz != nil && c.MaxFreqMaxMHz != nil {
				text += fmt.Sprintf(" %d-%dMHz", *c.MaxFreqMinMHz, *c.MaxFreqMaxMHz)
			}
		case store.SettingMaxPerf:
			if c.MaxPerfPctMin != nil {
				text += fmt.Sprintf(" %d-100%%", *c.MaxPerfPctMin)
			}
		}
		parts = append(parts, text)
	}
	if len(parts) == 0 {
		return "nothing the controller can change"
	}
	return strings.Join(parts, ", ")
}

// hardwareSet: kairo hardware set HOST DEVICE [--graphics-clock-max MHz]
// [--memory-clock MHz] [--power-limit W] [--cpu-max-freq MHz | --cpu-max-perf PCT].
func hardwareSet(args []string) error {
	fs := newFlags("hardware set")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	var settings store.DeviceSettings
	optionalInt(fs, &settings.GraphicsClockMaxMHz, "graphics-clock-max", "GPU: highest graphics clock, MHz")
	optionalInt(fs, &settings.MemoryClockMHz, "memory-clock", "GPU: memory clock to lock, MHz")
	optionalInt(fs, &settings.PowerLimitW, "power-limit", "GPU: power limit, W")
	optionalInt(fs, &settings.MaxFreqMHz, "cpu-max-freq", "CPU: highest frequency, MHz (Windows, Linux cpufreq)")
	optionalInt(fs, &settings.MaxPerfPct, "cpu-max-perf", "CPU: highest performance, percent (Linux intel_pstate)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return usageError("hardware set requires HOST and DEVICE (a GPU UUID or cpu)")
	}
	if settings.IsDefault() {
		return usageError("hardware set requires a setting (kairo hardware reset returns a device to its defaults)")
	}
	if settings.MaxFreqMHz != nil && settings.MaxPerfPct != nil {
		return usageError("--cpu-max-freq and --cpu-max-perf exclude each other")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodPost, settingsPath(positional[0], positional[1]), map[string]any{"settings": settings})
	if err != nil {
		return err
	}
	return printSettingResult(payload, *asJSON, false)
}

// hardwareReset: kairo hardware reset HOST DEVICE.
func hardwareReset(args []string) error {
	fs := newFlags("hardware reset")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		return usageError("hardware reset requires HOST and DEVICE (a GPU UUID or cpu)")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodDelete, settingsPath(positional[0], positional[1]), map[string]any{})
	if err != nil {
		return err
	}
	return printSettingResult(payload, *asJSON, true)
}

func settingsPath(host, device string) string {
	return "/api/hardware/" + url.PathEscape(host) + "/" + url.PathEscape(device) + "/settings"
}

// printSettingResult prints what a set or a reset left in effect.
func printSettingResult(payload []byte, asJSON, reset bool) error {
	if asJSON {
		_, err := os.Stdout.Write(payload)
		return err
	}
	var result store.DeviceSettingResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return fmt.Errorf("decode setting: %w", err)
	}
	effective := "defaults"
	if result.Effective != nil {
		effective = result.Effective.Settings.String() + " (" + result.Effective.Owner.String() + ")"
	}
	device := result.HostID + "/" + result.DeviceID
	switch {
	case !reset:
		fmt.Printf("%s: %s, generation %d; its controller applies it (kairo hardware show)\n", device, effective, result.Generation)
	case result.Cleared:
		fmt.Printf("%s: manual setting cleared; now %s, generation %d\n", device, effective, result.Generation)
	default:
		fmt.Printf("%s: no manual setting; now %s\n", device, effective)
	}
	return nil
}

// optionalInt is an int flag that sets *target only when given.
func optionalInt(fs *flag.FlagSet, target **int, name, usage string) {
	fs.Func(name, usage, func(raw string) error {
		v, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("not a whole number: %q", raw)
		}
		*target = &v
		return nil
	})
}
