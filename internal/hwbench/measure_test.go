package hwbench

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kairo/internal/store"
)

func TestSteadyStateFollowsTemperatureElseThroughput(t *testing.T) {
	s := Steady{Window: 60}
	// warming 1 °C per 10 s: 6 °C/min
	for i := 1; i <= 120; i++ {
		s.Add(float64(i), ptr(40+float64(i)/10), 100)
	}
	if s.Reached() {
		t.Fatal("a device warming 6 °C/min is steady")
	}
	// then 0.2 °C/min for a minute
	for i := 121; i <= 180; i++ {
		s.Add(float64(i), ptr(52+float64(i-120)/300), 100)
	}
	if !s.Reached() {
		t.Fatal("a device warming 0.2 °C/min is not steady")
	}
	// too little time yet is never steady
	short := Steady{Window: 60}
	for i := 1; i <= 30; i++ {
		short.Add(float64(i), ptr(50.0), 100)
	}
	if short.Reached() {
		t.Fatal("30 s of samples is steady over 60 s")
	}
	// without a temperature: the throughput's slope, as a share of itself
	rising, flat := Steady{Window: 60}, Steady{Window: 60}
	for i := 1; i <= 90; i++ {
		rising.Add(float64(i), nil, 1000+float64(i)) // 6%/min
		flat.Add(float64(i), nil, 1000+float64(i%2))
	}
	if rising.Reached() || !flat.Reached() {
		t.Fatalf("throughput: rising %v, flat %v", rising.Reached(), flat.Reached())
	}
}

func interval(i int, work float64, n *NVML) Record {
	return Record{Type: "interval", Index: i, BeginNS: int64(i) * 1e9, EndNS: int64(i+1) * 1e9, Work: map[string]float64{"flop": work}, NVML: n}
}

func TestWindowMetricsAndInvalidation(t *testing.T) {
	stat := func(v float64) *Stat { return &Stat{Min: ptr(v - 1), Mean: ptr(v), Max: ptr(v + 1)} }
	w := &window{unit: "flop", start: fakeWall}
	for i := 0; i < 3; i++ {
		reasons := int64(0x4)
		if i == 1 {
			reasons = 0x40
		}
		w.add(interval(i, 1e12, &NVML{EnergyMJ: ptr(250e3), PowerMW: stat(250e3), SMClockMHz: stat(1700), TemperatureC: stat(70 + float64(i)), UtilGPUPct: stat(99), UtilMemPct: stat(30), ClocksEventReasons: &reasons}))
	}
	m := w.metrics()
	if m.Seconds != 3 || m.WorkPerSecond != 1e12 || *m.Joules != 750 || *m.PowerW != 250 || *m.JoulesPerWork != 750/3e12 || m.EnergySource != "nvml" ||
		*m.SMClockMHz != 1700 || *m.TemperatureC != 71 || *m.TemperatureMaxC != 73 || *m.EventReasons != 0x44 || *m.UtilMemPct != 30 {
		t.Fatalf("metrics: %+v", m)
	}
	if m.Begin != "2026-10-27T10:00:00Z" || m.End != "2026-10-27T10:00:03Z" {
		t.Fatalf("window %s - %s", m.Begin, m.End)
	}
	// 1700 MHz held by thermal slowdown under a 2000 MHz cap: the cap says nothing
	if flags := thermalFlag(m, 2000); len(flags) != 1 || !invalidates(flags[0]) {
		t.Fatalf("thermal: %v", flags)
	}
	if flags := thermalFlag(m, 1700); len(flags) != 0 {
		t.Fatalf("the cap reached: %v", flags)
	}
	m.EventReasons = ptr(int64(0x4)) // software power cap: not thermal
	if flags := thermalFlag(m, 2000); len(flags) != 0 {
		t.Fatalf("power cap: %v", flags)
	}
	// an interval without its energy counter: no energy for the window
	w.add(interval(3, 1e12, &NVML{PowerMW: stat(240e3)}))
	if m = w.metrics(); m.Joules != nil || m.JoulesPerWork != nil || m.PowerW == nil {
		t.Fatalf("missing energy: %+v", m)
	}
	// CPU package energy covering too little of the window is no energy
	cpu := store.BenchMetrics{Seconds: 30, Work: 3e10}
	if flags := withCPUEnergy(&cpu, store.CPUEnergy{Joules: 1000, SecondsCovered: 20, Seconds: 30}); len(flags) != 1 || invalidates(flags[0]) || cpu.Joules != nil {
		t.Fatalf("uncovered: %v %+v", flags, cpu)
	}
	if flags := withCPUEnergy(&cpu, store.CPUEnergy{Joules: 2900, SecondsCovered: 29, Seconds: 30}); len(flags) != 0 || *cpu.PowerW != 100 || *cpu.Joules != 3000 || *cpu.JoulesPerWork != 1e-7 {
		t.Fatalf("covered: %v %+v", flags, cpu)
	}
	if !validFlags([]string{FlagNotSteady + " after 300 s", FlagNoCPUEnergy + ": 0 of 30 s"}) || validFlags([]string{FlagOtherWork + ": x"}) {
		t.Fatal("note flags invalidate, or other work does not")
	}
}

func TestRecordsOfAnotherSchemaAreRefused(t *testing.T) {
	if _, err := parseRecord([]byte(`{"schema":"kairo.bench/v2","type":"start"}`)); err == nil {
		t.Fatal("v2 record accepted")
	}
	r, err := parseRecord([]byte(`{"schema":"kairo.bench/v1","type":"interval","begin_ns":0,"end_ns":1000000000,"work":{"byte":5},"latency":null,"nvml":{"energy_mj":null,"power_mw":{"min":1,"mean":2,"max":3}}}`))
	if err != nil || r.Seconds() != 1 || r.Work["byte"] != 5 || r.NVML.EnergyMJ != nil || *r.NVML.PowerMW.Mean != 2 {
		t.Fatalf("record %+v %v", r, err)
	}
}

func TestClientCallsTheWorkerRoutesOfItsRun(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer kairo_worker_x" || r.URL.Query().Get("run") != "bnc-1" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"no","code":"unauthorized"}`))
			return
		}
		seen = append(seen, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/api/worker/bench":
			_ = json.NewEncoder(w).Encode(store.BenchStatus{Run: store.BenchRun{ID: "bnc-1"}, LeaseID: "l"})
		case "/api/worker/bench/settings":
			var body struct {
				Settings store.DeviceSettings `json:"settings"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Settings.GraphicsClockMaxMHz == nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"bad","code":"bad_request"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(store.DeviceSettingResult{Generation: 4})
		case "/api/worker/bench/cpu-energy":
			if _, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("from")); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no CPU package power","code":"not_found"}`))
		case "/api/worker/bench/finish":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"busy","code":"internal"}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "", "kairo_worker_x", "bnc-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if status, err := c.Status(ctx); err != nil || status.LeaseID != "l" {
		t.Fatalf("status %+v %v", status, err)
	}
	if result, err := c.SetSetting(ctx, store.DeviceSettings{GraphicsClockMaxMHz: ptr(1500)}); err != nil || result.Generation != 4 {
		t.Fatalf("set %+v %v", result, err)
	}
	_, err = c.SetSetting(ctx, store.DeviceSettings{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 400 || retryable(err) {
		t.Fatalf("refused setting: %v", err)
	}
	from := fakeWall
	if energy, err := c.CPUEnergy(ctx, from, from.Add(30*time.Second)); err != nil || energy.SecondsCovered != 0 || energy.Seconds != 30 {
		t.Fatalf("no samples: %+v %v", energy, err)
	}
	if err := c.AddPoints(ctx, []store.BenchPoint{{Seq: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(ctx, store.BenchFinish{Valid: true}); !retryable(err) {
		t.Fatalf("busy daemon: %v", err)
	}
	if got := strings.Join(seen, ", "); got != "GET /api/worker/bench, POST /api/worker/bench/settings, POST /api/worker/bench/settings, GET /api/worker/bench/cpu-energy, POST /api/worker/bench/points, POST /api/worker/bench/finish" {
		t.Fatalf("calls: %s", got)
	}
}
