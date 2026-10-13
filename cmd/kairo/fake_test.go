package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// contractAPI is a fake daemon implementing the project, task, attempt and
// log routes the task commands use (GET /api/projects/{p} with filters,
// GET/POST /api/projects/{p}/tasks/{t}[/cancel|/rerun], GET /api/attempts,
// GET /api/attempts/{id}/log, GET /health).
type contractAPI struct {
	mu       sync.Mutex
	tasks    []map[string]any          // TaskStatus objects
	details  map[string]map[string]any // task name -> TaskDetail
	attempts map[string][]map[string]any
	logs     map[string]map[string][]byte // attempt -> stream -> bytes
	version  string
	requests []string
	// projectFailures answers the next N project polls with this status.
	projectFailures int
	failureStatus   int
	cancel          func(task string, body map[string]any) (int, any)
	rerun           func(task string, body map[string]any) (int, any)
	apply           func(body map[string]any) (int, any)
	// fallback answers every other request (default: the mux's plain 404).
	fallback http.HandlerFunc
}

func newContractAPI() *contractAPI {
	return &contractAPI{details: map[string]map[string]any{}, attempts: map[string][]map[string]any{}, logs: map[string]map[string][]byte{}}
}

func (f *contractAPI) serve(t *testing.T) *httptest.Server {
	t.Helper()
	server := fakeAPI(t, http.HandlerFunc(f.handle))
	t.Cleanup(server.Close)
	return server
}

func writeJSONResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func notFoundJSON(w http.ResponseWriter, message string) {
	writeJSONResponse(w, http.StatusNotFound, map[string]string{"error": message, "code": "not_found"})
}

func (f *contractAPI) setTask(name string, update map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, task := range f.tasks {
		if task["name"] == name {
			for k, v := range update {
				task[k] = v
			}
			return
		}
	}
	update["name"] = name
	f.tasks = append(f.tasks, update)
}

func (f *contractAPI) appendLog(attempt, stream, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logs[attempt] == nil {
		f.logs[attempt] = map[string][]byte{}
	}
	f.logs[attempt][stream] = append(f.logs[attempt][stream], text...)
}

func (f *contractAPI) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *contractAPI) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.URL.Path == "/health":
		writeJSONResponse(w, http.StatusOK, map[string]any{"ok": true, "api": 1, "version": f.version})
	case r.Method == http.MethodPost && r.URL.Path == "/api/projects" && f.apply != nil:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		status, value := f.apply(body)
		writeJSONResponse(w, status, value)
	case r.Method == http.MethodGet && len(parts) == 3 && parts[1] == "projects":
		if f.projectFailures > 0 {
			f.projectFailures--
			writeJSONResponse(w, f.failureStatus, map[string]string{"error": "database is locked", "code": "internal"})
			return
		}
		if parts[2] != "p" {
			notFoundJSON(w, "project not found")
			return
		}
		writeJSONResponse(w, http.StatusOK, map[string]any{"project": f.project(r)})
	case len(parts) == 5 && parts[1] == "projects" && parts[3] == "tasks" && r.Method == http.MethodGet:
		detail, ok := f.details[parts[4]]
		if !ok || parts[2] != "p" {
			notFoundJSON(w, "task not found")
			return
		}
		writeJSONResponse(w, http.StatusOK, map[string]any{"task": detail})
	case len(parts) == 6 && parts[1] == "projects" && parts[5] == "cancel" && f.cancel != nil:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		status, value := f.cancel(parts[4], body)
		writeJSONResponse(w, status, value)
	case len(parts) == 6 && parts[1] == "projects" && parts[5] == "rerun" && f.rerun != nil:
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		status, value := f.rerun(parts[4], body)
		writeJSONResponse(w, status, value)
	case r.URL.Path == "/api/attempts":
		writeJSONResponse(w, http.StatusOK, map[string]any{"attempts": f.attempts[r.URL.Query().Get("execution_id")]})
	case len(parts) == 4 && parts[1] == "attempts" && parts[3] == "log":
		f.serveLog(w, r, parts[2])
	case f.fallback != nil:
		f.fallback(w, r)
	default:
		http.NotFound(w, r)
	}
}

// project applies the filters of GET /api/projects/{p}.
func (f *contractAPI) project(r *http.Request) map[string]any {
	q := r.URL.Query()
	counts := map[string]int{}
	tasks := []map[string]any{}
	states := splitList(q.Get("state"))
	since, _ := time.Parse(time.RFC3339, q.Get("since"))
	for _, task := range f.tasks {
		state, _ := task["state"].(string)
		counts[state]++
		name, _ := task["name"].(string)
		if names := q["task"]; len(names) > 0 && !contains(names, name) {
			continue
		}
		if m := q.Get("match"); m != "" && !strings.HasPrefix(name, m) {
			continue
		}
		if len(states) > 0 && !contains(states, state) {
			continue
		}
		if q.Get("live") == "1" && !liveState(state) {
			continue
		}
		if q.Get("since") != "" {
			updated, _ := time.Parse(time.RFC3339Nano, task["updated_at"].(string))
			if updated.Before(since) {
				continue
			}
		}
		tasks = append(tasks, task)
	}
	return map[string]any{"name": "p", "spec_digests": []string{"d1"}, "state_counts": counts, "tasks": tasks}
}

func (f *contractAPI) serveLog(w http.ResponseWriter, r *http.Request, attempt string) {
	stream := r.URL.Query().Get("stream")
	data, ok := f.logs[attempt][stream]
	if !ok {
		notFoundJSON(w, "attempt has no "+stream+" log")
		return
	}
	size := int64(len(data))
	var offset int64
	end := size
	if tail := r.URL.Query().Get("tail"); tail != "" {
		n, _ := strconv.ParseInt(tail, 10, 64)
		offset = max(0, size-n)
	} else {
		offset, _ = strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		offset = min(offset, size)
		if limit := r.URL.Query().Get("limit"); limit != "" {
			n, _ := strconv.ParseInt(limit, 10, 64)
			end = min(size, offset+n)
		}
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-Kairo-Log-Size", strconv.FormatInt(size, 10))
	w.Header().Set("X-Kairo-Log-Offset", strconv.FormatInt(offset, 10))
	_, _ = w.Write(data[offset:end])
}

// fakeClock replaces the polling clock: every sleep advances the time and
// runs onSleep (the test's way to change the daemon between polls).
type fakeClock struct {
	now     time.Time
	sleeps  int
	onSleep func(n int)
	// stopAfter cancels (as Ctrl+C would) after this many sleeps (0: never).
	stopAfter int
}

func useFakeClock(t *testing.T, start time.Time) *fakeClock {
	t.Helper()
	clock := &fakeClock{now: start}
	savedNow, savedSleep, savedLocal := nowFunc, sleepFunc, time.Local
	nowFunc = func() time.Time { return clock.now }
	sleepFunc = func(ctx context.Context, d time.Duration) bool {
		clock.sleeps++
		if clock.sleeps > 10000 {
			t.Fatal("poll loop did not end")
		}
		if clock.stopAfter > 0 && clock.sleeps >= clock.stopAfter {
			return false
		}
		clock.now = clock.now.Add(d)
		if clock.onSleep != nil {
			clock.onSleep(clock.sleeps)
		}
		return ctx.Err() == nil
	}
	time.Local = time.UTC
	t.Cleanup(func() { nowFunc, sleepFunc, time.Local = savedNow, savedSleep, savedLocal })
	return clock
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// lines splits output into lines without the trailing empty one.
func lines(out string) []string {
	out = strings.TrimRight(out, "\n")
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

func sortedKeys(m map[string]bool) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
