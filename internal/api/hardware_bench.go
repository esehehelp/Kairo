package api

import (
	"fmt"
	"net/http"
	"time"

	"kairo/internal/store"
)

// Hardware benches: an operator creates a bench (kairo hardware bench), which
// runs as an ordinary execution; its driver reports through the worker API
// with its attempt token, which reaches only the run that attempt carries out
// and only its own lease's bench setting.

func (s *Server) createBench(w http.ResponseWriter, r *http.Request) {
	var body store.BenchRequest
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if s.ObserveOnly && !body.DryRun {
		writeError(w, errObserveOnly)
		return
	}
	body.HostID, body.DeviceID, body.Actor = r.PathValue("host"), r.PathValue("device"), actorOr(r, body.Actor)
	plan, err := s.Store.CreateBenchRun(r.Context(), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) hardwareProfile(w http.ResponseWriter, r *http.Request) {
	host, device := r.PathValue("host"), r.URL.Query().Get("device")
	hosts, err := s.Store.ListHardware(r.Context(), host)
	if err != nil {
		writeError(w, err)
		return
	}
	runs, err := s.Store.ListBenchRuns(r.Context(), host, device, true)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"host": hosts[0], "runs": runs})
}

// benchWorker is the run the request's attempt carries out; a request naming
// another run (?run=) is forbidden.
func (s *Server) benchWorker(w http.ResponseWriter, r *http.Request) (store.WorkerIdentity, store.BenchRun, bool) {
	worker := workerIdentity(r)
	run, _, err := s.Store.BenchOfAttempt(r.Context(), worker.AttemptID)
	if err == nil {
		if want := r.URL.Query().Get("run"); want != "" && want != run.ID {
			err = fmt.Errorf("%w: attempt %s carries out bench run %s, not %s", ErrForbidden, worker.AttemptID, run.ID, want)
		}
	}
	if err != nil {
		writeError(w, err)
		return worker, run, false
	}
	return worker, run, true
}

func (s *Server) workerBenchStatus(w http.ResponseWriter, r *http.Request) {
	worker, _, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	status, err := s.Store.BenchStatusOf(r.Context(), worker.AttemptID, worker.LeaseID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) workerBenchSetting(w http.ResponseWriter, r *http.Request) {
	worker, _, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	var body struct {
		Settings store.DeviceSettings `json:"settings"`
	}
	if err := decodeOptional(r, &body); err != nil {
		writeError(w, err)
		return
	}
	if s.ObserveOnly {
		writeError(w, errObserveOnly)
		return
	}
	result, err := s.Store.SetBenchSetting(r.Context(), worker.AttemptID, worker.LeaseID, body.Settings)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) workerBenchCPUEnergy(w http.ResponseWriter, r *http.Request) {
	_, run, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	var window [2]time.Time
	for i, key := range []string{"from", "to"} {
		t, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get(key))
		if err != nil {
			writeError(w, fmt.Errorf("%s: an RFC 3339 time is required: %w", key, err))
			return
		}
		window[i] = t
	}
	if !window[1].After(window[0]) {
		writeError(w, fmt.Errorf("the window ends (%s) before it begins (%s)", window[1].Format(time.RFC3339Nano), window[0].Format(time.RFC3339Nano)))
		return
	}
	energy, err := s.Store.HostCPUEnergy(r.Context(), run.HostID, window[0], window[1])
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, energy)
}

func (s *Server) workerBenchStart(w http.ResponseWriter, r *http.Request) {
	worker, _, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	var body store.BenchStart
	err := decode(r, &body)
	if err == nil {
		err = s.Store.StartBenchRun(r.Context(), worker.AttemptID, body)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody)
}

func (s *Server) workerBenchPoints(w http.ResponseWriter, r *http.Request) {
	worker, _, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	var body struct {
		Points []store.BenchPoint `json:"points"`
	}
	err := decode(r, &body)
	if err == nil {
		err = s.Store.AddBenchPoints(r.Context(), worker.AttemptID, body.Points)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody)
}

func (s *Server) workerBenchFinish(w http.ResponseWriter, r *http.Request) {
	worker, _, ok := s.benchWorker(w, r)
	if !ok {
		return
	}
	var body store.BenchFinish
	err := decode(r, &body)
	if err == nil {
		err = s.Store.FinishBenchRun(r.Context(), worker.AttemptID, worker.LeaseID, body)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody)
}
