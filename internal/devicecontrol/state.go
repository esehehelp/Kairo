package devicecontrol

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
)

// State is what a controller must know across a crash, kept in a file: the
// devices it controls (so it can return them to their defaults when the
// daemon stays unreachable after a restart) and, for each Windows power
// scheme whose processor settings it changed, the values it found there
// before (which nothing else can tell after a crash). A file that cannot be
// read is an error, never replaced: it may hold those values.
type State struct {
	path string
	mu   sync.Mutex
	data stateData
}

type stateData struct {
	Devices []StateDevice `json:"devices"`
	// ProcessorSchemes are, by power scheme GUID, the AC values of the
	// processor settings (by alias) found before the controller changed
	// them. A scheme is listed from just before the first change until its
	// values are restored.
	ProcessorSchemes map[string]map[string]uint32 `json:"processor_schemes,omitempty"`
}

// StateDevice is a device the controller controlled at its latest poll.
type StateDevice struct {
	HostID   string `json:"host_id"`
	DeviceID string `json:"device_id"`
	Kind     string `json:"kind"`
}

// OpenState reads the state file at path; a missing file is an empty state.
func OpenState(path string) (*State, error) {
	s := &State{path: path}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read controller state %s: %w", path, err)
	}
	if err = json.Unmarshal(body, &s.data); err != nil {
		return nil, fmt.Errorf("controller state %s is unreadable (%w): it may hold the processor settings to restore; fix or remove it by hand", path, err)
	}
	return s, nil
}

func (s *State) Devices() []StateDevice {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StateDevice(nil), s.data.Devices...)
}

// SetDevices records the devices controlled now.
func (s *State) SetDevices(devices []StateDevice) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sort.Slice(devices, func(i, j int) bool {
		a, b := devices[i], devices[j]
		return a.HostID < b.HostID || a.HostID == b.HostID && a.DeviceID < b.DeviceID
	})
	s.data.Devices = devices
	return s.save()
}

// ProcessorScheme is the values recorded for a power scheme, nil when it
// holds none of the controller's.
func (s *State) ProcessorScheme(scheme string) map[string]uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.data.ProcessorSchemes[scheme])
}

// ProcessorSchemes are the schemes holding the controller's values.
func (s *State) ProcessorSchemes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.data.ProcessorSchemes))
}

// RecordProcessorScheme keeps a scheme's values before the controller
// changes them. A scheme already recorded keeps what it has: those are the
// values found before the first change, not ones the controller set.
func (s *State) RecordProcessorScheme(scheme string, original map[string]uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, recorded := s.data.ProcessorSchemes[scheme]; recorded {
		return nil
	}
	if s.data.ProcessorSchemes == nil {
		s.data.ProcessorSchemes = map[string]map[string]uint32{}
	}
	s.data.ProcessorSchemes[scheme] = maps.Clone(original)
	return s.save()
}

// ForgetProcessorScheme drops a scheme once its values are restored.
func (s *State) ForgetProcessorScheme(scheme string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, recorded := s.data.ProcessorSchemes[scheme]; !recorded {
		return nil
	}
	delete(s.data.ProcessorSchemes, scheme)
	return s.save()
}

// save writes the file whole: a new file renamed over the old one, so a
// crash leaves one or the other.
func (s *State) save() error {
	if s.path == "" {
		return nil
	}
	body, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".*")
	if err != nil {
		return fmt.Errorf("write controller state: %w", err)
	}
	if _, err = tmp.Write(append(body, '\n')); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), s.path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("write controller state %s: %w", s.path, err)
	}
	return nil
}
