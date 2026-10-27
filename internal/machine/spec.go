// Package machine parses the machine declaration accepted by `kairo machine
// apply`: which node holds which GPU, and which physical host holds which
// nodes. The declaration is the authority on GPU ownership; providers only
// observe what is there. A GPU declared for several nodes is SHARED: nodes on
// one host (containers that all see the card) may each schedule it, and a
// lease on it through any of them excludes the others.
//
// A host declares only policy and authority: how much slower than its maximum
// its devices may run (max_slowdown), which controller may change their
// settings, and which devices are never to be changed. Models, clocks and
// power are observed, never declared.
package machine

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

// CPU is the device id of a host's CPU package(s).
const CPU = "cpu"

// MaxSlowdownLimit bounds max_slowdown: [0, MaxSlowdownLimit).
const MaxSlowdownLimit = 0.5

// Spec is a whole machine declaration. It replaces the previous one: a GPU
// left out is no longer declared for any node, a host left out no longer
// exists.
type Spec struct {
	Nodes []Node `toml:"nodes" json:"nodes"`
	Hosts []Host `toml:"hosts" json:"hosts,omitempty"`
}

type Node struct {
	ID string `toml:"id" json:"id"`
	// GPUs are NVIDIA UUIDs as nvidia-smi -L prints them (GPU-...).
	GPUs []string `toml:"gpus" json:"gpus"`
}

// Host is a physical machine: the nodes (containers) on it, the controller
// node that may change its devices' settings (none: observe only), and the
// slowdown from maximum performance its devices may be run at (0: leave the
// hardware at its defaults).
type Host struct {
	ID          string   `toml:"id" json:"id"`
	Nodes       []string `toml:"nodes" json:"nodes"`
	Controller  string   `toml:"controller" json:"controller,omitempty"`
	MaxSlowdown float64  `toml:"max_slowdown" json:"max_slowdown,omitempty"`
	Devices     []Device `toml:"devices" json:"devices,omitempty"`
}

// Device overrides the host's policy for one of its devices: a GPU UUID, or
// CPU. A field left out takes the host's value; control defaults to true.
type Device struct {
	ID          string   `toml:"id" json:"id"`
	MaxSlowdown *float64 `toml:"max_slowdown" json:"max_slowdown,omitempty"`
	// Control false: its settings are never changed and it is never benched.
	Control *bool `toml:"control" json:"control,omitempty"`
}

// Parse reads a machines TOML file; unknown fields are an error.
func Parse(body string) (Spec, error) {
	var spec Spec
	metadata, err := toml.Decode(body, &spec)
	if err != nil {
		return Spec{}, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return Spec{}, fmt.Errorf("unknown machine field %q", undecoded[0].String())
	}
	return spec, spec.Validate()
}

// Validate checks that every node is named once and lists a GPU once, and
// that hosts partition nodes and devices consistently (see validateHosts).
func (s Spec) Validate() error {
	nodes := map[string]bool{}
	for _, node := range s.Nodes {
		if node.ID == "" {
			return errors.New("every [[nodes]] needs an id")
		}
		if nodes[node.ID] {
			return fmt.Errorf("node %s is declared twice", node.ID)
		}
		nodes[node.ID] = true
		listed := map[string]bool{}
		for _, uuid := range node.GPUs {
			if !strings.HasPrefix(uuid, "GPU-") || strings.TrimSpace(uuid) != uuid {
				return fmt.Errorf("node %s: %q is not a GPU UUID (GPU-..., as nvidia-smi -L prints it)", node.ID, uuid)
			}
			if listed[uuid] {
				return fmt.Errorf("node %s lists %s twice", node.ID, uuid)
			}
			listed[uuid] = true
		}
	}
	return s.validateHosts()
}

// validateHosts: host ids are unique, a node is on at most one host, a
// controller controls one host, max_slowdown is in [0, 0.5), and a device is
// listed once and is CPU or a GPU declared for one of the host's nodes. A
// GPU whose nodes are on two hosts is an error.
func (s Spec) validateHosts() error {
	hosts := map[string]bool{}
	hostOf := map[string]string{}
	controllers := map[string]string{}
	for _, host := range s.Hosts {
		if host.ID == "" {
			return errors.New("every [[hosts]] needs an id")
		}
		if hosts[host.ID] {
			return fmt.Errorf("host %s is declared twice", host.ID)
		}
		hosts[host.ID] = true
		if len(host.Nodes) == 0 {
			return fmt.Errorf("host %s lists no nodes", host.ID)
		}
		for _, node := range host.Nodes {
			if node == "" {
				return fmt.Errorf("host %s lists an empty node id", host.ID)
			}
			if other, taken := hostOf[node]; taken {
				if other == host.ID {
					return fmt.Errorf("host %s lists node %s twice", host.ID, node)
				}
				return fmt.Errorf("node %s is on hosts %s and %s (a node belongs to at most one host)", node, other, host.ID)
			}
			hostOf[node] = host.ID
		}
		if host.Controller != "" {
			if other, taken := controllers[host.Controller]; taken {
				return fmt.Errorf("node %s controls hosts %s and %s (a controller runs on one host)", host.Controller, other, host.ID)
			}
			controllers[host.Controller] = host.ID
		}
		if err := checkSlowdown(host.MaxSlowdown); err != nil {
			return fmt.Errorf("host %s: %w", host.ID, err)
		}
	}
	gpuHost := map[string]string{}
	gpus := s.GPUs()
	for _, uuid := range slices.Sorted(maps.Keys(gpus)) {
		for _, node := range gpus[uuid] {
			host, ok := hostOf[node]
			if !ok {
				continue
			}
			if other, seen := gpuHost[uuid]; seen && other != host {
				pair := []string{other, host}
				slices.Sort(pair)
				return fmt.Errorf("%s is declared for nodes on hosts %s and %s (a GPU is in one host)", uuid, pair[0], pair[1])
			}
			gpuHost[uuid] = host
		}
	}
	for _, host := range s.Hosts {
		listed := map[string]bool{}
		for _, device := range host.Devices {
			switch {
			case device.ID == "":
				return fmt.Errorf("host %s: every [[hosts.devices]] needs an id", host.ID)
			case listed[device.ID]:
				return fmt.Errorf("host %s lists device %s twice", host.ID, device.ID)
			case device.ID != CPU && gpuHost[device.ID] != host.ID:
				return fmt.Errorf("host %s: device %q is neither %q nor a GPU declared for one of its nodes", host.ID, device.ID, CPU)
			}
			listed[device.ID] = true
			if device.MaxSlowdown != nil {
				if err := checkSlowdown(*device.MaxSlowdown); err != nil {
					return fmt.Errorf("host %s device %s: %w", host.ID, device.ID, err)
				}
			}
		}
	}
	return nil
}

func checkSlowdown(v float64) error {
	if math.IsNaN(v) || v < 0 || v >= MaxSlowdownLimit {
		return fmt.Errorf("max_slowdown %v is outside [0, %v)", v, MaxSlowdownLimit)
	}
	return nil
}

// GPUs maps each declared GPU UUID to its nodes, sorted (several: a shared GPU).
func (s Spec) GPUs() map[string][]string {
	out := map[string][]string{}
	for _, node := range s.Nodes {
		for _, uuid := range node.GPUs {
			out[uuid] = append(out[uuid], node.ID)
		}
	}
	for _, nodes := range out {
		sort.Strings(nodes)
	}
	return out
}

// NodeIDs are the declared nodes, those of hosts and their controllers,
// sorted and without duplicates.
func (s Spec) NodeIDs() []string {
	var out []string
	for _, node := range s.Nodes {
		out = append(out, node.ID)
	}
	for _, host := range s.Hosts {
		out = append(out, host.Nodes...)
		if host.Controller != "" {
			out = append(out, host.Controller)
		}
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// Normalized is h with its nodes and devices sorted, for comparing
// declarations.
func (h Host) Normalized() Host {
	h.Nodes = slices.Sorted(slices.Values(h.Nodes))
	h.Devices = slices.SortedFunc(slices.Values(h.Devices), func(a, b Device) int { return strings.Compare(a.ID, b.ID) })
	return h
}

// DevicePolicy is the effective policy of one device of a host: its override,
// else the host's value.
type DevicePolicy struct {
	HostID   string `json:"host_id"`
	DeviceID string `json:"device_id"`
	// Kind is "gpu" or "cpu".
	Kind        string  `json:"kind"`
	MaxSlowdown float64 `json:"max_slowdown"`
	Control     bool    `json:"control"`
	// Controller is the host's controller node; nil: nothing may be changed.
	Controller *string `json:"controller,omitempty"`
}

// Controllable reports whether the device's settings may be changed: it is
// under control and its host names a controller.
func (p DevicePolicy) Controllable() bool { return p.Control && p.Controller != nil }

// DevicePolicies are the effective policies of every host's devices: each GPU
// declared for one of its nodes, then CPU, by host.
func (s Spec) DevicePolicies() []DevicePolicy {
	gpus := s.GPUs()
	var out []DevicePolicy
	for _, host := range s.Hosts {
		onHost := map[string]bool{}
		for _, node := range host.Nodes {
			onHost[node] = true
		}
		var uuids []string
		for uuid, nodes := range gpus {
			if slices.ContainsFunc(nodes, func(n string) bool { return onHost[n] }) {
				uuids = append(uuids, uuid)
			}
		}
		sort.Strings(uuids)
		overrides := map[string]Device{}
		for _, device := range host.Devices {
			overrides[device.ID] = device
		}
		var controller *string
		if host.Controller != "" {
			controller = &host.Controller
		}
		for _, id := range append(uuids, CPU) {
			policy := DevicePolicy{HostID: host.ID, DeviceID: id, Kind: "gpu", MaxSlowdown: host.MaxSlowdown, Control: true, Controller: controller}
			if id == CPU {
				policy.Kind = "cpu"
			}
			if o, ok := overrides[id]; ok {
				if o.MaxSlowdown != nil {
					policy.MaxSlowdown = *o.MaxSlowdown
				}
				if o.Control != nil {
					policy.Control = *o.Control
				}
			}
			out = append(out, policy)
		}
	}
	return out
}
