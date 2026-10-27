// Package machine parses the machine declaration accepted by `kairo machine
// apply`: which node holds which GPU. The declaration is the authority on GPU
// ownership; providers only observe what is there. A GPU declared for several
// nodes is SHARED: nodes on one host (containers that all see the card) may each
// schedule it, and a lease on it through any of them excludes the others.
package machine

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

// Spec is a whole machine declaration. It replaces the previous one: a GPU
// left out is no longer declared for any node.
type Spec struct {
	Nodes []Node `toml:"nodes" json:"nodes"`
}

type Node struct {
	ID string `toml:"id" json:"id"`
	// GPUs are NVIDIA UUIDs as nvidia-smi -L prints them (GPU-...).
	GPUs []string `toml:"gpus" json:"gpus"`
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

// Validate checks that every node is named once and lists a GPU once.
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

// NodeIDs are the declared nodes, sorted.
func (s Spec) NodeIDs() []string {
	var out []string
	for _, node := range s.Nodes {
		out = append(out, node.ID)
	}
	sort.Strings(out)
	return out
}
