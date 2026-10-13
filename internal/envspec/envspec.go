// Package envspec validates the environment variables an execution declares
// (ProjectSpec [tasks.execution.env], execution TOML [env], the API's "env").
package envspec

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Validate checks env. Names must be non-empty, without '=' or NUL and
// without surrounding spaces; values must not contain NUL. KAIRO_* and WSLENV
// belong to Kairo, and so does CUDA_VISIBLE_DEVICES when the execution holds
// GPUs (it is set from the lease); a CPU-only execution may set it, say to -1.
func Validate(env map[string]string, exclusiveGPU bool) error {
	for _, name := range Names(env) {
		switch {
		case name == "" || strings.TrimSpace(name) != name:
			return fmt.Errorf("env name %q is empty or has surrounding spaces", name)
		case strings.ContainsAny(name, "=\x00"):
			return fmt.Errorf("env name %q contains '=' or NUL", name)
		case strings.Contains(env[name], "\x00"):
			return fmt.Errorf("env %s value contains NUL", name)
		case Reserved(name):
			return fmt.Errorf("env %s is set by Kairo", name)
		case exclusiveGPU && strings.ToUpper(name) == "CUDA_VISIBLE_DEVICES":
			return errors.New("env CUDA_VISIBLE_DEVICES is set by Kairo from the GPU lease")
		}
	}
	return nil
}

// Normalize validates env and returns it, or nil when it is empty.
func Normalize(env map[string]string, exclusiveGPU bool) (map[string]string, error) {
	if len(env) == 0 {
		return nil, nil
	}
	if err := Validate(env, exclusiveGPU); err != nil {
		return nil, err
	}
	return env, nil
}

// Reserved reports whether a variable belongs to Kairo whatever the execution
// declares: KAIRO_* and WSLENV, in any case.
func Reserved(name string) bool {
	upper := strings.ToUpper(name)
	return strings.HasPrefix(upper, "KAIRO_") || upper == "WSLENV"
}

// Names returns the names of env in sorted order.
func Names(env map[string]string) []string {
	return slices.Sorted(maps.Keys(env))
}

// Merge returns base with over's values on top; nil when both are empty.
func Merge(base, over map[string]string) map[string]string {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(over))
	maps.Copy(out, base)
	maps.Copy(out, over)
	return out
}
