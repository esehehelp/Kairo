// Package datainput is what an execution declares it reads
// ([[tasks.execution.inputs]]): data handed to its attempts by path, never
// named by a path the task chose (store/inputs.go).
package datainput

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// Input is data an attempt reads, handed to it as KAIRO_INPUT_<NAME> (Name
// in upper case): exactly one of
//   - Task: the output of the latest run of that task of the same project,
//     which the declaring task must depend on (orchestrated tasks only);
//   - Data: the data object of the project of that name (an import's, say),
//     the newest packed;
//   - Object: a data object by id.
type Input struct {
	Name   string `toml:"name" json:"name"`
	Task   string `toml:"task" json:"task,omitempty"`
	Data   string `toml:"data" json:"data,omitempty"`
	Object string `toml:"object" json:"object,omitempty"`
}

// MaxInputs bounds the inputs of one execution.
const MaxInputs = 32

var namePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// EnvName is the variable an input's path is handed in.
func EnvName(name string) string { return "KAIRO_INPUT_" + strings.ToUpper(name) }

// Validate checks inputs: at most MaxInputs; names a letter then letters,
// digits and _ (64 at most), distinct without case (they name variables);
// exactly one source each; Task only where taskAllowed (an orchestrated
// task; the caller checks it is a dependency).
func Validate(inputs []Input, taskAllowed bool) error {
	if len(inputs) > MaxInputs {
		return fmt.Errorf("at most %d inputs, not %d", MaxInputs, len(inputs))
	}
	seen := map[string]bool{}
	for i := range inputs {
		in := &inputs[i]
		in.Name, in.Task, in.Data, in.Object = strings.TrimSpace(in.Name), strings.TrimSpace(in.Task), strings.TrimSpace(in.Data), strings.TrimSpace(in.Object)
		if !namePattern.MatchString(in.Name) {
			return fmt.Errorf("input %d: name %q is not a letter then letters, digits and _ (64 at most)", i, in.Name)
		}
		key := strings.ToUpper(in.Name)
		if seen[key] {
			return fmt.Errorf("input %q is named twice", in.Name)
		}
		seen[key] = true
		sources := 0
		for _, v := range []string{in.Task, in.Data, in.Object} {
			if v != "" {
				sources++
			}
		}
		if sources != 1 {
			return fmt.Errorf("input %q names exactly one of task, data and object", in.Name)
		}
		if in.Task != "" && !taskAllowed {
			return fmt.Errorf("input %q: task inputs are for tasks of a project (kairo project apply)", in.Name)
		}
		for _, v := range []string{in.Task, in.Data, in.Object} {
			if len(v) > 200 || strings.ContainsFunc(v, unicode.IsControl) {
				return fmt.Errorf("input %q: %q is not a name", in.Name, v)
			}
		}
	}
	return nil
}
