//go:build !linux && !windows

package fabric

import (
	"context"
	"errors"
	"io"
)

// errUnsupported: the fabric's interface is made on Linux only for now
// (issue #47: Windows nodes join later).
var errUnsupported = errors.New("the fabric is not yet made on this system (Linux, and Windows in its wslc VM)")

const hostOnFabric = false

type noLink struct{}

func newLink() link { return noLink{} }

func (noLink) ensure(context.Context, string, string, int, string, int) error { return errUnsupported }
func (noLink) peers(context.Context, string) (map[string]peerSeen, error) {
	return nil, errUnsupported
}
func (noLink) setPeer(context.Context, string, string, string, string) error { return errUnsupported }
func (noLink) removePeer(context.Context, string, string) error              { return errUnsupported }

// LinkCommand: the fabric-link command is Linux's (linkcmd_linux.go).
func LinkCommand(context.Context, []string, io.Writer) error { return errUnsupported }
