//go:build !linux

package fabric

import (
	"context"
	"errors"
	"time"
)

// errUnsupported: the fabric's interface is made on Linux only for now
// (issue #47: Windows nodes join later).
var errUnsupported = errors.New("the fabric is not yet made on this system (Linux only)")

type noLink struct{}

func newLink() link { return noLink{} }

func (noLink) ensure(context.Context, string, string, int, string, int) error { return errUnsupported }
func (noLink) peers(context.Context, string) (map[string]time.Time, error) {
	return nil, errUnsupported
}
func (noLink) setPeer(context.Context, string, string, string, string) error { return errUnsupported }
func (noLink) removePeer(context.Context, string, string) error              { return errUnsupported }
