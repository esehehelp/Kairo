//go:build !linux

package executor

import (
	"context"
	"time"
)

// Containers run on Linux only (container_linux.go).
func runContainerHelpers([]string) {}

const (
	containerSweepInterval = time.Hour
	containerSweepTimeout  = time.Minute
)

func (e *Local) sweepContainers(context.Context) {}
