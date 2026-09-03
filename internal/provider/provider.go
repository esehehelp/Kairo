package provider

import (
	"context"

	"kairo/internal/store"
)

type Snapshot struct {
	Resources    []store.ResourceInstance
	Observations []store.Observation
	Claims       []store.ExternalClaim
}
type Provider interface {
	ID() string
	Observe(context.Context) (Snapshot, error)
	Prepare(context.Context, store.ResourceInstance) error
	Release(context.Context, store.ResourceInstance) error
}
