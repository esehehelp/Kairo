package main

import (
	"testing"

	"kairo/internal/store"
)

func TestEnergyTextScalesAndSaysWhatIsMissing(t *testing.T) {
	for _, tc := range []struct {
		energy store.AttemptEnergy
		want   string
	}{
		{store.AttemptEnergy{GPUJoules: 4428000, GPUSecondsCovered: 3600}, "GPU 1.23 kWh"},
		{store.AttemptEnergy{GPUJoules: 135000, GPUSecondsCovered: 900, Partial: true}, "GPU 37.5 Wh (partial)"},
		{store.AttemptEnergy{Partial: true}, "GPU not measured"},
	} {
		if got := energyText(tc.energy); got != tc.want {
			t.Fatalf("energyText(%+v) = %q, want %q", tc.energy, got, tc.want)
		}
	}
}
