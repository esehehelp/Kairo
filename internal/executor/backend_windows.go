//go:build windows

package executor

import "context"

// sampleUsage samples a WSL executor's attempts: the Linux processes carrying
// each attempt's KAIRO_ATTEMPT_ID, in one wsl.exe run per round (sampleWSL).
func (b wslBackend) sampleUsage(ctx context.Context, targets []usageTarget) map[string]usageSample {
	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.attemptID
	}
	samples, err := sampleWSL(ctx, b.distro, ids)
	if err != nil {
		b.e.Logger.Warn("attempt usage unknown", "error", err)
	}
	return samples
}
