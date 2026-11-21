//go:build !linux && !windows

package agentclient

func uptimeSeconds() (float64, bool) { return 0, false }
