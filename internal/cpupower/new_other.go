//go:build !linux && !(windows && (amd64 || arm64))

package cpupower

// New is this machine's meter: none is read on this platform.
func New() Meter { return None{} }
