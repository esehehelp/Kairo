//go:build !(windows && (amd64 || arm64))

package provider

// wslcVRAM: a wslc VM runs only on Windows; elsewhere it holds nothing.
type wslcVRAM struct{}

func (*wslcVRAM) read() (map[string]int64, error) { return nil, nil }
