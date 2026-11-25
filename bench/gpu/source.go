// Package gpubench carries the source of kairo-bench-gpu (CUDA C++, cuBLAS,
// NVML), which a GPU bench builds in its container with the image's CUDA
// toolkit (hwbench.BuildGPU): no node needs the program installed.
package gpubench

import _ "embed"

// Source is kairo-bench-gpu.cu.
//
//go:embed kairo-bench-gpu.cu
var Source []byte
