# Builds kairo-bench-gpu for Linux (CUDA 12.x/13.x, x86_64 or aarch64).
#
#   make -C bench/gpu                   # links libcublas.so dynamically
#   make -C bench/gpu CUBLAS_STATIC=1   # links cuBLAS statically (larger binary,
#                                       # no libcublas/libcublasLt needed at run time)
#   make -C bench/gpu BUILD_REV=abc     # embed a given build string, skip git
#   make -C bench/gpu clean
#
# cudart is linked statically (nvcc default); NVML is dlopen'ed at run time
# (libnvidia-ml.so.1, optional). Default build string: git short HEAD (12),
# "-dirty" if bench/gpu has uncommitted changes, "unknown" without git.

CUDA_HOME ?= /usr/local/cuda
NVCC ?= $(CUDA_HOME)/bin/nvcc
CUBLAS_STATIC ?= 0

HERE := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SRC := $(HERE)/kairo-bench-gpu.cu
TARGET := $(HERE)/kairo-bench-gpu

ifeq ($(origin BUILD_REV),undefined)
BUILD_REV := $(shell r=$$(git -C '$(HERE)' rev-parse --short=12 HEAD 2>/dev/null) && \
  { [ -z "$$(git -C '$(HERE)' status --porcelain -- . 2>/dev/null)" ] || r="$$r-dirty"; \
    echo "$$r"; } || echo unknown)
endif

# sm_86 SASS for Ampere (RTX 3080/3060) plus compute_86 PTX for newer GPUs.
GENCODE := -gencode arch=compute_86,code=sm_86 -gencode arch=compute_86,code=compute_86
NVCCFLAGS := -O3 -std=c++17 $(GENCODE) -Xcompiler -Wall,-Wextra

ifeq ($(CUBLAS_STATIC),1)
CUBLAS_LIBS := -lcublas_static -lcublasLt_static -lculibos
else
CUBLAS_LIBS := -lcublas
endif
LDLIBS := -L$(CUDA_HOME)/lib64 $(CUBLAS_LIBS) -ldl -lpthread -lrt

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC) $(HERE)/Makefile
	$(NVCC) $(NVCCFLAGS) -DKAIRO_BENCH_BUILD='"$(BUILD_REV)"' -o $@ $(SRC) $(LDLIBS)

clean:
	rm -f $(TARGET)
