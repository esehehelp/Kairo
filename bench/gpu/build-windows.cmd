@echo off
rem Builds bench\gpu\kairo-bench-gpu.exe with CUDA 13.x and MSVC (x64).
rem
rem   build-windows.cmd                     (cmd.exe)
rem   cmd //c bench/gpu/build-windows.cmd   (Git Bash)
rem
rem Locates vcvars64.bat via vswhere (fallback: VS 2022 Build Tools) unless
rem an x64 developer environment is already active. nvcc is taken from PATH,
rem else from %CUDA_PATH%\bin.
rem
rem Optional environment:
rem   BUILD_REV   build string to embed; skips git (default: git short HEAD,
rem               "-dirty" if bench\gpu has uncommitted changes, else "unknown")
rem   CUBLAS_DLL  cuBLAS DLL to delay-load (default cublas64_13.dll)
rem
rem The executable links cudart statically and delay-loads cuBLAS, so it only
rem needs cublas64_13.dll + cublasLt64_13.dll (on PATH or next to the exe)
rem when running gemm or gemv. nvml.dll is loaded at run time from System32.

setlocal EnableExtensions
set "HERE=%~dp0"
if "%HERE:~-1%"=="\" set "HERE=%HERE:~0,-1%"
if not defined CUBLAS_DLL set "CUBLAS_DLL=cublas64_13.dll"

rem ---- MSVC x64 environment -------------------------------------------------
if /i "%VSCMD_ARG_TGT_ARCH%"=="x64" goto :have_vs
set "VSWHERE_DIR=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer"
set "VSINST="
if not exist "%VSWHERE_DIR%\vswhere.exe" goto :vs_fallback
rem Run vswhere via PATH (a quoted "(x86)" path inside for /f is mangled by
rem cmd /c); vcvars64.bat also expects vswhere.exe on PATH.
set "PATH=%VSWHERE_DIR%;%PATH%"
for /f "usebackq delims=" %%i in (`vswhere.exe -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSINST=%%i"
:vs_fallback
if not defined VSINST set "VSINST=%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools"
set "VCVARS=%VSINST%\VC\Auxiliary\Build\vcvars64.bat"
if exist "%VCVARS%" goto :call_vcvars
echo build-windows: vcvars64.bat not found at "%VCVARS%" 1>&2
exit /b 1
:call_vcvars
echo build-windows: MSVC environment from "%VCVARS%"
call "%VCVARS%" >nul
if errorlevel 1 goto :vcvars_failed
:have_vs
where cl >nul 2>nul
if errorlevel 1 goto :vcvars_failed

rem ---- nvcc ------------------------------------------------------------------
set "NVCC=nvcc"
where nvcc >nul 2>nul
if not errorlevel 1 goto :have_nvcc
set "NVCC=%CUDA_PATH%\bin\nvcc.exe"
if exist "%NVCC%" goto :have_nvcc
echo build-windows: nvcc not found on PATH or in %%CUDA_PATH%%\bin 1>&2
exit /b 1
:have_nvcc

rem ---- build revision (read-only git) ---------------------------------------
if defined BUILD_REV goto :have_rev
set "BUILD_REV=unknown"
where git >nul 2>nul
if errorlevel 1 goto :have_rev
for /f "usebackq delims=" %%i in (`git -C "%HERE%" rev-parse --short^=12 HEAD 2^>nul`) do set "BUILD_REV=%%i"
if "%BUILD_REV%"=="unknown" goto :have_rev
set "DIRTY="
for /f "usebackq delims=" %%i in (`git -C "%HERE%" status --porcelain -- . 2^>nul`) do set "DIRTY=1"
if defined DIRTY set "BUILD_REV=%BUILD_REV%-dirty"
:have_rev
echo build-windows: build %BUILD_REV%

rem ---- compile ---------------------------------------------------------------
rem sm_86 SASS for Ampere (RTX 3080/3060) plus compute_86 PTX for newer GPUs.
pushd "%HERE%"
"%NVCC%" -O3 -std=c++17 ^
  -gencode arch=compute_86,code=sm_86 -gencode arch=compute_86,code=compute_86 ^
  -Xcompiler "/W4 /EHsc /MT /utf-8" ^
  -DKAIRO_BENCH_BUILD=\"%BUILD_REV%\" ^
  -o kairo-bench-gpu.exe kairo-bench-gpu.cu ^
  -lcublas -ldelayimp -Xlinker /DELAYLOAD:%CUBLAS_DLL%
set "RC=%ERRORLEVEL%"
popd
if not "%RC%"=="0" echo build-windows: nvcc failed with exit code %RC% 1>&2
if "%RC%"=="0" echo build-windows: wrote %HERE%\kairo-bench-gpu.exe
exit /b %RC%

:vcvars_failed
echo build-windows: could not set up the MSVC x64 environment (cl.exe not found) 1>&2
exit /b 1
