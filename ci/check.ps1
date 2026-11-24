# The Windows checks of ci/check.sh, for a Kairo task on windows-local (ci/tasks.toml.in): an attempt there is confined
# (README: Confinement), it runs at Low integrity, and MSYS and Git Bash do not start so. The tree this script is in
# (an extracted commit) is checked; the first argument is the check:
#   windows      go vet, go test                 (PostgreSQL at KAIRO_TEST_PG, default 127.0.0.1:55432)
#   sdk-python   the Python SDK's tests
#   sdk-rust     the Rust SDK's tests
# Everything written goes to the attempt's scratch (the tree, TEMP) or the project's cache (KAIRO_CACHE_DIR: Go's,
# uv's and cargo's caches), the only places a confined attempt may write.
param([Parameter(Mandatory = $true)][string]$Check)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$env:PATH = "C:\Program Files\Go\bin;$env:USERPROFILE\.local\bin;$env:USERPROFILE\.cargo\bin;$env:PATH"
$cache = $env:KAIRO_CACHE_DIR
if (-not $cache) { $cache = Join-Path $env:LOCALAPPDATA 'kairo\ci-windows\cache' }
$env:GOCACHE = Join-Path $cache 'go-build'
$env:GOMODCACHE = Join-Path $cache 'go-mod'
$env:GOTELEMETRY = 'off'

function Step([string]$what) { Write-Host "`n== $what" }
function Run([string]$exe) {
  & $exe @args
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
function Need-PG {
  $dsn = $env:KAIRO_TEST_PG
  if (-not $dsn) { $dsn = 'postgres://kairo:kairo-test@127.0.0.1:55432/kairo' }
  $hostport = ($dsn -replace '^[a-z]+://[^@]*@([^/?]+).*$', '$1')
  $h, $p = $hostport -split ':'
  $client = New-Object System.Net.Sockets.TcpClient
  try { $client.Connect($h, [int]$p) } catch {
    [Console]::Error.WriteLine("PostgreSQL for the store tests is not reachable (KAIRO_TEST_PG or 127.0.0.1:55432)")
    exit 2
  } finally { $client.Dispose() }
}

Step "kairo-ci $Check on $env:COMPUTERNAME ($(go version))"
switch ($Check) {
  'windows' {
    Step 'go vet'
    Run go vet ./...
    Need-PG
    Step 'go test'
    Run go test -count=1 ./...
  }
  'sdk-python' {
    Step 'pytest (Python SDK)'
    $env:UV_CACHE_DIR = Join-Path $cache 'uv'
    Run uv run --project sdk/python pytest -q
  }
  'sdk-rust' {
    Step 'cargo test (Rust SDK)'
    if (-not $env:RUSTUP_HOME) { $env:RUSTUP_HOME = Join-Path $env:USERPROFILE '.rustup' }
    $env:CARGO_HOME = Join-Path $cache 'cargo-home'
    $env:CARGO_TARGET_DIR = Join-Path $cache 'cargo-target'
    Run cargo test --manifest-path sdk/rust/Cargo.toml
  }
  default {
    [Console]::Error.WriteLine("unknown check $Check")
    exit 2
  }
}
Step "kairo-ci $Check passed"
