# Replace this host's Kairo agent with -New (a kairo.exe build: ops/build.sh) and let the supervisor start it again.
# Run elevated: the agent runs in the S4U task's session 0, which an unelevated shell cannot stop.
#   Start-Process pwsh -Verb RunAs -Wait -ArgumentList '-NoProfile -File D:\Dev\Kairo\ops\windows\replace-agent.ps1 -New <build> -Adopting'
# A node holding a lease is refused unless -Adopting: an agent from main 2026-11-08 on runs its attempts under shims,
# which the new agent adopts (README: Agent restarts). Only the agent process is stopped (the shims are
# kairo-agent.exe too, and run its image, which is renamed aside, not overwritten).
param([Parameter(Mandatory)][string]$New, [switch]$Adopting)
$ErrorActionPreference = 'Stop'
$bin = Join-Path $env:LOCALAPPDATA 'Programs\kairo'
$state = Join-Path $env:LOCALAPPDATA 'kairo'
$exe = Join-Path $bin 'kairo-agent.exe'
$cfg = Join-Path $env:APPDATA 'kairo\agent.toml'
$hold = Join-Path $state 'agent-supervisor.hold'
$log = Join-Path $state 'logs\replace-agent.log'
function Log($m) { "$(Get-Date -Format o) $m" | Tee-Object -FilePath $log -Append }
$version = (& $New version 2>$null | Select-Object -First 1)
if ($version -notmatch '^kairo cli [0-9a-f]{12}$') { Log "refused: unexpected build $version"; exit 1 }
$node = if ((Get-Content $cfg -Raw) -match '(?ms)^\[node\]\s*\r?\n.*?^id\s*=\s*"([^"]+)"') { $Matches[1] } else { throw "no [node] id in $cfg" }
New-Item -ItemType File -Force $hold | Out-Null
# Checked right before the stop, with the hold file in place: an attempt that starts between an earlier check and
# the stop would be lost. Anything this node holds is a live attempt.
$held = @((& kairo resource status --json | ConvertFrom-Json).resources | Where-Object { $_.node_id -eq $node -and $_.derived_state -in 'leased', 'reserved' })
if ($held.Count -and $Adopting) {
  Log "$node holds leases ($(($held | ForEach-Object { $_.stable_identity }) -join ', ')); the new agent adopts their attempts"
} elseif ($held.Count) {
  Remove-Item $hold -Force
  Log "refused: $node holds leases ($(($held | ForEach-Object { $_.stable_identity }) -join ', ')); an attempt runs here"
  exit 1
}
foreach ($p in @(Get-CimInstance Win32_Process -Filter "Name='kairo-agent.exe'" | Where-Object { $_.CommandLine -like '* agent --config *' })) {
  Stop-Process -Id $p.ProcessId -Force
  Wait-Process -Id $p.ProcessId -Timeout 30 -ErrorAction SilentlyContinue
  Log "stopped agent pid $($p.ProcessId)"
}
# renamed, not copied over: the shims of running attempts keep the image open
$backup = Join-Path $bin "kairo-agent-before-$(Get-Date -Format yyyyMMddHHmmss).exe"
Move-Item $exe $backup -Force
# The stopped agent's image can stay locked for a moment. Retry; if it never goes through, start the old agent
# again rather than leave the node without one.
$copied = $false
for ($i = 0; $i -lt 30 -and -not $copied; $i++) {
  try { Copy-Item $New $exe -Force -ErrorAction Stop; $copied = $true } catch { Start-Sleep -Seconds 1 }
}
if (-not $copied) { Move-Item $backup $exe -Force }
Remove-Item $hold -Force
if (-not $copied) { Log "copy of $New failed; the old agent starts again"; exit 1 }
# The Linux build beside it is the init of wslc containers (ops/build.sh makes both). Unlinked, not written over:
# running containers hold hard links to the old one.
$linuxNew = $New -replace '\.exe$', '-linux'
if (Test-Path $linuxNew) {
  $linux = Join-Path $bin 'kairo-linux'
  if (Test-Path $linux) { Remove-Item $linux -Force }
  Copy-Item $linuxNew $linux
  Log "installed $linuxNew as $linux (the init of wslc containers)"
}
# The supervisor's script, current for its next start (the running one is not restarted: it would lose the agent's
# exit code it watches for, issue #19).
$supervisor = Join-Path $bin 'supervise-agent.ps1'
if ((Get-FileHash (Join-Path $PSScriptRoot 'supervise-agent.ps1')).Hash -ne (Get-FileHash $supervisor -ErrorAction SilentlyContinue).Hash) {
  Copy-Item (Join-Path $PSScriptRoot 'supervise-agent.ps1') $supervisor -Force
  Log "installed supervise-agent.ps1 (taken up when the supervisor next starts)"
}
Log "installed $version as $exe; the supervisor starts it within 15 s"
