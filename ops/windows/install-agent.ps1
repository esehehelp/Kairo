# Install this host's Kairo agent: -New (a kairo.exe build: ops/build.sh) as
# %LOCALAPPDATA%\Programs\kairo\kairo-agent.exe with supervise-agent.ps1 beside it, and the S4U scheduled task
# "kairo-agent" (at boot, as this user without an interactive session) that runs it. The configuration is
# %APPDATA%\kairo\agent.toml, written beforehand (README: Operating an installation). Run elevated:
#   Start-Process pwsh -Verb RunAs -Wait -ArgumentList '-NoProfile -File D:\Dev\Kairo\ops\windows\install-agent.ps1 -New <build>'
# An agent already running is stopped first, which is refused while this node holds a lease: an agent started
# with another configuration or log directory cannot adopt its attempts. A task "kairo-daemon" (the daemon ran on
# this host until 2026-10-28) is removed.
param([Parameter(Mandatory)][string]$New)
$ErrorActionPreference = 'Stop'
$bin = Join-Path $env:LOCALAPPDATA 'Programs\kairo'
$state = Join-Path $env:LOCALAPPDATA 'kairo'
$cfg = Join-Path $env:APPDATA 'kairo\agent.toml'
New-Item -ItemType Directory -Force $bin, (Join-Path $state 'logs') | Out-Null
$log = Join-Path $state 'logs\install-agent.log'
function Log($m) { "$(Get-Date -Format o) $m" | Tee-Object -FilePath $log -Append }
if (-not (Test-Path $cfg)) { Log "refused: $cfg is missing"; exit 1 }
$version = (& $New version 2>$null | Select-Object -First 1)
if ($version -notmatch '^kairo cli [0-9a-f]{12}$') { Log "refused: unexpected build $version"; exit 1 }
$node = if ((Get-Content $cfg -Raw) -match '(?ms)^\[node\]\s*\r?\n.*?^id\s*=\s*"([^"]+)"') { $Matches[1] } else { throw "no [node] id in $cfg" }
$held = @((& kairo resource status --json | ConvertFrom-Json).resources | Where-Object { $_.node_id -eq $node -and $_.derived_state -in 'leased', 'reserved' })
if ($held.Count) { Log "refused: $node holds leases ($(($held | ForEach-Object { $_.stable_identity }) -join ', ')); install when nothing runs here"; exit 1 }

# the supervisor first (it would start the agent again), then the agent: not its attempts' shims
Stop-ScheduledTask -TaskName 'kairo-agent' -ErrorAction SilentlyContinue
foreach ($p in @(Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" | Where-Object { $_.CommandLine -like '*supervise-agent.ps1*' })) {
  Stop-Process -Id $p.ProcessId -Force
  Log "stopped supervisor pid $($p.ProcessId)"
}
foreach ($p in @(Get-CimInstance Win32_Process -Filter "Name='kairo-agent.exe'" | Where-Object { $_.CommandLine -like '* agent --config *' })) {
  Stop-Process -Id $p.ProcessId -Force
  Wait-Process -Id $p.ProcessId -Timeout 30 -ErrorAction SilentlyContinue
  Log "stopped agent pid $($p.ProcessId)"
}
Copy-Item (Join-Path $PSScriptRoot 'supervise-agent.ps1') (Join-Path $bin 'supervise-agent.ps1') -Force
$exe = Join-Path $bin 'kairo-agent.exe'
if (Test-Path $exe) { Move-Item $exe (Join-Path $bin "kairo-agent-before-$(Get-Date -Format yyyyMMddHHmmss).exe") -Force }
Copy-Item $New $exe -Force

$pwsh = (Get-Command pwsh).Source
$user = "$env:USERDOMAIN\$env:USERNAME"
$action = New-ScheduledTaskAction -Execute $pwsh -Argument "-NoProfile -NonInteractive -File `"$(Join-Path $bin 'supervise-agent.ps1')`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) `
  -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType S4U -RunLevel Limited
Register-ScheduledTask -TaskName 'kairo-agent' -Description 'Kairo agent supervisor' `
  -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
if (Get-ScheduledTask -TaskName 'kairo-daemon' -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName 'kairo-daemon' -Confirm:$false
  Log 'removed the task kairo-daemon'
}
Start-ScheduledTask -TaskName 'kairo-agent'
Log "installed $version as $exe; task kairo-agent started"
