# Keeps the Kairo agent of this Windows host running. install-agent.ps1 copies this script beside the agent and
# registers the S4U scheduled task "kairo-agent" that starts it at boot; a second copy exits (the mutex is held),
# and an agent already running is adopted.
#   %LOCALAPPDATA%\Programs\kairo\kairo-agent.exe        the agent (a kairo build)
#   %APPDATA%\kairo\agent.toml                           its configuration
#   %LOCALAPPDATA%\kairo\logs\agent-<time>.{stdout,stderr}.log, agent-supervisor.log
# While %LOCALAPPDATA%\kairo\agent-supervisor.hold exists it starts nothing: replace-agent.ps1 makes the file before
# it stops the agent and removes it once the new one is in place.
$ErrorActionPreference = 'Stop'
$bin = Join-Path $env:LOCALAPPDATA 'Programs\kairo'
$state = Join-Path $env:LOCALAPPDATA 'kairo'
$logs = Join-Path $state 'logs'
$exe = Join-Path $bin 'kairo-agent.exe'
$cfg = Join-Path $env:APPDATA 'kairo\agent.toml'
$hold = Join-Path $state 'agent-supervisor.hold'
New-Item -ItemType Directory -Force $logs | Out-Null

$mutex = New-Object System.Threading.Mutex($false, 'Global\kairo-agent-supervisor')
if (-not $mutex.WaitOne(0)) { exit 0 }

function Log([string]$msg) {
  Add-Content -Path (Join-Path $logs 'agent-supervisor.log') -Value ('{0:o} {1}' -f (Get-Date), $msg)
}

# the agent, not an attempt's shim (kairo-agent.exe too, run as __kairo-shim)
function Find-Agent {
  Get-CimInstance Win32_Process -Filter "Name='kairo-agent.exe'" |
    Where-Object { $_.CommandLine -like '* agent --config *' -and $_.CommandLine.Contains($cfg) } | Select-Object -First 1
}

# The agent process this supervisor watches: when it is gone, its exit code and time are logged (issue #19: an agent
# that exits without a line of its own, killed from outside say, leaves at least that). The handle is taken while it
# runs, or the exit code cannot be read afterwards.
function Watch-Agent([int]$id) {
  try { $p = Get-Process -Id $id -ErrorAction Stop; $null = $p.Handle; return $p } catch { return $null }
}
function Log-Exit($p) {
  if (-not $p) { return }
  try {
    $p.WaitForExit(1000) | Out-Null
    $code = $p.ExitCode
    Log ("agent pid {0} exited with code {1} (0x{2:X8}) at {3:o}" -f $p.Id, $code, $code, $p.ExitTime)
  } catch { Log "agent pid $($p.Id) is gone; its exit code cannot be read ($($_.Exception.Message))" }
}

$starts = New-Object System.Collections.Generic.List[datetime]
$held = $false
$found = Find-Agent
$agent = if ($found) { Watch-Agent $found.ProcessId } else { $null }
Log "supervisor started (agent=$($found.ProcessId))"
while ($true) {
  if (Test-Path $hold) {
    if (-not $held) { Log 'hold file present: not starting the agent' }
    $held = $true
  } else {
    if ($held) { Log 'hold file gone' }
    $held = $false
    $running = Find-Agent
    if ($running -and (-not $agent -or $agent.Id -ne $running.ProcessId)) {
      $agent = Watch-Agent $running.ProcessId  # one started by replace-agent.ps1
    }
    if (-not $running) {
      Log-Exit $agent
      $agent = $null
      # more than 5 starts in 10 minutes: wait 5 minutes before the next
      $recent = @($starts | Where-Object { $_ -gt (Get-Date).AddMinutes(-10) })
      if ($recent.Count -ge 5) {
        Log "agent exited $($recent.Count) times in 10 minutes; waiting 5 minutes"
        Start-Sleep -Seconds 300
        $starts.Clear()
        continue
      }
      $ts = Get-Date -Format yyyyMMdd-HHmmss
      $p = Start-Process -FilePath $exe -ArgumentList "agent --config `"$cfg`"" -WorkingDirectory $state -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $logs "agent-$ts.stdout.log") `
        -RedirectStandardError (Join-Path $logs "agent-$ts.stderr.log")
      $null = $p.Handle
      $agent = $p
      $starts.Add((Get-Date))
      Log "agent not running; started pid $($p.Id) (logs agent-$ts.*)"
    }
  }
  Start-Sleep -Seconds 15
}
