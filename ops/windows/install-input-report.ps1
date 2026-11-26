# Install the owner's input reporter (README: Owner use): `kairo input-report`, run in the owner's session at logon,
# writes when they last used the keyboard or mouse to %LOCALAPPDATA%\kairo\input.json, which this host's agent reads
# (the agent runs in session 0, where that input is not seen). It runs the agent's binary
# (%LOCALAPPDATA%\Programs\kairo\kairo-agent.exe) without a window, as the scheduled task "kairo-input-report". No
# elevation:
#   pwsh -NoProfile -File D:\Dev\Kairo\ops\windows\install-input-report.ps1
# A newer agent binary is taken up at the next logon (replace-agent.ps1 renames the old one aside; the reporter
# already running goes on with it).
$ErrorActionPreference = 'Stop'
$exe = Join-Path $env:LOCALAPPDATA 'Programs\kairo\kairo-agent.exe'
if (-not (Test-Path $exe)) { throw "$exe is missing: install the agent first (install-agent.ps1)" }
& $exe help input-report *> $null
if ($LASTEXITCODE -ne 0) { throw "$exe has no input-report: replace the agent with a newer build first" }
$user = "$env:USERDOMAIN\$env:USERNAME"
$action = New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32\conhost.exe') -Argument "--headless `"$exe`" input-report"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) `
  -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName 'kairo-input-report' -Description "Kairo: the owner's last keyboard or mouse input, for the agent" `
  -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName 'kairo-input-report'
"installed: the task kairo-input-report runs $exe input-report at logon (started now)"
