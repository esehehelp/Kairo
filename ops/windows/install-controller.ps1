# Install this host's Kairo device controller: -New (a kairo.exe build: ops/build.sh) as
# %ProgramFiles%\Kairo\kairo-controller.exe and the scheduled task "kairo-controller" that runs it at boot as SYSTEM
# (it changes device settings: nvidia-smi, powercfg). A machine service's files are a machine service's places: its
# configuration %ProgramData%\Kairo\controller.toml (it names the CA and its state file, controller.state.json, there
# too, and holds the controller's token), which only SYSTEM and Administrators may read. -Config, -State and -CA copy
# those in (after the folder is closed to others). Run elevated:
#   Start-Process pwsh -Verb RunAs -Wait -ArgumentList '-NoProfile -File D:\Dev\Kairo\ops\windows\install-controller.ps1 -New <build> [-Config <toml>] [-State <json>] [-CA <pem>]'
# Stopping the controller may leave the devices as set; it reads its state file and applies the desired settings
# again at its first poll. No attempt is touched.
param([Parameter(Mandatory)][string]$New, [string]$Config, [string]$State, [string]$CA)
$ErrorActionPreference = 'Stop'
$bin = Join-Path $env:ProgramFiles 'Kairo'
$data = Join-Path $env:ProgramData 'Kairo'
$cfg = Join-Path $data 'controller.toml'
New-Item -ItemType Directory -Force $bin, $data | Out-Null
# only SYSTEM and Administrators: the configuration holds a token
& icacls $data /inheritance:r /grant:r '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls $data failed (exit $LASTEXITCODE): the token would not be kept to SYSTEM and Administrators" }
$log = Join-Path $data 'install-controller.log'
function Log($m) { "$(Get-Date -Format o) $m" | Tee-Object -FilePath $log -Append }
$version = (& $New version 2>$null | Select-Object -First 1)
if ($version -notmatch '^kairo cli [0-9a-f]{12}$') { Log "refused: unexpected build $version"; exit 1 }
if ($Config) { Copy-Item $Config $cfg -Force }
if ($State) { Copy-Item $State (Join-Path $data 'controller.state.json') -Force }
if ($CA) { Copy-Item $CA (Join-Path $data 'ca.pem') -Force }
if (-not (Test-Path $cfg)) { Log "refused: $cfg is missing (-Config)"; exit 1 }

Stop-ScheduledTask -TaskName 'kairo-controller' -ErrorAction SilentlyContinue
for ($i = 0; $i -lt 60 -and (Get-Process -Name 'kairo-controller' -ErrorAction SilentlyContinue); $i++) { Start-Sleep -Seconds 1 }
if (Get-Process -Name 'kairo-controller' -ErrorAction SilentlyContinue) { Log 'refused: the controller did not stop within 60 s'; exit 1 }
$exe = Join-Path $bin 'kairo-controller.exe'
if (Test-Path $exe) { Move-Item $exe (Join-Path $bin "kairo-controller-before-$(Get-Date -Format yyyyMMddHHmmss).exe") -Force }
Copy-Item $New $exe -Force

$action = New-ScheduledTaskAction -Execute $exe -Argument "controller --config `"$cfg`"" -WorkingDirectory $data
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0 -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName 'kairo-controller' -Description 'Kairo device controller' -Action $action `
  -Trigger (New-ScheduledTaskTrigger -AtStartup) -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName 'kairo-controller'
Start-Sleep -Seconds 5
Log "installed $version as $exe ($((Get-ScheduledTask -TaskName 'kairo-controller').State))"
