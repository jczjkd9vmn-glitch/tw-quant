$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

$LogDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$LogPath = Join-Path $LogDir ("daily_{0}.log" -f (Get-Date -Format "yyyyMMdd"))
$VenvActivate = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"

function Write-Log {
    param([string]$Message)
    $Line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $Line | Tee-Object -FilePath $LogPath -Append
}

function Invoke-LoggedCommand {
    param(
        [string]$Executable,
        [string[]]$Arguments
    )

    Write-Log ("Running: {0} {1}" -f $Executable, ($Arguments -join " "))
    & $Executable @Arguments 2>&1 | Tee-Object -FilePath $LogPath -Append
    if ($LASTEXITCODE -ne 0) {
        throw ("Command failed with exit code {0}: {1} {2}" -f $LASTEXITCODE, $Executable, ($Arguments -join " "))
    }
}

Write-Log "Daily task started"
Write-Log ("ProjectRoot={0}" -f $ProjectRoot)

if (-not (Test-Path $VenvActivate)) {
    throw ("Virtual environment activation script not found: {0}" -f $VenvActivate)
}

. $VenvActivate

Invoke-LoggedCommand -Executable "python" -Arguments @(
    "scripts/backfill.py",
    "--days", "10",
    "--timeout", "30",
    "--retries", "3",
    "--sleep", "1"
)

Invoke-LoggedCommand -Executable "python" -Arguments @(
    "scripts/run_all_daily.py",
    "--capital", "1000000"
)

Write-Log "Daily task completed"
