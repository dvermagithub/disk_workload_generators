# C:\Zerto\disk_workload_service.ps1
# Idempotent disk workload for imaging, designed to run as scheduled task at startup - deepak verma

$BaseDir = "C:\Zerto"
$DataFile = Join-Path $BaseDir "disk_workload.dat"
$LogFile = Join-Path $BaseDir "disk_workload.log"
$PidFile = Join-Path $BaseDir "disk_workload.pid"

# Config, tune $DailyWritePercent for test intensity
$FileSizeGB = 5
$DailyWritePercent = 100.0
$BlockSmall = 4KB
$BlockLarge = 128KB

# Helpers
function Log($m) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    "$ts $m" | Out-File -FilePath $LogFile -Append -Encoding utf8
}

# Ensure folder
if (-not (Test-Path $BaseDir)) {
    New-Item -ItemType Directory -Path $BaseDir -Force | Out-Null
}

# Single instance guard
if (Test-Path $PidFile) {
    try {
        $existing = Get-Content $PidFile -ErrorAction Stop | ForEach-Object { $_.Trim() } 
        if ($existing -match '^\d+$') {
            $proc = Get-Process -Id [int]$existing -ErrorAction SilentlyContinue
            if ($proc) {
                Log "Workload already running with pid $existing, exiting"
                exit 0
            }
        }
    } catch { }
    Remove-Item $PidFile -ErrorAction SilentlyContinue
}

# Save my PID
$MyPid = $PID
Set-Content -Path $PidFile -Value $MyPid -Encoding ascii

# Precompute rates
$FileSize = [int64]($FileSizeGB * 1GB)
$BytesPerDay = [int64]([math]::Round($FileSize * $DailyWritePercent))
$BytesPerSecond = [double]$BytesPerDay / 86400.0

Log "Starting workload pid $MyPid file ${FileSizeGB}GB dailyPercent $DailyWritePercent bytes/sec $([math]::Round($BytesPerSecond))"

# Create file if missing
if (-not (Test-Path $DataFile)) {
    Log "Creating workload file $DataFile size ${FileSizeGB}GB"
    $fs = [System.IO.File]::Create($DataFile)
    $fs.SetLength($FileSize)
    $fs.Close()
    Log "File created"
} else {
    Log "Using existing file"
}
