param(
    [switch]$Fast,
    [string]$OutputDirectory = ''
)

# Single-command Windows launcher for interview-only end-to-end measurements
# and three focused reliability checks; not a production release qualifier.
$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location -LiteralPath $root
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $env:TEMP ('astralog_interview_' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$OutputDirectory = (Resolve-Path $OutputDirectory).Path

$env:CARGO_BUILD_JOBS = '2'
$env:CARGO_INCREMENTAL = '0'
$env:ASTRALOG_INTERVIEW_CSV = Join-Path $OutputDirectory 'raw.csv'
$env:ASTRALOG_INTERVIEW_SAMPLES_CSV = Join-Path $OutputDirectory 'latency_samples.csv'
$env:ASTRALOG_INTERVIEW_REPEATS = if ($Fast) { '1' } else { '2' }
$env:ASTRALOG_INTERVIEW_DURATION_MS = if ($Fast) { '600' } else { '2500' }
if (Test-Path -LiteralPath $env:ASTRALOG_INTERVIEW_CSV) {
    throw 'OutputDirectory already contains raw.csv; choose a fresh folder to avoid mixing runs'
}

$sha = (& git rev-parse HEAD).Trim()
$dirty = @(& git status --porcelain)
$details = @(
    ('date_utc=' + (Get-Date).ToUniversalTime().ToString('o')),
    ('git_sha=' + $sha),
    ('git_dirty=' + ($dirty.Count -gt 0)),
    ('rustc=' + ((& rustc --version) -join ' ')),
    ('profile=' + $(if ($Fast) { 'debug_smoke_NOT_PERFORMANCE' } else { 'release' })),
    ('record_bytes=1024'),
    ('concurrent_producers=4'),
    ('repeats=' + $env:ASTRALOG_INTERVIEW_REPEATS),
    ('measurement_ms=' + $env:ASTRALOG_INTERVIEW_DURATION_MS),
    ('transport=local_loopback')
)
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$hostInfo = Get-CimInstance Win32_ComputerSystem
$os = Get-CimInstance Win32_OperatingSystem
$details += 'cpu=' + $cpu.Name.Trim()
$details += 'logical_processors=' + $cpu.NumberOfLogicalProcessors
$details += 'physical_ram_bytes=' + $hostInfo.TotalPhysicalMemory
$details += 'os=' + $os.Caption
$details += 'os_build=' + $os.BuildNumber
$details | Set-Content -LiteralPath (Join-Path $OutputDirectory 'environment.txt')

$nativeMethods = '[System.Runtime.InteropServices.DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);'
Add-Type -Namespace AstraLogDemo -Name Awake -MemberDefinition $nativeMethods
[AstraLogDemo.Awake]::SetThreadExecutionState([uint32]2147483649) | Out-Null
try {
    Start-Transcript -Path (Join-Path $OutputDirectory 'session.log') -Force | Out-Null
    # PowerShell transcripts do not consistently capture native Cargo output.
    # A separate stdout/stderr pair per command preserves the actual evidence.
    function Invoke-CargoLogged([string[]]$Arguments, [string]$Label) {
        $stdout = Join-Path $OutputDirectory ($Label + '.stdout.log')
        $stderr = Join-Path $OutputDirectory ($Label + '.stderr.log')
        # Start-Process -Wait can hang on descendant Windows console hosts;
        # Start-Process -PassThru without -Wait does not expose a reliable
        # ExitCode in PowerShell 5.1. A direct .NET Process provides both.
        $info = New-Object System.Diagnostics.ProcessStartInfo
        $info.FileName = 'cargo.exe'
        $info.Arguments = $Arguments -join ' ' # These controlled Cargo args have no spaces.
        $info.UseShellExecute = $false
        $info.CreateNoWindow = $true
        $info.RedirectStandardOutput = $true
        $info.RedirectStandardError = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $info
        try {
            [void]$process.Start()
            # Read both streams concurrently so a full stderr pipe cannot
            # block a process whose stdout pipe is also full.
            $outTask = $process.StandardOutput.ReadToEndAsync()
            $errTask = $process.StandardError.ReadToEndAsync()
            if (-not $process.WaitForExit(900000)) {
                throw "$Label exceeded its 15-minute wait; inspect Cargo PID $($process.Id)"
            }
            [System.IO.File]::WriteAllText($stdout, $outTask.GetAwaiter().GetResult())
            [System.IO.File]::WriteAllText($stderr, $errTask.GetAwaiter().GetResult())
            $code = $process.ExitCode
        }
        finally {
            $process.Dispose()
        }
        Get-Content -LiteralPath $stdout | ForEach-Object { Write-Host $_ }
        Get-Content -LiteralPath $stderr | ForEach-Object { Write-Host $_ }
        if ($code -ne 0) {
            throw "$Label failed with exit $code; evidence left in $OutputDirectory"
        }
    }
    $benchmarkArgs = @('test', '-p', 'astralog-broker', '--test', 'interview_benchmark', '--offline', '--locked')
    if (-not $Fast) { $benchmarkArgs += '--release' }
    $benchmarkArgs += @('--', '--nocapture', '--test-threads=1')
    Write-Host 'Starting real RF1/RF3 broker demo and benchmark...'
    Invoke-CargoLogged $benchmarkArgs 'benchmark'

    $checks = @(
        @('p2_network', 'single_broker_round_trip_survives_restart'),
        @('p7_failover', 'p7_rf3_leader_death_after_quorum_before_response_deduplicates_on_successor'),
        @('p7_failover', 'p7_rf3_committed_retry_survives_explicit_leadership_transfer')
    )
    foreach ($check in $checks) {
        Write-Host ('Checking ' + $check[1] + '...')
        Invoke-CargoLogged @('test', '-p', 'astralog-broker', '--test', $check[0], $check[1], '--offline', '--locked', '--', '--nocapture', '--test-threads=1') $check[1]
    }
    $mode = if ($Fast) { 'debug' } else { 'release' }
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'summarize-interview-benchmark.ps1') -RawCsv $env:ASTRALOG_INTERVIEW_CSV -SamplesCsv $env:ASTRALOG_INTERVIEW_SAMPLES_CSV -Output (Join-Path $OutputDirectory 'SUMMARY.md') -Mode $mode
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark CSV summarization failed' }
    Write-Host ('PASS: all targeted checks; output: ' + $OutputDirectory)
    if ($Fast) {
        Write-Warning 'Fast mode is a DEBUG functional smoke test; do not publish its performance numbers.'
    }
}
finally {
    Stop-Transcript -ErrorAction SilentlyContinue | Out-Null
    [AstraLogDemo.Awake]::SetThreadExecutionState([uint32]2147483648) | Out-Null
}
