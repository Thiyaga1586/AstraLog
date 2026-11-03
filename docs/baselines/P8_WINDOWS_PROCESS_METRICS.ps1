param(
    [Parameter(Mandatory = $true)]
    [int]$ProcessId
)

$ErrorActionPreference = 'Stop'

if (-not ('AstraLogMetrics.NativeMethods' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

namespace AstraLogMetrics {
    [StructLayout(LayoutKind.Sequential)]
    public struct IoCounters {
        public UInt64 ReadOperationCount;
        public UInt64 WriteOperationCount;
        public UInt64 OtherOperationCount;
        public UInt64 ReadTransferCount;
        public UInt64 WriteTransferCount;
        public UInt64 OtherTransferCount;
    }

    public static class NativeMethods {
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetProcessIoCounters(
            IntPtr hProcess,
            out IoCounters counters
        );
    }
}
'@
}

$process = Get-Process -Id $ProcessId
$cpuNanos = [uint64][math]::Round(([double]$process.CPU) * 1000000000.0)
$rssBytes = [uint64]$process.WorkingSet64

$io = New-Object AstraLogMetrics.IoCounters
$ok = [AstraLogMetrics.NativeMethods]::GetProcessIoCounters($process.Handle, [ref]$io)
if (-not $ok) {
    throw "GetProcessIoCounters failed: $([Runtime.InteropServices.Marshal]::GetLastWin32Error())"
}

$networkRx = [uint64]0
$networkTx = [uint64]0
$networkLines = & "$env:SystemRoot\System32\netstat.exe" -e
$bytesLine = $networkLines | Where-Object { $_ -match '^\s*Bytes\s+([0-9]+)\s+([0-9]+)\s*$' } | Select-Object -First 1
if ($null -ne $bytesLine -and $bytesLine -match '^\s*Bytes\s+([0-9]+)\s+([0-9]+)\s*$') {
    $networkRx = [uint64]$Matches[1]
    $networkTx = [uint64]$Matches[2]
}

Write-Output "cpu_nanos=$cpuNanos"
Write-Output "rss_bytes=$rssBytes"
# Windows GetProcessIoCounters supplies monotonic process I/O transfer bytes.
# The P8 engineering baseline records those in the disk-byte slots as a
# conservative process-I/O proxy; the closure report calls out this limitation.
Write-Output "disk_read_bytes=$([uint64]$io.ReadTransferCount)"
Write-Output "disk_write_bytes=$([uint64]$io.WriteTransferCount)"
Write-Output "network_rx_bytes=$networkRx"
Write-Output "network_tx_bytes=$networkTx"
# AstraLog V1 has no public fsync-latency or replication-lag metrics endpoint.
# These remain explicit zero/unavailable values in the P8 engineering baseline.
Write-Output "fsync_count=0"
Write-Output "fsync_total_ns=0"
Write-Output "replication_lag=0"
