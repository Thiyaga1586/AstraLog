param([string]$Destination = (Join-Path $env:TEMP 'AstraLog-public-snapshot'))

# Produces a new-history-safe tree for a NEW GitHub repository. No git init,
# commit, remote or push is performed here. The private commit history is never
# copied, so older logs with pre-redaction usernames stay private.
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location -LiteralPath $root
try {
    if (@(& git status --porcelain).Count -ne 0) {
        throw 'Commit or stash local changes before exporting a public snapshot'
    }
    if (Test-Path -LiteralPath $Destination) {
        throw 'Destination already exists; choose a new empty directory'
    }
    $zip = Join-Path $env:TEMP ('astralog-public-' + [guid]::NewGuid().ToString('N') + '.zip')
    try {
        & git archive '--format=zip' ('--output=' + $zip) HEAD
        if ($LASTEXITCODE -ne 0) { throw 'Git archive failed' }
        New-Item -ItemType Directory -Path $Destination -Force | Out-Null
        Expand-Archive -LiteralPath $zip -DestinationPath $Destination
    }
    finally {
        if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }
    }
    Write-Host ('Clean working-tree snapshot ready at: ' + $Destination)
    Write-Host 'Review this directory for personal paths before initializing a NEW GitHub repository.'
    Write-Host 'This script deliberately does NOT push, modify Git history, or make the repository public.'
}
finally {
    Pop-Location
}
