<#
Launch and secure the local AstraLog Prometheus/Grafana stack.
Callable from Windows CMD through scripts\run-monitoring.cmd.

The Grafana password is generated from OS cryptographic randomness and stored
outside Git in a Windows ACL-protected file. No secret is logged to stdout.
The existing Grafana admin password is rotated explicitly because Grafana's
GF_SECURITY_ADMIN_PASSWORD setting only initializes a new database.
#>

$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$compose = Join-Path $root 'observability\docker-compose.yml'
$secretDir = Join-Path $root 'observability\secrets'
$secretPath = Join-Path $secretDir 'grafana_admin_password.txt'

if (-not (Test-Path -LiteralPath $secretDir -PathType Container)) {
    New-Item -ItemType Directory -Path $secretDir -Force | Out-Null
}

$created = $false
if (-not (Test-Path -LiteralPath $secretPath -PathType Leaf)) {
    $bytes = New-Object byte[] 36
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
    } finally {
        $rng.Dispose()
    }
    # Prefix deliberately meets uppercase, lowercase, digit and special
    # character requirements; the random suffix supplies 288 bits of entropy.
    $password = 'Ag!9' + [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
    [IO.File]::WriteAllText($secretPath, $password, [Text.Encoding]::ASCII)
    $created = $true
}

$password = [IO.File]::ReadAllText($secretPath).Trim()
if ($password.Length -lt 24 -or
    $password -cnotmatch '[A-Z]' -or
    $password -cnotmatch '[a-z]' -or
    $password -notmatch '\d' -or
    $password -notmatch '[^a-zA-Z0-9]') {
    throw 'Grafana password file must contain at least 24 characters, including upper/lowercase, digits and a symbol.'
}
if ($password -ceq 'admin' -or $password -ceq 'password') {
    throw 'Unsafe Grafana password is prohibited.'
}

# Remove inherited access and grant only this Windows identity, SYSTEM and
# the built-in Administrators group. SIDs are locale independent.
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
& icacls.exe $secretDir '/inheritance:r' '/grant:r' "*${sid}:(OI)(CI)(F)" '*S-1-5-18:(OI)(CI)(F)' '*S-1-5-32-544:(OI)(CI)(F)' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Could not restrict Grafana secrets directory ACL.'
}
& icacls.exe $secretPath '/inheritance:r' '/grant:r' "*${sid}:(F)" '*S-1-5-18:(F)' '*S-1-5-32-544:(F)' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Could not restrict Grafana password file ACL.'
}

if ($created) {
    Write-Host 'Created a new random Grafana password in the Git-ignored local secret file.'
} else {
    Write-Host 'Using the existing Git-ignored Grafana password file.'
}

& docker compose -f $compose config --quiet
if ($LASTEXITCODE -ne 0) {
    throw 'Invalid Docker Compose configuration.'
}

function Get-GrafanaBasicHeader([string]$candidate) {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('admin:' + $candidate))
    return @{ Authorization = ('Basic ' + $encoded) }
}

function Test-GrafanaLogin([string]$candidate) {
    try {
        $response = Invoke-WebRequest -Uri 'http://127.0.0.1:3000/api/search?query=AstraLog' -Headers (Get-GrafanaBasicHeader $candidate) -UseBasicParsing -TimeoutSec 5
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

try {
    & docker compose -f $compose up -d
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Compose could not start Grafana and Prometheus.'
    }

    $ready = $false
    for ($i = 0; $i -lt 25; $i++) {
        try {
            $null = Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/health' -TimeoutSec 3
            $ready = $true
            break
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $ready) {
        throw 'Grafana did not become ready on loopback.'
    }

    # The initial admin password from a secret only applies to new databases.
    # Rotate an existing database from the known default via the official API.
    $rotated = $false
    if (-not (Test-GrafanaLogin $password)) {
        if (Test-GrafanaLogin 'admin') {
            $body = @{ password = $password } | ConvertTo-Json -Compress
            $null = Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/admin/users/1/password' -Method Put -Headers (Get-GrafanaBasicHeader 'admin') -ContentType 'application/json' -Body $body -TimeoutSec 5
            $rotated = $true
        } else {
            # The CLI fallback covers a reused Grafana database with a
            # nondefault password. Expansion only occurs inside the container.
            $resetCommand = 'grafana cli --homepath /usr/share/grafana --config /etc/grafana/grafana.ini admin reset-admin-password "$(cat /run/secrets/grafana_admin_password)"'
            & docker compose -f $compose exec -T grafana sh -ec $resetCommand | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw 'Could not rotate the existing Grafana admin password.'
            }
            $rotated = $true
        }
    }

    if (-not (Test-GrafanaLogin $password)) {
        throw 'Generated Grafana admin credentials were rejected.'
    }
    if (Test-GrafanaLogin 'admin') {
        throw 'The old default admin/admin credentials are still valid.'
    }

    $unauthenticatedRejected = $false
    try {
        $response = Invoke-WebRequest -Uri 'http://127.0.0.1:3000/api/search?query=AstraLog' -UseBasicParsing -TimeoutSec 5
        $unauthenticatedRejected = $response.StatusCode -eq 401
    } catch {
        $httpResponse = $_.Exception.Response
        $unauthenticatedRejected = $null -ne $httpResponse -and [int]$httpResponse.StatusCode -eq 401
    }
    if (-not $unauthenticatedRejected) {
        throw 'Anonymous Grafana access is still enabled.'
    }

    $adminHeaders = Get-GrafanaBasicHeader $password
    if ($rotated) {
        # A Grafana instance without prior login sessions may return HTTP 400
        # on a logout request. Inspect tokens first and only revoke if present.
        $sessions = @(Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/admin/users/1/auth-tokens' -Headers $adminHeaders -TimeoutSec 5)
        if ($sessions.Count -gt 0) {
            $null = Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/admin/users/1/logout' -Headers $adminHeaders -Method Post -TimeoutSec 5
        }
    }

    $users = @(Invoke-RestMethod -Uri 'http://127.0.0.1:3000/api/users?perpage=100' -Headers $adminHeaders -TimeoutSec 5)
    if ($users.Count -ne 1 -or $users[0].login -ne 'admin') {
        throw 'More than one Grafana account exists. Review user access before opening Grafana.'
    }

    Write-Host 'PASS: Grafana login verified. Anonymous access and old default credentials rejected.'
    Write-Host 'PASS: Exactly one Grafana user account exists.'
    Write-Host 'Login: http://127.0.0.1:3000 with username admin.'
    Write-Host 'Reveal password locally: type observability\secrets\grafana_admin_password.txt'
} catch {
    # Fail closed: do not leave unverified Grafana authentication available.
    & docker compose -f $compose stop grafana | Out-Null
    Write-Error ('Grafana stopped because authentication verification failed: ' + $_.Exception.Message)
    exit 1
} finally {
    $password = $null
}
