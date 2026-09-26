#Requires -Version 7.0
<#
.SYNOPSIS
    Build pg_ros2 from this checkout and verify it against a real
    PostgreSQL 18 + ROS 2 Humble container.

.DESCRIPTION
    Two phases, both driven through Docker:

      1. BUILD   - compile the release package in the ROS/pgrx builder image
                   (default: pg-ros2:action-dev, see .bootstrap/actions.Dockerfile).
      2. RUNTIME - install the freshly built pg_ros2.so into the runtime image
                   BEFORE the server first starts, then assert that the graph
                   worker waits for CREATE EXTENSION instead of crash-looping
                   (the historical InvalidPosition regression), and that it
                   installs a snapshot once the extension exists.

    The source tree is copied into the build container with docker cp, so the
    host does not need Docker file sharing for this checkout.

    Pass -SkipBuild to reuse a previously built .so from the artifacts
    directory (useful while iterating on the runtime assertions).

.EXAMPLE
    pwsh scripts/verify-docker.ps1

.EXAMPLE
    pwsh scripts/verify-docker.ps1 -SkipBuild -KeepArtifacts

.NOTES
    Exit code 0 means every assertion passed; 1 means a check failed or a
    Docker command failed. Containers are always removed, and the artifacts
    directory is removed unless -KeepArtifacts is given.
#>
[CmdletBinding()]
param(
    [string]$BuildImage = 'pg-ros2:action-dev',
    [string]$RuntimeImage = 'ghcr.io/sweatybridge/pg_ros2:latest',
    [string]$BuildContainer = 'pg-ros2-verify-build',
    [string]$RuntimeContainer = 'pg-ros2-verify-runtime',
    [string]$ArtifactsDir,
    [switch]$SkipBuild,
    [switch]$KeepArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# We inspect native exit codes explicitly instead of letting docker's stderr terminate the run.
$PSNativeCommandUseErrorActionPreference = $false

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not $ArtifactsDir) { $ArtifactsDir = Join-Path $RepoRoot '.verify-artifacts' }

$PgMajor = '18'
$WorkDir = '/home/builder/pg_ros2'
$TargetDir = '/tmp/target'
$PackageOut = "$TargetDir/release/pg_ros2-pg18"
$BuildScript = @'
set -eo pipefail
source /opt/ros/humble/setup.bash
rm -rf /home/builder/work
mkdir -p /home/builder/work
cp -r /home/builder/pg_ros2/. /home/builder/work/
cd /home/builder/work
echo BUILD_START
cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config --features pg18 --no-default-features
echo BUILD_DONE
'@

function Write-Stage([string]$Message) {
    Write-Host ''
    Write-Host ("=== " + $Message + " ===") -ForegroundColor Cyan
}

function Invoke-Docker {
    param([string[]]$Arguments)
    $lines = @(& docker @Arguments 2>&1 | ForEach-Object { $_.ToString() })
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        throw ("docker " + ($Arguments -join ' ') + " failed (exit " + $code + ")" + [Environment]::NewLine + ($lines -join [Environment]::NewLine))
    }
    return ($lines -join [Environment]::NewLine)
}

function Remove-Container([string]$Name) {
    & docker rm -f $Name *> $null
}

function Get-ContainerLog([string]$Name) {
    return ((& docker logs $Name 2>&1 | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
}

function Build-Package {
    Write-Stage ("Build release package in " + $BuildImage)
    Remove-Container $BuildContainer
    Invoke-Docker -Arguments @('create', '--name', $BuildContainer, '--user', 'builder', '-e', ("CARGO_TARGET_DIR=" + $TargetDir), '-w', $WorkDir, $BuildImage, 'bash', '-lc', $BuildScript) | Out-Null
    foreach ($name in @('Cargo.toml', 'Cargo.lock', 'pg_ros2.control')) {
        Invoke-Docker -Arguments @('cp', (Join-Path $RepoRoot $name), ($BuildContainer + ':' + $WorkDir + '/' + $name)) | Out-Null
    }
    Invoke-Docker -Arguments @('cp', (Join-Path $RepoRoot 'src'), ($BuildContainer + ':' + $WorkDir + '/src')) | Out-Null
    Invoke-Docker -Arguments @('start', $BuildContainer) | Out-Null

    $exitCode = (Invoke-Docker -Arguments @('wait', $BuildContainer)).Trim()
    $log = Invoke-Docker -Arguments @('logs', $BuildContainer)
    Set-Content -Path (Join-Path $ArtifactsDir 'build.log') -Value $log -Encoding utf8
    if ($exitCode -ne '0' -or $log -notmatch 'BUILD_DONE') {
        Write-Host $log
        throw ("Build failed (container exit " + $exitCode + "); full log in " + (Join-Path $ArtifactsDir 'build.log'))
    }
    Write-Host 'Build succeeded'
    Invoke-Docker -Arguments @('cp', ($BuildContainer + ':' + $PackageOut), $ArtifactsDir) | Out-Null
}

function Wait-Postgres([string]$Name) {
    $deadline = (Get-Date).AddSeconds(60)
    do {
        & docker exec -u postgres $Name pg_isready -q *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw ("PostgreSQL in " + $Name + " did not become ready")
}

function Wait-ForLog([string]$Name, [string]$Pattern, [int]$TimeoutSeconds = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $log = Get-ContainerLog $Name
        if ($log -match $Pattern) { return $log }
        # Stop early on the failure we are guarding against.
        if ($log -match 'InvalidPosition') { return $log }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    return (Get-ContainerLog $Name)
}

function Wait-ForHealthy([string]$Name, [int]$TimeoutSeconds = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $row = @(& docker exec -u postgres $Name psql -Atc 'SELECT last_refreshed IS NOT NULL AND last_error IS NULL FROM ros2.worker_status' 2>$null)
        if ((($row -join '')).Trim() -eq 't') { return $true }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    return $false
}

$passed = $true
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'docker CLI not found on PATH' }
    New-Item -ItemType Directory -Force -Path $ArtifactsDir | Out-Null

    if ($SkipBuild) {
        Write-Stage 'Skipping build (-SkipBuild)'
    } else {
        Build-Package
    }

    $soItem = Get-ChildItem -Recurse -File $ArtifactsDir -Filter pg_ros2.so -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $soItem) {
        throw ("Built pg_ros2.so not found under " + $ArtifactsDir + "; run without -SkipBuild first")
    }
    $so = $soItem.FullName

    Write-Stage 'Runtime: install built .so before the server first starts'
    Remove-Container $RuntimeContainer
    Invoke-Docker -Arguments @('create', '--name', $RuntimeContainer, '--ipc=shareable', '-e', 'ROS_DOMAIN_ID=73', $RuntimeImage) | Out-Null
    Invoke-Docker -Arguments @('cp', $so, ($RuntimeContainer + ':/usr/lib/postgresql/' + $PgMajor + '/lib/pg_ros2.so')) | Out-Null
    Invoke-Docker -Arguments @('start', $RuntimeContainer) | Out-Null
    Wait-Postgres $RuntimeContainer

    Write-Stage 'Assert: worker waits for CREATE EXTENSION instead of crashing'
    $waitingLog = Wait-ForLog $RuntimeContainer 'waiting_for_extension' 30
    $waitingHasWait = $waitingLog -match 'waiting_for_extension'
    $waitingHasInvalid = $waitingLog -match 'InvalidPosition'

    Write-Stage 'Create the extension'
    Invoke-Docker -Arguments @('exec', '-u', 'postgres', $RuntimeContainer, 'psql', '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE EXTENSION pg_ros2') | Out-Null

    Write-Stage 'Assert: worker installs a snapshot'
    $healthy = Wait-ForHealthy $RuntimeContainer 30
    $finalLog = Get-ContainerLog $RuntimeContainer
    $finalHasInvalid = $finalLog -match 'InvalidPosition'
    Set-Content -Path (Join-Path $ArtifactsDir 'runtime.log') -Value $finalLog -Encoding utf8

    Write-Stage 'Results'
    Write-Host ('waiting_for_extension logged : ' + $(if ($waitingHasWait) { 'PASS' } else { 'FAIL' }))
    Write-Host ('no InvalidPosition (waiting) : ' + $(if ($waitingHasInvalid) { 'FAIL' } else { 'PASS' }))
    Write-Host ('worker healthy after install : ' + $(if ($healthy) { 'PASS' } else { 'FAIL' }))
    Write-Host ('no InvalidPosition (final)   : ' + $(if ($finalHasInvalid) { 'FAIL' } else { 'PASS' }))

    if (-not ($waitingHasWait -and -not $waitingHasInvalid -and $healthy -and -not $finalHasInvalid)) {
        $passed = $false
        Write-Host ''
        Write-Host '--- runtime log ---'
        Write-Host $finalLog
    }
}
finally {
    Remove-Container $BuildContainer
    Remove-Container $RuntimeContainer
    if ($KeepArtifacts) {
        Write-Host ('Artifacts kept in ' + $ArtifactsDir)
    } else {
        Remove-Item -Recurse -Force $ArtifactsDir -ErrorAction SilentlyContinue
    }
}

Write-Stage $(if ($passed) { 'VERIFY PASSED' } else { 'VERIFY FAILED' })
if ($passed) { exit 0 } else { exit 1 }
