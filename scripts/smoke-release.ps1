[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DistRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$resolvedDist = (Resolve-Path -LiteralPath $DistRoot).Path
$smokeRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("InterceptSmoke-" + [guid]::NewGuid().ToString('N'))
$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

function Wait-LocalApi {
    param([string]$Uri)
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        try {
            return Invoke-RestMethod -Uri $Uri -TimeoutSec 2
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Локальный интерфейс не ответил: $Uri"
}

try {
    New-Item -ItemType Directory -Path $smokeRoot | Out-Null
    $cases = @(
        @{ Archive = 'InterceptClient-portable.zip'; Folder = 'Клиент Тест'; Exe = 'InterceptClient.exe'; Uri = 'http://127.0.0.1:17631/api/status' },
        @{ Archive = 'InterceptServer-portable.zip'; Folder = 'Сервер Тест'; Exe = 'InterceptServer.exe'; Uri = 'http://127.0.0.1:17632/api/status' }
    )
    foreach ($case in $cases) {
        $archive = Join-Path $resolvedDist $case.Archive
        $target = Join-Path $smokeRoot $case.Folder
        Expand-Archive -LiteralPath $archive -DestinationPath $target
        $executable = Get-ChildItem -LiteralPath $target -Filter $case.Exe -Recurse | Select-Object -First 1
        if ($null -eq $executable) {
            throw "В архиве $archive не найден $($case.Exe)"
        }
        $process = Start-Process -FilePath $executable.FullName -ArgumentList '--no-browser' -WorkingDirectory $executable.DirectoryName -WindowStyle Hidden -PassThru
        $processes.Add($process)
        $status = Wait-LocalApi -Uri $case.Uri
        if ([string]::IsNullOrWhiteSpace([string]$status.version)) {
            throw "Приложение $($case.Exe) вернуло статус без версии"
        }
        Stop-Process -Id $process.Id -Force
        $process.WaitForExit()
        $processes.Remove($process)
    }
    Write-Host 'Smoke-тест переносимых архивов пройден.' -ForegroundColor Green
}
finally {
    foreach ($process in $processes) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
    $resolvedSmoke = [System.IO.Path]::GetFullPath($smokeRoot)
    $resolvedTemp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
    if ($resolvedSmoke.StartsWith($resolvedTemp, [System.StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path -Leaf $resolvedSmoke).StartsWith('InterceptSmoke-')) {
        Remove-Item -LiteralPath $resolvedSmoke -Recurse -Force -ErrorAction SilentlyContinue
    }
}
