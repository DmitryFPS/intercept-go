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
    param(
        [string]$Uri,
        [System.Diagnostics.Process]$Process
    )
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        if ($Process.HasExited) {
            throw "Приложение завершилось до запуска локального интерфейса: $($Process.StartInfo.FileName)"
        }
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
        try {
            Invoke-RestMethod -Uri $case.Uri -TimeoutSec 1 | Out-Null
            throw "Порт локального интерфейса уже занят: $($case.Uri). Закройте ранее запущенную программу."
        }
        catch {
            if ($_.Exception.Message -like 'Порт локального интерфейса уже занят:*') {
                throw
            }
        }
        $archive = Join-Path $resolvedDist $case.Archive
        $target = Join-Path $smokeRoot $case.Folder
        Expand-Archive -LiteralPath $archive -DestinationPath $target
        $executable = Get-ChildItem -LiteralPath $target -Filter $case.Exe -Recurse | Select-Object -First 1
        if ($null -eq $executable) {
            throw "В архиве $archive не найден $($case.Exe)"
        }
        $process = Start-Process -FilePath $executable.FullName -ArgumentList '--no-browser' -WorkingDirectory $executable.DirectoryName -WindowStyle Hidden -PassThru
        $processes.Add($process)
        $status = Wait-LocalApi -Uri $case.Uri -Process $process
        if ($process.HasExited) {
            throw "Приложение $($case.Exe) завершилось во время smoke-теста"
        }
        if ([string]::IsNullOrWhiteSpace([string]$status.version)) {
            throw "Приложение $($case.Exe) вернуло статус без версии"
        }
		$diagnosticsUri = $case.Uri.Replace('/api/status', '/api/diagnostics')
		$diagnostics = Invoke-WebRequest -Uri $diagnosticsUri -UseBasicParsing -TimeoutSec 5
		if ($diagnostics.Headers['Content-Type'] -notlike 'application/zip*' -or $diagnostics.RawContentLength -lt 100) {
			throw "Приложение $($case.Exe) не сформировало диагностический ZIP"
		}
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
        $processes.Remove($process) | Out-Null
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
