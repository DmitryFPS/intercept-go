[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DistRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$resolvedDist = (Resolve-Path -LiteralPath $DistRoot).Path
$smokeRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("EAVCConnectSmoke-" + [guid]::NewGuid().ToString('N'))
$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()

function Wait-LocalApi {
    param(
        [string]$BootstrapUri,
        [string]$StatusUri,
        [System.Diagnostics.Process]$Process,
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session
    )
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        if ($Process.HasExited) {
            throw "Приложение завершилось до запуска локального интерфейса: $($Process.StartInfo.FileName)"
        }
        try {
            Invoke-WebRequest -Uri $BootstrapUri -WebSession $Session -UseBasicParsing -TimeoutSec 2 | Out-Null
            return Invoke-RestMethod -Uri $StatusUri -WebSession $Session -TimeoutSec 2
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Локальный интерфейс не ответил: $StatusUri"
}

function Test-TcpPort {
    param([uri]$Uri)
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $task = $client.ConnectAsync($Uri.Host, $Uri.Port)
        if ($task.Wait(500)) {
            return $client.Connected
        }
        return $false
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

try {
    New-Item -ItemType Directory -Path $smokeRoot | Out-Null
    $cases = @(
        @{ Archive = 'EAVC-Connect-Station-portable.zip'; Folder = 'Рабочая точка Тест'; Exe = 'EAVC Connect Station.exe'; Uri = 'http://127.0.0.1:17631/api/status' },
        @{ Archive = 'EAVC-Connect-Center-portable.zip'; Folder = 'Центр Тест'; Exe = 'EAVC Connect Center.exe'; Uri = 'http://127.0.0.1:17632/api/status' }
    )
    foreach ($case in $cases) {
        if (Test-TcpPort -Uri ([uri]$case.Uri)) {
            throw "Порт локального интерфейса уже занят: $($case.Uri). Закройте ранее запущенную программу."
        }
        $archive = Join-Path $resolvedDist $case.Archive
        $target = Join-Path $smokeRoot $case.Folder
        Expand-Archive -LiteralPath $archive -DestinationPath $target
        $executable = Get-ChildItem -LiteralPath $target -Filter $case.Exe -Recurse | Select-Object -First 1
        if ($null -eq $executable) {
            throw "В архиве $archive не найден $($case.Exe)"
        }
        $accessToken = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'))
        $process = Start-Process -FilePath $executable.FullName -ArgumentList @('--no-browser', "--access-token=$accessToken") -WorkingDirectory $executable.DirectoryName -WindowStyle Hidden -PassThru
        $processes.Add($process)
        $session = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
        $bootstrapUri = $case.Uri.Replace('/api/status', '/?access=' + [uri]::EscapeDataString($accessToken))
        $status = Wait-LocalApi -BootstrapUri $bootstrapUri -StatusUri $case.Uri -Process $process -Session $session
        if ($process.HasExited) {
            throw "Приложение $($case.Exe) завершилось во время smoke-теста"
        }
        if ([string]::IsNullOrWhiteSpace([string]$status.version)) {
            throw "Приложение $($case.Exe) вернуло статус без версии"
        }
		$diagnosticsUri = $case.Uri.Replace('/api/status', '/api/diagnostics')
		$diagnostics = Invoke-WebRequest -Uri $diagnosticsUri -WebSession $session -UseBasicParsing -TimeoutSec 5
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
        (Split-Path -Leaf $resolvedSmoke).StartsWith('EAVCConnectSmoke-')) {
        Remove-Item -LiteralPath $resolvedSmoke -Recurse -Force -ErrorAction SilentlyContinue
    }
}
