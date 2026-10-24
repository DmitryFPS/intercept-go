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
$succeeded = $false

function Wait-LocalApi {
    param(
        [string]$StatusUri,
        [System.Diagnostics.Process]$Process,
        [hashtable]$Headers
    )
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        if ($Process.HasExited) {
            throw "Приложение завершилось с кодом $($Process.ExitCode) до запуска локального интерфейса: $($Process.StartInfo.FileName)"
        }
        try {
            return Invoke-RestMethod -Uri $StatusUri -Headers $Headers -TimeoutSec 2
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
        @{
            Archive = 'EAVC-Connect-Center-portable.zip'
            Folder = 'Центр Тест'
            Exe = 'EAVC Connect Center.exe'
            Uri = 'http://127.0.0.1:17632/api/status'
            Guide = 'EAVC Connect Center\Документация\Полная установка и настройка EAVC Connect.md'
            GuidePage = 'http://127.0.0.1:17632/installation-guide/index.html'
        }
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
        if ($case.ContainsKey('Guide')) {
            $guide = Join-Path $target $case.Guide
            if (-not (Test-Path -LiteralPath $guide -PathType Leaf)) {
                throw "В архиве $archive не найдена полная инструкция: $($case.Guide)"
            }
            $guideText = Get-Content -LiteralPath $guide -Raw
            $imageReferences = [regex]::Matches($guideText, '!\[[^\]]*\]\(([^)]+)\)')
            if ($imageReferences.Count -eq 0) {
                throw "Полная инструкция не содержит ссылок на снимки: $guide"
            }
            foreach ($match in $imageReferences) {
                $relativeImage = $match.Groups[1].Value.Replace('/', [System.IO.Path]::DirectorySeparatorChar)
                $imagePath = Join-Path (Split-Path -Parent $guide) $relativeImage
                if (-not (Test-Path -LiteralPath $imagePath -PathType Leaf)) {
                    throw "В архиве $archive отсутствует снимок из инструкции: $relativeImage"
                }
            }
        }
        $accessToken = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'))
        $process = Start-Process -FilePath $executable.FullName -ArgumentList @('--no-browser', "--access-token=$accessToken") -WorkingDirectory $executable.DirectoryName -WindowStyle Hidden -PassThru
        $processes.Add($process)
        $headers = @{ 'X-EAVC-Access' = $accessToken }
        $status = Wait-LocalApi -StatusUri $case.Uri -Process $process -Headers $headers
        if ($process.HasExited) {
            throw "Приложение $($case.Exe) завершилось во время smoke-теста"
        }
        if ([string]::IsNullOrWhiteSpace([string]$status.version)) {
            throw "Приложение $($case.Exe) вернуло статус без версии"
        }
        if ($case.ContainsKey('GuidePage')) {
            $guidePage = Invoke-WebRequest -Uri $case.GuidePage -Headers $headers -UseBasicParsing -TimeoutSec 5
            if ($guidePage.StatusCode -ne 200 -or $guidePage.Content -notlike '*full-guide.js*') {
                throw "Полная инструкция не открылась через интерфейс Центра: $($case.GuidePage)"
            }
            $guideImageUri = $case.GuidePage.Replace('index.html', 'full-installation-guide/images/02-extracted-center-folder.png')
            $guideImage = Invoke-WebRequest -Uri $guideImageUri -Headers $headers -UseBasicParsing -TimeoutSec 5
            if ($guideImage.StatusCode -ne 200 -or $guideImage.RawContentLength -lt 100) {
                throw "Снимок полной инструкции не открылся через интерфейс Центра: $guideImageUri"
            }
        }
        $diagnosticsUri = $case.Uri.Replace('/api/status', '/api/diagnostics')
        $diagnostics = Invoke-WebRequest -Uri $diagnosticsUri -Headers $headers -UseBasicParsing -TimeoutSec 5
        if ($diagnostics.Headers['Content-Type'] -notlike 'application/zip*' -or $diagnostics.RawContentLength -lt 100) {
            throw "Приложение $($case.Exe) не сформировало диагностический ZIP"
        }
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            $process.WaitForExit()
        }
        $processes.Remove($process) | Out-Null
    }
    $succeeded = $true
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
    if ($succeeded -and
        $resolvedSmoke.StartsWith($resolvedTemp, [System.StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path -Leaf $resolvedSmoke).StartsWith('EAVCConnectSmoke-')) {
        Remove-Item -LiteralPath $resolvedSmoke -Recurse -Force -ErrorAction SilentlyContinue
    }
    elseif (-not $succeeded) {
        Write-Warning "Каталог неудачного smoke-теста сохранён: $resolvedSmoke"
    }
}
