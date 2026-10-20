[CmdletBinding()]
param(
    [string]$Version = '0.1.0',
    [switch]$SkipSmoke
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$workspaceRoot = (Resolve-Path (Join-Path $repoRoot '..')).Path
$releaseBase = Join-Path $workspaceRoot 'Intercept-Releases'
$distRoot = Join-Path $releaseBase $Version
# Все временные файлы сборки находятся рядом с рабочей областью. Это делает
# результат воспроизводимым и не зависит от прав на системные AppData/GOPATH.
$buildCacheRoot = Join-Path $workspaceRoot '.build-cache\release'
New-Item -ItemType Directory -Force -Path $buildCacheRoot | Out-Null
$clientRoot = Join-Path $distRoot 'EAVC Connect Station'
$serverRoot = Join-Path $distRoot 'EAVC Connect Center'
$env:GOPATH = Join-Path $buildCacheRoot 'gopath'
$env:GOCACHE = Join-Path $buildCacheRoot 'gocache'
$env:GOMODCACHE = Join-Path $buildCacheRoot 'gomodcache'
$env:GOTMPDIR = Join-Path $buildCacheRoot 'gotmp'
$env:CGO_ENABLED = '0'
$env:GOOS = 'windows'
$env:GOARCH = 'amd64'

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force -Path $env:GOTMPDIR, $releaseBase | Out-Null
    $unformatted = @(gofmt -l cmd internal)
    if ($unformatted.Count -ne 0) {
        throw "gofmt требуется для: $($unformatted -join ', ')"
    }
    go test ./...
    if ($LASTEXITCODE -ne 0) { throw 'Тесты завершились ошибкой.' }
    go vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'go vet завершился ошибкой.' }

    if (Test-Path -LiteralPath $distRoot) {
        $resolved = [System.IO.Path]::GetFullPath($distRoot)
        $resolvedBase = [System.IO.Path]::GetFullPath($releaseBase) + [System.IO.Path]::DirectorySeparatorChar
        if (-not $resolved.StartsWith($resolvedBase, [System.StringComparison]::OrdinalIgnoreCase) -or
            (Split-Path -Leaf $resolved) -ne $Version) {
            throw "Небезопасный путь очистки: $resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
    New-Item -ItemType Directory -Path $clientRoot, $serverRoot | Out-Null

    $clientFlags = "-s -w -H=windowsgui -X main.version=$Version"
    go build -trimpath -ldflags $clientFlags -o (Join-Path $clientRoot 'EAVC Connect Station.exe') ./cmd/intercept-client
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось собрать EAVC Connect — Рабочая точка.' }
    go build -trimpath -ldflags $clientFlags -o (Join-Path $serverRoot 'EAVC Connect Center.exe') ./cmd/intercept-server
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось собрать EAVC Connect — Центр управления.' }

    Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\client-readme.txt') -Destination (Join-Path $clientRoot 'ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt')
    Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\server-readme.txt') -Destination (Join-Path $serverRoot 'ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt')
    Copy-Item -LiteralPath (Join-Path $repoRoot 'assets\word_template.docx') -Destination (Join-Path $serverRoot 'word_template.docx')
    New-Item -ItemType Directory -Path (Join-Path $clientRoot 'data'), (Join-Path $clientRoot 'logs'), (Join-Path $serverRoot 'data'), (Join-Path $serverRoot 'logs') | Out-Null

    $clientArchive = Join-Path $distRoot 'EAVC-Connect-Station-portable.zip'
    $serverArchive = Join-Path $distRoot 'EAVC-Connect-Center-portable.zip'
    Compress-Archive -Path $clientRoot -DestinationPath $clientArchive -CompressionLevel Optimal
    Compress-Archive -Path $serverRoot -DestinationPath $serverArchive -CompressionLevel Optimal

    $hashes = Get-FileHash -Algorithm SHA256 -LiteralPath $clientArchive, $serverArchive
    $lines = $hashes | ForEach-Object { "$($_.Hash.ToLowerInvariant())  $([System.IO.Path]::GetFileName($_.Path))" }
    [System.IO.File]::WriteAllLines((Join-Path $distRoot 'SHA256SUMS.txt'), $lines, [System.Text.UTF8Encoding]::new($false))

    if (-not $SkipSmoke) {
        & (Join-Path $PSScriptRoot 'smoke-release.ps1') -DistRoot $distRoot
    }
    Write-Host "Готово: $distRoot" -ForegroundColor Green
}
finally {
    Pop-Location
}
