[CmdletBinding()]
param(
    [string]$Version = '0.1.0',
    [switch]$SkipSmoke,
    [string]$SigningCertificateThumbprint = '',
    [switch]$RequireSignature,
	[switch]$LabBuild,
    [string]$UpdateSigningKeyPath = ''
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
if (-not $LabBuild -and [string]::IsNullOrWhiteSpace($SigningCertificateThumbprint)) {
    throw 'Промышленная сборка требует -SigningCertificateThumbprint. Для явно лабораторной сборки укажите -LabBuild.'
}
if (-not $LabBuild -and [string]::IsNullOrWhiteSpace($UpdateSigningKeyPath)) {
    throw 'Промышленная сборка требует явно заданный -UpdateSigningKeyPath из защищённого хранилища.'
}
if ($LabBuild -and [string]::IsNullOrWhiteSpace($UpdateSigningKeyPath)) {
    $UpdateSigningKeyPath = Join-Path $workspaceRoot '.release-keys\update-ed25519.key'
}
$env:GOPATH = Join-Path $buildCacheRoot 'gopath'
$env:GOCACHE = Join-Path $buildCacheRoot 'gocache'
$env:GOMODCACHE = Join-Path $buildCacheRoot 'gomodcache'
$env:GOTMPDIR = Join-Path $buildCacheRoot 'gotmp'
$env:CGO_ENABLED = '0'
$env:GOOS = 'windows'
$env:GOARCH = 'amd64'
$env:GOTOOLCHAIN = 'go1.26.10'

function Add-DeterministicArchive {
    param([string]$SourceDirectory, [string]$DestinationPath)
    Add-Type -AssemblyName System.IO.Compression
    if (Test-Path -LiteralPath $DestinationPath) {
        Remove-Item -LiteralPath $DestinationPath -Force
    }
    $stream = [System.IO.File]::Open($DestinationPath, [System.IO.FileMode]::CreateNew)
    try {
        $archive = [System.IO.Compression.ZipArchive]::new(
            $stream, [System.IO.Compression.ZipArchiveMode]::Create, $false
        )
        try {
            $base = Split-Path -Parent $SourceDirectory
            $baseUri = [System.Uri]::new(([System.IO.Path]::GetFullPath($base).TrimEnd('\') + '\'))
            $fixedTime = [System.DateTimeOffset]::new(1980, 1, 1, 0, 0, 0, [System.TimeSpan]::Zero)
            $items = Get-ChildItem -LiteralPath $SourceDirectory -Recurse -Force |
                Sort-Object { $baseUri.MakeRelativeUri([System.Uri]::new($_.FullName)).ToString() }
            foreach ($item in $items) {
                $relative = [System.Uri]::UnescapeDataString(
                    $baseUri.MakeRelativeUri([System.Uri]::new($item.FullName)).ToString()
                )
                if ($item.PSIsContainer) {
                    $relative += '/'
                    $entry = $archive.CreateEntry($relative)
                    $entry.LastWriteTime = $fixedTime
                    continue
                }
                $entry = $archive.CreateEntry($relative, [System.IO.Compression.CompressionLevel]::Optimal)
                $entry.LastWriteTime = $fixedTime
                $input = [System.IO.File]::OpenRead($item.FullName)
                $output = $entry.Open()
                try { $input.CopyTo($output) } finally { $output.Dispose(); $input.Dispose() }
            }
        }
        finally { $archive.Dispose() }
    }
    finally { $stream.Dispose() }
}

function Protect-Executable {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($SigningCertificateThumbprint)) {
		if (-not $LabBuild -or $RequireSignature) { throw 'Для рабочей сборки требуется сертификат подписи EXE.' }
        Write-Warning "Лабораторная сборка без Authenticode: $Path"
        return $false
    }
    $thumbprint = $SigningCertificateThumbprint.Replace(' ', '')
    $certificate = Get-ChildItem Cert:\CurrentUser\My, Cert:\LocalMachine\My -ErrorAction SilentlyContinue |
        Where-Object Thumbprint -EQ $thumbprint | Select-Object -First 1
    if (-not $certificate) { throw "Сертификат Authenticode $thumbprint не найден." }
    $signature = Set-AuthenticodeSignature -FilePath $Path -Certificate $certificate -HashAlgorithm SHA256
    if ($signature.Status -ne 'Valid') { throw "Подпись $Path не прошла проверку: $($signature.StatusMessage)" }
    return $true
}

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force -Path $env:GOTMPDIR, $releaseBase | Out-Null
    $unformatted = @(gofmt -l cmd internal)
    if ($unformatted.Count -ne 0) {
        throw "gofmt требуется для: $($unformatted -join ', ')"
    }
    $actualGo = go env GOVERSION
    if ($actualGo -ne 'go1.26.10') { throw "Требуется Go 1.26.10, используется $actualGo" }
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

	if (-not $LabBuild) {
		$resolvedKey = [System.IO.Path]::GetFullPath($UpdateSigningKeyPath)
		$resolvedWorkspace = [System.IO.Path]::GetFullPath($workspaceRoot) + [System.IO.Path]::DirectorySeparatorChar
		if ($resolvedKey.StartsWith($resolvedWorkspace, [System.StringComparison]::OrdinalIgnoreCase)) {
			throw 'Промышленный ключ обновлений нельзя хранить внутри рабочей области.'
		}
	}
    if (-not (Test-Path -LiteralPath $UpdateSigningKeyPath)) {
		if (-not $LabBuild) { throw 'Промышленный ключ обновлений не найден; автоматическое создание запрещено.' }
        go run ./cmd/update-packager keygen -key $UpdateSigningKeyPath
        if ($LASTEXITCODE -ne 0) { throw 'Не удалось создать ключ подписи обновлений.' }
    }
    $updatePublicKey = (& go run ./cmd/update-packager public -key $UpdateSigningKeyPath | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($updatePublicKey)) {
        throw 'Не удалось получить открытый ключ обновлений.'
    }

    $applicationFlags = "-s -w -H=windowsgui -X main.version=$Version -X main.updatePublicKey=$updatePublicKey"
    $updaterFlags = "-s -w -H=windowsgui -X main.updatePublicKey=$updatePublicKey"
    go build -trimpath -ldflags $applicationFlags -o (Join-Path $clientRoot 'EAVC Connect Station.exe') ./cmd/intercept-client
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось собрать EAVC Connect — Рабочая точка.' }
    go build -trimpath -ldflags $applicationFlags -o (Join-Path $serverRoot 'EAVC Connect Center.exe') ./cmd/intercept-server
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось собрать EAVC Connect — Центр управления.' }
    $updaterBuild = Join-Path $buildCacheRoot 'EAVC Connect Updater.exe'
    go build -trimpath -ldflags $updaterFlags -o $updaterBuild ./cmd/intercept-updater
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось собрать помощник безопасного обновления.' }

    $clientSigned = Protect-Executable (Join-Path $clientRoot 'EAVC Connect Station.exe')
    $serverSigned = Protect-Executable (Join-Path $serverRoot 'EAVC Connect Center.exe')
    $updaterSigned = Protect-Executable $updaterBuild

    Copy-Item -LiteralPath $updaterBuild -Destination (Join-Path $clientRoot 'EAVC Connect Updater.exe')
    Copy-Item -LiteralPath $updaterBuild -Destination (Join-Path $serverRoot 'EAVC Connect Updater.exe')

    Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\client-readme.txt') -Destination (Join-Path $clientRoot 'ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt')
    Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\server-readme.txt') -Destination (Join-Path $serverRoot 'ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt')
    Copy-Item -LiteralPath (Join-Path $repoRoot 'assets\word_template.docx') -Destination (Join-Path $serverRoot 'word_template.docx')
    $stationTemplate = Join-Path $serverRoot 'station-template'
    New-Item -ItemType Directory -Path $stationTemplate | Out-Null
    Copy-Item -LiteralPath (Join-Path $clientRoot 'EAVC Connect Station.exe') -Destination (Join-Path $stationTemplate 'EAVC Connect Station.exe')
    Copy-Item -LiteralPath $updaterBuild -Destination (Join-Path $stationTemplate 'EAVC Connect Updater.exe')
    $stationFiles = @('EAVC Connect Station.exe', 'EAVC Connect Updater.exe') | ForEach-Object {
        $path = Join-Path $stationTemplate $_
        [ordered]@{
            path = $_
            size = (Get-Item -LiteralPath $path).Length
            sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()
        }
    }
    $stationManifest = [ordered]@{
        schema = 1
        product = 'eavc-connect-station-template'
        version = $Version
        files = @($stationFiles)
    } | ConvertTo-Json -Depth 5
    $stationManifestPath = Join-Path $stationTemplate 'manifest.json'
    [System.IO.File]::WriteAllText($stationManifestPath, $stationManifest, [System.Text.UTF8Encoding]::new($false))
    go run ./cmd/update-packager sign -input $stationManifestPath -output (Join-Path $stationTemplate 'manifest.sig') -key $UpdateSigningKeyPath
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось подписать комплект рабочей точки.' }
    New-Item -ItemType Directory -Path (Join-Path $clientRoot 'data'), (Join-Path $clientRoot 'logs'), (Join-Path $serverRoot 'data'), (Join-Path $serverRoot 'logs') | Out-Null

    $clientArchive = Join-Path $distRoot 'EAVC-Connect-Station-portable.zip'
    $serverArchive = Join-Path $distRoot 'EAVC-Connect-Center-portable.zip'
    Add-DeterministicArchive -SourceDirectory $clientRoot -DestinationPath $clientArchive
    Add-DeterministicArchive -SourceDirectory $serverRoot -DestinationPath $serverArchive

    $clientUpdate = Join-Path $distRoot 'EAVC-Connect-Station-update.zip'
    $serverUpdate = Join-Path $distRoot 'EAVC-Connect-Center-update.zip'
    go run ./cmd/update-packager package -source $clientRoot -product eavc-connect-station -version $Version -output $clientUpdate -key $UpdateSigningKeyPath
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось подписать пакет обновления рабочей точки.' }
    go run ./cmd/update-packager package -source $serverRoot -product eavc-connect-center -version $Version -output $serverUpdate -key $UpdateSigningKeyPath
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось подписать пакет обновления центра.' }

    $hashes = Get-FileHash -Algorithm SHA256 -LiteralPath $clientArchive, $serverArchive, $clientUpdate, $serverUpdate
    $lines = $hashes | ForEach-Object { "$($_.Hash.ToLowerInvariant())  $([System.IO.Path]::GetFileName($_.Path))" }
    [System.IO.File]::WriteAllLines((Join-Path $distRoot 'SHA256SUMS.txt'), $lines, [System.Text.UTF8Encoding]::new($false))
    $manifest = [ordered]@{
        version = $Version
        go = $actualGo
        cgo = $false
        client_authenticode = $clientSigned
        center_authenticode = $serverSigned
        updater_authenticode = $updaterSigned
        signed_updates = $true
        artifacts = @($hashes | ForEach-Object {
            [ordered]@{
                name = [System.IO.Path]::GetFileName($_.Path)
                sha256 = $_.Hash.ToLowerInvariant()
                size = (Get-Item -LiteralPath $_.Path).Length
            }
        })
        update_public_key_sha256 = $(
            $sha = [System.Security.Cryptography.SHA256]::Create()
            try {
                ([System.BitConverter]::ToString(
                    $sha.ComputeHash([System.Convert]::FromBase64String($updatePublicKey))
                )).Replace('-', '').ToLowerInvariant()
            }
            finally { $sha.Dispose() }
        )
        reproducible_archives = $true
		build_profile = $(if ($LabBuild) { 'laboratory' } else { 'production' })
    } | ConvertTo-Json -Depth 5
	$releaseManifestPath = Join-Path $distRoot 'release-manifest.json'
    [System.IO.File]::WriteAllText($releaseManifestPath, $manifest, [System.Text.UTF8Encoding]::new($false))
	go run ./cmd/update-packager sign -input $releaseManifestPath -output (Join-Path $distRoot 'release-manifest.sig') -key $UpdateSigningKeyPath
	if ($LASTEXITCODE -ne 0) { throw 'Не удалось подписать манифест выпуска.' }
	[System.IO.File]::WriteAllText((Join-Path $distRoot 'update-public-key.txt'), $updatePublicKey, [System.Text.UTF8Encoding]::new($false))
	go run ./cmd/update-packager verify -input $releaseManifestPath -signature (Join-Path $distRoot 'release-manifest.sig') -public-key $updatePublicKey
	if ($LASTEXITCODE -ne 0) { throw 'Подпись манифеста выпуска не прошла проверку.' }

    if (-not $SkipSmoke) {
        & (Join-Path $PSScriptRoot 'smoke-release.ps1') -DistRoot $distRoot
    }
    Write-Host "Готово: $distRoot" -ForegroundColor Green
}
finally {
    Pop-Location
}
