param(
    [switch]$Debug
)

$ErrorActionPreference = "Stop"

$AppRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $AppRoot
$FrontendRoot = Join-Path $AppRoot "frontend"
$DistRoot = Join-Path $RepoRoot "dist"
$BuildRoot = Join-Path $AppRoot "build"
$IconPath = Join-Path $RepoRoot "digital.ico"
$VersionMatch = [regex]::Match([System.IO.File]::ReadAllText((Join-Path $AppRoot "backend/main.py")), 'APP_VERSION = "([0-9.]+)"')
if (-not $VersionMatch.Success) { throw "Could not read APP_VERSION" }
$AppVersion = $VersionMatch.Groups[1].Value
$VersionParts = $AppVersion.Split('.')
if ($VersionParts.Count -ne 3) { throw "APP_VERSION must contain major.minor.patch" }
$VersionTuple = ($VersionParts -join ', ') + ', 0'
New-Item -ItemType Directory -Force $BuildRoot | Out-Null
$VersionFile = Join-Path $BuildRoot "windows_version_info.txt"
@"
VSVersionInfo(
  ffi=FixedFileInfo(filevers=($VersionTuple), prodvers=($VersionTuple), mask=0x3f, flags=0x0, OS=0x40004, fileType=0x1, subtype=0x0, date=(0, 0)),
  kids=[StringFileInfo([StringTable('040904B0', [
    StringStruct('FileDescription', 'CyberLab Case Tracker'),
    StringStruct('FileVersion', '$AppVersion.0'),
    StringStruct('InternalName', 'CyberLab Case Tracker'),
    StringStruct('OriginalFilename', 'CyberLab Case Tracker.exe'),
    StringStruct('ProductName', 'CyberLab Case Tracker'),
    StringStruct('ProductVersion', '$AppVersion')
  ])]), VarFileInfo([VarStruct('Translation', [1033, 1200])])]
)
"@ | Set-Content -LiteralPath $VersionFile -Encoding utf8

# Bundle runtime sources and map assets without test fixtures or Python caches.
$BackendBundleRoot = Join-Path $BuildRoot ("runtime-backend-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force $BackendBundleRoot | Out-Null
Get-ChildItem -LiteralPath (Join-Path $AppRoot "backend") -File -Filter "*.py" |
    Where-Object { $_.Name -notlike "test_*" } |
    ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination $BackendBundleRoot }
Copy-Item -LiteralPath (Join-Path $AppRoot "backend/map_assets") -Destination (Join-Path $BackendBundleRoot "map_assets") -Recurse

Push-Location $FrontendRoot
try {
    npm run build
    if ($LASTEXITCODE -ne 0) { throw "Frontend build failed" }
}
finally {
    Pop-Location
}

Push-Location $AppRoot
try {
    $pyinstallerArgs = @(
        "-m", "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--name", "CyberLab Case Tracker",
        "--distpath", $DistRoot,
        "--workpath", $BuildRoot,
        "--paths", "backend",
        "--add-data", "$BackendBundleRoot;backend",
        "--add-data", "frontend\dist;frontend\dist",
        "--hidden-import", "main",
        "--hidden-import", "database",
        "--hidden-import", "geocoding",
        "--hidden-import", "paths",
        "--hidden-import", "legacy_import",
        "--hidden-import", "exports",
        "--hidden-import", "native_exports",
        "--hidden-import", "cyberlab_workflow",
        "--hidden-import", "family_report",
        "--hidden-import", "custom_report",
        "--hidden-import", "portable_backup",
        "--hidden-import", "update_installer",
        "--hidden-import", "browser_lifecycle",
        "--hidden-import", "coordinates",
        "--hidden-import", "sqlite3",
        "--hidden-import", "openpyxl",
        "--hidden-import", "reportlab",
        "--hidden-import", "PIL",
        "--hidden-import", "matplotlib",
        "--collect-submodules", "uvicorn",
        "--collect-submodules", "fastapi",
        "--collect-submodules", "starlette",
        "--collect-submodules", "pydantic",
        "--collect-submodules", "reportlab",
        "--collect-submodules", "cryptography",
        "--hidden-import", "matplotlib.backends.backend_agg",
        "--hidden-import", "matplotlib.backends.backend_pdf",
        "--icon", $IconPath,
        "--version-file", $VersionFile,
        "--exclude-module", "PySide6",
        "--exclude-module", "PyQt5",
        "--exclude-module", "PyQt6",
        "--exclude-module", "pandas",
        "--exclude-module", "scipy",
        "--exclude-module", "pyarrow",
        "--exclude-module", "dask",
        "--exclude-module", "xarray",
        "--exclude-module", "h5py",
        "--exclude-module", "netCDF4"
    )
    if ($Debug) {
        $pyinstallerArgs += "--console"
    }
    else {
        $pyinstallerArgs += "--windowed"
    }
    $pyinstallerArgs += "launch_modern_app.py"
    python @pyinstallerArgs
    if ($LASTEXITCODE -ne 0) { throw "EXE build failed" }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Build complete:"
Write-Host (Join-Path $DistRoot "CyberLab Case Tracker.exe")
if ($Debug) {
    Write-Host "Debug console build enabled."
}
else {
    Write-Host "Windowed user build enabled. Use .\modern_app\build_windows.ps1 -Debug for console logs."
}
Write-Host ""
Write-Host "Place caselog_gui_v6.db and app_data beside the EXE, or import them from Settings."
