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

Push-Location $FrontendRoot
try {
    npm run build
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
        "--add-data", "backend;backend",
        "--add-data", "frontend\dist;frontend\dist",
        "--hidden-import", "main",
        "--hidden-import", "database",
        "--hidden-import", "paths",
        "--hidden-import", "legacy_import",
        "--hidden-import", "exports",
        "--hidden-import", "native_exports",
        "--hidden-import", "cyberlab_workflow",
        "--hidden-import", "family_report",
        "--hidden-import", "custom_report",
        "--hidden-import", "portable_backup",
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
        "--icon", $IconPath,
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
