# Build and code-sign the CyberLab Case Tracker executable using PyInstaller and signtool
# Usage:
#   .\scripts\build_and_sign.ps1 -CertPath "C:\path\to\cert.pfx" -CertPassword "secret" -TimestampUrl "http://timestamp.sectigo.com" -PyinstallerSpec "CyberLabCaseTracker.spec"

param(
  [Parameter(Mandatory=$true)][string]$CertPath,
  [Parameter(Mandatory=$true)][string]$CertPassword,
  [Parameter(Mandatory=$false)][string]$TimestampUrl = "http://timestamp.sectigo.com",
  [Parameter(Mandatory=$false)][string]$PyinstallerSpec = "CyberLabCaseTracker.spec"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/3] Cleaning previous build/dist..."
if (Test-Path build) { Remove-Item -Recurse -Force build }
if (Test-Path dist) { Remove-Item -Recurse -Force dist }

Write-Host "[2/3] Building with PyInstaller..."
pyinstaller $PyinstallerSpec

$exe = Join-Path (Join-Path (Get-Location) "dist") "CyberLabCaseTracker\CyberLabCaseTracker.exe"
if (-not (Test-Path $exe)) {
  throw "Executable not found: $exe"
}

Write-Host "[3/3] Code-signing $exe ..."
# Locate signtool (requires Windows 10 SDK)
$signTool = (Get-Command signtool.exe -ErrorAction SilentlyContinue).Source
if (-not $signTool) {
  throw "signtool.exe not found. Install Windows 10/11 SDK or ensure it's on PATH."
}

& $signTool sign /f $CertPath /p $CertPassword /tr $TimestampUrl /td SHA256 /fd SHA256 $exe

Write-Host "Done. Signed EXE at: $exe"
