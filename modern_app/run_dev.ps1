$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backend = Join-Path $root "backend"
$frontend = Join-Path $root "frontend"

Write-Host "Starting CyberLab Modern backend on http://127.0.0.1:8768"
$backendProcess = Start-Process -FilePath python -ArgumentList @("-m", "uvicorn", "main:app", "--reload", "--host", "127.0.0.1", "--port", "8768") -WorkingDirectory $backend -PassThru -WindowStyle Hidden

try {
    Write-Host "Starting CyberLab Modern frontend on http://127.0.0.1:5173"
    Push-Location $frontend
    npm run dev
}
finally {
    Pop-Location
    if ($backendProcess -and -not $backendProcess.HasExited) {
        Stop-Process -Id $backendProcess.Id -Force
    }
}
