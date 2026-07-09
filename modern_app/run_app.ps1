$ErrorActionPreference = "Stop"

$AppRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$FrontendDist = Join-Path $AppRoot "frontend\dist\index.html"

if (-not (Test-Path $FrontendDist)) {
    Push-Location (Join-Path $AppRoot "frontend")
    try {
        npm run build
    }
    finally {
        Pop-Location
    }
}

python (Join-Path $AppRoot "launch_modern_app.py")
