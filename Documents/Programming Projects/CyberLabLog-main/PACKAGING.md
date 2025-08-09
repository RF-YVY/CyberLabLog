# Packaging and Code Signing (Windows)

## Build with PyInstaller

- Ensure Python dependencies are installed (see requirements.txt).
- Build using the provided spec file:
  - PowerShell: `pyinstaller CyberLabCaseTracker.spec`
- The EXE will be in `dist/CyberLabCaseTracker/CyberLabCaseTracker.exe`.

## Code Signing (Optional)

- Use `scripts/build_and_sign.ps1` to sign the EXE with a PFX certificate.
- Requires Windows SDK (signtool.exe) on PATH.
- Example:
  - `./scripts/build_and_sign.ps1 -CertPath "C:\path\to\cert.pfx" -CertPassword "******" -TimestampUrl "http://timestamp.sectigo.com"`

## Update Checks

- The app provides a notify-only update check.
- About tab includes a "Check for Updates" button; app also performs a silent check on startup.
- It compares the built-in version to the latest GitHub Release and opens the releases page if newer.
