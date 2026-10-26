"""Independent Windows handoff for the portable, single-file application."""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
import subprocess
import sys


def installer_environment() -> dict[str, str]:
    # Match APRS-propview: the old one-file extraction directory is short-lived.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("_PYI_", "_MEIPASS"))}
    env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
    # PowerShell 7's module path can hide Windows PowerShell's built-in modules.
    env.pop("PSModulePath", None)
    return env


def _ps_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def handoff_script(staged: Path, target: Path, sha256: str, parent_pid: int,
                   launch_args: list[str] | None = None) -> str:
    arguments = ""
    if launch_args:
        arguments = " -ArgumentList " + _ps_string(subprocess.list2cmdline(launch_args))
    return f"""
$ErrorActionPreference = 'Stop'
$staged = {_ps_string(staged)}
$target = {_ps_string(target)}
$folder = {_ps_string(target.parent)}
$log = {_ps_string(target.parent / 'app_data' / 'update-install.log')}
$backup = $target + '.previous'
$pending = $target + '.new'
$replaced = $false
try {{
    Wait-Process -Id {int(parent_pid)} -Timeout 120 -ErrorAction SilentlyContinue
    if (Get-Process -Id {int(parent_pid)} -ErrorAction SilentlyContinue) {{ throw 'The application did not close in time. No files were replaced.' }}
    $hashAlgorithm = [System.Security.Cryptography.SHA256]::Create()
    $stream = [System.IO.File]::OpenRead($staged)
    try {{ $actualHash = [BitConverter]::ToString($hashAlgorithm.ComputeHash($stream)).Replace('-', '') }}
    finally {{ $stream.Dispose(); $hashAlgorithm.Dispose() }}
    if ($actualHash -ne {_ps_string(sha256)}) {{ throw 'Update checksum changed before installation.' }}
    Copy-Item -LiteralPath $target -Destination $backup -Force
    Copy-Item -LiteralPath $staged -Destination $pending -Force
    $copied = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {{
        try {{ Move-Item -LiteralPath $pending -Destination $target -Force; $copied = $true; break }}
        catch {{ Start-Sleep -Milliseconds 500 }}
    }}
    if (-not $copied) {{ throw 'The executable is still locked. Close other CyberLab windows and try again.' }}
    $replaced = $true
    $env:CYBERLAB_APP_ROOT = $folder
    $env:PYINSTALLER_RESET_ENVIRONMENT = '1'
    Start-Process -FilePath $target -WorkingDirectory $folder -WindowStyle Hidden{arguments} -ErrorAction Stop
    'Update installed and restart initiated.' | Set-Content -LiteralPath $log
}} catch {{
    $failure = $_.Exception.Message
    if ($replaced) {{
        try {{ Copy-Item -LiteralPath $backup -Destination $target -Force }}
        catch {{ $failure += ' Rollback failed: ' + $_.Exception.Message }}
    }}
    ('Update failed: ' + $failure) | Set-Content -LiteralPath $log
}}
"""


def launch_handoff(staged: Path, target: Path, sha256: str, parent_pid: int,
                   launch_args: list[str] | None = None) -> None:
    if sys.platform != "win32":
        raise RuntimeError("In-app installation requires Windows")
    staged, target = staged.resolve(), target.resolve()
    if staged == target or not target.is_file():
        raise ValueError("The installed executable could not be identified")
    with staged.open("rb") as stream:
        if stream.read(2) != b"MZ":
            raise ValueError("The downloaded file is not a Windows executable")
        stream.seek(0)
        if hashlib.file_digest(stream, "sha256").hexdigest().lower() != sha256.lower():
            raise ValueError("The downloaded update checksum changed")
    (target.parent / "app_data").mkdir(parents=True, exist_ok=True)
    script = handoff_script(staged, target, sha256, parent_pid, launch_args)
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    powershell = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    subprocess.Popen(
        [str(powershell), "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-EncodedCommand", encoded],
        env=installer_environment(), cwd=str(target.parent),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), close_fds=True,
    )
