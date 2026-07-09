from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from paths import LEGACY_SCRIPT


def run_automated_exports_bridge(
    output_dir: str | None = None,
    report_types: list[str] | None = None,
    page_size: str | None = None,
    orientation: str | None = None,
) -> dict[str, Any]:
    if not LEGACY_SCRIPT.exists():
        raise FileNotFoundError(f"Legacy export engine not found: {LEGACY_SCRIPT}")

    cmd = [sys.executable, str(LEGACY_SCRIPT), "--run-automated-exports"]
    if output_dir:
        cmd.extend(["--output-dir", output_dir])
    if report_types:
        cmd.append("--report-types")
        cmd.extend(report_types)
    if page_size:
        cmd.extend(["--page-size", page_size])
    if orientation:
        cmd.extend(["--orientation", orientation])

    result = subprocess.run(
        cmd,
        cwd=str(LEGACY_SCRIPT.parent),
        capture_output=True,
        text=True,
        timeout=600,
    )
    return {
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "command": cmd,
    }

