from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(os.environ.get("CYBERLAB_APP_ROOT", Path(__file__).resolve().parents[2])).resolve()
LEGACY_SCRIPT = PROJECT_ROOT / "CyberLabCaseTracker.py"
DB_NAME = "caselog_gui_v6.db"
DATA_DIR_NAME = "app_data"


def active_db_path() -> Path:
    return PROJECT_ROOT / DB_NAME


def data_dir() -> Path:
    path = PROJECT_ROOT / DATA_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def backup_dir() -> Path:
    path = data_dir() / "backups"
    path.mkdir(parents=True, exist_ok=True)
    return path


def automated_reports_dir() -> Path:
    path = data_dir() / "automated_reports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def logo_path() -> Path:
    return data_dir() / "logo.png"


def marker_icon_path() -> Path:
    return data_dir() / "marker_icon.png"
