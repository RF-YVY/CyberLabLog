from __future__ import annotations

import shutil
import zipfile
from datetime import datetime
from pathlib import Path

from database import validate_legacy_database
from paths import active_db_path, backup_dir, data_dir


def backup_current_database() -> Path | None:
    current = active_db_path()
    if not current.exists():
        return None
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    target = backup_dir() / f"{current.stem}_pre_modern_import_{stamp}.db"
    shutil.copy2(current, target)
    return target


def import_database_file(source: Path) -> dict[str, str | None]:
    validate_legacy_database(source)
    backup_path = backup_current_database()
    shutil.copy2(source, active_db_path())
    return {
        "database": str(active_db_path()),
        "backup": str(backup_path) if backup_path else None,
    }


def import_app_data_zip(source_zip: Path) -> dict[str, int | str]:
    target_root = data_dir()
    copied = 0
    with zipfile.ZipFile(source_zip) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            member_path = Path(member.filename)
            parts = [part for part in member_path.parts if part not in {"", ".", "app_data"}]
            if not parts:
                continue
            destination = target_root.joinpath(*parts).resolve()
            if target_root.resolve() not in destination.parents and destination != target_root.resolve():
                raise ValueError(f"Unsafe path in archive: {member.filename}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as src, destination.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            copied += 1
    return {"app_data": str(target_root), "files_imported": copied}

