from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import struct
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from paths import active_db_path, backup_dir, data_dir


MAGIC = b"CYBERLAB-BACKUP-1\n"
ITERATIONS = 600_000


def _derive_key(password: str, salt: bytes) -> bytes:
    if len(password) < 8:
        raise ValueError("Backup password must be at least 8 characters")
    return PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=ITERATIONS).derive(password.encode("utf-8"))


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _database_snapshot(target: Path) -> None:
    source = active_db_path()
    if not source.exists():
        raise FileNotFoundError("No active database was found")
    src = sqlite3.connect(source)
    dst = sqlite3.connect(target)
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()


def _portable_files(database_snapshot: Path) -> list[tuple[Path, str]]:
    files = [(database_snapshot, active_db_path().name)]
    root = data_dir()
    for item in root.rglob("*"):
        if not item.is_file():
            continue
        relative = item.relative_to(root)
        if relative.parts and relative.parts[0].lower() in {"backups", "automated_reports"}:
            continue
        files.append((item, str(Path("app_data") / relative).replace("\\", "/")))
    return files


def create_encrypted_backup(password: str) -> Path:
    destination = backup_dir() / f"cyberlab_portable_{datetime.now().strftime('%Y%m%d_%H%M%S')}.clbackup"
    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = _derive_key(password, salt)
    with tempfile.TemporaryDirectory(prefix="cyberlab_portable_backup_") as temp_value:
        temp = Path(temp_value)
        database_snapshot = temp / active_db_path().name
        archive_path = temp / "payload.zip"
        _database_snapshot(database_snapshot)
        entries: list[dict[str, Any]] = []
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for source, archive_name in _portable_files(database_snapshot):
                entries.append({"path": archive_name, "size": source.stat().st_size, "sha256": _hash_file(source)})
                archive.write(source, archive_name)
            manifest = {
                "format": 1,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "database": active_db_path().name,
                "files": entries,
            }
            archive.writestr("manifest.json", json.dumps(manifest, indent=2))

        encryptor = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
        with archive_path.open("rb") as source, destination.open("wb") as output:
            output.write(MAGIC)
            output.write(struct.pack(">I", ITERATIONS))
            output.write(salt)
            output.write(nonce)
            while chunk := source.read(1024 * 1024):
                output.write(encryptor.update(chunk))
            output.write(encryptor.finalize())
            output.write(encryptor.tag)
    return destination


def _decrypt_backup(source: Path, password: str, target: Path) -> None:
    with source.open("rb") as encrypted:
        if encrypted.read(len(MAGIC)) != MAGIC:
            raise ValueError("This is not a CyberLab encrypted backup")
        iterations = struct.unpack(">I", encrypted.read(4))[0]
        salt = encrypted.read(16)
        nonce = encrypted.read(12)
        encrypted.seek(0, 2)
        total_size = encrypted.tell()
        tag_position = total_size - 16
        encrypted.seek(tag_position)
        tag = encrypted.read(16)
        encrypted.seek(len(MAGIC) + 4 + 16 + 12)
        remaining = tag_position - encrypted.tell()
        key = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=iterations).derive(password.encode("utf-8"))
        decryptor = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
        with target.open("wb") as output:
            while remaining > 0:
                chunk = encrypted.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ValueError("Encrypted backup is incomplete")
                remaining -= len(chunk)
                output.write(decryptor.update(chunk))
            try:
                output.write(decryptor.finalize())
            except Exception as exc:
                raise ValueError("Incorrect password or backup integrity verification failed") from exc


def restore_encrypted_backup(source: Path, password: str) -> dict[str, Any]:
    source = source.expanduser().resolve()
    root = backup_dir().resolve()
    if source.suffix.lower() != ".clbackup" or not source.exists() or (source.parent != root and root not in source.parents):
        raise ValueError("Encrypted backup must be selected from the application backup folder")
    with tempfile.TemporaryDirectory(prefix="cyberlab_portable_restore_") as temp_value:
        temp = Path(temp_value)
        archive_path = temp / "payload.zip"
        extracted = temp / "extracted"
        extracted.mkdir()
        _decrypt_backup(source, password, archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            names = set(archive.namelist())
            if "manifest.json" not in names:
                raise ValueError("Backup manifest is missing")
            manifest = json.loads(archive.read("manifest.json"))
            for member in archive.infolist():
                resolved = (extracted / member.filename).resolve()
                if extracted.resolve() not in resolved.parents and resolved != extracted.resolve():
                    raise ValueError("Backup contains an unsafe file path")
            archive.extractall(extracted)
        for entry in manifest.get("files") or []:
            path = extracted / str(entry.get("path") or "")
            if not path.is_file() or _hash_file(path) != str(entry.get("sha256") or ""):
                raise ValueError(f"Backup integrity check failed for {entry.get('path')}")

        safety = backup_dir() / f"pre_portable_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        _database_snapshot(safety)
        database_file = extracted / str(manifest.get("database") or active_db_path().name)
        if not database_file.exists():
            raise ValueError("Backup database is missing")
        shutil.copy2(database_file, active_db_path())
        restored_files = 1
        app_data_source = extracted / "app_data"
        if app_data_source.exists():
            for item in app_data_source.rglob("*"):
                if item.is_file():
                    destination = data_dir() / item.relative_to(app_data_source)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item, destination)
                    restored_files += 1
    return {"restored_files": restored_files, "safety_backup": str(safety), "manifest": manifest}
