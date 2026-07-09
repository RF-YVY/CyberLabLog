from __future__ import annotations

import json
import time
import shutil
import sqlite3
import tempfile
import threading
import urllib.error
import urllib.request
import zipfile
from calendar import monthrange
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from database import (
    complete_in_progress_case,
    create_case,
    delete_case,
    duplicate_case,
    add_combo_value,
    delete_combo_value,
    rename_combo_value,
    get_analytics_summary,
    get_combo_values,
    get_json_setting,
    get_map_markers,
    get_stats,
    list_cases,
    list_in_progress,
    set_json_setting,
    update_case,
)
from exports import run_automated_exports_bridge
from legacy_import import import_app_data_zip, import_database_file
from native_exports import run_native_exports
from paths import active_db_path, backup_dir, data_dir, logo_path, marker_icon_path


FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
FRONTEND_ASSETS = FRONTEND_DIST / "assets"
RUNTIME_STARTED_AT = time.monotonic()
LAST_BROWSER_HEARTBEAT = 0.0
SHUTDOWN_REQUESTED = False
APP_VERSION = "3.0.3"
GITHUB_REPO = "RF-YVY/CyberLabLog"
UPDATE_CACHE_TTL_SECONDS = 900
UPDATE_CACHE: dict[str, Any] = {"checked_at": 0.0, "value": None}
SCHEDULER_LOCK = threading.Lock()
SCHEDULER_STARTED = False
SCHEDULER_STATUS: dict[str, Any] = {
    "enabled": False,
    "last_checked": "",
    "last_run": "",
    "last_result": None,
    "last_error": "",
}


app = FastAPI(title="CyberLab Modern API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(127\.0\.0\.1|localhost):51[0-9]{2}",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_ASSETS.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_ASSETS), name="assets")


DEFAULT_AUTOMATED_CONFIG = {
    "output_dir": "",
    "frequency": "manual",
    "date_range_mode": "current_week",
    "report_types": ["total_summary_pdf", "all_cases_pdf"],
    "page_size": "Letter",
    "orientation": "Auto",
    "recent_only": False,
    "recent_days": 31,
    "schedule_weekday": "Monday",
    "schedule_month_day": "1",
    "schedule_time": "08:00",
    "enable_schedule": False,
    "report_output_dirs": {},
    "graph_settings": {
        "include_png": True,
        "include_csv": True,
        "year_filter": "All",
        "types": ["Offense Type", "Device Type", "Agency"],
    },
    "map_settings": {
        "include_completed": True,
        "include_in_progress": True,
        "include_case_details": True,
        "include_data_file": True,
    },
}


class AutomatedRunRequest(BaseModel):
    output_dir: str | None = None
    report_types: list[str] | None = None
    page_size: str | None = None
    orientation: str | None = None


class JsonSettingRequest(BaseModel):
    value: dict[str, Any]


class OutputPathRequest(BaseModel):
    path: str


class ComboValueRequest(BaseModel):
    value: str


class ComboRenameRequest(BaseModel):
    old_value: str
    new_value: str


class BackupRestoreRequest(BaseModel):
    path: str


class CasePayload(BaseModel):
    case_number: str | None = None
    examiner: str | None = None
    investigator: str | None = None
    agency: str | None = None
    city_of_offense: str | None = None
    state_of_offense: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    volume_size_gb: float | str | None = None
    offense_type: str | None = None
    device_type: str | None = None
    model: str | None = None
    os: str | None = None
    forensic_tool: str | None = None
    data_recovered: str | bool | None = None
    fpr_complete: bool | int | str | None = None
    notes: str | None = None
    custom_fields: dict[str, Any] | str | None = None
    priority: str | None = None
    target_due_date: str | None = None
    workflow_status: str | None = None


def runtime_state() -> dict[str, Any]:
    return {
        "started_at": RUNTIME_STARTED_AT,
        "last_browser_heartbeat": LAST_BROWSER_HEARTBEAT,
        "shutdown_requested": SHUTDOWN_REQUESTED,
    }


def _version_parts(value: str | None) -> tuple[int, ...]:
    text = str(value or "").strip().lower().removeprefix("v")
    parts: list[int] = []
    for chunk in text.replace("-", ".").split("."):
        digits = "".join(ch for ch in chunk if ch.isdigit())
        if digits == "":
            break
        parts.append(int(digits))
    return tuple(parts or [0])


def _is_newer_version(remote: str | None, local: str) -> bool:
    remote_parts = _version_parts(remote)
    local_parts = _version_parts(local)
    width = max(len(remote_parts), len(local_parts))
    return remote_parts + (0,) * (width - len(remote_parts)) > local_parts + (0,) * (width - len(local_parts))


def _github_json(path: str) -> Any:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPO}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": f"CyberLab-Case-Tracker/{APP_VERSION}",
        },
    )
    with urllib.request.urlopen(request, timeout=2.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _latest_github_version() -> dict[str, Any]:
    now = time.monotonic()
    if UPDATE_CACHE["value"] and now - float(UPDATE_CACHE["checked_at"]) < UPDATE_CACHE_TTL_SECONDS:
        return UPDATE_CACHE["value"]

    latest = ""
    source = ""
    html_url = f"https://github.com/{GITHUB_REPO}"
    try:
        release = _github_json("/releases/latest")
        latest = str(release.get("tag_name") or release.get("name") or "")
        source = "release"
        html_url = str(release.get("html_url") or html_url)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, OSError):
        try:
            tags = _github_json("/tags")
            if isinstance(tags, list) and tags:
                latest = str(tags[0].get("name") or "")
                source = "tag"
                html_url = str(tags[0].get("zipball_url") or html_url)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, OSError, AttributeError):
            latest = ""

    value = {
        "latest_version": latest,
        "update_available": _is_newer_version(latest, APP_VERSION),
        "source": source,
        "url": html_url,
        "checked": bool(latest),
    }
    UPDATE_CACHE.update({"checked_at": now, "value": value})
    return value


def _file_info(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "name": path.name,
        "path": str(path),
        "size": stat.st_size,
        "modified": stat.st_mtime,
    }


def _backup_database_file(prefix: str = "caselog_gui_v6") -> Path:
    source = active_db_path()
    if not source.exists():
        raise FileNotFoundError("No active database found to back up.")
    target = backup_dir() / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        with sqlite3.connect(source) as src, sqlite3.connect(target) as dst:
            src.backup(dst)
    except sqlite3.DatabaseError:
        shutil.copy2(source, target)
    return target


def _safe_backup_path(value: str) -> Path:
    path = Path(value).expanduser().resolve()
    root = backup_dir().resolve()
    if root not in path.parents and path != root:
        raise ValueError("Restore path must be inside the app backup folder.")
    if path.suffix.lower() != ".db" or not path.exists():
        raise ValueError("Selected backup database was not found.")
    return path


def _automated_config() -> dict[str, Any]:
    value = DEFAULT_AUTOMATED_CONFIG.copy()
    stored = get_json_setting("automated_reports", {})
    if isinstance(stored, dict):
        value.update(stored)
        if isinstance(stored.get("graph_settings"), dict):
            graph_settings = DEFAULT_AUTOMATED_CONFIG["graph_settings"].copy()
            graph_settings.update(stored["graph_settings"])
            value["graph_settings"] = graph_settings
        if isinstance(stored.get("map_settings"), dict):
            map_settings = DEFAULT_AUTOMATED_CONFIG["map_settings"].copy()
            map_settings.update(stored["map_settings"])
            value["map_settings"] = map_settings
    return value


def _schedule_token(config: dict[str, Any], now: datetime) -> str | None:
    if not config.get("enable_schedule"):
        return None
    frequency = str(config.get("frequency") or "manual").lower()
    if frequency == "manual":
        return None
    schedule_time = str(config.get("schedule_time") or "08:00").strip()
    try:
        hour, minute = [int(part) for part in schedule_time.split(":", 1)]
    except (ValueError, TypeError):
        hour, minute = 8, 0
    if (now.hour, now.minute) < (hour, minute):
        return None
    if frequency == "daily":
        return now.strftime("daily:%Y-%m-%d")
    if frequency == "weekly":
        weekday = str(config.get("schedule_weekday") or "Monday").lower()
        if now.strftime("%A").lower() == weekday:
            return now.strftime("weekly:%Y-%m-%d")
    if frequency == "monthly":
        try:
            month_day = max(1, min(31, int(config.get("schedule_month_day") or 1)))
        except (TypeError, ValueError):
            month_day = 1
        if now.day == min(month_day, monthrange(now.year, now.month)[1]):
            return now.strftime("monthly:%Y-%m")
    return None


def _scheduler_loop() -> None:
    while True:
        try:
            config = _automated_config()
            now = datetime.now()
            token = _schedule_token(config, now)
            state = get_json_setting("automated_report_scheduler", {})
            last_token = state.get("last_token") if isinstance(state, dict) else ""
            SCHEDULER_STATUS.update({
                "enabled": bool(config.get("enable_schedule")),
                "last_checked": now.isoformat(timespec="seconds"),
                "last_error": "",
            })
            if token and token != last_token:
                result = run_native_exports(config)
                set_json_setting("automated_report_scheduler", {
                    "last_token": token,
                    "last_run": now.isoformat(timespec="seconds"),
                    "last_output_dir": result.get("output_dir", ""),
                })
                SCHEDULER_STATUS.update({
                    "last_run": now.isoformat(timespec="seconds"),
                    "last_result": {
                        "ok": result.get("ok"),
                        "files": len(result.get("files") or []),
                        "output_dir": result.get("output_dir", ""),
                    },
                })
        except Exception as exc:
            SCHEDULER_STATUS.update({
                "last_checked": datetime.now().isoformat(timespec="seconds"),
                "last_error": str(exc),
            })
        time.sleep(60)


def _start_scheduler_once() -> None:
    global SCHEDULER_STARTED
    with SCHEDULER_LOCK:
        if SCHEDULER_STARTED:
            return
        SCHEDULER_STARTED = True
        threading.Thread(target=_scheduler_loop, name="cyberlab-report-scheduler", daemon=True).start()


@app.on_event("startup")
def start_background_services() -> None:
    _start_scheduler_once()


@app.post("/api/runtime/heartbeat")
def browser_heartbeat() -> dict[str, Any]:
    global LAST_BROWSER_HEARTBEAT
    LAST_BROWSER_HEARTBEAT = time.monotonic()
    return {"ok": True}


@app.post("/api/runtime/shutdown")
def browser_shutdown() -> dict[str, Any]:
    global SHUTDOWN_REQUESTED
    SHUTDOWN_REQUESTED = True
    return {"ok": True}


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "database": str(active_db_path()),
        "app_data": str(data_dir()),
        "stats": get_stats(),
    }


@app.get("/api/app-info")
def app_info() -> dict[str, Any]:
    update_info = _latest_github_version()
    return {
        "name": "CyberLab Case Tracker",
        "version": APP_VERSION,
        "repository": GITHUB_REPO,
        **update_info,
    }


@app.get("/api/cases")
def cases(
    search: str = "",
    sort: str = "newest",
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    return list_cases(search=search, sort=sort, limit=limit, offset=offset)


@app.post("/api/cases")
def create_completed_case(payload: CasePayload) -> dict[str, Any]:
    try:
        return create_case(payload.model_dump(), in_progress=False)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/cases/{case_id}")
def update_completed_case(case_id: int, payload: CasePayload) -> dict[str, Any]:
    try:
        return update_case(case_id, payload.model_dump(), in_progress=False)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/cases/{case_id}")
def delete_completed_case(case_id: int) -> dict[str, Any]:
    try:
        delete_case(case_id, in_progress=False)
        return {"deleted": case_id}
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/cases/{case_id}/duplicate")
def duplicate_completed_case(case_id: int) -> dict[str, Any]:
    try:
        return duplicate_case(case_id, in_progress=False)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/in-progress")
def in_progress_cases(
    search: str = "",
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    return list_in_progress(search=search, limit=limit, offset=offset)


@app.post("/api/in-progress")
def create_in_progress_case(payload: CasePayload) -> dict[str, Any]:
    try:
        return create_case(payload.model_dump(), in_progress=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/in-progress/{case_id}")
def update_in_progress_case(case_id: int, payload: CasePayload) -> dict[str, Any]:
    try:
        return update_case(case_id, payload.model_dump(), in_progress=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/in-progress/{case_id}")
def delete_in_progress_case(case_id: int) -> dict[str, Any]:
    try:
        delete_case(case_id, in_progress=True)
        return {"deleted": case_id}
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/in-progress/{case_id}/duplicate")
def duplicate_in_progress(case_id: int) -> dict[str, Any]:
    try:
        return duplicate_case(case_id, in_progress=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/in-progress/{case_id}/complete")
def complete_case(case_id: int) -> dict[str, Any]:
    try:
        return complete_in_progress_case(case_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/stats")
def stats() -> dict[str, Any]:
    return get_stats()


@app.get("/api/analytics/summary")
def analytics_summary() -> dict[str, Any]:
    return get_analytics_summary()


@app.get("/api/map/markers")
def map_markers() -> dict[str, Any]:
    return {"markers": get_map_markers()}


@app.get("/api/settings/json/{key}")
def read_json_setting(key: str) -> dict[str, Any]:
    return {"key": key, "value": get_json_setting(key, {})}


@app.put("/api/settings/json/{key}")
def write_json_setting(key: str, payload: JsonSettingRequest) -> dict[str, Any]:
    try:
        set_json_setting(key, payload.value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"key": key, "value": payload.value}


@app.get("/api/settings/combos")
def read_combo_values() -> dict[str, list[str]]:
    keys = ["examiner", "investigator", "agency", "city_of_offense", "state_of_offense", "offense_type", "device_type", "forensic_tool"]
    return {key: get_combo_values(key) for key in keys}


@app.post("/api/settings/combos/{key}")
def write_combo_value(key: str, payload: ComboValueRequest) -> dict[str, Any]:
    try:
        return {"key": key, "values": add_combo_value(key, payload.value)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/settings/combos/{key}")
def rename_combo_endpoint(key: str, payload: ComboRenameRequest) -> dict[str, Any]:
    try:
        return {"key": key, "values": rename_combo_value(key, payload.old_value, payload.new_value)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/settings/combos/{key}")
def remove_combo_value(key: str, value: str = Query("")) -> dict[str, Any]:
    try:
        return {"key": key, "values": delete_combo_value(key, value)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/backups")
def list_backups() -> dict[str, Any]:
    backups = sorted(
        [path for pattern in ("*.db", "*.zip") for path in backup_dir().glob(pattern)],
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    return {"backup_dir": str(backup_dir()), "files": [_file_info(path) for path in backups[:100]]}


@app.post("/api/backups/create")
def create_backup() -> dict[str, Any]:
    try:
        backup = _backup_database_file()
        return {"ok": True, "backup": _file_info(backup)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/backups/restore")
def restore_backup(payload: BackupRestoreRequest) -> dict[str, Any]:
    try:
        selected = _safe_backup_path(payload.path)
        pre_restore = _backup_database_file("pre_restore")
        shutil.copy2(selected, active_db_path())
        return {
            "ok": True,
            "restored": _file_info(active_db_path()),
            "pre_restore_backup": _file_info(pre_restore),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/backups/support-bundle")
def create_support_bundle() -> dict[str, Any]:
    try:
        bundle = backup_dir() / f"cyberlab_support_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as archive:
            if active_db_path().exists():
                archive.write(active_db_path(), active_db_path().name)
            for item in data_dir().rglob("*"):
                if item.is_file() and item != bundle:
                    archive.write(item, Path("app_data") / item.relative_to(data_dir()))
        return {"ok": True, "bundle": _file_info(bundle)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/settings/logo")
def read_logo_info() -> dict[str, Any]:
    path = logo_path()
    return {"exists": path.exists(), "path": str(path) if path.exists() else "", "version": path.stat().st_mtime_ns if path.exists() else 0}


@app.get("/api/settings/logo/image")
def read_logo_image() -> FileResponse:
    path = logo_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="No report logo configured")
    return FileResponse(path, headers={"Cache-Control": "no-store"})


@app.post("/api/settings/logo")
async def upload_logo(file: UploadFile = File(...)) -> dict[str, Any]:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg"}:
        raise HTTPException(status_code=400, detail="Logo must be a PNG or JPG image")
    target = logo_path()
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        if suffix == ".png":
            shutil.copy2(tmp_path, target)
        else:
            try:
                from PIL import Image

                with Image.open(tmp_path) as image:
                    image.save(target, "PNG")
            except Exception:
                shutil.copy2(tmp_path, target)
        return {"exists": True, "path": str(target), "version": target.stat().st_mtime_ns}
    finally:
        tmp_path.unlink(missing_ok=True)


@app.get("/api/settings/marker-icon")
def read_marker_icon_info() -> dict[str, Any]:
    path = marker_icon_path()
    return {"exists": path.exists(), "path": str(path) if path.exists() else "", "version": path.stat().st_mtime_ns if path.exists() else 0}


@app.get("/api/settings/marker-icon/image")
def read_marker_icon_image() -> FileResponse:
    path = marker_icon_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="No map marker icon configured")
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "no-store"})


@app.post("/api/settings/marker-icon")
async def upload_marker_icon(file: UploadFile = File(...)) -> dict[str, Any]:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".bmp", ".png", ".jpg", ".jpeg"}:
        raise HTTPException(status_code=400, detail="Marker icon must be a BMP, PNG, or JPG image")
    target = marker_icon_path()
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            from PIL import Image

            with Image.open(tmp_path) as image:
                image = image.convert("RGBA")
                image.thumbnail((96, 96), Image.Resampling.LANCZOS)
                canvas = Image.new("RGBA", (96, 96), (255, 255, 255, 0))
                x = (96 - image.width) // 2
                y = (96 - image.height) // 2
                canvas.alpha_composite(image, (x, y))
                canvas.save(target, "PNG")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not process marker image: {exc}") from exc
        return {"exists": True, "path": str(target), "version": target.stat().st_mtime_ns}
    finally:
        tmp_path.unlink(missing_ok=True)


@app.get("/api/automated-exports/config")
def read_automated_exports_config() -> dict[str, Any]:
    return {"value": _automated_config()}


@app.put("/api/automated-exports/config")
def write_automated_exports_config(payload: JsonSettingRequest) -> dict[str, Any]:
    try:
        set_json_setting("automated_reports", payload.value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"value": payload.value}


@app.get("/api/automated-exports/scheduler")
def automated_export_scheduler_status() -> dict[str, Any]:
    config = _automated_config()
    state = get_json_setting("automated_report_scheduler", {})
    return {
        **SCHEDULER_STATUS,
        "configured": {
            "enabled": bool(config.get("enable_schedule")),
            "frequency": config.get("frequency", "manual"),
            "schedule_time": config.get("schedule_time", "08:00"),
            "schedule_weekday": config.get("schedule_weekday", "Monday"),
            "schedule_month_day": config.get("schedule_month_day", "1"),
        },
        "state": state if isinstance(state, dict) else {},
    }


@app.post("/api/import/database")
async def import_database(file: UploadFile = File(...)) -> dict[str, Any]:
    suffix = Path(file.filename or "").suffix or ".db"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        safety_backup = None
        if active_db_path().exists():
            safety_backup = _backup_database_file("pre_import")
        result = import_database_file(tmp_path)
        if safety_backup:
            result["safety_backup"] = str(safety_backup)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        tmp_path.unlink(missing_ok=True)


@app.post("/api/import/app-data")
async def import_app_data(file: UploadFile = File(...)) -> dict[str, Any]:
    suffix = Path(file.filename or "").suffix or ".zip"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        safety_backup = None
        if active_db_path().exists():
            safety_backup = _backup_database_file("pre_app_data_import")
        result = import_app_data_zip(tmp_path)
        if safety_backup:
            result["safety_backup"] = str(safety_backup)
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        tmp_path.unlink(missing_ok=True)


@app.post("/api/automated-exports/run")
def run_automated_exports(payload: AutomatedRunRequest) -> dict[str, Any]:
    result = run_automated_exports_bridge(
        output_dir=payload.output_dir,
        report_types=payload.report_types,
        page_size=payload.page_size,
        orientation=payload.orientation,
    )
    if not result["ok"]:
        raise HTTPException(status_code=500, detail=result)
    return result


@app.post("/api/native-exports/run")
def run_native_export(payload: JsonSettingRequest) -> dict[str, Any]:
    try:
        return run_native_exports(payload.value)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/files/list-output")
def list_output_files(payload: OutputPathRequest) -> dict[str, Any]:
    path = Path(payload.path).expanduser()
    if not path.exists() or not path.is_dir():
        return {"path": str(path), "exists": False, "files": []}
    files = []
    for item in sorted((p for p in path.rglob("*") if p.is_file()), key=lambda p: p.stat().st_mtime, reverse=True)[:50]:
        if item.is_file():
            stat = item.stat()
            files.append({
                "name": str(item.relative_to(path)),
                "path": str(item),
                "size": stat.st_size,
                "modified": stat.st_mtime,
            })
    return {"path": str(path), "exists": True, "files": files}


@app.get("/")
def serve_frontend_index() -> FileResponse:
    index_path = FRONTEND_DIST / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend build not found. Run npm run build in modern_app/frontend.")
    return FileResponse(index_path)


@app.get("/{path:path}")
def serve_frontend_route(path: str) -> FileResponse:
    if path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    index_path = FRONTEND_DIST / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend build not found. Run npm run build in modern_app/frontend.")
    return FileResponse(index_path)
