from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from paths import active_db_path


CASE_COLUMNS = [
    "id",
    "case_number",
    "examiner",
    "investigator",
    "agency",
    "city_of_offense",
    "state_of_offense",
    "start_date",
    "end_date",
    "volume_size_gb",
    "offense_type",
    "device_type",
    "model",
    "os",
    "forensic_tool",
    "data_recovered",
    "fpr_complete",
    "notes",
    "custom_fields",
    "created_at",
]

CASE_WRITE_COLUMNS = [
    "case_number",
    "examiner",
    "investigator",
    "agency",
    "city_of_offense",
    "state_of_offense",
    "start_date",
    "end_date",
    "volume_size_gb",
    "offense_type",
    "device_type",
    "model",
    "os",
    "forensic_tool",
    "data_recovered",
    "fpr_complete",
    "notes",
    "custom_fields",
]

IN_PROGRESS_COLUMNS = [
    *CASE_COLUMNS,
    "priority",
    "target_due_date",
    "workflow_status",
]

IN_PROGRESS_WRITE_COLUMNS = [
    *CASE_WRITE_COLUMNS,
    "priority",
    "target_due_date",
    "workflow_status",
]

SORT_COLUMNS = {
    "newest": ("created_at", "DESC"),
    "oldest": ("created_at", "ASC"),
    "start_newest": ("start_date", "DESC"),
    "start_oldest": ("start_date", "ASC"),
    "case_number": ("case_number", "ASC"),
    "agency": ("agency", "ASC"),
    "offense": ("offense_type", "ASC"),
}

COMBO_COLUMNS = {
    "examiner": "examiner",
    "investigator": "investigator",
    "agency": "agency",
    "city_of_offense": "city_of_offense",
    "state_of_offense": "state_of_offense",
    "offense_type": "offense_type",
    "device_type": "device_type",
    "forensic_tool": "forensic_tool",
}

DEVICE_TYPE_ALIASES = {
    "android": "Android",
    "chrome os": "ChromeOS",
    "chromeos": "ChromeOS",
    "digital camera": "Digital Camera",
    "hdd": "HDD",
    "ios": "iOS",
    "iphone": "iOS",
    "ipad": "iOS",
    "laptop": "Laptop",
    "nas": "NAS",
    "other": "Other",
    "sd": "SD",
    "sdd": "SSD",
    "ssd": "SSD",
    "sim": "SIM",
    "usb": "USB",
    "windows": "Windows",
}


def _read_setting_json_list(conn: sqlite3.Connection, key: str) -> list[str]:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if not row or not row["value"]:
        return []
    decoded = json.loads(row["value"])
    if not isinstance(decoded, list):
        return []
    return [str(item).strip() for item in decoded if str(item).strip()]


def _write_setting_json_list(conn: sqlite3.Connection, key: str, values: list[str]) -> None:
    conn.execute(
        "REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, json.dumps(values)),
    )


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or active_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def database_exists() -> bool:
    return active_db_path().exists()


def ensure_schema() -> None:
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS case_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT,
                examiner TEXT,
                offense_type TEXT,
                device_type TEXT,
                start_date TEXT,
                end_date TEXT,
                volume_size_gb REAL,
                city_of_offense TEXT,
                state_of_offense TEXT,
                investigator TEXT,
                agency TEXT,
                model TEXT,
                os TEXT,
                forensic_tool TEXT,
                data_recovered TEXT,
                fpr_complete INTEGER,
                notes TEXT,
                custom_fields TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS in_progress_cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT,
                examiner TEXT,
                offense_type TEXT,
                device_type TEXT,
                start_date TEXT,
                end_date TEXT,
                volume_size_gb REAL,
                city_of_offense TEXT,
                state_of_offense TEXT,
                investigator TEXT,
                agency TEXT,
                model TEXT,
                os TEXT,
                forensic_tool TEXT,
                data_recovered TEXT,
                fpr_complete INTEGER,
                notes TEXT,
                custom_fields TEXT,
                created_at TEXT,
                priority TEXT DEFAULT 'Medium',
                target_due_date TEXT,
                workflow_status TEXT DEFAULT 'Intake'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS geocache (
                location_key TEXT PRIMARY KEY,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                last_accessed TEXT
            )
            """
        )
        for table in ("case_log", "in_progress_cases"):
            try:
                existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                if "custom_fields" not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN custom_fields TEXT")
            except sqlite3.DatabaseError:
                pass
        conn.commit()


def validate_legacy_database(db_path: Path) -> None:
    if not db_path.exists():
        raise ValueError(f"Database does not exist: {db_path}")
    try:
        with connect(db_path) as conn:
            tables = {
                row["name"]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
    except sqlite3.DatabaseError as exc:
        raise ValueError("Selected file is not a readable SQLite database.") from exc

    required = {"case_log", "settings"}
    missing = sorted(required - tables)
    if missing:
        raise ValueError(f"Database is missing required table(s): {', '.join(missing)}")


def get_stats() -> dict[str, Any]:
    ensure_schema()

    with connect() as conn:
        def count(table: str) -> int:
            try:
                return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])
            except sqlite3.DatabaseError:
                return 0

        try:
            total_volume = float(conn.execute("SELECT COALESCE(SUM(volume_size_gb), 0) AS total FROM case_log").fetchone()["total"] or 0)
        except sqlite3.DatabaseError:
            total_volume = 0.0
        return {
            "database_exists": database_exists(),
            "db_path": str(active_db_path()),
            "completed_cases": count("case_log"),
            "in_progress_cases": count("in_progress_cases"),
            "settings": count("settings"),
            "total_volume_gb": total_volume,
        }


def list_cases(
    search: str = "",
    sort: str = "newest",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    ensure_schema()

    sort_col, sort_dir = SORT_COLUMNS.get(sort, SORT_COLUMNS["newest"])
    where = ""
    params: list[Any] = []
    if search:
        like = f"%{search.lower()}%"
        searchable = [
            "case_number",
            "examiner",
            "investigator",
            "agency",
            "city_of_offense",
            "state_of_offense",
            "offense_type",
            "device_type",
            "model",
            "os",
            "forensic_tool",
            "notes",
        ]
        where = "WHERE " + " OR ".join([f"LOWER(COALESCE({col}, '')) LIKE ?" for col in searchable])
        params.extend([like] * len(searchable))

    with connect() as conn:
        total_sql = f"SELECT COUNT(*) AS n FROM case_log {where}"
        total = int(conn.execute(total_sql, params).fetchone()["n"])
        cols = ", ".join(CASE_COLUMNS)
        sql = (
            f"SELECT {cols} FROM case_log {where} "
            f"ORDER BY datetime({sort_col}) {sort_dir}, id {sort_dir} "
            "LIMIT ? OFFSET ?"
        )
        rows = [dict(row) for row in conn.execute(sql, [*params, limit, offset]).fetchall()]
    return {"rows": rows, "total": total, "limit": limit, "offset": offset}


def list_in_progress(
    search: str = "",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    ensure_schema()
    where = ""
    params: list[Any] = []
    if search:
        like = f"%{search.lower()}%"
        searchable = [
            "case_number",
            "examiner",
            "investigator",
            "agency",
            "city_of_offense",
            "state_of_offense",
            "offense_type",
            "device_type",
            "priority",
            "workflow_status",
            "notes",
        ]
        where = "WHERE " + " OR ".join([f"LOWER(COALESCE({col}, '')) LIKE ?" for col in searchable])
        params.extend([like] * len(searchable))

    with connect() as conn:
        total = int(conn.execute(f"SELECT COUNT(*) AS n FROM in_progress_cases {where}", params).fetchone()["n"])
        cols = ", ".join(IN_PROGRESS_COLUMNS)
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT {cols} FROM in_progress_cases {where} ORDER BY datetime(created_at) DESC, id DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        ]
    return {"rows": rows, "total": total, "limit": limit, "offset": offset}


def _normalize_case_payload(payload: dict[str, Any], in_progress: bool = False) -> dict[str, Any]:
    allowed = IN_PROGRESS_WRITE_COLUMNS if in_progress else CASE_WRITE_COLUMNS
    data = {key: payload.get(key) for key in allowed}
    for key in ("case_number", "examiner", "investigator", "agency", "city_of_offense", "state_of_offense", "offense_type", "device_type", "model", "os", "forensic_tool", "notes", "priority", "workflow_status"):
        if key in data and data[key] is not None:
            data[key] = str(data[key]).strip()
    if "custom_fields" in data:
        custom_fields = data.get("custom_fields")
        if isinstance(custom_fields, dict):
            data["custom_fields"] = json.dumps({str(k): str(v) for k, v in custom_fields.items()})
        elif custom_fields in ("", None):
            data["custom_fields"] = "{}"
        else:
            try:
                parsed = json.loads(str(custom_fields))
                data["custom_fields"] = json.dumps(parsed if isinstance(parsed, dict) else {})
            except Exception:
                data["custom_fields"] = "{}"
    if data.get("device_type"):
        data["device_type"] = DEVICE_TYPE_ALIASES.get(str(data["device_type"]).strip().lower(), str(data["device_type"]).strip())
    if data.get("volume_size_gb") in ("", None):
        data["volume_size_gb"] = None
    else:
        data["volume_size_gb"] = float(data["volume_size_gb"])
    dr_val = data.get("data_recovered")
    if isinstance(dr_val, bool):
        data["data_recovered"] = "Yes" if dr_val else "No"
    elif isinstance(dr_val, str) and dr_val.lower() in {"yes", "true", "1", "y"}:
        data["data_recovered"] = "Yes"
    elif isinstance(dr_val, str) and dr_val.lower() in {"no", "false", "0", "n"}:
        data["data_recovered"] = "No"
    else:
        data["data_recovered"] = ""
    data["fpr_complete"] = 1 if data.get("fpr_complete") in (True, 1, "1", "true", "True", "yes", "Yes") else 0
    if in_progress:
        data["priority"] = data.get("priority") or "Medium"
        data["workflow_status"] = data.get("workflow_status") or "Intake"
    return data


def _ensure_case_number_available(
    conn: sqlite3.Connection,
    table: str,
    case_number: str | None,
    exclude_id: int | None = None,
) -> None:
    case_number = (case_number or "").strip()
    if not case_number:
        return
    sql = f"SELECT id FROM {table} WHERE LOWER(TRIM(case_number)) = LOWER(TRIM(?))"
    params: list[Any] = [case_number]
    if exclude_id is not None:
        sql += " AND id <> ?"
        params.append(exclude_id)
    row = conn.execute(sql, params).fetchone()
    if row:
        raise ValueError(f"Case number already exists: {case_number}")


def _generate_unique_case_number(conn: sqlite3.Connection, table: str, base: str | None) -> str:
    base = (base or "Case").strip() or "Case"
    candidate = f"{base} (copy)"
    n = 2
    while True:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE LOWER(TRIM(case_number)) = LOWER(TRIM(?))",
            (candidate,),
        ).fetchone()
        if not row:
            return candidate
        candidate = f"{base} (copy {n})"
        n += 1


def create_case(payload: dict[str, Any], in_progress: bool = False) -> dict[str, Any]:
    ensure_schema()
    table = "in_progress_cases" if in_progress else "case_log"
    data = _normalize_case_payload(payload, in_progress=in_progress)
    data["created_at"] = payload.get("created_at") or __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    columns = list(data.keys())
    placeholders = ", ".join(["?"] * len(columns))
    with connect() as conn:
        _ensure_case_number_available(conn, table, data.get("case_number"))
        cursor = conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [data[col] for col in columns],
        )
        conn.commit()
        return get_case(cursor.lastrowid, in_progress=in_progress) or {"id": cursor.lastrowid}


def get_case(case_id: int, in_progress: bool = False) -> dict[str, Any] | None:
    ensure_schema()
    table = "in_progress_cases" if in_progress else "case_log"
    columns = IN_PROGRESS_COLUMNS if in_progress else CASE_COLUMNS
    with connect() as conn:
        row = conn.execute(
            f"SELECT {', '.join(columns)} FROM {table} WHERE id = ?",
            (case_id,),
        ).fetchone()
    return dict(row) if row else None


def update_case(case_id: int, payload: dict[str, Any], in_progress: bool = False) -> dict[str, Any]:
    ensure_schema()
    table = "in_progress_cases" if in_progress else "case_log"
    data = _normalize_case_payload(payload, in_progress=in_progress)
    assignments = ", ".join([f"{key} = ?" for key in data])
    with connect() as conn:
        _ensure_case_number_available(conn, table, data.get("case_number"), exclude_id=case_id)
        cursor = conn.execute(
            f"UPDATE {table} SET {assignments} WHERE id = ?",
            [*data.values(), case_id],
        )
        conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"Case not found: {case_id}")
    updated = get_case(case_id, in_progress=in_progress)
    if not updated:
        raise ValueError(f"Case not found: {case_id}")
    return updated


def delete_case(case_id: int, in_progress: bool = False) -> None:
    ensure_schema()
    table = "in_progress_cases" if in_progress else "case_log"
    with connect() as conn:
        cursor = conn.execute(f"DELETE FROM {table} WHERE id = ?", (case_id,))
        conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"Case not found: {case_id}")


def complete_in_progress_case(case_id: int) -> dict[str, Any]:
    source = get_case(case_id, in_progress=True)
    if not source:
        raise ValueError(f"In-progress case not found: {case_id}")
    completed = create_case(source, in_progress=False)
    delete_case(case_id, in_progress=True)
    return completed


def duplicate_case(case_id: int, in_progress: bool = False) -> dict[str, Any]:
    source = get_case(case_id, in_progress=in_progress)
    if not source:
        raise ValueError(f"Case not found: {case_id}")
    table = "in_progress_cases" if in_progress else "case_log"
    with connect() as conn:
        source["case_number"] = _generate_unique_case_number(conn, table, source.get("case_number"))
    source.pop("id", None)
    source.pop("created_at", None)
    return create_case(source, in_progress=in_progress)


def _top_counts(conn: sqlite3.Connection, table: str, column: str, limit: int = 8) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM({column}), ''), 'Unknown') AS label, COUNT(*) AS value
        FROM {table}
        GROUP BY label
        ORDER BY value DESC, label ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [{"label": row["label"], "value": int(row["value"])} for row in rows]


def _top_volume_sums(conn: sqlite3.Connection, table: str, column: str, limit: int = 8) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM({column}), ''), 'Unknown') AS label,
            COALESCE(SUM(volume_size_gb), 0) AS value
        FROM {table}
        GROUP BY label
        ORDER BY value DESC, label ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [{"label": row["label"], "value": round(float(row["value"] or 0), 2), "unit": "gb"} for row in rows]


def get_analytics_summary() -> dict[str, Any]:
    ensure_schema()
    with connect() as conn:
        return {
            "offenses": _top_counts(conn, "case_log", "offense_type"),
            "agencies": _top_counts(conn, "case_log", "agency"),
            "devices": _top_counts(conn, "case_log", "device_type"),
            "examiners": _top_counts(conn, "case_log", "examiner"),
            "investigators": _top_counts(conn, "case_log", "investigator"),
            "cities": _top_counts(conn, "case_log", "city_of_offense"),
            "states": _top_counts(conn, "case_log", "state_of_offense"),
            "tools": _top_counts(conn, "case_log", "forensic_tool"),
            "models": _top_counts(conn, "case_log", "model"),
            "operating_systems": _top_counts(conn, "case_log", "os"),
            "data_recovered": _top_counts(conn, "case_log", "data_recovered"),
            "volume_by_examiner": _top_volume_sums(conn, "case_log", "examiner"),
            "volume_by_agency": _top_volume_sums(conn, "case_log", "agency"),
            "volume_by_device": _top_volume_sums(conn, "case_log", "device_type"),
            "volume_by_offense": _top_volume_sums(conn, "case_log", "offense_type"),
            "volume_by_city": _top_volume_sums(conn, "case_log", "city_of_offense"),
        }


def get_map_markers() -> list[dict[str, Any]]:
    ensure_schema()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
                c.city_of_offense AS city,
                c.state_of_offense AS state,
                COUNT(*) AS case_count,
                COALESCE(SUM(c.volume_size_gb), 0) AS total_volume_gb,
                g.latitude,
                g.longitude
            FROM case_log c
            LEFT JOIN geocache g
                ON g.location_key = TRIM(c.city_of_offense) || '|' || TRIM(c.state_of_offense)
            WHERE COALESCE(TRIM(c.city_of_offense), '') <> ''
              AND COALESCE(TRIM(c.state_of_offense), '') <> ''
            GROUP BY city, state, g.latitude, g.longitude
            ORDER BY case_count DESC, city ASC
            LIMIT 80
            """
        ).fetchall()
    return [
        {
            "city": row["city"],
            "state": row["state"],
            "case_count": int(row["case_count"]),
            "total_volume_gb": float(row["total_volume_gb"] or 0),
            "latitude": row["latitude"],
            "longitude": row["longitude"],
        }
        for row in rows
    ]


def get_json_setting(key: str, default: Any) -> Any:
    ensure_schema()
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = ?",
                (f"combo_json_{key}",),
            ).fetchone()
            if not row:
                return default
            values = json.loads(row["value"])
            if values and values[0]:
                return json.loads(values[0])
    except Exception:
        return default
    return default


def set_json_setting(key: str, value: Any) -> None:
    ensure_schema()
    encoded = json.dumps([json.dumps(value)])
    with connect() as conn:
        conn.execute(
            "REPLACE INTO settings (key, value) VALUES (?, ?)",
            (f"combo_json_{key}", encoded),
        )
        conn.commit()


def get_combo_values(key: str) -> list[str]:
    ensure_schema()
    if key not in COMBO_COLUMNS:
        raise ValueError(f"Unsupported combo key: {key}")
    stored: list[str] = []
    hidden: list[str] = []
    try:
        with connect() as conn:
            stored = _read_setting_json_list(conn, f"combo_{key}")
            hidden = _read_setting_json_list(conn, f"combo_hidden_{key}")
    except Exception:
        stored = []
        hidden = []

    discovered: list[str] = []
    column = COMBO_COLUMNS[key]
    with connect() as conn:
        for table in ("case_log", "in_progress_cases"):
            try:
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT TRIM({column}) AS value
                    FROM {table}
                    WHERE COALESCE(TRIM({column}), '') <> ''
                    ORDER BY value COLLATE NOCASE
                    LIMIT 250
                    """
                ).fetchall()
            except sqlite3.DatabaseError:
                continue
            discovered.extend(str(row["value"]).strip() for row in rows if row["value"])

    merged: list[str] = []
    seen: set[str] = set()
    hidden_markers = {value.lower() for value in hidden}
    for value in [*stored, *discovered]:
        marker = value.lower()
        if marker in hidden_markers:
            continue
        if marker not in seen:
            seen.add(marker)
            merged.append(value)
    return merged


def add_combo_value(key: str, value: str | None) -> list[str]:
    ensure_schema()
    if key not in COMBO_COLUMNS:
        raise ValueError(f"Unsupported combo key: {key}")
    value = (value or "").strip()
    values = get_combo_values(key)
    if value:
        # A value may already be discoverable from the case that was just saved.
        # Promote it anyway so newly used entries are immediately visible in Settings.
        values = [item for item in values if item.lower() != value.lower()]
        values.insert(0, value)
    with connect() as conn:
        hidden = _read_setting_json_list(conn, f"combo_hidden_{key}")
        hidden = [item for item in hidden if item.lower() != value.lower()]
        _write_setting_json_list(conn, f"combo_hidden_{key}", hidden)
        _write_setting_json_list(conn, f"combo_{key}", values)
        conn.commit()
    return values


def delete_combo_value(key: str, value: str | None) -> list[str]:
    ensure_schema()
    if key not in COMBO_COLUMNS:
        raise ValueError(f"Unsupported combo key: {key}")
    value = (value or "").strip()
    if not value:
        return get_combo_values(key)
    with connect() as conn:
        stored = _read_setting_json_list(conn, f"combo_{key}")
        stored = [item for item in stored if item.lower() != value.lower()]
        hidden = _read_setting_json_list(conn, f"combo_hidden_{key}")
        if value.lower() not in {item.lower() for item in hidden}:
            hidden.insert(0, value)
        _write_setting_json_list(conn, f"combo_{key}", stored)
        _write_setting_json_list(conn, f"combo_hidden_{key}", hidden)
        conn.commit()
    return get_combo_values(key)


def rename_combo_value(key: str, old_value: str | None, new_value: str | None) -> list[str]:
    ensure_schema()
    if key not in COMBO_COLUMNS:
        raise ValueError(f"Unsupported combo key: {key}")
    old_value = (old_value or "").strip()
    new_value = (new_value or "").strip()
    if not old_value or not new_value:
        return get_combo_values(key)

    current_values = get_combo_values(key)
    renamed: list[str] = []
    seen: set[str] = set()
    for item in current_values:
        candidate = new_value if item.lower() == old_value.lower() else item
        marker = candidate.lower()
        if marker not in seen:
            seen.add(marker)
            renamed.append(candidate)

    if new_value.lower() not in seen:
        renamed.insert(0, new_value)

    with connect() as conn:
        hidden = _read_setting_json_list(conn, f"combo_hidden_{key}")
        hidden = [item for item in hidden if item.lower() != new_value.lower()]
        if old_value.lower() != new_value.lower() and old_value.lower() not in {item.lower() for item in hidden}:
            hidden.insert(0, old_value)
        _write_setting_json_list(conn, f"combo_hidden_{key}", hidden)
        _write_setting_json_list(conn, f"combo_{key}", renamed)
        conn.commit()
    return get_combo_values(key)
