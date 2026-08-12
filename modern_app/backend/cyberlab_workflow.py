from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any

from database import CASE_WRITE_COLUMNS, IN_PROGRESS_WRITE_COLUMNS, add_combo_value, connect, delete_combo_value, ensure_schema


CASE_FAMILY_PATTERN = re.compile(r"^(.+?-\d{3,})-(\d+)$")


def ensure_workflow_schema() -> None:
    ensure_schema()
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS case_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                description TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS evidence_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT NOT NULL,
                evidence_number TEXT,
                item_type TEXT,
                description TEXT,
                serial_number TEXT,
                storage_location TEXT,
                received_date TEXT,
                returned_date TEXT,
                status TEXT NOT NULL DEFAULT 'In Custody',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_evidence_case_number
            ON evidence_items(case_number COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS custody_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                person TEXT,
                location TEXT,
                event_at TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_custody_evidence
            ON custody_events(evidence_id);

            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                case_number TEXT,
                action TEXT NOT NULL,
                summary TEXT,
                changes_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_audit_case_number
            ON audit_events(case_number COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS data_quality_dismissals (
                fingerprint TEXT PRIMARY KEY,
                dismissed_at TEXT NOT NULL
            );
            """
        )
        conn.commit()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def family_base(case_number: str | None) -> str:
    value = str(case_number or "").strip()
    match = CASE_FAMILY_PATTERN.match(value)
    return match.group(1) if match else value


def family_suffix(case_number: str | None) -> int | None:
    match = CASE_FAMILY_PATTERN.match(str(case_number or "").strip())
    return int(match.group(2)) if match else None


def _case_rows() -> list[dict[str, Any]]:
    ensure_workflow_schema()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, case_number, examiner, investigator, investigation_subject, agency, city_of_offense,
                   state_of_offense, start_date, end_date, volume_size_gb, offense_type,
                   device_type, model, forensic_tool, created_at, 'completed' AS source,
                   'Completed' AS workflow_status, '' AS priority
            FROM case_log
            UNION ALL
            SELECT id, case_number, examiner, investigator, investigation_subject, agency, city_of_offense,
                   state_of_offense, start_date, end_date, volume_size_gb, offense_type,
                   device_type, model, forensic_tool, created_at, 'progress' AS source,
                   workflow_status, priority
            FROM in_progress_cases
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_case_families() -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _case_rows():
        number = str(row.get("case_number") or "").strip()
        if not number:
            continue
        grouped.setdefault(family_base(number), []).append(row)

    families: list[dict[str, Any]] = []
    for base, members in grouped.items():
        ordered = sorted(members, key=lambda row: (family_suffix(row.get("case_number")) is None, family_suffix(row.get("case_number")) or 0))
        latest = max((str(row.get("created_at") or "") for row in members), default="")
        completed = sum(1 for row in members if row.get("source") == "completed")
        families.append({
            "base_case_number": base,
            "case_count": len(members),
            "device_count": len(members),
            "completed_count": completed,
            "active_count": len(members) - completed,
            "total_volume_gb": round(sum(float(row.get("volume_size_gb") or 0) for row in members), 2),
            "agency": next((row.get("agency") for row in ordered if row.get("agency")), ""),
            "offense_type": next((row.get("offense_type") for row in ordered if row.get("offense_type")), ""),
            "latest_created_at": latest,
            "members": ordered,
        })
    return sorted(families, key=lambda item: item["latest_created_at"], reverse=True)


def get_case_family(case_number: str) -> dict[str, Any]:
    base = family_base(case_number)
    family = next((item for item in list_case_families() if item["base_case_number"].lower() == base.lower()), None)
    if family:
        return family
    return {"base_case_number": base, "case_count": 0, "device_count": 0, "completed_count": 0, "active_count": 0, "total_volume_gb": 0, "members": []}


def next_subcase_number(case_number: str) -> str:
    family = get_case_family(case_number)
    base = family["base_case_number"] or str(case_number or "").strip()
    suffixes = [family_suffix(row.get("case_number")) for row in family.get("members", [])]
    used = [value for value in suffixes if value is not None]
    return f"{base}-{max(used, default=0) + 1}"


def _decode_payload(value: str | None) -> dict[str, Any]:
    try:
        decoded = json.loads(value or "{}")
        return decoded if isinstance(decoded, dict) else {}
    except json.JSONDecodeError:
        return {}


def list_templates() -> list[dict[str, Any]]:
    ensure_workflow_schema()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM case_templates ORDER BY name COLLATE NOCASE").fetchall()
    return [{**dict(row), "payload": _decode_payload(row["payload_json"])} for row in rows]


def save_template(payload: dict[str, Any], template_id: int | None = None) -> dict[str, Any]:
    ensure_workflow_schema()
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("Template name is required")
    description = str(payload.get("description") or "").strip()
    template_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    now = _now()
    with connect() as conn:
        if template_id:
            cursor = conn.execute(
                "UPDATE case_templates SET name = ?, description = ?, payload_json = ?, updated_at = ? WHERE id = ?",
                (name, description, json.dumps(template_payload), now, template_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Template not found")
        else:
            cursor = conn.execute(
                "INSERT INTO case_templates (name, description, payload_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (name, description, json.dumps(template_payload), now, now),
            )
            template_id = int(cursor.lastrowid)
        conn.commit()
        row = conn.execute("SELECT * FROM case_templates WHERE id = ?", (template_id,)).fetchone()
    return {**dict(row), "payload": _decode_payload(row["payload_json"])}


def delete_template(template_id: int) -> None:
    ensure_workflow_schema()
    with connect() as conn:
        cursor = conn.execute("DELETE FROM case_templates WHERE id = ?", (template_id,))
        conn.commit()
    if cursor.rowcount == 0:
        raise ValueError("Template not found")


def record_audit(
    entity_type: str,
    entity_id: str | int | None,
    action: str,
    case_number: str | None = None,
    summary: str = "",
    changes: dict[str, Any] | None = None,
) -> None:
    ensure_workflow_schema()
    with connect() as conn:
        conn.execute(
            "INSERT INTO audit_events (entity_type, entity_id, case_number, action, summary, changes_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (entity_type, str(entity_id or ""), str(case_number or ""), action, summary, json.dumps(changes or {}, default=str), _now()),
        )
        conn.commit()


def case_changes(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, Any]:
    before = before or {}
    after = after or {}
    ignored = {"id", "created_at"}
    return {
        key: {"from": before.get(key), "to": after.get(key)}
        for key in sorted(set(before) | set(after))
        if key not in ignored and before.get(key) != after.get(key)
    }


def list_audit(case_number: str = "", limit: int = 100) -> list[dict[str, Any]]:
    ensure_workflow_schema()
    with connect() as conn:
        if case_number:
            rows = conn.execute(
                "SELECT * FROM audit_events WHERE LOWER(case_number) = LOWER(?) ORDER BY id DESC LIMIT ?",
                (case_number, limit),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [{**dict(row), "changes": _decode_payload(row["changes_json"])} for row in rows]


def list_evidence(case_number: str) -> list[dict[str, Any]]:
    ensure_workflow_schema()
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM evidence_items WHERE LOWER(case_number) = LOWER(?) ORDER BY id DESC",
            (case_number,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            events = conn.execute(
                "SELECT * FROM custody_events WHERE evidence_id = ? ORDER BY datetime(event_at) DESC, id DESC",
                (item["id"],),
            ).fetchall()
            item["custody_events"] = [dict(event) for event in events]
            result.append(item)
    return result


def save_evidence(payload: dict[str, Any], evidence_id: int | None = None) -> dict[str, Any]:
    ensure_workflow_schema()
    case_number = str(payload.get("case_number") or "").strip()
    if not case_number:
        raise ValueError("Case number is required")
    fields = ["evidence_number", "item_type", "description", "serial_number", "storage_location", "received_date", "returned_date", "status", "notes"]
    values = {field: str(payload.get(field) or "").strip() for field in fields}
    values["status"] = values["status"] or "In Custody"
    now = _now()
    with connect() as conn:
        if evidence_id:
            cursor = conn.execute(
                """UPDATE evidence_items SET evidence_number = ?, item_type = ?, description = ?, serial_number = ?,
                   storage_location = ?, received_date = ?, returned_date = ?, status = ?, notes = ?, updated_at = ? WHERE id = ?""",
                (*[values[field] for field in fields], now, evidence_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Evidence item not found")
        else:
            cursor = conn.execute(
                """INSERT INTO evidence_items (case_number, evidence_number, item_type, description, serial_number,
                   storage_location, received_date, returned_date, status, notes, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (case_number, *[values[field] for field in fields], now, now),
            )
            evidence_id = int(cursor.lastrowid)
        conn.commit()
        row = conn.execute("SELECT * FROM evidence_items WHERE id = ?", (evidence_id,)).fetchone()
    result = dict(row)
    result["custody_events"] = []
    return result


def delete_evidence(evidence_id: int) -> None:
    ensure_workflow_schema()
    with connect() as conn:
        conn.execute("DELETE FROM custody_events WHERE evidence_id = ?", (evidence_id,))
        cursor = conn.execute("DELETE FROM evidence_items WHERE id = ?", (evidence_id,))
        conn.commit()
    if cursor.rowcount == 0:
        raise ValueError("Evidence item not found")


def add_custody_event(evidence_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_workflow_schema()
    event_type = str(payload.get("event_type") or "").strip()
    if not event_type:
        raise ValueError("Custody event type is required")
    event_at = str(payload.get("event_at") or "").strip() or _now()
    with connect() as conn:
        evidence = conn.execute("SELECT case_number FROM evidence_items WHERE id = ?", (evidence_id,)).fetchone()
        if not evidence:
            raise ValueError("Evidence item not found")
        cursor = conn.execute(
            "INSERT INTO custody_events (evidence_id, event_type, person, location, event_at, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (evidence_id, event_type, str(payload.get("person") or "").strip(), str(payload.get("location") or "").strip(), event_at, str(payload.get("notes") or "").strip(), _now()),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM custody_events WHERE id = ?", (cursor.lastrowid,)).fetchone()
    record_audit("evidence", evidence_id, "custody", evidence["case_number"], f"Custody event: {event_type}", dict(row))
    return dict(row)


def dashboard_summary() -> dict[str, Any]:
    ensure_workflow_schema()
    with connect() as conn:
        active_aging = conn.execute(
            """SELECT COUNT(*) AS value FROM in_progress_cases
               WHERE julianday('now') - julianday(COALESCE(NULLIF(created_at, ''), 'now')) >= 30"""
        ).fetchone()["value"]
        overdue = conn.execute(
            """SELECT COUNT(*) AS value FROM in_progress_cases
               WHERE COALESCE(target_due_date, '') <> '' AND date(target_due_date) < date('now')"""
        ).fetchone()["value"]
        avg_turnaround = conn.execute(
            """SELECT AVG(julianday(end_date) - julianday(start_date)) AS value FROM case_log
               WHERE date(start_date) IS NOT NULL AND date(end_date) IS NOT NULL AND date(end_date) >= date(start_date)"""
        ).fetchone()["value"]
        evidence_count = conn.execute("SELECT COUNT(*) AS value FROM evidence_items").fetchone()["value"]
        monthly = conn.execute(
            """SELECT strftime('%Y-%m', COALESCE(NULLIF(start_date, ''), created_at)) AS label, COUNT(*) AS value
               FROM case_log WHERE date(COALESCE(NULLIF(start_date, ''), created_at)) >= date('now', '-11 months', 'start of month')
               GROUP BY label ORDER BY label"""
        ).fetchall()
        workload = conn.execute(
            """SELECT COALESCE(NULLIF(TRIM(examiner), ''), 'Unassigned') AS label, COUNT(*) AS value
               FROM in_progress_cases GROUP BY label ORDER BY value DESC, label LIMIT 8"""
        ).fetchall()
    return {
        "family_count": len(list_case_families()),
        "evidence_count": int(evidence_count or 0),
        "active_aging_count": int(active_aging or 0),
        "overdue_count": int(overdue or 0),
        "average_turnaround_days": round(float(avg_turnaround or 0), 1),
        "monthly_completed": [dict(row) for row in monthly],
        "examiner_workload": [dict(row) for row in workload],
    }


def data_quality_summary() -> dict[str, Any]:
    rows = _case_rows()
    issues: list[dict[str, Any]] = []
    seen_numbers: dict[str, list[dict[str, Any]]] = {}
    value_groups: dict[tuple[str, str], set[str]] = {}
    fields = ["examiner", "investigator", "agency", "city_of_offense", "offense_type", "device_type"]

    for row in rows:
        number = str(row.get("case_number") or "").strip()
        marker = number.lower()
        if marker:
            seen_numbers.setdefault(marker, []).append(row)
        for required, label in (("case_number", "case number"), ("examiner", "examiner"), ("agency", "agency"), ("offense_type", "offense type")):
            if not str(row.get(required) or "").strip():
                issues.append({"severity": "high" if required == "case_number" else "medium", "type": "missing", "record_id": row.get("id"), "case_number": number, "source": row.get("source"), "message": f"Missing {label}"})
        if row.get("start_date") and row.get("end_date") and str(row["end_date"]) < str(row["start_date"]):
            issues.append({"severity": "high", "type": "date", "record_id": row.get("id"), "case_number": number, "source": row.get("source"), "message": "End date is before start date"})
        if float(row.get("volume_size_gb") or 0) < 0:
            issues.append({"severity": "high", "type": "volume", "record_id": row.get("id"), "case_number": number, "source": row.get("source"), "message": "Volume cannot be negative"})
        for field in fields:
            value = str(row.get(field) or "").strip()
            normalized = re.sub(r"[^a-z0-9]+", "", value.lower())
            if normalized:
                value_groups.setdefault((field, normalized), set()).add(value)

    for duplicates in seen_numbers.values():
        if len(duplicates) > 1:
            records = [
                {
                    "id": row.get("id"),
                    "source": row.get("source"),
                    "case_number": row.get("case_number"),
                    "examiner": row.get("examiner"),
                    "device_type": row.get("device_type"),
                    "model": row.get("model"),
                    "created_at": row.get("created_at"),
                }
                for row in duplicates
            ]
            issues.append({"severity": "high", "type": "duplicate", "case_number": duplicates[0].get("case_number"), "source": "mixed", "records": records, "message": "Case number appears more than once"})
    for (field, _), variants in value_groups.items():
        if len(variants) > 1:
            values = sorted(variants, key=str.lower)
            issues.append({"severity": "low", "type": "inconsistent", "field": field, "variants": values, "case_number": "", "source": "all", "message": f"Inconsistent {field.replace('_', ' ')}: {' / '.join(values[:4])}"})

    severity_order = {"high": 0, "medium": 1, "low": 2}
    for issue in issues:
        identity = json.dumps(
            {key: issue.get(key) for key in ("type", "record_id", "case_number", "source", "message")},
            sort_keys=True,
            separators=(",", ":"),
        )
        issue["fingerprint"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    ensure_workflow_schema()
    with connect() as conn:
        dismissed = {row["fingerprint"] for row in conn.execute("SELECT fingerprint FROM data_quality_dismissals").fetchall()}
    visible_issues = [issue for issue in issues if issue["fingerprint"] not in dismissed]
    visible_issues.sort(key=lambda item: (severity_order.get(item["severity"], 9), item["message"]))
    return {
        "issue_count": len(visible_issues),
        "dismissed_count": len(issues) - len(visible_issues),
        "high_count": sum(1 for item in visible_issues if item["severity"] == "high"),
        "medium_count": sum(1 for item in visible_issues if item["severity"] == "medium"),
        "low_count": sum(1 for item in visible_issues if item["severity"] == "low"),
        "issues": visible_issues[:150],
    }


def dismiss_data_quality_issues(fingerprints: list[str]) -> dict[str, Any]:
    ensure_workflow_schema()
    values = sorted({str(value or "").strip() for value in fingerprints if str(value or "").strip()})
    if not values:
        return data_quality_summary()
    with connect() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO data_quality_dismissals (fingerprint, dismissed_at) VALUES (?, ?)",
            [(fingerprint, _now()) for fingerprint in values],
        )
        conn.commit()
    return data_quality_summary()


def restore_data_quality_issues() -> dict[str, Any]:
    ensure_workflow_schema()
    with connect() as conn:
        conn.execute("DELETE FROM data_quality_dismissals")
        conn.commit()
    return data_quality_summary()


def get_review_case(source: str, record_id: int) -> dict[str, Any]:
    table = "in_progress_cases" if source == "progress" else "case_log"
    ensure_workflow_schema()
    with connect() as conn:
        row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,)).fetchone()
    if not row:
        raise ValueError("The selected case record was not found")
    return {**dict(row), "source": source}


def normalize_case_value(field: str, variants: list[str], canonical: str) -> dict[str, Any]:
    allowed = {"examiner", "investigator", "agency", "city_of_offense", "offense_type", "device_type"}
    if field not in allowed:
        raise ValueError("This field cannot be normalized")
    canonical = str(canonical or "").strip()
    values = sorted({str(value or "").strip() for value in variants if str(value or "").strip()})
    if not canonical or not values:
        raise ValueError("A canonical value and at least one variant are required")
    normalized_values = sorted({value.lower() for value in values})
    placeholders = ", ".join("?" for _ in normalized_values)
    changed = 0
    with connect() as conn:
        for table in ("case_log", "in_progress_cases"):
            cursor = conn.execute(
                f"UPDATE {table} SET {field} = ? WHERE LOWER(TRIM({field})) IN ({placeholders})",
                [canonical, *normalized_values],
            )
            changed += cursor.rowcount
        conn.commit()
    for value in values:
        if value.lower() != canonical.lower():
            delete_combo_value(field, value)
    add_combo_value(field, canonical)
    return {"field": field, "canonical": canonical, "changed": changed, "quality": data_quality_summary()}


def merge_duplicate_cases(case_number: str, keep_source: str, keep_id: int) -> dict[str, Any]:
    case_number = str(case_number or "").strip()
    if not case_number:
        raise ValueError("Case number is required")
    matches = [row for row in _case_rows() if str(row.get("case_number") or "").strip().lower() == case_number.lower()]
    rows = [get_review_case(str(row.get("source")), int(row.get("id") or 0)) for row in matches]
    keeper = next((row for row in rows if row.get("source") == keep_source and int(row.get("id") or 0) == int(keep_id)), None)
    if not keeper or len(rows) < 2:
        raise ValueError("Duplicate records or selected keeper were not found")
    keep_table = "in_progress_cases" if keep_source == "progress" else "case_log"
    allowed = IN_PROGRESS_WRITE_COLUMNS if keep_source == "progress" else CASE_WRITE_COLUMNS
    merged = dict(keeper)
    for donor in rows:
        if donor is keeper:
            continue
        for field in allowed:
            if field == "case_number":
                continue
            if merged.get(field) in (None, "", 0) and donor.get(field) not in (None, ""):
                merged[field] = donor[field]
    update_fields = [field for field in allowed if field in merged]
    with connect() as conn:
        conn.execute(
            f"UPDATE {keep_table} SET {', '.join(f'{field} = ?' for field in update_fields)} WHERE id = ?",
            [*[merged.get(field) for field in update_fields], keep_id],
        )
        removed = 0
        for donor in rows:
            if donor.get("source") == keep_source and int(donor.get("id") or 0) == int(keep_id):
                continue
            donor_table = "in_progress_cases" if donor.get("source") == "progress" else "case_log"
            removed += conn.execute(f"DELETE FROM {donor_table} WHERE id = ?", (donor.get("id"),)).rowcount
        conn.commit()
    return {"kept": get_review_case(keep_source, keep_id), "removed": removed, "quality": data_quality_summary()}


def work_queue_rows() -> list[dict[str, Any]]:
    ensure_workflow_schema()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT p.*,
                   (SELECT COUNT(*) FROM evidence_items e WHERE LOWER(TRIM(e.case_number)) = LOWER(TRIM(p.case_number))) AS evidence_count
            FROM in_progress_cases p
            ORDER BY
                CASE WHEN COALESCE(p.target_due_date, '') = '' THEN 1 ELSE 0 END,
                date(p.target_due_date), datetime(p.created_at) DESC
            """
        ).fetchall()
    return [dict(row) for row in rows]
