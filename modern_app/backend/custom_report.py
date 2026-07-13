from __future__ import annotations

import csv
import html
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from database import connect, ensure_schema, get_json_setting
from paths import logo_path


FILTER_FIELDS = {"examiner": "Examiner", "investigator": "Investigator", "agency": "Agency"}


def _validate_date(value: str, label: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a valid date") from exc


def custom_report_data(start_date: str, end_date: str, filter_field: str, filter_value: str = "") -> dict[str, Any]:
    start_date = _validate_date(start_date, "Start date")
    end_date = _validate_date(end_date, "End date")
    if start_date > end_date:
        raise ValueError("Start date must be on or before end date")
    if filter_field not in FILTER_FIELDS:
        raise ValueError("Filter must be examiner, investigator, or agency")
    filter_value = str(filter_value or "").strip()
    ensure_schema()
    where = "date(COALESCE(NULLIF(end_date, ''), NULLIF(start_date, ''), substr(created_at, 1, 10))) BETWEEN date(?) AND date(?)"
    params: list[Any] = [start_date, end_date]
    if filter_value:
        where += f" AND LOWER(TRIM({filter_field})) = LOWER(TRIM(?))"
        params.append(filter_value)
    with connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT id, case_number, examiner, investigator, agency, start_date, end_date,
                       device_type, model, volume_size_gb, forensic_tool
                FROM case_log
                WHERE {where}
                ORDER BY date(COALESCE(NULLIF(end_date, ''), NULLIF(start_date, ''), substr(created_at, 1, 10))), case_number
                """,
                params,
            ).fetchall()
        ]
    device_types = Counter(str(row.get("device_type") or "Unspecified").strip() or "Unspecified" for row in rows)
    return {
        "start_date": start_date,
        "end_date": end_date,
        "filter_field": filter_field,
        "filter_label": FILTER_FIELDS[filter_field],
        "filter_value": filter_value,
        "device_count": len(rows),
        "total_volume_gb": sum(float(row.get("volume_size_gb") or 0) for row in rows),
        "device_types": sorted(device_types.items(), key=lambda item: (-item[1], item[0].lower())),
        "rows": rows,
    }


def _volume(value: Any) -> str:
    gb = float(value or 0)
    return f"{gb / 1024:.2f} TB" if gb >= 1024 else f"{gb:,.1f} GB"


def generate_custom_pdf(data: dict[str, Any], target: Path) -> Path:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    target.parent.mkdir(parents=True, exist_ok=True)
    profile = get_json_setting("app_profile", {})
    profile = profile if isinstance(profile, dict) else {}
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CustomSmall", parent=styles["BodyText"], fontSize=8, leading=10, textColor=colors.HexColor("#40566b")))
    doc = SimpleDocTemplate(str(target), pagesize=letter, leftMargin=0.5 * inch, rightMargin=0.5 * inch, topMargin=0.48 * inch, bottomMargin=0.48 * inch, title="Custom Workload Report")
    story: list[Any] = []
    title = Paragraph("Monthly Device Processing Report", styles["Title"])
    if logo_path().exists():
        try:
            title = Table([[title, Image(str(logo_path()), width=0.62 * inch, height=0.62 * inch, kind="proportional")]], colWidths=[6.25 * inch, 0.75 * inch])
        except Exception:
            pass
    story.extend([title, Paragraph(html.escape(str(profile.get("organization") or "")), styles["CustomSmall"]), Spacer(1, 8)])
    selected = data["filter_value"] or "All"
    story.append(Paragraph(f"<b>Period:</b> {data['start_date']} through {data['end_date']} &nbsp;&nbsp; <b>{data['filter_label']}:</b> {html.escape(selected)}", styles["BodyText"]))
    summary = Table(
        [["Devices Processed", str(data["device_count"]), "Total Volume", _volume(data["total_volume_gb"])]],
        colWidths=[1.45 * inch, 1.0 * inch, 1.2 * inch, 1.4 * inch],
    )
    summary.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#e8f1f7")), ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b8cad7")), ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 10), ("ALIGN", (1, 0), (1, 0), "CENTER")]))
    story.extend([Spacer(1, 10), summary, Spacer(1, 12), Paragraph("Device Type Breakdown", styles["Heading2"])])
    breakdown_rows = [["Device Type", "Count", "Share"]]
    for device_type, count in data["device_types"]:
        share = (count / data["device_count"] * 100) if data["device_count"] else 0
        breakdown_rows.append([html.escape(device_type), str(count), f"{share:.1f}%"])
    if len(breakdown_rows) == 1:
        breakdown_rows.append(["No completed devices in this range", "0", "0.0%"])
    breakdown = Table(breakdown_rows, colWidths=[3.4 * inch, 0.8 * inch, 0.8 * inch], repeatRows=1)
    breakdown.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#174f78")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#c9d7e0")), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 8), ("ALIGN", (1, 1), (-1, -1), "RIGHT")]))
    story.extend([breakdown, Spacer(1, 12), Paragraph("Completed Device Details", styles["Heading2"])])
    detail_rows = [["Completed", "Case", "Device / Model", "Examiner", "Agency", "Volume"]]
    for row in data["rows"]:
        detail_rows.append([
            row.get("end_date") or row.get("start_date") or "-",
            row.get("case_number") or "-",
            f"{row.get('device_type') or '-'} / {row.get('model') or '-'}",
            row.get("examiner") or "-",
            row.get("agency") or "-",
            _volume(row.get("volume_size_gb")),
        ])
    if len(detail_rows) == 1:
        detail_rows.append(["-", "No matching cases", "-", "-", "-", "0.0 GB"])
    details = Table(detail_rows, colWidths=[0.72 * inch, 1.05 * inch, 1.45 * inch, 1.05 * inch, 1.25 * inch, 0.72 * inch], repeatRows=1)
    details.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#174f78")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1dce4")), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 6.8), ("VALIGN", (0, 0), (-1, -1), "TOP")]))
    story.append(details)
    doc.build(story)
    return target


def generate_custom_csv(data: dict[str, Any], target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = ["end_date", "case_number", "device_type", "model", "volume_size_gb", "examiner", "investigator", "agency", "forensic_tool"]
    with target.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fields} for row in data["rows"])
    return target
