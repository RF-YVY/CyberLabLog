from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from cyberlab_workflow import get_case_family, list_evidence
from database import get_json_setting
from paths import logo_path


def _text(value: Any) -> str:
    return html.escape(str(value or "-").strip() or "-")


def _volume(value: Any) -> str:
    gb = float(value or 0)
    return f"{gb / 1024:.2f} TB" if gb >= 1024 else f"{gb:,.1f} GB"


def generate_case_family_pdf(case_number: str, target: Path) -> Path:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    family = get_case_family(case_number)
    if not family.get("members"):
        raise ValueError("No case-family records were found")
    profile = get_json_setting("app_profile", {})
    profile = profile if isinstance(profile, dict) else {}
    organization = str(profile.get("organization") or "").strip()
    contact = str(profile.get("name") or "").strip()

    target.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(target),
        pagesize=letter,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title=f"Case Family {family['base_case_number']}",
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="FamilyTitle", parent=styles["Title"], fontSize=19, leading=23, textColor=colors.HexColor("#12385a"), alignment=TA_LEFT, spaceAfter=5))
    styles.add(ParagraphStyle(name="Section", parent=styles["Heading2"], fontSize=12, leading=15, textColor=colors.HexColor("#174f78"), spaceBefore=10, spaceAfter=6))
    styles.add(ParagraphStyle(name="Small", parent=styles["BodyText"], fontSize=8, leading=11, textColor=colors.HexColor("#40566b")))
    styles.add(ParagraphStyle(name="Custody", parent=styles["BodyText"], fontSize=8, leading=10, leftIndent=10, borderColor=colors.HexColor("#d8e2ea"), borderWidth=0, borderPadding=2))

    story: list[Any] = []
    header_cells: list[Any] = []
    if logo_path().exists():
        try:
            header_cells.append(Image(str(logo_path()), width=0.7 * inch, height=0.7 * inch, kind="proportional"))
        except Exception:
            header_cells.append("")
    else:
        header_cells.append("")
    heading = [Paragraph(f"Case Family {_text(family['base_case_number'])}", styles["FamilyTitle"])]
    if organization:
        heading.append(Paragraph(_text(organization), styles["Small"]))
    if contact:
        heading.append(Paragraph(f"Prepared for {_text(contact)}", styles["Small"]))
    header_cells.append(heading)
    header = Table([header_cells], colWidths=[0.85 * inch, 6.45 * inch])
    header.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LINEBELOW", (0, 0), (-1, -1), 1, colors.HexColor("#8eb7d2")), ("BOTTOMPADDING", (0, 0), (-1, -1), 9)]))
    story.extend([header, Spacer(1, 10)])

    summary = [
        ["Devices", str(family.get("device_count") or 0), "Completed", str(family.get("completed_count") or 0), "Active", str(family.get("active_count") or 0), "Combined Volume", _volume(family.get("total_volume_gb"))],
        ["Agency", _text(family.get("agency")), "Offense", _text(family.get("offense_type")), "", "", "", ""],
    ]
    summary_table = Table(summary, colWidths=[0.72 * inch, 0.78 * inch, 0.72 * inch, 0.7 * inch, 0.55 * inch, 0.58 * inch, 1.0 * inch, 1.15 * inch])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8f1f7")),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#21384a")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTNAME", (4, 0), (4, -1), "Helvetica-Bold"),
        ("FONTNAME", (6, 0), (6, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#c6d5df")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("SPAN", (1, 1), (1, 1)),
    ]))
    story.extend([summary_table, Spacer(1, 12)])

    for index, member in enumerate(family["members"]):
        if index and index % 3 == 0:
            story.append(PageBreak())
        device_title = f"{_text(member.get('case_number'))} - {_text(member.get('device_type'))} {_text(member.get('model'))}"
        details = [
            ["Status", _text(member.get("workflow_status") or member.get("source")), "Examiner", _text(member.get("examiner")), "Investigator", _text(member.get("investigator"))],
            ["Dates", f"{_text(member.get('start_date'))} to {_text(member.get('end_date'))}", "Volume", _volume(member.get("volume_size_gb")), "Tool", _text(member.get("forensic_tool"))],
            ["Location", f"{_text(member.get('city_of_offense'))}, {_text(member.get('state_of_offense'))}", "Agency", _text(member.get("agency")), "Offense", _text(member.get("offense_type"))],
        ]
        detail_table = Table(details, colWidths=[0.62 * inch, 1.55 * inch, 0.62 * inch, 1.4 * inch, 0.65 * inch, 1.7 * inch])
        detail_table.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#d6e0e7")), ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#eef4f8")), ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#eef4f8")), ("BACKGROUND", (4, 0), (4, -1), colors.HexColor("#eef4f8")), ("FONTNAME", (0, 0), (-1, -1), "Helvetica"), ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"), ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"), ("FONTNAME", (4, 0), (4, -1), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 7.5), ("VALIGN", (0, 0), (-1, -1), "TOP")]))
        section: list[Any] = [Paragraph(device_title, styles["Section"]), detail_table]
        evidence_items = list_evidence(str(member.get("case_number") or ""))
        if evidence_items:
            section.append(Paragraph("Evidence and Chain of Custody", styles["Small"]))
            for item in evidence_items:
                section.append(Paragraph(f"<b>{_text(item.get('evidence_number') or 'Unnumbered')}</b> - {_text(item.get('item_type'))}; serial {_text(item.get('serial_number'))}; status {_text(item.get('status'))}; storage {_text(item.get('storage_location'))}", styles["Small"]))
                for event in item.get("custody_events") or []:
                    section.append(Paragraph(f"{_text(event.get('event_at'))}: <b>{_text(event.get('event_type'))}</b> - {_text(event.get('person'))}, {_text(event.get('location'))}", styles["Custody"]))
        else:
            section.append(Paragraph("No evidence inventory recorded for this device.", styles["Small"]))
        story.append(KeepTogether(section))

    doc.build(story)
    return target
