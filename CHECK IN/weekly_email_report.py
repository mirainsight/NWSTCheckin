"""
Weekly PSQ email + PDF for NWST Check In.

PDF is titled **Weekly Check-In Report**: NWST cell-health table first (Historical Cell Status or CG Combined),
then NWST Check In counts from **ATTENDANCE_SHEET_ID** (same roster as the app). Cell-health always uses
**current** NWST Health data even when the attendance section is for a historical date. Configure recipients via Streamlit secrets or env:
  WEEKLY_REPORT_TO or PSQ_EMAIL — primary To line (person A)
  WEEKLY_REPORT_CC or PSQ_CC — optional Cc (comma-separated; person B, etc.)
  NWST_CORE_TEAM_TO — optional separate To for send_to_nwst_core_team
  NWST_CORE_TEAM_CC — optional Cc for Core Team email

SENDER_PASSWORD must be a Gmail *app password* (Google Account → Security → App passwords),
not your normal Gmail login password, when using smtp.gmail.com.
"""

from __future__ import annotations

import io
import os
import smtplib
import sys
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nwst_shared.nwst_cell_health_report import (
    attendance_fraction_for_pdf,
    build_cell_health_table_rows,
    compute_member_attendance_stats,
    load_cg_combined_df,
    load_nwst_attendance_rollup_df,
    nwst_health_sheet_id,
)

try:
    import streamlit as st
except ImportError:
    st = None  # type: ignore[assignment]


def _gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials

    creds_dict = None
    if st is not None and hasattr(st, "secrets") and "google" in st.secrets:
        creds_dict = dict(st.secrets["google"])
    if creds_dict is None:
        import json

        path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if path and os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                creds_dict = json.load(f)
        raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        if creds_dict is None and raw:
            creds_dict = json.loads(raw)
    if not creds_dict:
        raise RuntimeError(
            "Google credentials missing: set st.secrets['google'], GOOGLE_APPLICATION_CREDENTIALS, "
            "or GOOGLE_SERVICE_ACCOUNT_JSON."
        )
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def _sender_creds() -> tuple[str, str] | None:
    if st is not None and hasattr(st, "secrets"):
        if "SENDER_EMAIL" in st.secrets and "SENDER_PASSWORD" in st.secrets:
            return str(st.secrets["SENDER_EMAIL"]), str(st.secrets["SENDER_PASSWORD"])
    s = os.getenv("SENDER_EMAIL", "").strip()
    p = os.getenv("SENDER_PASSWORD", "").strip()
    if s and p:
        return s, p
    return None


def _weekly_recipient() -> str:
    if st is not None and hasattr(st, "secrets"):
        for k in ("WEEKLY_REPORT_TO", "PSQ_EMAIL"):
            if k in st.secrets:
                return str(st.secrets[k]).strip()
    return (os.getenv("WEEKLY_REPORT_TO") or os.getenv("PSQ_EMAIL") or "").strip()


def _normalize_cc_list(raw: str | None) -> str | None:
    if not raw or not str(raw).strip():
        return None
    parts = [p.strip() for p in str(raw).replace(";", ",").split(",") if p.strip()]
    return ", ".join(parts) if parts else None


def _weekly_cc() -> str | None:
    if st is not None and hasattr(st, "secrets"):
        for k in ("WEEKLY_REPORT_CC", "PSQ_CC"):
            if k in st.secrets:
                return _normalize_cc_list(str(st.secrets[k]))
    return _normalize_cc_list(os.getenv("WEEKLY_REPORT_CC") or os.getenv("PSQ_CC"))


def _nwst_core_recipient() -> str:
    if st is not None and hasattr(st, "secrets") and "NWST_CORE_TEAM_TO" in st.secrets:
        return str(st.secrets["NWST_CORE_TEAM_TO"]).strip()
    return (os.getenv("NWST_CORE_TEAM_TO") or "").strip()


def _nwst_core_cc() -> str | None:
    if st is not None and hasattr(st, "secrets") and "NWST_CORE_TEAM_CC" in st.secrets:
        return _normalize_cc_list(str(st.secrets["NWST_CORE_TEAM_CC"]))
    return _normalize_cc_list(os.getenv("NWST_CORE_TEAM_CC"))


_OPTIONS_TAB = "Options"
_ATTENDANCE_TAB = "Attendance"


def _attendance_sheet_id() -> str | None:
    sid = (os.getenv("ATTENDANCE_SHEET_ID") or "").strip()
    if sid:
        return sid
    if st is not None and hasattr(st, "secrets"):
        try:
            if "ATTENDANCE_SHEET_ID" in st.secrets:
                return str(st.secrets["ATTENDANCE_SHEET_ID"]).strip()
        except Exception:
            pass
    return None


def _parse_name_cell_group(name_cell_group_str: str) -> tuple[str | None, str | None]:
    if not name_cell_group_str:
        return None, None
    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return parts[0].strip(), "Unknown"


def _fetch_attendance_checked_in_count(client, sheet_id: str, target_date: str) -> int:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        attendance_sheet = spreadsheet.worksheet(_ATTENDANCE_TAB)
    except Exception:
        return 0
    all_rows = attendance_sheet.get_all_values()
    if len(all_rows) <= 1:
        return 0
    checked_in_set: set[str] = set()
    for row in all_rows[1:]:
        if len(row) < 2:
            continue
        ts = (row[0] or "").strip()
        opt = (row[1] or "").strip()
        if not ts or not opt or len(ts) < 10:
            continue
        if ts[:10] != target_date:
            continue
        if opt not in checked_in_set:
            checked_in_set.add(opt)
    return len(checked_in_set)


def _report_attendance_date_str(target_date: str | None) -> str:
    myt = timezone(timedelta(hours=8))
    now = datetime.now(myt)
    return (target_date or now.strftime("%Y-%m-%d")).strip()


def _fetch_checked_in_options_for_date(client, sheet_id: str, target_date: str) -> set[str]:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        attendance_sheet = spreadsheet.worksheet(_ATTENDANCE_TAB)
    except Exception:
        return set()
    all_rows = attendance_sheet.get_all_values()
    if len(all_rows) <= 1:
        return set()
    checked: set[str] = set()
    for row in all_rows[1:]:
        if len(row) < 2:
            continue
        ts = (row[0] or "").strip()
        opt = (row[1] or "").strip()
        if not ts or not opt or len(ts) < 10:
            continue
        if ts[:10] != target_date:
            continue
        checked.add(opt)
    return checked


def _roster_option_strings_from_options(client, sheet_id: str) -> list[str]:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(_OPTIONS_TAB)
    except Exception:
        return []
    col_c = ws.col_values(3)
    if len(col_c) <= 1:
        return []
    out: list[str] = []
    for value in col_c[1:]:
        v = (value or "").strip()
        if not v:
            continue
        name, _ = _parse_name_cell_group(v)
        if name:
            out.append(v)
    return out


def _build_checkin_roster_for_pdf(
    client,
    attendance_sid: str,
    nwst_health_sid: str,
    target_date: str,
) -> dict[str, Any] | None:
    """
    Roster from Options col C vs Attendance for ``target_date``, grouped by cell (Options suffix).

    Each group has ``cell``, ``n_checked``, ``n_total``, and name lists for PDF styling
    (blue checked-in, grey italic pending). Display names are member names with optional ``(x/y)``.
    """
    roster = _roster_option_strings_from_options(client, attendance_sid)
    if not roster:
        return None
    checked = _fetch_checked_in_options_for_date(client, attendance_sid, target_date)
    cg_df = load_cg_combined_df(client, nwst_health_sid)
    att_df = load_nwst_attendance_rollup_df(client, nwst_health_sid)
    stats = (
        compute_member_attendance_stats(att_df, cg_df)
        if att_df is not None and cg_df is not None
        else {}
    )
    by_cell: dict[str, list[tuple[str, bool]]] = defaultdict(
        list
    )  # (display label, is_checked)
    for opt in roster:
        name, cell = _parse_name_cell_group(opt)
        name_s = (name or "").strip()
        if not name_s:
            continue
        cell_s = (cell or "Unknown").strip()
        cell_for_stats = "" if cell_s in ("", "Unknown") else cell_s
        frac = attendance_fraction_for_pdf(name_s, cell_for_stats, stats)
        display = f"{name_s} ({frac})" if frac else name_s
        by_cell[cell_s].append((display, opt in checked))

    groups: list[dict[str, Any]] = []
    for cell_s in sorted(by_cell.keys(), key=lambda c: c.lower()):
        entries = by_cell[cell_s]
        checked_labels = sorted([d for d, ck in entries if ck], key=str.lower)
        pending_labels = sorted([d for d, ck in entries if not ck], key=str.lower)
        groups.append(
            {
                "cell": cell_s,
                "n_checked": len(checked_labels),
                "n_total": len(entries),
                "checked_labels": checked_labels,
                "pending_labels": pending_labels,
            }
        )

    return {"groups": groups}


def _roster_count_from_options(client, sheet_id: str) -> int:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(_OPTIONS_TAB)
    except Exception:
        return 0
    col_c = ws.col_values(3)
    if len(col_c) <= 1:
        return 0
    n = 0
    for value in col_c[1:]:
        v = (value or "").strip()
        if not v:
            continue
        name, _ = _parse_name_cell_group(v)
        if name:
            n += 1
    return n


def _build_checkin_summary(client, target_date: str | None) -> dict | None:
    sid = _attendance_sheet_id()
    if not sid:
        return None
    myt = timezone(timedelta(hours=8))
    now = datetime.now(myt)
    date_str = _report_attendance_date_str(target_date)
    try:
        display_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%A, %B %d, %Y")
    except ValueError:
        display_date = date_str
    h12 = now.hour % 12
    if h12 == 0:
        h12 = 12
    ap = "AM" if now.hour < 12 else "PM"
    gen_at = f"{h12}:{now.minute:02d} {ap} MYT"
    n_in = _fetch_attendance_checked_in_count(client, sid, date_str)
    roster = _roster_count_from_options(client, sid)
    pct = int(round(100.0 * n_in / roster)) if roster > 0 else 0
    return {
        "meta_line": f"{display_date} | Generated at {gen_at}",
        "total_checked_in": n_in,
        "total_members": roster,
        "pct": pct,
    }


def _build_report_pdf_bytes(
    rows: list[dict],
    subtitle: str,
    report_label: str,
    checkin_summary: dict | None = None,
    checkin_roster: dict[str, Any] | None = None,
) -> tuple[bytes, str | None]:
    """Build PDF with ReportLab: weekly check-in cover, cell-health table, optional attendance KPIs."""
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    styles = getSampleStyleSheet()
    green = colors.HexColor("#15803d")
    title_style = ParagraphStyle(
        name="NwstTitle",
        parent=styles["Title"],
        fontSize=14,
        leading=18,
        spaceAfter=6,
    )
    sub_style = ParagraphStyle(
        name="NwstSub",
        parent=styles["Normal"],
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#444444"),
        spaceAfter=8,
    )
    cover_title_style = ParagraphStyle(
        name="CoverTitle",
        parent=styles["Title"],
        fontSize=20,
        leading=24,
        alignment=TA_CENTER,
        textColor=green,
        spaceAfter=8,
        fontName="Helvetica-Bold",
    )
    cover_meta_style = ParagraphStyle(
        name="CoverMeta",
        parent=styles["Normal"],
        fontSize=9,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#666666"),
        spaceAfter=16,
    )
    sec_style = ParagraphStyle(
        name="NwstSec",
        parent=styles["Normal"],
        fontSize=11,
        leading=14,
        fontName="Helvetica-Bold",
        spaceAfter=6,
    )
    th_style = ParagraphStyle(
        name="NwstTH",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        fontName="Helvetica-Bold",
    )
    td_style = ParagraphStyle(
        name="NwstTD",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
    )
    bar_title_style = ParagraphStyle(
        name="BarTitle",
        parent=styles["Normal"],
        fontSize=11,
        fontName="Helvetica-Bold",
        textColor=green,
        leading=14,
        leftIndent=6,
    )
    bar_muted_style = ParagraphStyle(
        name="BarMuted",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#555555"),
        leading=12,
        leftIndent=6,
    )
    kpi_big_style = ParagraphStyle(
        name="KpiBig",
        parent=styles["Normal"],
        fontSize=36,
        fontName="Helvetica-Bold",
        textColor=green,
        leading=42,
        spaceAfter=4,
    )
    kpi_lbl_style = ParagraphStyle(
        name="KpiLbl",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#666666"),
        leading=14,
        spaceAfter=12,
    )
    member_body_style = ParagraphStyle(
        name="MemberBody",
        parent=styles["Normal"],
        fontSize=8,
        leading=11,
        textColor=colors.HexColor("#333333"),
        spaceAfter=6,
    )
    member_note_style = ParagraphStyle(
        name="MemberNote",
        parent=styles["Normal"],
        fontSize=7,
        leading=10,
        textColor=colors.HexColor("#666666"),
        spaceAfter=10,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
    )
    story: list = []

    story.append(Paragraph("Weekly Check-In Report", cover_title_style))
    if checkin_summary:
        story.append(Paragraph(escape(checkin_summary["meta_line"]), cover_meta_style))
    else:
        story.append(
            Paragraph(
                escape("Attendance summary requires ATTENDANCE_SHEET_ID — cell health table below."),
                cover_meta_style,
            )
        )

    story.append(Paragraph("Cell health (NWST)", sec_style))

    hdr = ["Zone", "Cell", "New", "Regular", "Irregular", "Follow Up"]
    data: list[list] = [[Paragraph(escape(h), th_style) for h in hdr]]

    for r in rows:
        data.append(
            [
                Paragraph(escape(str(r.get("zone", ""))), td_style),
                Paragraph(escape(str(r.get("cell", ""))), td_style),
                # Bucket cells contain <b> tags for bold percentage - don't escape
                Paragraph(str(r.get("new_s", "")), td_style),
                Paragraph(str(r.get("regular_s", "")), td_style),
                Paragraph(str(r.get("irregular_s", "")), td_style),
                Paragraph(str(r.get("follow_up_s", "")), td_style),
            ]
        )

    if len(data) == 1:
        data.append(
            [
                Paragraph(escape("No NWST Health cell data available."), td_style),
                Paragraph("", td_style),
                Paragraph("", td_style),
                Paragraph("", td_style),
                Paragraph("", td_style),
                Paragraph("", td_style),
            ]
        )

    usable_w = A4[0] - 72
    col_widths = [usable_w * f for f in (0.12, 0.20, 0.14, 0.17, 0.17, 0.20)]

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eeeeee")),
                ("ALIGN", (2, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [colors.white, colors.HexColor("#fafafa")],
                ),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 12))

    if checkin_summary:
        usable_w = A4[0] - 72
        grey = colors.HexColor("#e8e8e8")

        def _bar_table(paragraph: Paragraph) -> Table:
            t = Table([[paragraph]], colWidths=[usable_w])
            t.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, -1), grey),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("TOPPADDING", (0, 0), (-1, -1), 8),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                        ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ]
                )
            )
            return t

        story.append(_bar_table(Paragraph("NWST Check In", bar_title_style)))
        story.append(Spacer(1, 8))
        story.append(
            Paragraph(str(checkin_summary["total_checked_in"]), kpi_big_style)
        )
        story.append(Paragraph("Total Checked In", kpi_lbl_style))
        roster = int(checkin_summary["total_members"])
        pct = int(checkin_summary["pct"])
        n_in = int(checkin_summary["total_checked_in"])
        if roster > 0:
            stat_line = f"{n_in} of {roster} members ({pct}%)"
        else:
            stat_line = f"{n_in} checked in (roster count unavailable from Options tab)"
        story.append(_bar_table(Paragraph(escape(stat_line), bar_muted_style)))
        story.append(Spacer(1, 6))
        story.append(
            _bar_table(
                Paragraph(
                    escape("Legend: Blue = Checked In | Grey/Italic = Did not come."),
                    bar_muted_style,
                )
            )
        )
        story.append(Spacer(1, 10))
        roster_groups = (checkin_roster or {}).get("groups") if checkin_roster else None
        if roster_groups:
            group_bar_green = colors.HexColor("#15803d")
            group_hdr_para_style = ParagraphStyle(
                name="CheckinGroupHdr",
                parent=styles["Normal"],
                fontSize=10,
                fontName="Helvetica-Bold",
                leading=13,
                spaceBefore=8,
                spaceAfter=5,
                textColor=colors.HexColor("#111111"),
            )
            for g in roster_groups:
                cell_title = str(g.get("cell") or "Unknown")
                n_chk = int(g.get("n_checked") or 0)
                n_tot = int(g.get("n_total") or 0)
                hdr_p = Paragraph(
                    escape(f"{cell_title} ({n_chk}/{n_tot})"),
                    group_hdr_para_style,
                )
                hdr_tbl = Table([[hdr_p]], colWidths=[usable_w - 6])
                hdr_tbl.setStyle(
                    TableStyle(
                        [
                            ("LINEBEFORE", (0, 0), (0, 0), 4, group_bar_green),
                            ("LEFTPADDING", (0, 0), (0, 0), 10),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ]
                    )
                )
                story.append(hdr_tbl)
                chk = list(g.get("checked_labels") or [])
                pend = list(g.get("pending_labels") or [])
                parts: list[str] = []
                for lab in chk:
                    parts.append(f'<font color="#2563eb">{escape(lab)}</font>')
                for lab in pend:
                    parts.append(f'<font color="#6b7280"><i>{escape(lab)}</i></font>')
                if parts:
                    story.append(Paragraph(", ".join(parts), member_body_style))
        story.append(Spacer(1, 18))

    story.append(Paragraph(escape(report_label), title_style))
    story.append(Paragraph(escape(subtitle), sub_style))

    try:
        doc.build(story)
    except Exception as e:
        return b"", str(e)
    return buf.getvalue(), None


def _send_pdf_email(
    *,
    pdf_bytes: bytes,
    subject: str,
    to_addr: str,
    cc_addr: str | None = None,
    body_text: str,
    attachment_name: str,
) -> tuple[bool, str]:
    creds = _sender_creds()
    if not creds:
        return False, "SENDER_EMAIL / SENDER_PASSWORD not configured."
    if not to_addr:
        return False, "Recipient not configured (WEEKLY_REPORT_TO / PSQ_EMAIL / NWST_CORE_TEAM_TO)."
    sender_email, sender_password = creds
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_addr
    if cc_addr:
        msg["Cc"] = cc_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    att = MIMEApplication(pdf_bytes, _subtype="pdf")
    att.add_header("Content-Disposition", "attachment", filename=attachment_name)
    msg.attach(att)
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
    except Exception as e:
        return False, str(e)
    return True, "ok"


def _run_report(
    *,
    target_date: str | None,
    recipient: str,
    cc: str | None = None,
    subject_prefix: str,
) -> None:
    client = _gspread_client()
    sheet_ch = nwst_health_sheet_id()
    # Cell health always reflects latest NWST Health (today), even when sending for a historical attendance date.
    rows, subtitle = build_cell_health_table_rows(client, sheet_ch, target_date_str=None)
    if target_date:
        subtitle = f"{subtitle} Attendance report date: {target_date}."
    label = f"{subject_prefix} — {target_date}" if target_date else subject_prefix
    checkin_summary = _build_checkin_summary(client, target_date)
    date_for_roster = _report_attendance_date_str(target_date)
    attendance_sid = _attendance_sheet_id()
    checkin_roster = (
        _build_checkin_roster_for_pdf(client, attendance_sid, sheet_ch, date_for_roster)
        if attendance_sid
        else None
    )
    pdf_bytes, err = _build_report_pdf_bytes(
        rows,
        subtitle,
        label,
        checkin_summary=checkin_summary,
        checkin_roster=checkin_roster,
    )
    if err or not pdf_bytes:
        print(f"FAIL: PDF: {err or 'empty'}")
        return
    safe_date = (target_date or "").replace("/", "-") or "latest"
    fname = f"weekly_checkin_report_{safe_date}.pdf"
    body = (
        f"{label}\n\n{subtitle}\n\n"
        f"PDF: Weekly Check-In Report — cell-health table first, then NWST Check In summary "
        f"(when ATTENDANCE_SHEET_ID is set).\n"
    )
    ok, detail = _send_pdf_email(
        pdf_bytes=pdf_bytes,
        subject=label,
        to_addr=recipient,
        cc_addr=cc,
        body_text=body,
        attachment_name=fname,
    )
    if ok:
        print("SUCCESS: Weekly report emailed with NWST Health PDF.")
    else:
        print(f"FAIL: {detail}")


def main(target_date: str | None = None) -> None:
    """Send the standard PSQ weekly PDF (NWST Health table first)."""
    to_addr = _weekly_recipient()
    _run_report(
        target_date=target_date,
        recipient=to_addr,
        cc=_weekly_cc(),
        subject_prefix="NWST Weekly Report",
    )


def send_to_nwst_core_team(target_date: str | None = None) -> None:
    to_addr = _nwst_core_recipient()
    _run_report(
        target_date=target_date,
        recipient=to_addr,
        cc=_nwst_core_cc(),
        subject_prefix="NWST Weekly Report (Core Team)",
    )
