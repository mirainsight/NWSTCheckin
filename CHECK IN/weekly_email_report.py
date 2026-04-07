"""
Weekly PSQ email + PDF for NWST Check In.

PDF starts with a NWST Health cell-mix table (latest Historical Cell Status snapshot when present,
else live CG Combined). That table always uses **current** NWST data, even if the report is sent
while viewing a historical attendance date. Configure recipients via Streamlit secrets or env:
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
from pathlib import Path
from xml.sax.saxutils import escape

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from nwst_shared.nwst_cell_health_report import (
    build_cell_health_table_rows,
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


def _build_report_pdf_bytes(
    rows: list[dict],
    subtitle: str,
    report_label: str,
) -> tuple[bytes, str | None]:
    """Build PDF with ReportLab so the cell-health table is always visible (xhtml2pdf tables are unreliable)."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        name="NwstTitle",
        parent=styles["Title"],
        fontSize=16,
        leading=20,
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

    # Cell-health table first (per module docstring); report title/subtitle follow.
    story.append(Paragraph("Cell health (NWST)", sec_style))

    hdr = ["Zone", "Cell", "New", "Regular", "Irregular", "Follow Up"]
    data: list[list] = [[Paragraph(escape(h), th_style) for h in hdr]]

    for r in rows:
        data.append(
            [
                Paragraph(escape(str(r.get("zone", ""))), td_style),
                Paragraph(escape(str(r.get("cell", ""))), td_style),
                Paragraph(escape(str(r.get("new_s", ""))), td_style),
                Paragraph(escape(str(r.get("regular_s", ""))), td_style),
                Paragraph(escape(str(r.get("irregular_s", ""))), td_style),
                Paragraph(escape(str(r.get("follow_up_s", ""))), td_style),
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
    sheet_id = nwst_health_sheet_id()
    # Cell health always reflects latest NWST Health (today), even when sending for a historical attendance date.
    rows, subtitle = build_cell_health_table_rows(client, sheet_id, target_date_str=None)
    if target_date:
        subtitle = f"{subtitle} Attendance report date: {target_date}."
    label = f"{subject_prefix} — {target_date}" if target_date else subject_prefix
    pdf_bytes, err = _build_report_pdf_bytes(rows, subtitle, label)
    if err or not pdf_bytes:
        print(f"FAIL: PDF: {err or 'empty'}")
        return
    safe_date = (target_date or "").replace("/", "-") or "latest"
    fname = f"nwst_weekly_{safe_date}.pdf"
    body = (
        f"{label}\n\n{subtitle}\n\nCell health table is on page 1 of the PDF attachment.\n"
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
