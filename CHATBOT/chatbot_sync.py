from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

MYT = timezone(timedelta(hours=8))

_CHATBOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _CHATBOT_DIR.parent
_CHECK_IN_DIR = _REPO_ROOT / "CHECK IN"

for _p in [str(_CHATBOT_DIR), str(_REPO_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from chatbot_redis import get_redis_client, get_unsynced_logs, mark_synced


def _get_sheet_id() -> str:
    sid = os.getenv("ATTENDANCE_SHEET_ID", "").strip()
    if not sid:
        try:
            import streamlit as st
            sid = (st.secrets.get("ATTENDANCE_SHEET_ID") or "").strip()
        except Exception:
            pass
    return sid


def _gsheet_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        return None

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = None

    # 1. st.secrets
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            info = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(info, scopes=scope)
    except Exception:
        pass

    # 2. GCP_SERVICE_ACCOUNT_JSON env var
    if creds is None:
        json_blob = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
        if json_blob:
            try:
                info = json.loads(json_blob)
                creds = Credentials.from_service_account_info(info, scopes=scope)
            except Exception:
                pass

    # 3. credentials.json file
    if creds is None:
        for p in [_CHECK_IN_DIR / "credentials.json", _REPO_ROOT / "credentials.json"]:
            if p.is_file():
                try:
                    creds = Credentials.from_service_account_file(str(p), scopes=scope)
                    break
                except Exception:
                    pass

    if creds is None:
        return None
    try:
        return gspread.authorize(creds)
    except Exception:
        return None


def _ensure_chatbot_logs_worksheet(spreadsheet):
    import gspread

    try:
        ws = spreadsheet.worksheet("Chatbot Logs")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet("Chatbot Logs", rows=5000, cols=10)
        ws.append_row(
            ["Date", "Time (MYT)", "User Name", "Question", "Answer", "Tokens Used"],
            value_input_option="USER_ENTERED",
        )
        return ws

    if not ws.row_values(1):
        ws.append_row(
            ["Date", "Time (MYT)", "User Name", "Question", "Answer", "Tokens Used"],
            value_input_option="USER_ENTERED",
        )
    return ws


def sync_chatbot_logs() -> None:
    r = get_redis_client()
    if not r:
        return

    now = datetime.now(MYT)
    today_str = now.strftime("%Y-%m-%d")
    yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    logs = get_unsynced_logs(r, today_str)

    if not logs:
        mark_synced(r, yesterday_str)
        return

    gc = _gsheet_client()
    if not gc:
        return

    sheet_id = _get_sheet_id()
    if not sheet_id:
        return

    try:
        spreadsheet = gc.open_by_key(sheet_id)
        ws = _ensure_chatbot_logs_worksheet(spreadsheet)
        rows = [
            [
                entry.get("date", ""),
                entry.get("timestamp", ""),
                entry.get("user_name", ""),
                entry.get("question", ""),
                entry.get("answer", ""),
                entry.get("tokens_used", ""),
            ]
            for entry in logs
        ]
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        mark_synced(r, yesterday_str)
    except Exception:
        pass  # retry on next sync
