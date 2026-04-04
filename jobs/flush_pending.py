#!/usr/bin/env python3
"""
Append pending check-in rows from Upstash to Google Sheets (same queues as attendance_app).

Run from cron or a scheduler, not via Streamlit:
  cd "PROJECTS/CHECK IN" && python jobs/flush_pending.py

Env (same as the app):
  ATTENDANCE_SHEET_ID
  UPSTASH_REDIS_REST_URL
  UPSTASH_REDIS_REST_TOKEN

Google credentials (first match wins):
  - GCP_SERVICE_ACCOUNT_JSON — full service-account JSON as a single string (good for CI / env-based hosts)
  - CHECK IN/credentials.json
  - ./credentials.json from current working directory
  - GOOGLE_APPLICATION_CREDENTIALS — path to a .json key file

Note: Streamlit Community Cloud keeps [gcp_service_account] in st.secrets only — it is not on disk and
not in os.environ. This script cannot read st.secrets. Use credentials.json / env on the machine that runs
this script, or rely on the app's **Update names → Sync** to flush from the Streamlit process.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# CHECK IN folder (parent of jobs/)
_CHECK_IN_ROOT = Path(__file__).resolve().parent.parent

# Load .env from CHECK IN if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv(_CHECK_IN_ROOT / ".env")
    load_dotenv()
except ImportError:
    pass

import gspread
from google.oauth2.service_account import Credentials

try:
    from upstash_redis import Redis
except ImportError:
    Redis = None  # type: ignore

ATTENDANCE_TAB_NAME = "Attendance"
LEADERS_ATTENDANCE_TAB_NAME = "Leaders Attendance"
MINISTRY_ATTENDANCE_TAB_NAME = "Ministry Attendance"
REDIS_PENDING_ATTENDANCE_PREFIX = "attendance:pending_rows:"


def get_today_myt_date() -> str:
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


def _redis_client():
    if Redis is None:
        return None
    url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
    if not url or not token:
        return None
    try:
        return Redis(url=url, token=token)
    except Exception as e:
        print(f"[flush_pending] Redis connection failed: {e}", file=sys.stderr)
        return None


def _gsheet_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = None

    json_blob = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if json_blob:
        try:
            info = json.loads(json_blob)
            creds = Credentials.from_service_account_info(info, scopes=scope)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            print(f"[flush_pending] GCP_SERVICE_ACCOUNT_JSON is invalid: {e}", file=sys.stderr)
            return None

    if creds is None:
        paths_to_try = [
            _CHECK_IN_ROOT / "credentials.json",
            Path.cwd() / "credentials.json",
        ]
        gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if gac:
            paths_to_try.append(Path(gac).expanduser())

        for p in paths_to_try:
            if p.is_file():
                try:
                    creds = Credentials.from_service_account_file(str(p), scopes=scope)
                    break
                except Exception as e:
                    print(f"[flush_pending] Could not load {p}: {e}", file=sys.stderr)
                    return None

    if creds is None:
        print(
            "[flush_pending] No Google credentials. Use one of:\n"
            "  - GCP_SERVICE_ACCOUNT_JSON (entire service account JSON string)\n"
            "  - credentials.json in CHECK IN/ or cwd\n"
            "  - GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json\n"
            "Streamlit [gcp_service_account] in secrets.toml is only visible inside Streamlit, not to this script.",
            file=sys.stderr,
        )
        return None
    return gspread.authorize(creds)


def _ensure_attendance_worksheet(spreadsheet, tab_name: str):
    try:
        sh = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        sh = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=20)
        sh.append_row(["Timestamp", "Option"])
        return sh
    hdr = sh.row_values(1)
    if not hdr:
        sh.append_row(["Timestamp", "Option"])
    return sh


def flush_pending_attendance_for_tabs(client, sheet_id: str, tab_names: list[str]) -> tuple[bool, str]:
    if not client or not sheet_id.strip():
        return False, "Google Sheets client or ATTENDANCE_SHEET_ID not available."
    redis_client = _redis_client()
    if not redis_client:
        print("[flush_pending] No Upstash Redis configured; nothing to flush from queues.")
        return True, ""
    today_myt = get_today_myt_date()
    total_rows = 0
    try:
        spreadsheet = client.open_by_key(sheet_id)
        for tab_name in tab_names:
            pk = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
            raw_items = redis_client.lrange(pk, 0, -1)
            if not raw_items:
                continue
            rows = []
            for raw in raw_items:
                s = raw.decode() if isinstance(raw, bytes) else raw
                d = json.loads(s)
                rows.append([d["ts"], d["opt"]])
            sh = _ensure_attendance_worksheet(spreadsheet, tab_name)
            sh.append_rows(rows)
            redis_client.delete(pk)
            total_rows += len(rows)
            print(f"[flush_pending] {today_myt} {tab_name}: wrote {len(rows)} row(s), cleared pending key.")
        if total_rows == 0:
            print(f"[flush_pending] {today_myt}: no pending rows for tabs {tab_names}.")
        return True, ""
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return False, "API quota exceeded. Try again in a moment."
        return False, str(e)
    except Exception as e:
        return False, str(e)


def main() -> int:
    parser = argparse.ArgumentParser(description="Flush pending attendance from Upstash to Google Sheets.")
    parser.add_argument(
        "--tabs",
        nargs="*",
        choices=["attendance", "leaders", "ministry", "all"],
        default=["all"],
        help="Which queues to flush (default: all three sheet tabs).",
    )
    args = parser.parse_args()

    sheet_id = os.getenv("ATTENDANCE_SHEET_ID", "").strip()
    if not sheet_id:
        print("[flush_pending] Set ATTENDANCE_SHEET_ID in the environment.", file=sys.stderr)
        return 1

    if "all" in args.tabs:
        tab_names = [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME, MINISTRY_ATTENDANCE_TAB_NAME]
    else:
        mapping = {
            "attendance": ATTENDANCE_TAB_NAME,
            "leaders": LEADERS_ATTENDANCE_TAB_NAME,
            "ministry": MINISTRY_ATTENDANCE_TAB_NAME,
        }
        tab_names = [mapping[t] for t in args.tabs]

    client = _gsheet_client()
    if not client:
        return 1

    ok, err = flush_pending_attendance_for_tabs(client, sheet_id, tab_names)
    if not ok:
        print(f"[flush_pending] FAILED: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
