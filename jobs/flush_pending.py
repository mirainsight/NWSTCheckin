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
  - GCP_SERVICE_ACCOUNT_JSON — env var with the full JSON string
  - .streamlit/secrets.toml — table [gcp_service_account] (same shapes as local Streamlit / attendance_app)
  - CHECK IN/credentials.json, cwd credentials.json, or GOOGLE_APPLICATION_CREDENTIALS

Using the same **Streamlit Secrets** as attendance_app:

- **Inside** `streamlit run` (e.g. call `main()` from a button callback in your app): this file reads
  `st.secrets["gcp_service_account"]`, `ATTENDANCE_SHEET_ID`, and Upstash keys from `st.secrets` when env vars
  are unset—same as the UI you configured.
- **Outside** Streamlit (`python jobs/flush_pending.py` on Cloud CI): Secrets UI is **not** loaded; use
  `.streamlit/secrets.toml`, `credentials.json`, or env vars instead.
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


def _tomllib_load(path: Path) -> dict:
    data = path.read_bytes()
    try:
        import tomllib

        return tomllib.loads(data.decode("utf-8"))
    except ImportError:
        try:
            import tomli as tomllib_alt

            return tomllib_alt.loads(data.decode("utf-8"))
        except ImportError:
            raise RuntimeError("Install Python 3.11+ or `pip install tomli` to read .streamlit/secrets.toml")


def _credentials_from_streamlit_secrets_toml(scope: list[str]):
    """Load [gcp_service_account] from the same TOML file Streamlit uses locally."""
    candidates = []
    env_path = os.getenv("STREAMLIT_SECRETS_FILE", "").strip()
    if env_path:
        candidates.append(Path(env_path).expanduser())
    for base in (_CHECK_IN_ROOT, _CHECK_IN_ROOT.parent, Path.cwd()):
        candidates.append(base / ".streamlit" / "secrets.toml")

    seen = set()
    for path in candidates:
        try:
            key = str(path.resolve())
        except OSError:
            continue
        if key in seen:
            continue
        seen.add(key)
        if not path.is_file():
            continue
        try:
            data = _tomllib_load(path)
        except Exception as e:
            print(f"[flush_pending] Could not read {path}: {e}", file=sys.stderr)
            continue
        gsa = data.get("gcp_service_account")
        if not isinstance(gsa, dict):
            continue
        try:
            return Credentials.from_service_account_info(gsa, scopes=scope)
        except Exception as e:
            print(f"[flush_pending] Invalid gcp_service_account in {path}: {e}", file=sys.stderr)
            continue
    return None


def _redis_client():
    if Redis is None:
        return None
    url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
    if not url or not token:
        try:
            import streamlit as st

            if not url:
                url = (st.secrets.get("UPSTASH_REDIS_REST_URL") or "").strip()
            if not token:
                token = (st.secrets.get("UPSTASH_REDIS_REST_TOKEN") or "").strip()
        except Exception:
            pass
    if not url or not token:
        return None
    try:
        return Redis(url=url, token=token)
    except Exception as e:
        print(f"[flush_pending] Redis connection failed: {e}", file=sys.stderr)
        return None


def _credentials_from_streamlit_secrets_runtime(scope: list[str]):
    """Use Secrets UI / st.secrets — only works in the same process as `streamlit run` (e.g. attendance_app)."""
    try:
        import streamlit as st
    except ImportError:
        return None
    try:
        if "gcp_service_account" not in st.secrets:
            return None
        info = dict(st.secrets["gcp_service_account"])
        return Credentials.from_service_account_info(info, scopes=scope)
    except (TypeError, KeyError, ValueError) as e:
        print(f"[flush_pending] st.secrets['gcp_service_account'] unusable: {e}", file=sys.stderr)
        return None
    except Exception:
        return None


def _gsheet_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = None

    creds = _credentials_from_streamlit_secrets_runtime(scope)

    json_blob = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if creds is None and json_blob:
        try:
            info = json.loads(json_blob)
            creds = Credentials.from_service_account_info(info, scopes=scope)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            print(f"[flush_pending] GCP_SERVICE_ACCOUNT_JSON is invalid: {e}", file=sys.stderr)
            return None

    if creds is None:
        try:
            creds = _credentials_from_streamlit_secrets_toml(scope)
        except RuntimeError as e:
            print(f"[flush_pending] {e}", file=sys.stderr)

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
            "[flush_pending] No Google credentials. Tried (in order):\n"
            "  1) st.secrets['gcp_service_account'] — only when this runs inside the same process as Streamlit\n"
            "  2) GCP_SERVICE_ACCOUNT_JSON\n"
            "  3) .streamlit/secrets.toml [gcp_service_account]\n"
            "  4) credentials.json / GOOGLE_APPLICATION_CREDENTIALS\n"
            "Plain `python jobs/flush_pending.py` on Streamlit Cloud does not load the Secrets UI; run flush from\n"
            "the app (e.g. import and call main() from a Streamlit callback), or use files/env on the host.",
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
        try:
            import streamlit as st

            sheet_id = (st.secrets.get("ATTENDANCE_SHEET_ID") or "").strip()
        except Exception:
            pass
    if not sheet_id:
        print("[flush_pending] Set ATTENDANCE_SHEET_ID in the environment or Streamlit secrets.", file=sys.stderr)
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
