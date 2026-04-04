#!/usr/bin/env python3
"""
Append pending check-in rows from Upstash to Google Sheets (same queues as attendance_app).

**CLI (cron / terminal):**
  cd "PROJECTS/CHECK IN" && python jobs/flush_pending.py
  python jobs/flush_pending.py --tabs attendance leaders

**Streamlit UI (button + on-screen log):**
  cd "PROJECTS/CHECK IN" && streamlit run jobs/flush_pending.py

Env: ATTENDANCE_SHEET_ID, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
Google auth: st.secrets (when using Streamlit), GCP_SERVICE_ACCOUNT_JSON, .streamlit/secrets.toml,
  credentials.json, GOOGLE_APPLICATION_CREDENTIALS — see module doc in code below.
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
SESSION_LOG_KEY = "flush_pending_session_log"


def get_today_myt_date() -> str:
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


def _log_ts() -> str:
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")


def _emit(
    message: str,
    log_lines: list[str] | None,
    *,
    err: bool = False,
    with_ts: bool = True,
) -> None:
    line = f"[{_log_ts()} MYT] {message}" if with_ts else message
    if log_lines is not None:
        log_lines.append(line)
    print(line, file=sys.stderr if err else sys.stdout, flush=True)


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
            _emit(f"Could not read {path}: {e}", None, err=True)
            continue
        gsa = data.get("gcp_service_account")
        if not isinstance(gsa, dict):
            continue
        try:
            return Credentials.from_service_account_info(gsa, scopes=scope)
        except Exception as e:
            _emit(f"Invalid gcp_service_account in {path}: {e}", None, err=True)
            continue
    return None


def _redis_client(log_lines: list[str] | None = None):
    if Redis is None:
        _emit("upstash_redis package not installed.", log_lines, err=True)
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
        _emit(f"Redis connection failed: {e}", log_lines, err=True)
        return None


def _credentials_from_streamlit_secrets_runtime(scope: list[str]):
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
        _emit(f"st.secrets['gcp_service_account'] unusable: {e}", None, err=True)
        return None
    except Exception:
        return None


def _gsheet_client(log_lines: list[str] | None = None):
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = None

    creds = _credentials_from_streamlit_secrets_runtime(scope)
    if creds and log_lines is not None:
        _emit("Using Google credentials from Streamlit secrets (gcp_service_account).", log_lines)

    json_blob = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if creds is None and json_blob:
        try:
            info = json.loads(json_blob)
            creds = Credentials.from_service_account_info(info, scopes=scope)
            if log_lines is not None:
                _emit("Using GCP_SERVICE_ACCOUNT_JSON from environment.", log_lines)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            _emit(f"GCP_SERVICE_ACCOUNT_JSON is invalid: {e}", log_lines, err=True)
            return None

    if creds is None:
        try:
            creds = _credentials_from_streamlit_secrets_toml(scope)
            if creds and log_lines is not None:
                _emit("Using [gcp_service_account] from .streamlit/secrets.toml.", log_lines)
        except RuntimeError as e:
            _emit(str(e), log_lines, err=True)

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
                    if log_lines is not None:
                        _emit(f"Using service account file: {p}", log_lines)
                    break
                except Exception as e:
                    _emit(f"Could not load {p}: {e}", log_lines, err=True)
                    return None

    if creds is None:
        _emit(
            "No Google credentials. Tried: st.secrets → GCP_SERVICE_ACCOUNT_JSON → "
            "secrets.toml → credentials.json / GOOGLE_APPLICATION_CREDENTIALS.",
            log_lines,
            err=True,
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


def _resolve_sheet_id(log_lines: list[str] | None = None) -> str:
    sheet_id = os.getenv("ATTENDANCE_SHEET_ID", "").strip()
    if not sheet_id:
        try:
            import streamlit as st

            sheet_id = (st.secrets.get("ATTENDANCE_SHEET_ID") or "").strip()
            if sheet_id and log_lines is not None:
                _emit("Using ATTENDANCE_SHEET_ID from Streamlit secrets.", log_lines)
        except Exception:
            pass
    return sheet_id


def flush_pending_attendance_for_tabs(
    client,
    sheet_id: str,
    tab_names: list[str],
    log_lines: list[str] | None = None,
) -> tuple[bool, str]:
    if not client or not sheet_id.strip():
        m = "Google Sheets client or ATTENDANCE_SHEET_ID not available."
        _emit(m, log_lines, err=True)
        return False, m

    redis_client = _redis_client(log_lines)
    if not redis_client:
        m = "Upstash not configured — there is no pending queue (check-ins write straight to the sheet)."
        _emit(m, log_lines)
        return True, m

    today_myt = get_today_myt_date()
    _emit(f"Today (MYT): {today_myt}; tabs: {', '.join(tab_names)}", log_lines)

    try:
        spreadsheet = client.open_by_key(sheet_id)
        total_rows = 0
        parts = []
        for tab_name in tab_names:
            pk = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
            raw_items = redis_client.lrange(pk, 0, -1)
            if not raw_items:
                _emit(f"  {tab_name}: pending queue empty (skip).", log_lines)
                continue
            rows = []
            for raw in raw_items:
                s = raw.decode() if isinstance(raw, bytes) else raw
                d = json.loads(s)
                rows.append([d["ts"], d["opt"]])
            sh = _ensure_attendance_worksheet(spreadsheet, tab_name)
            sh.append_rows(rows)
            redis_client.delete(pk)
            n = len(rows)
            total_rows += n
            parts.append(f"{tab_name}: {n}")
            _emit(f"  {tab_name}: appended {n} row(s), cleared Redis pending key.", log_lines)

        if total_rows == 0:
            summary = f"No pending rows for {today_myt} (all queues empty or already flushed)."
            _emit(summary, log_lines)
            return True, summary

        summary = f"Wrote {total_rows} pending row(s) to Google Sheets — " + "; ".join(parts)
        _emit(summary, log_lines)
        return True, summary
    except gspread.exceptions.APIError as e:
        m = "API quota exceeded. Try again in a moment." if (
            "429" in str(e) or "Quota exceeded" in str(e)
        ) else str(e)
        _emit(m, log_lines, err=True)
        return False, m
    except Exception as e:
        _emit(str(e), log_lines, err=True)
        return False, str(e)


def main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Flush pending attendance from Upstash to Google Sheets.")
    parser.add_argument(
        "--tabs",
        nargs="*",
        choices=["attendance", "leaders", "ministry", "all"],
        default=["all"],
        help="Which queues to flush (default: all three sheet tabs).",
    )
    args = parser.parse_args(argv)

    log_lines: list[str] = []
    sheet_id = _resolve_sheet_id(log_lines)
    if not sheet_id:
        _emit("Set ATTENDANCE_SHEET_ID in the environment or Streamlit secrets.", log_lines, err=True)
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

    client = _gsheet_client(log_lines)
    if not client:
        return 1

    ok, _msg = flush_pending_attendance_for_tabs(client, sheet_id, tab_names, log_lines=log_lines)
    return 0 if ok else 1


def run_streamlit_app() -> None:
    import streamlit as st

    st.set_page_config(page_title="Flush pending check-ins", layout="centered")
    st.title("Flush pending check-ins")
    st.markdown(
        "Writes queued Upstash rows to **Google Sheets** (Attendance, Leaders Attendance, Ministry Attendance "
        "for today in MYT). Uses the same **Secrets** as your main app when deployed on Streamlit."
    )

    if SESSION_LOG_KEY not in st.session_state:
        st.session_state[SESSION_LOG_KEY] = []

    tab_choice = st.multiselect(
        "Tabs to flush",
        options=["attendance", "leaders", "ministry", "all"],
        default=["all"],
        help="Choose which sheet tabs to process. Include **all** to clear every pending queue for today.",
    )
    if not tab_choice:
        st.warning("Select at least one tab.")
        tab_choice = ["all"]

    if st.button("Run flush now", type="primary", use_container_width=True):
        run_log: list[str] = []
        st.session_state[SESSION_LOG_KEY].append(f"--- run started ---")

        sheet_id = _resolve_sheet_id(run_log)
        if not sheet_id:
            st.error("ATTENDANCE_SHEET_ID missing (env or Streamlit secrets).")
            st.session_state[SESSION_LOG_KEY].extend(run_log)
        else:
            with st.spinner("Flushing…"):
                client = _gsheet_client(run_log)
                if not client:
                    st.session_state[SESSION_LOG_KEY].extend(run_log)
                    st.error("Could not build Google Sheets client — see log below.")
                else:
                    if "all" in tab_choice:
                        tab_names = [
                            ATTENDANCE_TAB_NAME,
                            LEADERS_ATTENDANCE_TAB_NAME,
                            MINISTRY_ATTENDANCE_TAB_NAME,
                        ]
                    else:
                        mapping = {
                            "attendance": ATTENDANCE_TAB_NAME,
                            "leaders": LEADERS_ATTENDANCE_TAB_NAME,
                            "ministry": MINISTRY_ATTENDANCE_TAB_NAME,
                        }
                        tab_names = [mapping[t] for t in tab_choice if t != "all"]

                    ok, summary = flush_pending_attendance_for_tabs(
                        client, sheet_id, tab_names, log_lines=run_log
                    )
                    st.session_state[SESSION_LOG_KEY].extend(run_log)
                    if ok:
                        st.success(summary)
                        toast = getattr(st, "toast", None)
                        if toast:
                            toast("Flush completed", icon="✅")
                    else:
                        st.error(summary or "Flush failed.")

        st.rerun()

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Clear on-screen log"):
            st.session_state[SESSION_LOG_KEY] = []
            st.rerun()
    with col_b:
        st.caption(f"{len(st.session_state[SESSION_LOG_KEY])} line(s) in session log")

    with st.expander("Session log", expanded=True):
        st.code("\n".join(st.session_state[SESSION_LOG_KEY]) or "(empty)", language="text")


def _inside_streamlit_script_run() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _inside_streamlit_script_run():
        run_streamlit_app()
    else:
        sys.exit(main_cli())
