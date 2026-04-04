#!/usr/bin/env python3
"""
Full sheet sync for CHECK IN (aligned with ``attendance_app.perform_hard_sheet_resync``):

1. Append pending check-in rows from Upstash to Google Sheets (per selected tabs).
2. Clear Upstash caches: options, zone mapping, today’s attendance snapshots (all three tabs),
   ministry option keys, newcomers snapshot.
3. Refresh Theme Override snapshot into Upstash (same as main app after resync).

**CLI (default = full sync):**
  cd "PROJECTS/CHECK IN" && python jobs/flush_pending.py
  python jobs/flush_pending.py --pending-only
  python jobs/flush_pending.py --tabs attendance leaders --pending-only

**Streamlit UI:** ``streamlit run jobs/flush_pending.py`` — page **Click me to update**, orange CTA, progress bar, run log resets every button press.

Env: ATTENDANCE_SHEET_ID, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
Google auth: st.secrets (when using Streamlit), GCP_SERVICE_ACCOUNT_JSON, .streamlit/secrets.toml,
  credentials.json, GOOGLE_APPLICATION_CREDENTIALS — see module doc in code below.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# CHECK IN folder (parent of jobs/)
_CHECK_IN_ROOT = Path(__file__).resolve().parent.parent
if str(_CHECK_IN_ROOT.resolve()) not in sys.path:
    sys.path.insert(0, str(_CHECK_IN_ROOT.resolve()))

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
REDIS_OPTIONS_KEY = "attendance:options"
REDIS_ZONE_MAPPING_KEY = "attendance:zone_mapping"
REDIS_ATTENDANCE_KEY_PREFIX = "attendance:data:"
REDIS_NEWCOMERS_KEY_PREFIX = "attendance:newcomers:"
# Same list as attendance_app.MINISTRY_LIST (ministry option cache keys)
MINISTRY_LIST = ["Worship", "Hype", "VS", "Frontlines"]
SESSION_LOG_KEY = "flush_pending_session_log"
ALL_PENDING_TABS = [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME, MINISTRY_ATTENDANCE_TAB_NAME]

_nwst_accent_cfg_mod = None


def _load_nwst_accent_cfg():
    """Same accent module as attendance_app (CHECK IN root)."""
    global _nwst_accent_cfg_mod
    if _nwst_accent_cfg_mod is not None:
        return _nwst_accent_cfg_mod
    cfg = _CHECK_IN_ROOT / "nwst_accent_config.py"
    if not cfg.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_flush_pending_nwst_accent", cfg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _nwst_accent_cfg_mod = mod
    return mod


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


def _pending_queues_nonempty(redis_client, today_myt: str, tab_names: list[str]) -> bool:
    for tab_name in tab_names:
        pk = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
        try:
            if redis_client.lrange(pk, 0, 0):
                return True
        except Exception:
            continue
    return False


def _clear_full_resync_redis_keys(redis_client, today_myt: str, log_lines: list[str] | None) -> None:
    """Match congregation + ministry branches of ``perform_hard_sheet_resync`` (union of keys)."""
    try:
        redis_client.delete(REDIS_OPTIONS_KEY)
        redis_client.delete(REDIS_ZONE_MAPPING_KEY)
        redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{ATTENDANCE_TAB_NAME}")
        redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{LEADERS_ATTENDANCE_TAB_NAME}")
        redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{MINISTRY_ATTENDANCE_TAB_NAME}")
        redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
        for ministry in MINISTRY_LIST:
            redis_client.delete(f"attendance:ministry_options:{ministry}")
        redis_client.delete("attendance:ministry_options:all")
        _emit(
            "Cleared Redis: options, zone map, today’s attendance cache (3 tabs), newcomers, ministry option keys.",
            log_lines,
        )
    except Exception as e:
        _emit(f"Redis cache clear partial failure: {e}", log_lines, err=True)


def _refresh_theme_override_shared(
    redis_client,
    gsheet_client,
    sheet_id: str,
    log_lines: list[str] | None,
) -> None:
    if not (sheet_id or "").strip() or not redis_client or not gsheet_client:
        return
    mod = _load_nwst_accent_cfg()
    if mod is None:
        _emit("nwst_accent_config not found; skipping Theme Override snapshot.", log_lines)
        return
    try:
        mod.refresh_theme_override_shared_cache(redis_client, gsheet_client, sheet_id)
        _emit("Theme Override snapshot refreshed in Upstash.", log_lines)
    except Exception as e:
        _emit(f"Theme Override refresh failed: {e}", log_lines, err=True)


def _progress_set(bar, value: float, text: str) -> None:
    """Streamlit progress bar; ``text=`` is supported in newer Streamlit versions."""
    if bar is None:
        return
    try:
        bar.progress(value, text=text)
    except TypeError:
        bar.progress(value)


def run_full_sheet_resync(
    client,
    sheet_id: str,
    pending_tab_names: list[str],
    log_lines: list[str] | None,
    progress_bar=None,
) -> tuple[bool, str]:
    """Flush selected pending queues, then full Redis cache clear + theme snapshot (see module doc)."""
    if not client or not sheet_id.strip():
        msg = "Google Sheets client or ATTENDANCE_SHEET_ID not available."
        _emit(msg, log_lines, err=True)
        return False, msg

    _progress_set(progress_bar, 0.08, "Writing pending check-ins to Google Sheets…")
    ok_flush, flush_summary = flush_pending_attendance_for_tabs(
        client, sheet_id, pending_tab_names, log_lines=log_lines
    )
    if not ok_flush:
        _progress_set(progress_bar, 1.0, "Stopped — error flushing pending rows.")
        return False, flush_summary

    _progress_set(progress_bar, 0.38, "Pending queues processed.")

    today_myt = get_today_myt_date()
    redis_client = _redis_client(log_lines)
    if not redis_client:
        extra = " Upstash not configured — skipped Redis cache clear and Theme Override refresh."
        _emit(extra.strip(), log_lines)
        _progress_set(progress_bar, 1.0, "Done (no Upstash).")
        return True, flush_summary + extra

    _progress_set(progress_bar, 0.45, "Clearing Upstash caches (options, attendance, ministry)…")
    _clear_full_resync_redis_keys(redis_client, today_myt, log_lines)
    _progress_set(progress_bar, 0.72, "Refreshing Theme Override snapshot…")
    _refresh_theme_override_shared(redis_client, client, sheet_id, log_lines)
    _emit(
        "Full resync finished. Upstash mirrors are cleared; the main app reads Sheets on the next cache miss.",
        log_lines,
    )
    _emit(
        "Tip: Refresh the Church Check-in browser tab if lists still look stale (Streamlit caches ~30–60s).",
        log_lines,
    )
    _progress_set(progress_bar, 1.0, "All done.")
    return True, flush_summary


def _tab_names_from_multiselect(tab_choice: list[str]) -> list[str]:
    if not tab_choice or "all" in tab_choice:
        return list(ALL_PENDING_TABS)
    mapping = {
        "attendance": ATTENDANCE_TAB_NAME,
        "leaders": LEADERS_ATTENDANCE_TAB_NAME,
        "ministry": MINISTRY_ATTENDANCE_TAB_NAME,
    }
    return [mapping[t] for t in tab_choice if t != "all"]


def main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Full sheet sync (flush pending + clear caches + theme), or pending-only."
    )
    parser.add_argument(
        "--pending-only",
        action="store_true",
        help="Only append pending rows to Sheets; do not clear Redis caches or refresh Theme Override.",
    )
    parser.add_argument(
        "--tabs",
        nargs="*",
        choices=["attendance", "leaders", "ministry", "all"],
        default=["all"],
        help="Which pending queues to flush before cache steps (default: all three sheets).",
    )
    args = parser.parse_args(argv)

    log_lines: list[str] = []
    sheet_id = _resolve_sheet_id(log_lines)
    if not sheet_id:
        _emit("Set ATTENDANCE_SHEET_ID in the environment or Streamlit secrets.", log_lines, err=True)
        return 1

    tab_names = _tab_names_from_multiselect(list(args.tabs))

    client = _gsheet_client(log_lines)
    if not client:
        if args.pending_only:
            return 1
        rc = _redis_client(log_lines)
        today_chk = get_today_myt_date()
        if rc and _pending_queues_nonempty(rc, today_chk, tab_names):
            _emit(
                "Google Sheets not connected, but pending check-ins exist. Fix credentials and retry.",
                log_lines,
                err=True,
            )
        return 1

    if args.pending_only:
        ok, _msg = flush_pending_attendance_for_tabs(client, sheet_id, tab_names, log_lines=log_lines)
    else:
        ok, _msg = run_full_sheet_resync(client, sheet_id, tab_names, log_lines)

    return 0 if ok else 1


def run_streamlit_app() -> None:
    import streamlit as st

    st.set_page_config(
        page_title="Click me to update",
        page_icon="🔄",
        layout="centered",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,500;0,9..40,700;1,9..40,500&display=swap');
.stApp {
  background: linear-gradient(165deg, #0c0f14 0%, #161b26 45%, #0a0d12 100%) !important;
}
h1 {
  font-family: 'DM Sans', system-ui, sans-serif !important;
  color: #f0f3f6 !important;
  text-align: center !important;
  font-weight: 700 !important;
  font-size: 1.75rem !important;
  letter-spacing: -0.03em !important;
  margin-bottom: 0.5rem !important;
}
.sync-sub {
  text-align: center;
  color: #8b949e;
  font-size: 0.92rem;
  max-width: 26rem;
  margin: 0 auto 1.25rem auto;
  line-height: 1.45;
  font-family: 'DM Sans', system-ui, sans-serif;
}
div[data-testid="stMainBlockContainer"] button[kind="primary"] {
  background: linear-gradient(145deg, #ff9f45 0%, #e85d04 42%, #c2410c 100%) !important;
  color: #fff !important;
  border: none !important;
  border-radius: 16px !important;
  font-size: 1.22rem !important;
  font-weight: 700 !important;
  padding: 1.05rem 1.35rem !important;
  box-shadow:
    0 12px 36px rgba(232, 93, 4, 0.48),
    0 0 0 1px rgba(255, 255, 255, 0.12) inset !important;
  font-family: 'DM Sans', system-ui, sans-serif !important;
  letter-spacing: 0.02em !important;
  transition: transform 0.14s ease, box-shadow 0.14s ease !important;
}
div[data-testid="stMainBlockContainer"] button[kind="primary"]:hover {
  transform: translateY(-3px) scale(1.02) !important;
  box-shadow: 0 16px 44px rgba(232, 93, 4, 0.58) !important;
}
.stProgress > div > div > div > div {
  border-radius: 8px !important;
  background: linear-gradient(90deg, #e85d04, #fbbf24) !important;
}
.log-caption {
  color: #8b949e !important;
  font-size: 0.82rem !important;
  margin-top: 1.25rem !important;
  margin-bottom: 0.35rem !important;
  font-family: 'DM Sans', system-ui, sans-serif !important;
}
pre code {
  font-size: 0.78rem !important;
}
</style>
""",
        unsafe_allow_html=True,
    )

    st.title("Click me to update")
    st.markdown(
        '<p class="sync-sub">Push queued check-ins to Google Sheets, wipe Upstash roster caches, '
        "refresh theme. <strong>Run log</strong> below resets every time you tap the button — then refresh "
        "Church Check-in.</p>",
        unsafe_allow_html=True,
    )

    if SESSION_LOG_KEY not in st.session_state:
        st.session_state[SESSION_LOG_KEY] = []

    progress_slot = st.empty()
    clicked = st.button(
        "🔄  Click me to update",
        type="primary",
        use_container_width=True,
        key="flush_run",
        help="Full sync: all pending rows → Sheets, then clear options / attendance / ministry / newcomers caches "
        "+ Theme Override in Upstash.",
    )

    if clicked:
        run_log: list[str] = []
        bar = progress_slot.progress(0)
        _progress_set(bar, 0.0, "Starting…")

        sheet_id = _resolve_sheet_id(run_log)
        if not sheet_id:
            st.session_state[SESSION_LOG_KEY] = run_log
            st.error("ATTENDANCE_SHEET_ID missing (env or Streamlit secrets).")
        else:
            client = _gsheet_client(run_log)
            if not client:
                st.session_state[SESSION_LOG_KEY] = run_log
                st.error("Could not connect to Google Sheets — see log below.")
            else:
                ok, summary = run_full_sheet_resync(
                    client,
                    sheet_id,
                    list(ALL_PENDING_TABS),
                    log_lines=run_log,
                    progress_bar=bar,
                )
                st.session_state[SESSION_LOG_KEY] = run_log
                if ok:
                    st.success(summary)
                    toast = getattr(st, "toast", None)
                    if toast:
                        toast("Update complete — refresh Church Check-in if lists look stale.", icon="🔄")
                else:
                    st.error(summary or "Update failed.")
        st.rerun()

    st.markdown('<p class="log-caption">Run log — cleared on each update</p>', unsafe_allow_html=True)
    st.code(
        "\n".join(st.session_state[SESSION_LOG_KEY]) or "(empty — press the button above)",
        language="text",
    )


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
