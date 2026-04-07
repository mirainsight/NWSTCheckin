#!/usr/bin/env python3
"""
Full sheet sync for CHECK IN (aligned with ``attendance_app.perform_hard_sheet_resync``):

1. Append pending check-in rows from Upstash to Google Sheets (per selected tabs).
2. Clear Upstash caches: options, zone mapping, today’s attendance snapshots (all three tabs),
   ministry option keys, newcomers snapshot.
3. Refresh Theme Override snapshot into Upstash (same as main app after resync).

**CLI (default = full sync):**
  cd "CHECK IN" && python flush_pending.py
  python flush_pending.py --pending-only
  python flush_pending.py --tabs attendance leaders --pending-only

**Streamlit UI:** ``streamlit run flush_pending.py`` — NWST styling matches ``attendance_app`` (Theme Override in Upstash, ``nwst_shared/nwst_accent_overrides.json`` at repo root, env/secrets ``ATTENDANCE_ACCENT_OVERRIDE_*``, daily MYT palette fallback). Run log resets each press.

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

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from nwst_shared.paths import resolved_nwst_accent_config_path
from nwst_shared.nwst_daily_palette import (
    generate_colors_for_date as _generate_colors_for_date,
    normalize_primary_hex as _normalize_primary_hex,
    theme_from_primary_hex as _theme_from_primary_hex,
)

# CHECK IN folder (this script lives next to ``attendance_app.py``)
_CHECK_IN_ROOT = Path(__file__).resolve().parent
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
REDIS_LAST_SYNC_TIMESTAMP_KEY = "attendance:last_sync_timestamp"
# Same list as attendance_app.MINISTRY_LIST (ministry option cache keys)
MINISTRY_LIST = ["Worship", "Hype", "VS", "Frontlines"]
SESSION_LOG_KEY = "flush_pending_session_log"
ALL_PENDING_TABS = [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME, MINISTRY_ATTENDANCE_TAB_NAME]

_nwst_accent_cfg_mod = None


def _load_nwst_accent_cfg():
    """Load shared accent module (``nwst_shared``)."""
    global _nwst_accent_cfg_mod
    if _nwst_accent_cfg_mod is not None:
        return _nwst_accent_cfg_mod
    cfg = resolved_nwst_accent_config_path()
    if cfg is None:
        return None
    spec = importlib.util.spec_from_file_location("_flush_pending_nwst_accent", cfg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _nwst_accent_cfg_mod = mod
    return mod


def get_today_myt_date() -> str:
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


def _theme_overrides_from_redis_ui() -> dict:
    """Theme Override snapshot in Upstash — same key as ``attendance_app._theme_overrides_from_redis``."""
    mod = _load_nwst_accent_cfg()
    rc = _redis_client(None)
    if not mod or not rc:
        return {}
    try:
        return mod.read_theme_override_from_redis(rc)
    except Exception:
        return {}


def _resolve_theme_override_row_for_today_flush(from_sheet: dict | None = None) -> dict:
    """Mirror ``attendance_app.resolve_theme_override_row_for_today`` (JSON + Redis + env + secrets)."""
    mod = _load_nwst_accent_cfg()
    from_file = mod.get_accent_override_by_date() if mod else {}
    if from_sheet is None:
        from_sheet = _theme_overrides_from_redis_ui()
    if not from_sheet:
        return {}
    if mod:
        row = mod.resolve_latest_cached_theme_row(from_file, from_sheet)
    else:
        latest = max(from_sheet.keys())
        merged = {
            k: {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
            for k in set(from_file) | set(from_sheet)
            if {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
        }
        row = dict(merged.get(latest) or {})
    today = get_today_myt_date()
    if not row.get("primary"):
        env_d = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_DATE", "").strip()
        env_h = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_HEX", "").strip()
        if env_d == today and env_h:
            row["primary"] = env_h.strip()
        else:
            try:
                import streamlit as st

                sd = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_DATE", "")).strip()
                sh = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_HEX", "")).strip()
                if sd == today and sh:
                    row["primary"] = sh.strip()
            except Exception:
                pass
    return row


def _generate_daily_colors_for_sync_ui() -> dict:
    """Same rules as ``attendance_app.generate_daily_colors`` (Theme Override / JSON / daily MYT fallback / banner keys)."""
    today_str = get_today_myt_date()
    from_sheet = _theme_overrides_from_redis_ui()
    row = _resolve_theme_override_row_for_today_flush(from_sheet=from_sheet)
    hex_override = row.get("primary")
    base: dict | None = None
    if hex_override:
        pn = _normalize_primary_hex(hex_override)
        if pn:
            base = _theme_from_primary_hex(pn)
    if base is None:
        base = _generate_colors_for_date(today_str)
    b_raw = row.get("banner")
    mod = _load_nwst_accent_cfg()
    if b_raw and mod:
        safe = mod.sanitize_banner_filename(b_raw)
        if safe:
            base = {**base, "banner": safe}
    if not from_sheet and mod:
        safe = mod.sanitize_banner_filename("banner.gif")
        if safe:
            base = {**base, "banner": safe}
    return base


def _nwst_page_colors() -> dict:
    """NWST palette: primary/light from same pipeline as ``attendance_app`` (dark chrome)."""
    base = _generate_daily_colors_for_sync_ui()
    return {
        "primary": base["primary"],
        "light": base["light"],
        "background": "#000000",
        "text": "#ffffff",
        "text_muted": "#999999",
        "card_bg": "#0a0a0a",
        "border": base["primary"],
    }


def _log_ts() -> str:
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")


def _get_last_sync_timestamp() -> str | None:
    """Retrieve last sync timestamp from Upstash Redis."""
    rc = _redis_client(None)
    if not rc:
        return None
    try:
        val = rc.get(REDIS_LAST_SYNC_TIMESTAMP_KEY)
        if val:
            return val.decode() if isinstance(val, bytes) else val
    except Exception:
        pass
    return None


def _save_last_sync_timestamp() -> None:
    """Save current MYT timestamp to Upstash Redis."""
    rc = _redis_client(None)
    if not rc:
        return
    try:
        myt = timezone(timedelta(hours=8))
        ts = datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")
        rc.set(REDIS_LAST_SYNC_TIMESTAMP_KEY, ts)
    except Exception:
        pass


def _relative_time_from_timestamp(timestamp_str: str) -> str:
    """Calculate relative time (e.g., '2 seconds ago', '5 minutes ago', '3 days ago')."""
    try:
        myt = timezone(timedelta(hours=8))
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=myt)
        now = datetime.now(myt)
        diff = now - timestamp

        total_seconds = int(diff.total_seconds())

        if total_seconds < 60:
            return f"{total_seconds} second{'s' if total_seconds != 1 else ''} ago"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif total_seconds < 86400:
            hours = total_seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        else:
            days = total_seconds // 86400
            return f"{days} day{'s' if days != 1 else ''} ago"
    except Exception:
        return ""


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
        m = "Error: Google Sheets client or ATTENDANCE_SHEET_ID not available."
        _emit(m, log_lines, err=True, with_ts=False)
        return False, m

    redis_client = _redis_client(log_lines)
    if not redis_client:
        m = "No pending queue (Upstash not configured)."
        _emit(m, log_lines, with_ts=False)
        return True, m

    today_myt = get_today_myt_date()

    try:
        _emit("Reading check-ins from Upstash...", log_lines, with_ts=False)
        spreadsheet = client.open_by_key(sheet_id)
        total_rows = 0
        results = []
        for tab_name in tab_names:
            pk = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
            raw_items = redis_client.lrange(pk, 0, -1)
            if not raw_items:
                results.append(f"  {tab_name}: 0 rows")
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
            results.append(f"  {tab_name}: +{n} rows")

        _emit("", log_lines, with_ts=False)
        _emit(f"Writing to Google Sheets ({today_myt}):", log_lines, with_ts=False)
        for r in results:
            _emit(r, log_lines, with_ts=False)

        if total_rows == 0:
            summary = "no new rows"
            return True, summary

        summary = f"+{total_rows} rows"
        return True, summary
    except gspread.exceptions.APIError as e:
        m = "Error: API quota exceeded." if (
            "429" in str(e) or "Quota exceeded" in str(e)
        ) else f"Error: {e}"
        _emit(m, log_lines, err=True, with_ts=False)
        return False, m
    except Exception as e:
        _emit(f"Error: {e}", log_lines, err=True, with_ts=False)
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


def _clear_full_resync_redis_keys(redis_client, today_myt: str, log_lines: list[str] | None) -> bool:
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
        return True
    except Exception as e:
        _emit(f"Error: Cache clear failed ({e})", log_lines, err=True, with_ts=False)
        return False


def _refresh_theme_override_shared(
    redis_client,
    gsheet_client,
    sheet_id: str,
    log_lines: list[str] | None,
) -> bool:
    if not (sheet_id or "").strip() or not redis_client or not gsheet_client:
        return True
    mod = _load_nwst_accent_cfg()
    if mod is None:
        return True
    try:
        mod.refresh_theme_override_shared_cache(redis_client, gsheet_client, sheet_id)
        return True
    except Exception as e:
        _emit(f"Error: Theme refresh failed ({e})", log_lines, err=True, with_ts=False)
        return False


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
        msg = "Error: Google Sheets client or ATTENDANCE_SHEET_ID not available."
        _emit(msg, log_lines, err=True, with_ts=False)
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
        _emit("", log_lines, with_ts=False)
        _emit("Done (no Upstash configured).", log_lines, with_ts=False)
        _progress_set(progress_bar, 1.0, "Done (no Upstash).")
        return True, flush_summary

    _emit("", log_lines, with_ts=False)
    _emit("Clearing old data from Upstash...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.45, "Clearing Upstash caches (options, attendance, ministry)…")
    cache_ok = _clear_full_resync_redis_keys(redis_client, today_myt, log_lines)
    if cache_ok:
        _emit("  Cache cleared", log_lines, with_ts=False)

    _emit("", log_lines, with_ts=False)
    _emit("Refreshing theme...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.72, "Refreshing Theme Override snapshot…")
    theme_ok = _refresh_theme_override_shared(redis_client, client, sheet_id, log_lines)
    if theme_ok:
        _emit("  Theme updated", log_lines, with_ts=False)

    _save_last_sync_timestamp()
    _progress_set(progress_bar, 1.0, "All done.")

    # Summary line
    _emit("", log_lines, with_ts=False)
    _emit(f"Done: {flush_summary}.", log_lines, with_ts=False)

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

    pc = _nwst_page_colors()
    st.markdown(
        f"""
<style>
    /* Same font imports as attendance_app NWST chrome (Outfit + Inter stack used across the app). */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

    /* Baseline: kill Streamlit theme primary on this app (matches attendance_app NWST) */
    .stApp {{
        background-color: {pc["background"]} !important;
        font-family: 'Inter', sans-serif !important;
        --primary-color: {pc["primary"]} !important;
    }}
    html, body {{
        font-family: 'Inter', sans-serif !important;
    }}

    .stApp h1, .stApp h2, .stApp h3,
    .stApp .stMarkdown, .stApp .stMarkdown p, .stApp .stMarkdown span, .stApp .stMarkdown li {{
        font-family: 'Inter', sans-serif !important;
    }}
    .stApp .stMarkdown, .stApp .stMarkdown p, .stApp .stMarkdown span, .stApp .stMarkdown div, .stApp .stMarkdown li {{
        color: {pc["text"]} !important;
    }}

    /* Hide default Streamlit title */
    .stApp h1 {{
        display: none !important;
    }}

    /* Custom vibrant title styling */
    .flush-title {{
        text-align: center;
        margin: 1.5rem 0 1rem 0;
    }}
    .flush-title-text {{
        font-family: 'Outfit', 'Inter', -apple-system, sans-serif !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.04em !important;
        text-transform: uppercase !important;
        background: linear-gradient(135deg, {pc["primary"]} 0%, {pc["light"]} 50%, {pc["primary"]} 100%);
        background-size: 200% 200%;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        animation: gradient-shift 3s ease infinite;
        display: inline-block;
    }}
    @keyframes gradient-shift {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}
    .flush-subtitle {{
        font-family: 'Inter', sans-serif !important;
        font-size: 0.85rem !important;
        color: {pc["text_muted"]} !important;
        letter-spacing: 0.05em !important;
        margin-top: 0.25rem !important;
    }}

    .log-caption {{
        color: {pc["text_muted"]} !important;
        font-size: 0.8rem !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.4rem !important;
        font-family: 'Inter', sans-serif !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        font-weight: 600 !important;
    }}

    [data-testid="stVerticalBlock"] {{
        gap: 0.5rem !important;
    }}
    .element-container {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
    }}

    /* Primary CTA: wrapper class st-key-flush_run; also baseButton-primary (Streamlit ≥1.32). */
    .stApp div[class*="st-key-flush_run"] button,
    .stApp .st-key-flush_run button,
    .stApp .st-key-flush_run [data-testid="baseButton-primary"],
    .stApp button[data-testid="baseButton-primary"],
    .stApp .stButton > button[kind="primary"],
    .stApp .stButton > button[data-testid="baseButton-primary"] {{
        background: linear-gradient(135deg, {pc["primary"]} 0%, {pc["light"]} 100%) !important;
        color: {pc["background"]} !important;
        border: none !important;
        border-radius: 0px !important;
        font-family: 'Outfit', 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
        text-transform: uppercase !important;
        font-size: 1rem !important;
        min-height: 3.5rem !important;
        padding: 1rem 2rem !important;
        box-shadow: 0 4px 20px {pc["primary"]}40, 0 0 40px {pc["primary"]}20 !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        position: relative !important;
        overflow: hidden !important;
    }}
    .stApp div[class*="st-key-flush_run"] button::before,
    .stApp .st-key-flush_run button::before,
    .stApp button[data-testid="baseButton-primary"]::before {{
        content: '' !important;
        position: absolute !important;
        top: 0 !important;
        left: -100% !important;
        width: 100% !important;
        height: 100% !important;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent) !important;
        transition: left 0.5s ease !important;
    }}
    .stApp div[class*="st-key-flush_run"] button:hover,
    .stApp .st-key-flush_run button:hover,
    .stApp .st-key-flush_run [data-testid="baseButton-primary"]:hover,
    .stApp button[data-testid="baseButton-primary"]:hover,
    .stApp .stButton > button[kind="primary"]:hover,
    .stApp .stButton > button[data-testid="baseButton-primary"]:hover {{
        transform: translateY(-2px) scale(1.02) !important;
        box-shadow: 0 8px 30px {pc["primary"]}60, 0 0 60px {pc["primary"]}30 !important;
    }}
    .stApp div[class*="st-key-flush_run"] button:hover::before,
    .stApp .st-key-flush_run button:hover::before,
    .stApp button[data-testid="baseButton-primary"]:hover::before {{
        left: 100% !important;
    }}
    .stApp div[class*="st-key-flush_run"] button:active,
    .stApp .st-key-flush_run button:active,
    .stApp button[data-testid="baseButton-primary"]:active {{
        transform: translateY(0) scale(0.98) !important;
    }}

    /* Progress bar styling */
    .stProgress > div > div > div {{
        background-color: rgba(255,255,255,0.1) !important;
        border-radius: 0px !important;
    }}
    .stProgress > div > div > div > div {{
        background: linear-gradient(90deg, {pc["primary"]}, {pc["light"]}) !important;
        border-radius: 0px !important;
        box-shadow: 0 0 10px {pc["primary"]}60 !important;
    }}
    .stProgress [role="progressbar"] {{
        border-radius: 0px !important;
    }}
    .stProgress div[aria-valuemax="1"] {{
        border-radius: 0px !important;
    }}

    /* Code/log block styling */
    [data-testid="stCode"],
    [data-testid="stCodeBlock"] {{
        border: 1px solid {pc["primary"]}40 !important;
        border-radius: 0px !important;
        background: {pc["card_bg"]} !important;
        box-shadow: inset 0 2px 10px rgba(0,0,0,0.3) !important;
    }}
    .stApp pre, .stApp code {{
        font-size: 0.75rem !important;
        font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace !important;
        color: {pc["text_muted"]} !important;
    }}

    /* Success/error message styling */
    .stSuccess {{
        background: linear-gradient(135deg, rgba(34,197,94,0.15) 0%, rgba(34,197,94,0.05) 100%) !important;
        border-left: 3px solid #22c55e !important;
        border-radius: 0px !important;
    }}
    .stError {{
        background: linear-gradient(135deg, rgba(239,68,68,0.15) 0%, rgba(239,68,68,0.05) 100%) !important;
        border-left: 3px solid #ef4444 !important;
        border-radius: 0px !important;
    }}

    /* Last sync timestamp styling */
    .last-sync-text {{
        color: {pc["text_muted"]} !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.82rem !important;
        text-align: center !important;
        margin: 1.25rem 0 0.5rem 0 !important;
        font-style: italic !important;
        opacity: 0.8 !important;
    }}
</style>
""",
        unsafe_allow_html=True,
    )

    # Custom vibrant title with gradient animation
    st.markdown(
        f"""
        <div class="flush-title">
            <div class="flush-title-text">Sync & Update</div>
            <div class="flush-subtitle">Flush pending check-ins to Google Sheets</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if SESSION_LOG_KEY not in st.session_state:
        st.session_state[SESSION_LOG_KEY] = []

    progress_slot = st.empty()
    clicked = st.button(
        "⚡ SYNC NOW",
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

    # Display last refreshed timestamp
    last_sync = _get_last_sync_timestamp()
    if last_sync:
        relative_time = _relative_time_from_timestamp(last_sync)
        st.markdown(
            f'<p class="last-sync-text">Last update: {last_sync} MYT ({relative_time})</p>',
            unsafe_allow_html=True,
        )

    st.markdown('<p class="log-caption">📋 Activity Log</p>', unsafe_allow_html=True)
    st.code(
        "\n".join(st.session_state[SESSION_LOG_KEY]) or "(press sync to see activity)",
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
