#!/usr/bin/env python3
"""
Full sheet sync for CHECK IN + NWST Health apps:

1. Append pending check-in rows from Upstash to Google Sheets (per selected tabs).
2. Clear Upstash caches: options, zone mapping, today's attendance snapshots (all three tabs),
   ministry option keys, newcomers snapshot.
3. Sync NWST Health data (same as "Sync from Google Sheets" button in NWST HEALTH/app.py):
   - CG Combined data (nwst_cg_combined_data)
   - Ministries Combined data (nwst_ministries_combined_data)
   - Attendance stats (nwst_attendance_stats)
   - Last sync time (nwst_last_sync_time)
4. Refresh Theme Override snapshot into Upstash.
5. Refresh birthday data from CG Combined.
6. Refresh Cell Health data (single source of truth for KPI cards and PDF reports).

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

_CHATBOT_DIR = _REPO_ROOT / "CHATBOT"
if str(_CHATBOT_DIR) not in sys.path:
    sys.path.insert(0, str(_CHATBOT_DIR))

try:
    from chatbot_redis import (
        get_chatbot_redis_client,
        get_unsynced_logs,
        get_unsynced_change_requests,
        mark_synced,
        mark_change_requests_synced,
    )
    _chatbot_redis_available = True
except ImportError:
    _chatbot_redis_available = False
from nwst_shared.paths import resolved_nwst_accent_config_path
from nwst_shared.nwst_daily_palette import (
    generate_colors_for_date as _generate_colors_for_date,
    normalize_primary_hex as _normalize_primary_hex,
    theme_from_primary_hex as _theme_from_primary_hex,
)
from nwst_shared.nwst_cell_health_report import (
    load_cg_combined_df,
    load_historical_cell_status_df,
    nwst_health_sheet_id,
    nwst_attendance_sheet_id,
    NWST_KEY_VALUES_TAB,
    extract_cell_sheet_status_type,
)
from nwst_shared.nwst_cell_health_cache import (
    store_cell_health_in_redis,
    build_cell_health_row,
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
REDIS_BIRTHDAYS_KEY = "attendance:birthdays_data"
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


def _refresh_birthdays_cache(
    redis_client,
    gsheet_client,
    log_lines: list[str] | None,
) -> bool:
    """Refresh CG Combined birthday data into Upstash (read by attendance_app)."""
    health_sid = (nwst_health_sheet_id() or "").strip()
    if not health_sid or not redis_client or not gsheet_client:
        return True
    try:
        df = load_cg_combined_df(gsheet_client, health_sid)
        if df is not None and not df.empty:
            redis_client.set(REDIS_BIRTHDAYS_KEY, df.to_json(), ex=86400)  # 24h TTL
            _emit("  Birthday data cached", log_lines, with_ts=False)
            return True
        _emit("  Birthday data: empty or unavailable (skipped)", log_lines, with_ts=False)
        return True
    except Exception as e:
        _emit(f"Error: Birthday cache refresh failed ({e})", log_lines, err=True, with_ts=False)
        return False


def _refresh_nwst_health_data(
    redis_client,
    gsheet_client,
    log_lines: list[str] | None,
) -> bool:
    """
    Refresh NWST Health data into Upstash - same as app.py "Sync from Google Sheets" button.

    Syncs:
    - CG Combined data (nwst_cg_combined_data)
    - Ministries Combined data (nwst_ministries_combined_data)
    - Attendance stats (nwst_attendance_stats)
    - Last sync time (nwst_last_sync_time)
    - Clears attendance chart grid cache
    """
    import pandas as pd

    health_sid = (nwst_health_sheet_id() or "").strip()
    if not health_sid or not redis_client or not gsheet_client:
        return True

    try:
        spreadsheet = gsheet_client.open_by_key(health_sid)

        # 1. Sync CG Combined data
        try:
            worksheet = spreadsheet.worksheet("CG Combined")
            data = worksheet.get_all_values()

            if data:
                df = pd.DataFrame(data[1:], columns=data[0])
                cache_data = {
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist()
                }
                redis_client.set("nwst_cg_combined_data", json.dumps(cache_data))
                _emit(f"  CG Combined: {len(df)} members cached (chatbot data updated)", log_lines, with_ts=False)
            else:
                _emit("  CG Combined: no data found", log_lines, with_ts=False)
                return True
        except Exception as e:
            _emit(f"  CG Combined sync failed: {e}", log_lines, err=True, with_ts=False)
            return False

        # 2. Sync Ministries Combined data
        try:
            ministries_worksheet = spreadsheet.worksheet("Ministries Combined")
            ministries_data = ministries_worksheet.get_all_values()

            if ministries_data:
                ministries_df = pd.DataFrame(ministries_data[1:], columns=ministries_data[0])
                cache_data = {
                    "columns": ministries_df.columns.tolist(),
                    "rows": ministries_df.values.tolist()
                }
                redis_client.set("nwst_ministries_combined_data", json.dumps(cache_data))
                _emit(f"  Ministries: {len(ministries_df)} members cached", log_lines, with_ts=False)
        except Exception as e:
            _emit(f"  Ministries sync skipped: {e}", log_lines, with_ts=False)

        # 3. Sync Attendance stats
        try:
            att_worksheet = spreadsheet.worksheet("Attendance")
            att_data = att_worksheet.get_all_values()

            if att_data and len(att_data) >= 2:
                att_headers = att_data[0]
                att_df = pd.DataFrame(att_data[1:], columns=att_headers)

                # Load CG Combined for name/cell mapping
                cg_worksheet = spreadsheet.worksheet("CG Combined")
                cg_data = cg_worksheet.get_all_values()

                if cg_data and len(cg_data) >= 2:
                    cg_headers = cg_data[0]
                    cg_df = pd.DataFrame(cg_data[1:], columns=cg_headers)

                    # Find name and cell columns in CG Combined
                    cg_name_col = None
                    cg_cell_col = None
                    for col in cg_df.columns:
                        if col.lower().strip() in ['name', 'member name', 'member']:
                            cg_name_col = col
                        if col.lower().strip() in ['cell', 'group']:
                            cg_cell_col = col

                    if not cg_name_col:
                        cg_name_col = cg_df.columns[0]

                    # Calculate attendance stats
                    attendance_stats = {}
                    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None
                    # Date column headers (cols D onwards) — used for last_attended lookup
                    date_columns = [col for col_idx, col in enumerate(att_df.columns) if col_idx >= 3]
                    recent_n = min(8, len(date_columns))
                    recent_window = date_columns[-recent_n:] if date_columns else []

                    if att_name_col:
                        for att_name in att_df[att_name_col].unique():
                            if pd.isna(att_name) or att_name == '':
                                continue

                            att_name_str = str(att_name).strip()
                            member_att_data = att_df[att_df[att_name_col] == att_name]

                            attendance_count = 0
                            total_services = 0

                            for col_idx, col in enumerate(att_df.columns):
                                if col_idx >= 3:  # Skip columns A, B, C
                                    total_services += 1
                                    values = member_att_data[col].values
                                    if len(values) > 0 and str(values[0]).strip() == '1':
                                        attendance_count += 1

                            # Last attended date + recent attendance pattern (last 8 sessions)
                            last_attended = None
                            recent_attended = 0
                            for col in reversed(recent_window):
                                values = member_att_data[col].values
                                if len(values) > 0 and str(values[0]).strip() == '1':
                                    if last_attended is None:
                                        last_attended = col
                                    recent_attended += 1
                            # Search older columns for last_attended if not found in recent window
                            if last_attended is None:
                                older_cols = date_columns[:-recent_n] if len(date_columns) > recent_n else []
                                for col in reversed(older_cols):
                                    values = member_att_data[col].values
                                    if len(values) > 0 and str(values[0]).strip() == '1':
                                        last_attended = col
                                        break

                            # Find cell from CG Combined
                            cell_info = ""
                            if cg_name_col and cg_cell_col:
                                cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
                                if not cg_match.empty:
                                    cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

                            if total_services > 0:
                                key = att_name_str + cell_info
                                attendance_stats[key] = {
                                    'attendance': attendance_count,
                                    'total': total_services,
                                    'percentage': round(attendance_count / total_services * 100) if total_services > 0 else 0,
                                    'last_attended': last_attended,
                                    'recent_attended': recent_attended,
                                    'recent_total': recent_n,
                                }

                    redis_client.set("nwst_attendance_stats", json.dumps(attendance_stats))
                    _emit(f"  Attendance: {len(attendance_stats)} members · {len(date_columns)} sessions tracked", log_lines, with_ts=False)
        except Exception as e:
            _emit(f"  Attendance sync skipped: {e}", log_lines, with_ts=False)

        # 4. Store last sync time (MYT)
        myt = timezone(timedelta(hours=8))
        sync_time_myt = datetime.now(myt)
        sync_time_str = sync_time_myt.strftime("%Y-%m-%d %H:%M:%S MYT")
        redis_client.set("nwst_last_sync_time", sync_time_str)

        # 5. Clear attendance chart grid cache + chatbot pre-built context
        try:
            redis_client.delete("nwst_attendance_chart_grid")
            redis_client.delete("chatbot:data_context")
        except Exception:
            pass

        return True
    except Exception as e:
        _emit(f"Error: NWST Health sync failed ({e})", log_lines, err=True, with_ts=False)
        return False


def _refresh_cell_health_cache(
    redis_client,
    gsheet_client,
    log_lines: list[str] | None,
) -> bool:
    """
    Refresh Cell Health data into Upstash (single source of truth for KPI cards and PDF reports).

    Same calculation as app.py calculate_and_cache_cell_health().
    """
    from datetime import date
    import pandas as pd

    health_sid = (nwst_health_sheet_id() or "").strip()
    if not health_sid or not redis_client or not gsheet_client:
        return True

    try:
        # Load CG Combined for live member counts
        cg_df = load_cg_combined_df(gsheet_client, health_sid)
        if cg_df is None or cg_df.empty:
            _emit("  Cell health: CG Combined empty or unavailable (skipped)", log_lines, with_ts=False)
            return True

        # Load Historical Cell Status for WoW deltas
        hist_df = load_historical_cell_status_df(gsheet_client, health_sid)

        # Load cell-to-zone map from Attendance sheet Key Values tab
        cell_to_zone_map = {"all": "PSQ"}
        try:
            att_sid = (nwst_attendance_sheet_id() or "").strip()
            if att_sid:
                att_spreadsheet = gsheet_client.open_by_key(att_sid)
                kv_ws = att_spreadsheet.worksheet(NWST_KEY_VALUES_TAB)
                kv_data = kv_ws.get_all_values()
                if kv_data and len(kv_data) > 1:
                    for row in kv_data[1:]:
                        if len(row) >= 3:
                            cn = row[0].strip()
                            zn = row[2].strip()
                            if cn and zn:
                                cell_to_zone_map[cn.lower()] = zn
        except Exception:
            pass

        # Find status and cell columns in CG Combined
        status_columns = [col for col in cg_df.columns if "status" in col.lower()]
        status_col = status_columns[0] if status_columns else None

        cg_cell_col = None
        for col in cg_df.columns:
            if col.lower().strip() in ("cell", "group"):
                cg_cell_col = col
                break

        if not cg_cell_col:
            _emit("  Cell health: no cell column found (skipped)", log_lines, with_ts=False)
            return True

        work_df = cg_df.copy()
        if status_col:
            work_df["_status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)

        # Get WoW deltas from Historical Cell Status (simplified approach)
        wow_by_cell = {}
        if hist_df is not None and not hist_df.empty:
            # Parse snapshot dates
            lk = {str(c).strip().lower(): c for c in hist_df.columns}
            snap_c = lk.get("snapshot date") or lk.get("snapshot")
            cell_c = lk.get("cell")

            if snap_c and cell_c:
                hist_df["_snap_parsed"] = pd.to_datetime(hist_df[snap_c], errors="coerce")
                valid_hist = hist_df[hist_df["_snap_parsed"].notna()].copy()

                if not valid_hist.empty:
                    # Get unique dates sorted descending
                    all_dates = sorted(valid_hist["_snap_parsed"].dt.date.unique(), reverse=True)

                    if len(all_dates) >= 2:
                        snap_curr, snap_prev = all_dates[0], all_dates[1]
                        valid_hist["_d"] = valid_hist["_snap_parsed"].dt.date

                        # Calculate deltas for each cell
                        for cell_name in valid_hist[cell_c].dropna().unique():
                            cell_s = str(cell_name).strip()
                            if not cell_s or cell_s.lower() in ("all", "archive"):
                                continue

                            curr_rows = valid_hist[(valid_hist["_d"] == snap_curr) & (valid_hist[cell_c] == cell_name)]
                            prev_rows = valid_hist[(valid_hist["_d"] == snap_prev) & (valid_hist[cell_c] == cell_name)]

                            def _sum_col(df, *names):
                                for n in names:
                                    col = lk.get(n.lower())
                                    if col and col in df.columns:
                                        return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
                                return 0

                            if not curr_rows.empty and not prev_rows.empty:
                                d_new = _sum_col(curr_rows, "new") - _sum_col(prev_rows, "new")
                                d_reg = _sum_col(curr_rows, "regular") - _sum_col(prev_rows, "regular")
                                d_irr = _sum_col(curr_rows, "irregular") - _sum_col(prev_rows, "irregular")
                                d_fu = _sum_col(curr_rows, "follow up", "follow_up") - _sum_col(prev_rows, "follow up", "follow_up")
                                wow_by_cell[cell_s.lower()] = (d_new, d_reg, d_irr, d_fu)

                        # Calculate "All" deltas (sum across all cells)
                        curr_all = valid_hist[valid_hist["_d"] == snap_curr]
                        prev_all = valid_hist[valid_hist["_d"] == snap_prev]
                        if not curr_all.empty and not prev_all.empty:
                            d_new = _sum_col(curr_all, "new") - _sum_col(prev_all, "new")
                            d_reg = _sum_col(curr_all, "regular") - _sum_col(prev_all, "regular")
                            d_irr = _sum_col(curr_all, "irregular") - _sum_col(prev_all, "irregular")
                            d_fu = _sum_col(curr_all, "follow up", "follow_up") - _sum_col(prev_all, "follow up", "follow_up")
                            wow_by_cell["all"] = (d_new, d_reg, d_irr, d_fu)

        # Calculate counts per cell from CG Combined (live data)
        cell_rows = []
        all_counts = {"new": 0, "regular": 0, "irregular": 0, "follow_up": 0, "red": 0, "graduated": 0}

        for cell_name, group in work_df.groupby(cg_cell_col):
            cell_s = str(cell_name).strip()
            if not cell_s or cell_s.lower() in ("all", "archive"):
                continue

            if status_col:
                new_c = len(group[group["_status_type"] == "New"])
                reg_c = len(group[group["_status_type"] == "Regular"])
                irr_c = len(group[group["_status_type"] == "Irregular"])
                fu_c = len(group[group["_status_type"] == "Follow Up"])
                red_c = len(group[group["_status_type"] == "Red"])
                grad_c = len(group[group["_status_type"] == "Graduated"])
            else:
                n = len(group)
                new_c = max(1, int(n * 0.20))
                reg_c = max(1, int(n * 0.40))
                irr_c = max(1, int(n * 0.20))
                fu_c = max(1, int(n * 0.10))
                red_c = max(1, int(n * 0.05))
                grad_c = max(0, n - new_c - reg_c - irr_c - fu_c - red_c)

            all_counts["new"] += new_c
            all_counts["regular"] += reg_c
            all_counts["irregular"] += irr_c
            all_counts["follow_up"] += fu_c
            all_counts["red"] += red_c
            all_counts["graduated"] += grad_c

            deltas = wow_by_cell.get(cell_s.lower(), (0, 0, 0, 0))
            zone = cell_to_zone_map.get(cell_s.lower(), "")

            cell_rows.append(build_cell_health_row(
                cell_name=cell_s,
                zone=zone,
                new_count=new_c,
                regular_count=reg_c,
                irregular_count=irr_c,
                follow_up_count=fu_c,
                red_count=red_c,
                graduated_count=grad_c,
                delta_new=deltas[0],
                delta_regular=deltas[1],
                delta_irregular=deltas[2],
                delta_follow_up=deltas[3],
            ))

        # Build "All" row
        all_deltas = wow_by_cell.get("all", (0, 0, 0, 0))
        all_row = build_cell_health_row(
            cell_name="All",
            zone="PSQ",
            new_count=all_counts["new"],
            regular_count=all_counts["regular"],
            irregular_count=all_counts["irregular"],
            follow_up_count=all_counts["follow_up"],
            red_count=all_counts["red"],
            graduated_count=all_counts["graduated"],
            delta_new=all_deltas[0],
            delta_regular=all_deltas[1],
            delta_irregular=all_deltas[2],
            delta_follow_up=all_deltas[3],
        )

        # Build and store cache payload
        cell_health_data = {
            "snapshot_date": date.today().isoformat(),
            "all_row": all_row,
            "cell_rows": cell_rows,
            "source": "CG Combined + Historical Cell Status",
        }

        if store_cell_health_in_redis(redis_client, cell_health_data):
            _emit("  Cell health data cached", log_lines, with_ts=False)
        else:
            _emit("  Cell health: failed to store in Redis", log_lines, with_ts=False)

        return True
    except Exception as e:
        _emit(f"Error: Cell health cache refresh failed ({e})", log_lines, err=True, with_ts=False)
        return False


def _resolve_chatbot_sheet_id(log_lines: list[str] | None = None) -> str:
    sheet_id = os.getenv("CHATBOT_SHEET_ID", "").strip()
    if not sheet_id:
        try:
            import streamlit as st
            sheet_id = (st.secrets.get("CHATBOT_SHEET_ID") or "").strip()
        except Exception:
            pass
    return sheet_id


def _ensure_chatbot_log_worksheet(spreadsheet):
    headers = ["Date", "Time", "User Name", "Email", "Cell", "Question", "Answer", "Tokens"]
    try:
        ws = spreadsheet.worksheet("Chatbot Logs")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="Chatbot Logs", rows=5000, cols=len(headers))
        ws.append_row(headers)
        return ws
    if not ws.row_values(1):
        ws.append_row(headers)
    return ws


def _ensure_change_req_worksheet(spreadsheet):
    headers = ["Date", "Time", "Requested By", "Member", "Cell", "Field", "Current Value", "New Value", "Reason", "Status"]
    try:
        ws = spreadsheet.worksheet("Change Requests")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="Change Requests", rows=1000, cols=len(headers))
        ws.append_row(headers)
        return ws
    if not ws.row_values(1):
        ws.append_row(headers)
    return ws


def _sync_chatbot_to_sheets(
    gsheet_client,
    chatbot_sheet_id: str,
    log_lines: list[str] | None,
) -> tuple[bool, str]:
    if not _chatbot_redis_available:
        return False, "chatbot_redis module not found — check CHATBOT folder path."

    rc = get_chatbot_redis_client()
    if not rc:
        return False, "Chatbot Upstash not configured (UPSTASH_CHATBOT_REST_URL / UPSTASH_CHATBOT_REST_TOKEN missing)."

    today_str = get_today_myt_date()

    try:
        spreadsheet = gsheet_client.open_by_key(chatbot_sheet_id)
    except Exception as e:
        return False, f"Could not open chatbot sheet: {e}"

    total = 0

    logs = get_unsynced_logs(rc, today_str)
    if logs:
        ws = _ensure_chatbot_log_worksheet(spreadsheet)
        rows = [[
            e.get("date", ""), e.get("timestamp", ""),
            e.get("user_name", ""), e.get("email", ""), e.get("cell", ""),
            e.get("question", ""), e.get("answer", ""), e.get("tokens_used", 0),
        ] for e in logs]
        ws.append_rows(rows)
        from datetime import date as _date, timedelta as _td
        yesterday = (_date.fromisoformat(today_str) - _td(days=1)).isoformat()
        mark_synced(rc, yesterday)
        total += len(logs)
        _emit(f"  Chat logs: +{len(logs)} rows", log_lines, with_ts=False)
    else:
        _emit("  Chat logs: nothing to sync", log_lines, with_ts=False)

    reqs = get_unsynced_change_requests(rc, today_str)
    if reqs:
        ws2 = _ensure_change_req_worksheet(spreadsheet)
        rows2 = [[
            e.get("date", ""), e.get("timestamp", ""),
            e.get("requester", ""), e.get("member_name", ""), e.get("member_cell", ""),
            e.get("field", ""), e.get("current_value", ""), e.get("new_value", ""),
            e.get("reason", ""), e.get("status", "Pending"),
        ] for e in reqs]
        ws2.append_rows(rows2)
        from datetime import date as _date, timedelta as _td
        yesterday = (_date.fromisoformat(today_str) - _td(days=1)).isoformat()
        mark_change_requests_synced(rc, yesterday)
        total += len(reqs)
        _emit(f"  Change requests: +{len(reqs)} rows", log_lines, with_ts=False)
    else:
        _emit("  Change requests: nothing to sync", log_lines, with_ts=False)

    if total == 0:
        return True, "nothing to sync"
    return True, f"+{total} rows synced to sheet"


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
    _progress_set(progress_bar, 0.40, "Clearing Upstash caches (options, attendance, ministry)…")
    cache_ok = _clear_full_resync_redis_keys(redis_client, today_myt, log_lines)
    if cache_ok:
        _emit("  Cache cleared", log_lines, with_ts=False)

    _emit("", log_lines, with_ts=False)
    _emit("Syncing NWST Health data...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.50, "Syncing NWST Health (CG Combined, Ministries, Attendance)…")
    _refresh_nwst_health_data(redis_client, client, log_lines)

    _emit("", log_lines, with_ts=False)
    _emit("Refreshing theme...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.65, "Refreshing Theme Override snapshot…")
    theme_ok = _refresh_theme_override_shared(redis_client, client, sheet_id, log_lines)
    if theme_ok:
        _emit("  Theme updated", log_lines, with_ts=False)

    _emit("", log_lines, with_ts=False)
    _emit("Refreshing birthdays...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.75, "Refreshing birthday data…")
    _refresh_birthdays_cache(redis_client, client, log_lines)

    _emit("", log_lines, with_ts=False)
    _emit("Refreshing cell health...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.88, "Refreshing cell health data (NWST Health)…")
    _refresh_cell_health_cache(redis_client, client, log_lines)

    _emit("", log_lines, with_ts=False)
    _emit("Syncing chatbot logs...", log_lines, with_ts=False)
    _progress_set(progress_bar, 0.94, "Syncing chatbot logs and change requests…")
    chatbot_sheet_id = _resolve_chatbot_sheet_id(log_lines)
    if chatbot_sheet_id:
        _ok_cb, _msg_cb = _sync_chatbot_to_sheets(client, chatbot_sheet_id, log_lines)
        if not _ok_cb:
            _emit(f"  Chatbot sync skipped: {_msg_cb}", log_lines, with_ts=False)
    else:
        _emit("  Chatbot sync skipped: CHATBOT_SHEET_ID not set", log_lines, with_ts=False)

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
