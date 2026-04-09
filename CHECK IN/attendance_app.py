import os
import re
import json
import html
import importlib.util
import sys
from pathlib import Path
from collections import defaultdict

_CHECK_IN_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _CHECK_IN_DIR.parent
# CHECK IN must precede repo root so `weekly_email_report` loads from this folder,
# not a duplicate or stub at repo root (common on Streamlit Cloud / monorepos).
for _p in (_REPO_ROOT, _CHECK_IN_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
from nwst_shared.paths import resolved_nwst_accent_config_path
from nwst_shared.nwst_cell_health_report import (
    load_cg_combined_df,
    nwst_health_sheet_id,
    _resolve_cg_name_cell_columns,
)
from nwst_shared.nwst_daily_palette import (
    generate_colors_for_date,
    normalize_primary_hex as _normalize_primary_hex,
    theme_from_primary_hex,
)

import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta, timezone
import pandas as pd
import plotly.express as px
import hashlib
import colorsys

# Upstash Redis for caching (reduces Google Sheets API calls)
try:
    from upstash_redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Load environment variables
load_dotenv()

# Configuration - check .env first, then Streamlit secrets
SHEET_ID = os.getenv("ATTENDANCE_SHEET_ID", "")
if not SHEET_ID:
    try:
        if hasattr(st, 'secrets') and 'ATTENDANCE_SHEET_ID' in st.secrets:
            SHEET_ID = st.secrets["ATTENDANCE_SHEET_ID"]
    except FileNotFoundError:
        pass  # No secrets.toml file, continue with empty SHEET_ID
OPTIONS_TAB_NAME = "Options"  # Tab name where options are stored
ATTENDANCE_TAB_NAME = "Attendance"  # Tab name where attendance is recorded
LEADERS_ATTENDANCE_TAB_NAME = "Leaders Attendance"  # Tab name for leaders discipleship check-in
KEY_VALUES_TAB_NAME = "Key Values"  # Tab name for cell-to-zone mapping
MINISTRY_OPTIONS_TAB_NAME = "Options - Ministry"  # Tab name for ministry options
MINISTRY_ATTENDANCE_TAB_NAME = "Ministry Attendance"  # Tab name for ministry check-in
FORM_RESPONSES_TAB_NAME = "Form Responses 1"  # Tab name for newcomer form responses (P=Area of residence, Q=Status)
MINISTRY_LIST = ["Worship", "Hype", "VS", "Frontlines"]  # Available ministries

# One-off accent: optional tab **Theme Override** on this spreadsheet (ATTENDANCE_SHEET_ID),
# plus nwst_shared/nwst_accent_overrides.json at repo root — see nwst_accent_gsheet.py there.
# Optional: ATTENDANCE_ACCENT_OVERRIDE_DATE + ATTENDANCE_ACCENT_OVERRIDE_HEX env or Streamlit secrets.

# Redis cache configuration
REDIS_CACHE_TTL = 86400  # 24 hours in seconds (cache resets daily via key)
REDIS_HISTORICAL_TTL = 86400  # 24 hours for historical data (past data doesn't change)
REDIS_OPTIONS_KEY = "attendance:options"
REDIS_ATTENDANCE_KEY_PREFIX = "attendance:data:"  # Will be suffixed with date and tab name
REDIS_HISTORICAL_KEY_PREFIX = "attendance:historical:"  # For historical date queries
REDIS_ZONE_MAPPING_KEY = "attendance:zone_mapping"
REDIS_NEWCOMERS_KEY_PREFIX = "attendance:newcomers:"  # Will be suffixed with date
REDIS_BIRTHDAYS_KEY = "attendance:birthdays_data"  # CG Combined cached for birthday display
# Rows not yet appended to Google Sheets (flushed via admin flush-pending job / tooling)
REDIS_PENDING_ATTENDANCE_PREFIX = "attendance:pending_rows:"


def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")

def get_now_myt():
    """Get current datetime in MYT timezone"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt)


def _with_checkin_progress(caption: str, fn):
    """Show a simple ``st.progress`` (themed via global CSS) while ``fn`` runs; slot cleared when done."""
    slot = st.empty()
    with slot.container():
        try:
            bar = st.progress(0, text=caption)
        except TypeError:
            st.caption(caption)
            bar = st.progress(0)
    try:
        return fn()
    finally:
        try:
            bar.progress(1.0)
        except Exception:
            pass
        slot.empty()


@st.cache_resource
def get_redis_client():
    """Initialize Upstash Redis client - cached as resource to reuse connection"""
    if not REDIS_AVAILABLE:
        return None

    # Try environment variables first, then Streamlit secrets
    redis_url = os.getenv("UPSTASH_REDIS_REST_URL", "")
    redis_token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

    if not redis_url or not redis_token:
        try:
            if hasattr(st, 'secrets'):
                redis_url = st.secrets.get("UPSTASH_REDIS_REST_URL", "")
                redis_token = st.secrets.get("UPSTASH_REDIS_REST_TOKEN", "")
        except:
            pass

    if redis_url and redis_token:
        try:
            return Redis(url=redis_url, token=redis_token)
        except Exception as e:
            st.warning(f"Redis connection failed: {e}. Falling back to Google Sheets.")
            return None
    return None

def clear_redis_cache_for_today(tab_name=None):
    """Clear Redis cache for today's attendance data. Used for manual refresh."""
    redis_client = get_redis_client()
    if not redis_client:
        return

    today_myt = get_today_myt_date()
    try:
        if tab_name:
            # Clear specific tab
            redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
            redis_client.delete(redis_key)
        else:
            # Clear all attendance tabs for today
            for tn in [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME]:
                redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tn}"
                redis_client.delete(redis_key)
        # Also clear options and zone mapping
        redis_client.delete(REDIS_OPTIONS_KEY)
        redis_client.delete(REDIS_ZONE_MAPPING_KEY)
    except Exception:
        pass


def _refresh_theme_override_redis_after_resync():
    """CHECK IN trigger: after Update names, snapshot Theme Override into shared Upstash (same key NWST uses)."""
    if not (SHEET_ID or "").strip():
        return
    redis_client = get_redis_client()
    if not redis_client:
        return
    client = get_gsheet_client()
    if not client:
        return
    cfg = resolved_nwst_accent_config_path()
    if cfg is None:
        return
    spec = importlib.util.spec_from_file_location("_nwst_accent_cfg_refresh", cfg)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        mod.refresh_theme_override_shared_cache(redis_client, client, SHEET_ID)
    except Exception:
        pass


def perform_hard_sheet_resync(mode="congregation"):
    """Flush pending check-ins to Google Sheets, then clear Redis + Streamlit caches.

    Use after edits to roster / Options / ministry tabs. Hits more API calls than Refresh;
    avoid repeated clicks to reduce quota timeouts.

    mode: \"congregation\" — main & leaders attendance, options, zones, newcomers.
          \"ministry\" — ministry options & attendance, main options (roles), newcomers.
    """
    gclient = get_gsheet_client()
    flush_tabs = (
        [MINISTRY_ATTENDANCE_TAB_NAME]
        if mode == "ministry"
        else [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME]
    )
    if not gclient:
        rc = get_redis_client()
        today_chk = get_today_myt_date()
        if rc:
            for t in flush_tabs:
                pk = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_chk}:{t}"
                if rc.lrange(pk, 0, 0):
                    st.error(
                        "Google Sheets is not connected, but there are check-ins waiting to be written. "
                        "Fix credentials, then try again."
                    )
                    return
    else:
        ok, err = flush_pending_attendance_for_tabs(gclient, flush_tabs)
        if not ok:
            st.error(f"Could not write pending check-ins to Google Sheets: {err}")
            return

    st.session_state.refresh_counter = st.session_state.get("refresh_counter", 0) + 1
    st.session_state.last_refresh_time = get_now_myt()
    get_newcomers_count.clear()
    get_today_attendance_data.clear()
    today_myt = get_today_myt_date()
    redis_client = get_redis_client()

    if mode == "ministry":
        get_ministry_options_from_sheet.clear()
        get_options_from_sheet.clear()
        if redis_client:
            try:
                redis_client.delete(REDIS_OPTIONS_KEY)
                for ministry in MINISTRY_LIST:
                    redis_client.delete(f"attendance:ministry_options:{ministry}")
                redis_client.delete("attendance:ministry_options:all")
                redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{MINISTRY_ATTENDANCE_TAB_NAME}")
                redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
            except Exception:
                pass
        _refresh_theme_override_redis_after_resync()
        return

    # congregation (default)
    get_options_from_sheet.clear()
    get_cell_to_zone_mapping.clear()
    if redis_client:
        try:
            redis_client.delete(REDIS_OPTIONS_KEY)
            redis_client.delete(REDIS_ZONE_MAPPING_KEY)
            redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{ATTENDANCE_TAB_NAME}")
            redis_client.delete(f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{LEADERS_ATTENDANCE_TAB_NAME}")
            redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
        except Exception:
            pass
    _refresh_theme_override_redis_after_resync()


def get_gsheet_client():
    """Connect to Google Sheets - works with both local files and Streamlit secrets"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = None
        
        # Try credentials.json file first (for local development)
        # Check multiple possible locations
        possible_paths = [
            'credentials.json',  # Same folder as app
            'CHECK IN/credentials.json',  # In CHECK IN folder
            '../credentials.json',  # Parent folder
            os.path.join(os.path.dirname(__file__), 'credentials.json'),  # Same dir as script
        ]
        
        credentials_path = None
        for path in possible_paths:
            if os.path.exists(path):
                credentials_path = path
                break
        
        if credentials_path:
            try:
                creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
            except Exception as e:
                st.error(f"❌ Error reading credentials.json from {credentials_path}: {str(e)}")
                return None
        # Try Streamlit secrets (for cloud deployment)
        elif hasattr(st, 'secrets'):
            try:
                if 'gcp_service_account' in st.secrets:
                    creds_dict = dict(st.secrets['gcp_service_account'])
                    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            except Exception as e:
                # If secrets exist but can't be read, fall through to error
                pass
        
        if creds is None:
            st.error("❌ No Google Sheets credentials found.")
            _check_in_dir = Path(__file__).resolve().parent
            _repo_root = _check_in_dir.parent
            checked_locations = [
                'credentials.json',
                'CHECK IN/credentials.json',
                '../credentials.json',
            ]
            location_status = '\n'.join([f'- {path} {"✅" if os.path.exists(path) else "❌"}' for path in checked_locations])
            st.info(f"""
            **For local development, you need:**
            1. Download `credentials.json` from Google Cloud Console
            2. Place it in one of these locations:
               - `{_check_in_dir / "credentials.json"}` ✅ (preferred)
               - `{_repo_root / "credentials.json"}`
            
            **Checked locations:**
            {location_status}
            
            **See the setup guide for detailed instructions.**
            """)
            return None
            
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Could not connect to Google Sheets: {str(e)}")
        return None


def _ensure_attendance_worksheet(spreadsheet, tab_name):
    """Open or create the attendance worksheet; ensure header row exists."""
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


def flush_pending_attendance_for_tabs(client, tab_names):
    """Append queued check-in rows from Upstash to Google Sheets, then clear each pending list.

    Returns:
        tuple: (ok: bool, error_message: str)
    """
    if not client or not (SHEET_ID or "").strip():
        return False, "Google Sheets client or ATTENDANCE_SHEET_ID not available."
    redis_client = get_redis_client()
    if not redis_client:
        return True, "Upstash not configured — there is no pending queue (check-ins write straight to the sheet)."
    today_myt = get_today_myt_date()
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        total_rows = 0
        parts = []
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
            n = len(rows)
            total_rows += n
            parts.append(f"{tab_name}: {n}")
        if total_rows == 0:
            return True, f"No pending rows for {today_myt} (queues empty or already flushed)."
        return True, f"Wrote {total_rows} pending row(s) to Google Sheets — " + "; ".join(parts)
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return False, "API quota exceeded. Try again in a moment."
        return False, str(e)
    except Exception as e:
        return False, str(e)


def _is_email_format(value):
    """Return True if value looks like an email address (treat role as blank)."""
    if not value or not isinstance(value, str):
        return False
    v = value.strip()
    return "@" in v and "." in v


@st.cache_data(ttl=30)  # Local cache for 30 seconds - allows more frequent Upstash reads
def get_options_from_sheet(_client, sheet_id):
    """Read options from Column C and role from Column D of the Options tab in Google Sheets.
    Uses Redis cache to minimize API calls.
    Returns (options, name_to_role, error_msg). name_to_role maps member name -> role (email format treated as blank)."""

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_OPTIONS_KEY)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("options"), data.get("name_to_role", {}), None
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the Options worksheet
        try:
            options_sheet = spreadsheet.worksheet(OPTIONS_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return None, {}, f"❌ Tab '{OPTIONS_TAB_NAME}' not found. Please create it in your Google Sheet."

        # Read column C (names) and column D (roles)
        column_c_values = options_sheet.col_values(3)  # Column C (1-indexed)
        column_d_values = options_sheet.col_values(4)  # Column D (1-indexed) - role

        if not column_c_values:
            return {}, {}, "⚠️ Column C in Options sheet is empty. Please add options to column C."

        # Get the header from first row (C1)
        header = column_c_values[0].strip() if column_c_values[0] else "Name"

        # Build options and name_to_role from row 2 onwards
        option_values = []
        name_to_role = {}
        for i, value in enumerate(column_c_values[1:]):  # Skip first row (header)
            value = value.strip()
            if value:  # Only add non-empty values
                option_values.append(value)
                name, _ = parse_name_cell_group(value)
                if name:
                    role_raw = column_d_values[i + 1].strip() if i + 1 < len(column_d_values) else ""
                    role = "" if _is_email_format(role_raw) else (role_raw or "")
                    if role:
                        name_to_role[name] = role

        if not option_values:
            return {}, {}, "⚠️ No options found in column C (starting from row 2)."

        # Return single option type with all column C values
        options = {header: option_values}

        # Store in Redis cache
        if redis_client:
            try:
                redis_client.set(REDIS_OPTIONS_KEY, json.dumps({"options": options, "name_to_role": name_to_role}), ex=REDIS_CACHE_TTL)
            except Exception:
                pass  # Redis write failed, continue anyway

        return options, name_to_role, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return None, {}, "⚠️ API quota exceeded. Please wait a moment and refresh the page."
        return None, {}, f"❌ Error reading options from column C: {str(e)}"
    except Exception as e:
        return None, {}, f"❌ Error reading options from column C: {str(e)}"

@st.cache_data(ttl=30)  # Local cache for 30 seconds - allows more frequent Upstash reads
def get_cell_to_zone_mapping(_client, sheet_id):
    """Read cell-to-zone mapping from Key Values tab in Google Sheets.
    Column A = Cell Names, Column C = Zones. Uses Redis cache."""

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_ZONE_MAPPING_KEY)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("mapping", {}), None
        except Exception:
            pass  # Redis failed, fall back to Sheets

    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the Key Values worksheet
        try:
            key_values_sheet = spreadsheet.worksheet(KEY_VALUES_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return {}, f"Tab '{KEY_VALUES_TAB_NAME}' not found."

        # Get all values from the sheet
        all_values = key_values_sheet.get_all_values()

        if len(all_values) <= 1:  # Only header row or empty
            return {}, "Key Values sheet is empty or has only headers."

        # Build mapping from Cell Name to Zone (skip header row)
        # Column A (index 0) = Cell Names, Column C (index 2) = Zones
        cell_to_zone = {}
        for row in all_values[1:]:  # Skip header
            if len(row) >= 3:
                cell_name = row[0].strip()  # Column A: "Anchor Street"
                zone = row[2].strip()        # Column C: "Syd"
                if cell_name and zone:
                    cell_to_zone[cell_name.lower()] = zone

        # Store in Redis cache
        if redis_client:
            try:
                redis_client.set(REDIS_ZONE_MAPPING_KEY, json.dumps({"mapping": cell_to_zone}), ex=REDIS_CACHE_TTL)
            except Exception:
                pass

        return cell_to_zone, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return {}, "API quota exceeded."
        return {}, f"Error reading Key Values: {str(e)}"
    except Exception as e:
        return {}, f"Error reading Key Values: {str(e)}"

@st.cache_data(ttl=30)  # Local cache for 30 seconds - allows more frequent Upstash reads
def get_ministry_options_from_sheet(_client, sheet_id, ministry_filter=None):
    """Read options from the Options - Ministry tab in Google Sheets.
    Column A = Name, Column B = Department (Ministry: Dept format), Column C = Options

    Args:
        _client: Google Sheets client
        sheet_id: Sheet ID
        ministry_filter: Optional ministry name to filter by (e.g., "Worship", "Hype")

    Returns:
        tuple: (options dict, error message or None)
    """
    redis_key = f"attendance:ministry_options:{ministry_filter or 'all'}"

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(redis_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("options"), None
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the Ministry Options worksheet
        try:
            ministry_sheet = spreadsheet.worksheet(MINISTRY_OPTIONS_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return None, f"❌ Tab '{MINISTRY_OPTIONS_TAB_NAME}' not found. Please create it in your Google Sheet."

        # Get all values from the sheet
        all_values = ministry_sheet.get_all_values()

        if len(all_values) <= 1:  # Only header row or empty
            return {}, "⚠️ Ministry options sheet is empty."

        # Get all options from row 2 onwards (skip header row)
        # Format: Name (A), Department (B), Options (C)
        # Department format is "Ministry: Dept" e.g., "Worship: LCD"
        option_values = []
        for row in all_values[1:]:  # Skip header
            if len(row) < 3:
                continue

            name = row[0].strip() if row[0] else ""
            department = row[1].strip() if row[1] else ""
            option = row[2].strip() if row[2] else ""

            if not option:
                continue

            # Filter by ministry if specified
            if ministry_filter:
                # Department format is "Ministry: Dept" or just "Ministry"
                if ":" in department:
                    ministry_part = department.split(":")[0].strip()
                else:
                    ministry_part = department  # No colon means department IS the ministry
                if ministry_part.lower() != ministry_filter.lower():
                    continue

            option_values.append(option)

        if not option_values:
            filter_msg = f" for ministry '{ministry_filter}'" if ministry_filter else ""
            return {}, f"⚠️ No options found{filter_msg}."

        # Return single option type with all values
        options = {"Name": option_values}

        # Store in Redis cache
        if redis_client:
            try:
                redis_client.set(redis_key, json.dumps({"options": options}), ex=REDIS_CACHE_TTL)
            except Exception:
                pass  # Redis write failed, continue anyway

        return options, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return None, "⚠️ API quota exceeded. Please wait a moment and refresh the page."
        return None, f"❌ Error reading ministry options: {str(e)}"
    except Exception as e:
        return None, f"❌ Error reading ministry options: {str(e)}"

def get_ministry_members_by_department(_client, sheet_id, ministry_filter=None):
    """Get ministry members grouped by department.

    Args:
        _client: Google Sheets client
        sheet_id: Sheet ID
        ministry_filter: Optional ministry name to filter by

    Returns:
        dict: Department -> list of member names
    """
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        ministry_sheet = spreadsheet.worksheet(MINISTRY_OPTIONS_TAB_NAME)
        all_values = ministry_sheet.get_all_values()

        if len(all_values) <= 1:
            return {}

        members_by_dept = {}
        for row in all_values[1:]:
            if len(row) < 3:
                continue

            name = row[0].strip() if row[0] else ""
            department = row[1].strip() if row[1] else ""

            if not name or not department:
                continue

            # Filter by ministry if specified
            if ministry_filter:
                if ":" in department:
                    ministry_part = department.split(":")[0].strip()
                else:
                    ministry_part = department  # No colon means department IS the ministry
                if ministry_part.lower() != ministry_filter.lower():
                    continue

            if department not in members_by_dept:
                members_by_dept[department] = []
            members_by_dept[department].append(name)

        return members_by_dept
    except Exception:
        return {}

_nwst_accent_cfg_mod = None


def _accent_overrides_from_project_config():
    """Load shared ``nwst_accent_config.py`` (see ``nwst_shared.paths``)."""
    global _nwst_accent_cfg_mod
    if _nwst_accent_cfg_mod is None:
        cfg = resolved_nwst_accent_config_path()
        if cfg is not None:
            spec = importlib.util.spec_from_file_location("_nwst_accent_cfg", cfg)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _nwst_accent_cfg_mod = mod
    if _nwst_accent_cfg_mod is None:
        return {}
    return _nwst_accent_cfg_mod.get_accent_override_by_date()


def _theme_overrides_from_redis():
    """Theme Override rows from Upstash (refreshed on Update names / NWST sheet sync)."""
    redis_client = get_redis_client()
    if not _nwst_accent_cfg_mod:
        _accent_overrides_from_project_config()
    if _nwst_accent_cfg_mod:
        try:
            return _nwst_accent_cfg_mod.read_theme_override_from_redis(redis_client)
        except Exception:
            return {}
    return {}


def resolve_theme_override_row_for_today(from_sheet=None):
    """Latest-dated row from the Theme Override Upstash snapshot (+ JSON merge for that date).

    If the snapshot is empty, returns ``{}`` so callers use ``banner.gif`` and generated colors.
    """
    from_file = _accent_overrides_from_project_config()
    if from_sheet is None:
        from_sheet = _theme_overrides_from_redis()
    if not from_sheet:
        return {}
    if _nwst_accent_cfg_mod:
        row = _nwst_accent_cfg_mod.resolve_latest_cached_theme_row(from_file, from_sheet)
    else:
        latest = max(from_sheet.keys())
        keys = set(from_file) | set(from_sheet)
        merged = {
            k: {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
            for k in keys
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
                if hasattr(st, "secrets"):
                    sd = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_DATE", "")).strip()
                    sh = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_HEX", "")).strip()
                    if sd == today and sh:
                        row["primary"] = sh.strip()
            except Exception:
                pass
    return row


def generate_daily_colors():
    """Daily MYT generated palette unless Theme Override supplies primary/banner (Upstash / JSON / env)."""
    today_str = get_today_myt_date()
    from_sheet = _theme_overrides_from_redis()
    row = resolve_theme_override_row_for_today(from_sheet=from_sheet)
    hex_override = row.get("primary")
    base = None
    if hex_override:
        pn = _normalize_primary_hex(hex_override)
        if pn:
            base = theme_from_primary_hex(pn)
    if base is None:
        base = generate_colors_for_date(today_str)
    b_raw = row.get("banner")
    if b_raw:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename(b_raw)
            if safe:
                base = {**base, "banner": safe}
    if not from_sheet:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename("banner.gif")
            if safe:
                base = {**base, "banner": safe}
    return base

def generate_daily_colors_legacy():
    """Legacy version - kept for reference"""
    today = get_today_myt_date()
    # Use date as seed for consistent colors throughout the day
    seed = int(hashlib.md5(today.encode()).hexdigest(), 16)
    
    # Generate vibrant colors using the seed
    import random
    random.seed(seed)
    
    # Generate a primary accent color (bright, vibrant)
    hue = random.random()  # 0.0 to 1.0
    saturation = random.uniform(0.7, 1.0)
    lightness = random.uniform(0.45, 0.65)
    
    # Convert HSL to RGB then to hex
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0]*255), 
        int(rgb[1]*255), 
        int(rgb[2]*255)
    )
    
    # Generate a lighter variant for accents
    rgb_light = colorsys.hls_to_rgb(hue, min(lightness + 0.2, 0.9), saturation)
    light_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb_light[0]*255), 
        int(rgb_light[1]*255), 
        int(rgb_light[2]*255)
    )
    
    return {
        'primary': primary_color,
        'light': light_color,
        'background': '#000000',  # Keep black background for edgy style
        'accent': primary_color
    }

def format_name_badge(name, role, badge_class="name-badge"):
    """Format a name badge with optional role (below name, formatted as 'N. Label:')."""
    if not role:
        role_html = ''
    else:
        # Format as "N. Label:" (e.g. "1. Co Leader" -> "1. Co Leader:")
        role_display = f"{role.rstrip(':')}:" if role.strip() else ""
        role_html = f'<span class="name-badge-role">{role_display}</span>' if role_display else ''
    return f'<span class="{badge_class}"><span class="name-badge-name">{name}</span>{role_html}</span>'


def _role_sort_key(role):
    """Extract sort key and display label from role (e.g. '1. Co Leader' -> (1, 'Co Leader:'))."""
    m = re.match(r'^(\d+)\.\s*(.+)$', role.strip())
    if m:
        return (int(m.group(1)), f"{m.group(2).strip()}:")
    return (999, f"{role.strip()}:")  # No number prefix: sort last


def build_role_grouped_badges(all_names, checked_in_set, name_to_role, badge_class_checked, badge_class_pending):
    """Build HTML with role rows: 'Role Label: tile | tile' and 'Remaining cell members: tile | tile'.
    Tiles show name only (no role inside)."""
    # Group names by role
    role_to_names = {}
    no_role_names = []
    for name in all_names:
        role = name_to_role.get(name, '')
        if role and role.strip():
            if role not in role_to_names:
                role_to_names[role] = []
            role_to_names[role].append(name)
        else:
            no_role_names.append(name)

    parts = []
    # Sort roles by number prefix (1, 2, 3...)
    for role in sorted(role_to_names.keys(), key=_role_sort_key):
        sort_key, role_label = _role_sort_key(role)
        names_in_role = role_to_names[role]
        checked = sorted([n for n in names_in_role if n in checked_in_set])
        pending = sorted([n for n in names_in_role if n not in checked_in_set])
        badges = ''.join([format_name_badge(n, '', badge_class_checked) for n in checked])
        badges += ''.join([format_name_badge(n, '', badge_class_pending) for n in pending])
        if badges:
            parts.append(f'<div class="role-row"><span class="role-label">{role_label}</span> {badges}</div>')

    # Remaining cell members (no role)
    if no_role_names:
        checked = sorted([n for n in no_role_names if n in checked_in_set])
        pending = sorted([n for n in no_role_names if n not in checked_in_set])
        badges = ''.join([format_name_badge(n, '', badge_class_checked) for n in checked])
        badges += ''.join([format_name_badge(n, '', badge_class_pending) for n in pending])
        if badges:
            parts.append(f'<div class="role-row"><span class="role-label">Remaining cell members:</span> {badges}</div>')

    return ''.join(parts)


def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)"""
    if not name_cell_group_str:
        return None, None
    
    # Split by " - " to separate name and cell group
    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    else:
        # If no " - " found, treat entire string as name, cell group as "Unknown"
        return parts[0].strip(), "Unknown"


def _valid_month_day(month: int, day: int) -> bool:
    """True if month/day is a valid calendar day (handles leap day against 2004)."""
    for y in (2004, 2005):
        try:
            datetime(y, month, day)
            return True
        except ValueError:
            continue
    return False


def _month_day_from_sheets_serial(n: float) -> tuple[int, int] | None:
    """Google Sheets / Excel-style serial (days after 1899-12-30) → (month, day)."""
    if n != n or n < 200 or n > 800000:  # NaN or implausible
        return None
    base = date(1899, 12, 30)
    try:
        d = base + timedelta(days=int(round(n)))
    except (OverflowError, ValueError):
        return None
    return (d.month, d.day)


def _parse_en_dd_mmm_yyyy(s: str) -> tuple[int, int] | None:
    """
    CG Combined column G style: ``09 Oct 2026`` (DD + space + 3-letter English month + space + YYYY).
    Locale-independent; year is ignored for recurring birthday.
    """
    mon_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    m = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", (s or "").strip())
    if not m:
        return None
    try:
        d = int(m.group(1))
    except ValueError:
        return None
    key = m.group(2).strip().lower()[:3]
    month = mon_map.get(key)
    if month is None or not _valid_month_day(month, d):
        return None
    return (month, d)


def _parse_birthday_month_day(val) -> tuple[int, int] | None:
    """Parse a sheet Birthday value to (month, day). Supports dates, serials, and common text formats."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, date):
        return (val.month, val.day)
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        md = _month_day_from_sheets_serial(float(val))
        if md:
            return md
    if hasattr(val, "month") and hasattr(val, "day") and not isinstance(val, str):
        try:
            return (int(val.month), int(val.day))
        except (TypeError, ValueError):
            pass
    s_raw = str(val).strip()
    if not s_raw:
        return None
    s = re.sub(r"[\u00a0\u202f]", " ", s_raw)
    s = re.sub(r"\s+", " ", s).strip()
    dd_mmm = _parse_en_dd_mmm_yyyy(s)
    if dd_mmm:
        return dd_mmm
    parts = re.split(r"[/.\-]", s)
    nums: list[int] = []
    for p in parts:
        p = p.strip()
        if not p or not p.isdigit():
            if nums:
                return None
            continue
        nums.append(int(p))
    if len(nums) == 2:
        a, b = nums
        if a > 12:
            month, day = b, a
        elif b > 12:
            month, day = a, b
        else:
            month, day = b, a
        if _valid_month_day(month, day):
            return (month, day)
    if len(nums) >= 3:
        a, b, c = nums[0], nums[1], nums[2]
        month, day = 0, 0
        if a > 1000:
            month, day = b, c
        elif c > 1000:
            if a > 12:
                month, day = b, a
            elif b > 12:
                month, day = a, b
            else:
                month, day = b, a
        if month > 0 and _valid_month_day(month, day):
            return (month, day)
    for fmt in (
        "%d/%m/%Y",
        "%d/%m/%y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.month, dt.day)
        except ValueError:
            continue
    try:
        md = _month_day_from_sheets_serial(float(s.replace(",", ".")))
        if md:
            return md
    except ValueError:
        pass
    return None


_BIRTHDAY_HEADER_MARKERS = (
    "birthday",
    "birth day",
    "date of birth",
    "birthdate",
    "dob",
    "b'day",
    "bday",
)


def _find_cg_birthday_column(cg_df: pd.DataFrame) -> str | None:
    for c in cg_df.columns:
        low = str(c).lower().strip()
        if any(m in low for m in _BIRTHDAY_HEADER_MARKERS):
            return c
    return None


def _birthday_md_to_date_in_window(
    month: int, day: int, center: date, delta_days: int
) -> date | None:
    md_to_date: dict[tuple[int, int], date] = {}
    d = center - timedelta(days=delta_days)
    end = center + timedelta(days=delta_days)
    while d <= end:
        md_to_date[(d.month, d.day)] = d
        d += timedelta(days=1)
    return md_to_date.get((month, day))


@st.cache_data(ttl=300, show_spinner=False)
def _cg_combined_df_for_birthdays(_health_sheet_id: str):
    """
    Load CG Combined from Upstash (populated by flush_pending sync), with Sheets fallback.
    @st.cache_data provides 5-min local cache on top of Upstash for same-instance users.
    """
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_BIRTHDAYS_KEY)
            if cached:
                raw = cached.decode() if isinstance(cached, bytes) else cached
                return pd.read_json(raw)
        except Exception:
            pass
    # Fallback: direct Sheets read (first load before any sync, or Upstash miss)
    return load_cg_combined_df(get_gsheet_client(), _health_sheet_id)


def _group_birthdays_near_date(
    cg: pd.DataFrame,
    bcol: str,
    ncol: str,
    ccol: str | None,
    center: date,
    delta_days: int,
) -> list[tuple[date, list[tuple[str, str]]]]:
    by_date: dict[date, list[tuple[str, str]]] = defaultdict(list)
    for _, row in cg.iterrows():
        md = _parse_birthday_month_day(row.get(bcol))
        if not md:
            continue
        m, d = md
        occ = _birthday_md_to_date_in_window(m, d, center, delta_days)
        if not occ:
            continue
        name = str(row.get(ncol) or "").strip()
        if not name:
            continue
        cell = str(row.get(ccol) or "").strip() if ccol else ""
        if not cell:
            cell = "—"
        by_date[occ].append((name, cell))

    out: list[tuple[date, list[tuple[str, str]]]] = []
    for dt in sorted(by_date.keys()):
        lines = sorted(by_date[dt], key=lambda t: (t[0].lower(), t[1].lower()))
        out.append((dt, lines))
    return out


def _chunk_birthday_days_into_cards(
    grouped: list[tuple[date, list[tuple[str, str]]]],
) -> list[list[tuple[date, list[tuple[str, str]]]]]:
    """Merge up to **two consecutive calendar days** (that have birthdays) into one horizontal card."""
    if not grouped:
        return []
    cards: list[list[tuple[date, list[tuple[str, str]]]]] = []
    i = 0
    n = len(grouped)
    while i < n:
        d0, p0 = grouped[i]
        chunk: list[tuple[date, list[tuple[str, str]]]] = [(d0, p0)]
        if i + 1 < n:
            d1, p1 = grouped[i + 1]
            if d1 == d0 + timedelta(days=1):
                chunk.append((d1, p1))
                i += 2
                cards.append(chunk)
                continue
        i += 1
        cards.append(chunk)
    return cards


def _hex_to_rgb_for_css(h: str) -> tuple[int, int, int]:
    try:
        hx = h.lstrip("#")
        if len(hx) == 6:
            return (int(hx[0:2], 16), int(hx[2:4], 16), int(hx[4:6], 16))
    except ValueError:
        pass
    return (91, 192, 235)


def _contrasting_gradient_rgb_stops(primary_hex: str, light_hex: str) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """
    Complementary-hue accent versus the daily theme (HLS hue + 0.5), keeping a strong + light leg
    so the 135° primary→light→primary card texture matches non-today cards.
    """
    rp, gp, bp = _hex_to_rgb_for_css(primary_hex)
    rl0, gl0, bl0 = _hex_to_rgb_for_css(light_hex)
    r = (rp + rl0) / 510.0
    g = (gp + gl0) / 510.0
    b = (bp + bl0) / 510.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    h2 = (h + 0.5) % 1.0
    if s < 0.08:
        s2 = 0.72
    else:
        s2 = min(1.0, s * 1.08)
    l2 = min(0.7, max(0.36, l))
    r2, g2, b2 = colorsys.hls_to_rgb(h2, l2, s2)
    s3 = max(0.12, s2 * 0.65)
    l3 = min(0.9, l2 + 0.2)
    r3, g3, b3 = colorsys.hls_to_rgb(h2, l3, s3)
    return (
        (int(r2 * 255), int(g2 * 255), int(b2 * 255)),
        (int(r3 * 255), int(g3 * 255), int(b3 * 255)),
    )


def _relative_luminance_srgb(rc: int, gc: int, bc: int) -> float:
    """WCAG relative luminance for sRGB 0–255 channels."""

    def _lin(c: int) -> float:
        x = c / 255.0
        return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4

    R, G, B = _lin(rc), _lin(gc), _lin(bc)
    return 0.2126 * R + 0.7152 * G + 0.0722 * B


def _hex_accent_readable_on_dark_card(ar: int, ag: int, ab: int) -> str:
    """
    Keep saturated hue for “today” / date labels but lift HLS lightness if the accent
    is too dark to read on charcoal + translucent gradient (≈ WCAG-minded).
    """
    lum = _relative_luminance_srgb(ar, ag, ab)
    if lum >= 0.58:
        return f"#{ar:02x}{ag:02x}{ab:02x}"
    r, g, b = ar / 255.0, ag / 255.0, ab / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    target = 0.62
    l = min(0.9, max(l, target - 0.15 * (1.0 - s)))
    if lum < 0.35:
        l = min(0.9, l + 0.18)
    s = max(s, 0.38)
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r2 * 255):02x}{int(g2 * 255):02x}{int(b2 * 255):02x}"


def _card_body_text_hex(theme_text: str) -> str:
    """Prefer theme body colour; fall back to light gray if theme text would be too dark on cards."""
    t = (theme_text or "").strip()
    if t.startswith("#") and len(t) >= 7:
        tr, tg, tb = _hex_to_rgb_for_css(t[:7])
        if _relative_luminance_srgb(tr, tg, tb) < 0.48:
            return "#e8eaed"
        return t[:7]
    return "#e8eaed"


def birthdays_notice_payload(
    health_sheet_id: str, center_myt_iso: str, delta_days: int = 5
) -> tuple[str, list[tuple[date, list[tuple[str, str]]]], str | None]:
    """
    (status, grouped, user_hint).

    status: ``ok`` | ``empty_window`` | ``load_failed`` | ``empty_sheet`` | ``no_birthday_col``
    | ``no_name_col`` | ``no_sid``
    """
    sid = (health_sheet_id or "").strip()
    if not sid:
        return "no_sid", [], "Configure **NWST_HEALTH_SHEET_ID** (or rely on the built-in default) for the Health workbook."
    cg = _cg_combined_df_for_birthdays(sid)
    if cg is None:
        return "load_failed", [], (
            "Could not read **CG Combined** from the NWST Health spreadsheet. "
            "Share that workbook with the **same Google service account** as Check In, then refresh."
        )
    if cg.empty:
        return "empty_sheet", [], "CG Combined is empty—NWST Health has no roster rows to read."
    bcol = _find_cg_birthday_column(cg)
    ncol, ccol = _resolve_cg_name_cell_columns(cg)
    if not bcol:
        return "no_birthday_col", [], "Add a column whose header mentions Birthday, DOB, or Date of Birth."
    if not ncol:
        return "no_name_col", [], "CG Combined needs a Name column so birthdays can be listed."
    try:
        center = datetime.strptime(center_myt_iso, "%Y-%m-%d").date()
    except ValueError:
        return "empty_window", [], None

    grouped = _group_birthdays_near_date(cg, bcol, ncol, ccol, center, delta_days)
    if not grouped:
        return "empty_window", [], None
    return "ok", grouped, None


def render_birthdays_notice_board(page_colors: dict) -> None:
    """Notice-board block: under banner, above instruction pill; uses CG Combined + NWST_HEALTH_SHEET_ID."""
    sid = (nwst_health_sheet_id() or "").strip()
    today_s = get_today_myt_date()
    status, grouped, hint = birthdays_notice_payload(sid, today_s, delta_days=5)

    if status in ("no_sid", "load_failed", "empty_sheet", "no_birthday_col", "no_name_col"):
        if hint:
            st.info(hint)
        return

    prim = page_colors.get("primary", "#5BC0EB")
    bg = page_colors.get("background", "#0b1020")
    text_main = page_colors.get("text", "#e8eaed")

    def _fmt_day(d: date) -> str:
        return f"{d.strftime('%a')}, {d.day} {d.strftime('%b')}"

    prim_e = html.escape(prim, quote=True)
    body_hex = _card_body_text_hex(text_main)
    text_e = html.escape(body_hex, quote=True)
    light = page_colors.get("light", prim)
    r, g, b = _hex_to_rgb_for_css(prim)
    rl, gl, bl = _hex_to_rgb_for_css(light)
    try:
        today_d = datetime.strptime(today_s, "%Y-%m-%d").date()
    except ValueError:
        today_d = date.today()

    (crx, cgy, cbz), (crlx, cgly, cblz) = _contrasting_gradient_rgb_stops(prim, light)
    cards_html: list[str] = []

    if grouped:
        for chunk in _chunk_birthday_days_into_cards(grouped):
            days_only = [d for d, _ in chunk]
            if len(chunk) == 1:
                card_title = html.escape(_fmt_day(days_only[0]), quote=True)
            else:
                t = f"{_fmt_day(days_only[0])} – {_fmt_day(days_only[1])}"
                card_title = html.escape(t, quote=True)

            has_today = any(dt == today_d for dt, _ in chunk)
            ar, ag, ab = (crx, cgy, cbz) if has_today else (r, g, b)
            arl, agl, abl = (crlx, cgly, cblz) if has_today else (rl, gl, bl)
            card_bg_layers = (
                f"linear-gradient(135deg, rgba({ar},{ag},{ab},0.48) 0%, rgba({arl},{agl},{abl},0.32) 50%, rgba({ar},{ag},{ab},0.42) 100%), "
                f"linear-gradient(180deg, #26262a 0%, #18181c 100%)"
            )

            txt_sh = "0 1px 3px rgba(0,0,0,0.75),0 0 1px rgba(0,0,0,0.55)"
            sub_today_e = html.escape(_hex_accent_readable_on_dark_card(ar, ag, ab), quote=True)
            sub_other_e = html.escape(_hex_accent_readable_on_dark_card(r, g, b), quote=True)

            body_parts: list[str] = []
            for dt, pairs in chunk:
                if len(chunk) > 1:
                    sub_l = html.escape(_fmt_day(dt), quote=True)
                    sub_col_e = sub_today_e if dt == today_d else sub_other_e
                    body_parts.append(
                        f'<div style="margin-top:0.55rem;font-family:Inter,sans-serif;font-size:0.72rem;'
                        f"font-weight:600;color:{sub_col_e};letter-spacing:0.02em;text-shadow:{txt_sh};\">{sub_l}</div>"
                    )
                for name, cell in pairs:
                    line = html.escape(f"{name} - {cell}", quote=True)
                    body_parts.append(
                        f'<div style="margin-top:0.35rem;font-family:Inter,sans-serif;font-size:0.8rem;'
                        f"line-height:1.35;color:{text_e};text-shadow:{txt_sh};\">{line}</div>"
                    )

            n_b = sum(len(p) for _, p in chunk)
            foot_n = html.escape(str(n_b), quote=True)
            foot_label = html.escape("birthday" if n_b == 1 else "birthdays", quote=True)
            cards_html.append(
                f'<div class="nwst-bday-card" style="'
                f"flex:0 0 auto;width:min(300px,85vw);scroll-snap-align:start;"
                f"background:{card_bg_layers};border-radius:18px;padding:14px 14px 12px 14px;"
                f"border:1px solid rgba({ar},{ag},{ab},0.42);"
                f"box-shadow:0 8px 24px rgba(0,0,0,0.4),0 4px 18px rgba({ar},{ag},{ab},0.16);"
                f'">'
                f'<div style="font-family:Inter,sans-serif;font-weight:700;font-size:0.95rem;'
                f"color:#f5f5f7;line-height:1.25;text-shadow:{txt_sh};\">{card_title}</div>"
                f"{''.join(body_parts)}"
                f'<div style="margin-top:12px;font-family:Inter,sans-serif;font-size:0.74rem;'
                f"color:rgba(220,220,225,0.95);text-shadow:{txt_sh};\">🎂 {foot_n} {foot_label}</div>"
                f"</div>"
            )
    else:
        empty_txt = html.escape(
            "No birthdays in this ±5 day window (MYT), or Birthday cells are empty / not recognised.",
            quote=True,
        )
        cards_html.append(
            f'<div style="flex:1 1 auto;min-width:min(300px,100%);font-family:Inter,sans-serif;'
            f"font-size:0.82rem;color:{text_e};padding:12px 4px;\">{empty_txt}</div>"
        )

    scroll_row = "".join(cards_html)
    title = html.escape("Birthdays this week", quote=True)
    board = f"""
<div class="nwst-birthday-board" style="
    margin-bottom:2.5rem;
    padding:0.85rem 1rem 1rem 1rem;
    border-radius:12px;
    border:none;
    background:
        linear-gradient(180deg, rgba(139,90,43,0.15) 0%, rgba(24,24,26,0.92) 100%),
        rgba(0,0,0,0.5);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 4px 14px rgba(0,0,0,0.35);
">
  <div style="margin-bottom:0.75rem;">
    <span style="font-family:'Inter',sans-serif;font-weight:800;font-size:0.72rem;
                 letter-spacing:0.12em;text-transform:uppercase;color:{bg};
                 background:{prim};padding:0.25rem 0.55rem;">📌 {title}</span>
  </div>
  <div class="nwst-bday-scroll" style="
      display:flex;
      flex-direction:row;
      gap:12px;
      overflow-x:auto;
      overflow-y:hidden;
      padding:4px 2px 12px 2px;
      scroll-snap-type:x mandatory;
      -webkit-overflow-scrolling:touch;
      scrollbar-color:rgba({r},{g},{b},0.5) transparent;
  ">
    {scroll_row}
  </div>
</div>
"""
    st.markdown(board, unsafe_allow_html=True)


def _rebuild_attendance_structures_from_recent(recent_checkins):
    """Rebuild checked_in_list and cell_group_data from ordered recent_checkins (newest first)."""
    checked_in_list = []
    seen = set()
    for _ts, opt in reversed(recent_checkins):
        if opt not in seen:
            seen.add(opt)
            checked_in_list.append(opt)
    cell_group_data = {}
    for opt in checked_in_list:
        name, cell_group = parse_name_cell_group(opt)
        if cell_group not in cell_group_data:
            cell_group_data[cell_group] = []
        cell_group_data[cell_group].append(name)
    return cell_group_data, checked_in_list


@st.cache_data(ttl=30)  # Local cache for 30 seconds - allows more frequent Upstash reads
def get_today_attendance_data(_client, sheet_id, refresh_key=0, tab_name=ATTENDANCE_TAB_NAME):
    """Get today's attendance data with names and cell groups grouped.
    Uses Redis cache to minimize API calls - cache key includes date so it resets daily."""

    today_myt = get_today_myt_date()
    redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(redis_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return (
                    data.get("cell_group_data", {}),
                    data.get("checked_in_list", []),
                    data.get("recent_checkins", [])
                )
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the specified worksheet
        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            return {}, [], []

        # Get all rows from the Attendance sheet
        all_rows = attendance_sheet.get_all_values()

        if len(all_rows) <= 1:  # Only header row or empty
            return {}, [], []

        # Dictionary to store cell group -> list of names
        cell_group_data = {}
        # List to store all checked-in entries (for set deduplication)
        checked_in_set = set()
        checked_in_list = []  # Keep order for first occurrence
        # List to store recent check-ins with timestamps (timestamp, name_cell_group)
        recent_checkins = []

        # Skip header row (index 0), check from row 1 onwards
        for row in all_rows[1:]:
            if len(row) < 2:  # Skip incomplete rows
                continue

            timestamp_str = row[0].strip() if row[0] else ""
            name_cell_group = row[1].strip() if len(row) > 1 and row[1] else ""

            if not timestamp_str or not name_cell_group:
                continue

            # Parse timestamp - it should be in format "YYYY-MM-DD HH:MM:SS"
            # Extract just the date part (first 10 characters)
            try:
                # Handle different timestamp formats
                if len(timestamp_str) >= 10:
                    date_part = timestamp_str[:10]  # Get "YYYY-MM-DD" part

                    # Check if this timestamp is from today (MYT)
                    if date_part == today_myt:
                        # Add to recent checkins (include duplicates for the table)
                        recent_checkins.append((timestamp_str, name_cell_group))

                        # Only add if not already in set (avoid duplicates for counts)
                        if name_cell_group not in checked_in_set:
                            checked_in_set.add(name_cell_group)
                            checked_in_list.append(name_cell_group)

                            # Parse name and cell group
                            name, cell_group = parse_name_cell_group(name_cell_group)

                            # Add to cell group data
                            if cell_group not in cell_group_data:
                                cell_group_data[cell_group] = []
                            cell_group_data[cell_group].append(name)
            except Exception as e:
                # If parsing fails, skip this row
                continue

        # Sort recent_checkins by timestamp descending (most recent first)
        recent_checkins.sort(key=lambda x: x[0], reverse=True)

        # Store in Redis cache
        if redis_client:
            try:
                cache_data = {
                    "cell_group_data": cell_group_data,
                    "checked_in_list": checked_in_list,
                    "recent_checkins": recent_checkins
                }
                redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)
            except Exception:
                pass

        return cell_group_data, checked_in_list, recent_checkins

    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            # Return empty data but don't show error (will use cached data if available)
            return {}, [], []
        return {}, [], []
    except Exception as e:
        return {}, [], []

def get_checked_in_today(client, sheet_id, tab_name=ATTENDANCE_TAB_NAME):
    """Get a set of people who have already checked in today (MYT date)"""
    try:
        refresh_key = st.session_state.get('refresh_counter', 0)
        _, checked_in_list, _ = get_today_attendance_data(client, sheet_id, refresh_key, tab_name)
        return set(checked_in_list)
    except Exception as e:
        # If there's an error reading attendance, return empty set (show all options)
        return set()


@st.cache_data(ttl=60)
def get_newcomers_count(_client, sheet_id, refresh_key=0):
    """Count newcomers from Form Responses 1: rows where Column P (Status) = 'New'
    and Column Q (Processed) is false/empty.
    Returns: tuple (count, list_of_newcomers)
        where list_of_newcomers is a list of dicts with 'name' and 'cell' keys

    Uses Upstash Redis caching — see ``REDIS_CACHE_TTL``; **Refresh** in the app clears the newcomers key
    so counts update after new form responses.
    """
    today_myt = get_today_myt_date()
    redis_key = f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}"

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(redis_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("count", 0), data.get("newcomers_list", [])
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        form_sheet = spreadsheet.worksheet(FORM_RESPONSES_TAB_NAME)
        all_rows = form_sheet.get_all_values()
        if len(all_rows) <= 1:
            # Store empty result in Redis
            if redis_client:
                try:
                    redis_client.set(redis_key, json.dumps({"count": 0, "newcomers_list": []}), ex=REDIS_CACHE_TTL)
                except Exception:
                    pass
            return 0, []
        newcomers = []
        for row in all_rows[1:]:
            # Column P = index 15 (Status), Column Q = index 16 (Processed)
            # Column B = index 1 (Name), Column C = index 2 (Assigned Cell)
            name = row[1].strip() if len(row) > 1 and row[1] else ""
            cell = row[2].strip() if len(row) > 2 and row[2] else ""
            p_val = row[15].strip() if len(row) > 15 and row[15] else ""
            q_val = row[16].strip() if len(row) > 16 and row[16] else ""
            if p_val.lower() == "new" and (not q_val or q_val.lower() == "false"):
                newcomers.append({"name": name, "cell": cell})

        count = len(newcomers)

        # Store in Redis cache
        if redis_client:
            try:
                cache_data = {
                    "count": count,
                    "newcomers_list": newcomers
                }
                redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)
            except Exception:
                pass

        return count, newcomers
    except Exception:
        return 0, []

def get_attendance_data_for_date(_client, sheet_id, target_date, tab_name=ATTENDANCE_TAB_NAME):
    """Get attendance data for a specific date (YYYY-MM-DD format).
    Uses Redis caching for historical data to reduce Google Sheets API calls.
    Args:
        _client: Google Sheets client
        sheet_id: Google Sheet ID
        target_date: Date string in 'YYYY-MM-DD' format
        tab_name: Tab name to read from
    Returns:
        tuple: (cell_group_data, checked_in_list, recent_checkins)
    """
    # Try Redis cache first for historical data
    redis_client = get_redis_client()
    cache_key = f"{REDIS_HISTORICAL_KEY_PREFIX}{target_date}:{tab_name}"

    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data["cell_group_data"], data["checked_in_list"], data["recent_checkins"]
        except Exception:
            pass  # Cache miss or error, continue to fetch from Sheets

    # Cache miss - fetch from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the specified worksheet
        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            return {}, [], []

        # Get all rows from the Attendance sheet
        all_rows = attendance_sheet.get_all_values()

        if len(all_rows) <= 1:  # Only header row or empty
            return {}, [], []

        # Dictionary to store cell group -> list of names
        cell_group_data = {}
        # List to store all checked-in entries (for set deduplication)
        checked_in_set = set()
        checked_in_list = []  # Keep order for first occurrence
        # List to store recent check-ins with timestamps (timestamp, name_cell_group)
        recent_checkins = []

        # Skip header row (index 0), check from row 1 onwards
        for row in all_rows[1:]:
            if len(row) < 2:  # Skip incomplete rows
                continue

            timestamp_str = row[0].strip() if row[0] else ""
            name_cell_group = row[1].strip() if len(row) > 1 and row[1] else ""

            if not timestamp_str or not name_cell_group:
                continue

            # Parse timestamp - it should be in format "YYYY-MM-DD HH:MM:SS"
            # Extract just the date part (first 10 characters)
            try:
                if len(timestamp_str) >= 10:
                    date_part = timestamp_str[:10]  # Get "YYYY-MM-DD" part

                    # Check if this timestamp matches target date
                    if date_part == target_date:
                        # Add to recent checkins (include duplicates for the table)
                        recent_checkins.append((timestamp_str, name_cell_group))

                        # Only add if not already in set (avoid duplicates for counts)
                        if name_cell_group not in checked_in_set:
                            checked_in_set.add(name_cell_group)
                            checked_in_list.append(name_cell_group)

                            # Parse name and cell group
                            name, cell_group = parse_name_cell_group(name_cell_group)

                            # Add to cell group data
                            if cell_group not in cell_group_data:
                                cell_group_data[cell_group] = []
                            cell_group_data[cell_group].append(name)
            except Exception:
                # If parsing fails, skip this row
                continue

        # Sort recent_checkins by timestamp descending (most recent first)
        recent_checkins.sort(key=lambda x: x[0], reverse=True)

        # Store in Redis cache for future requests (only if we got data)
        if redis_client:
            try:
                cache_data = {
                    "cell_group_data": cell_group_data,
                    "checked_in_list": checked_in_list,
                    "recent_checkins": recent_checkins
                }
                redis_client.setex(cache_key, REDIS_HISTORICAL_TTL, json.dumps(cache_data))
            except Exception:
                pass  # Cache write failed, not critical

        return cell_group_data, checked_in_list, recent_checkins

    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return {}, [], []
        return {}, [], []
    except Exception:
        return {}, [], []

def save_attendance_to_sheet(client, attendance_data, tab_name=ATTENDANCE_TAB_NAME):
    """Queue check-ins in Upstash first; rows are appended to Google Sheets when pending queues are flushed
    (admin **flush pending** job / tooling).

    If Redis is unavailable, falls back to appending directly to the sheet (legacy behaviour).
    Supports batch check-ins."""
    myt = timezone(timedelta(hours=8))
    timestamp = datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")
    today_myt = get_today_myt_date()

    selected_options = attendance_data.get("selected_options", [])
    if not selected_options:
        selected_option = attendance_data.get("selected_option", "")
        if selected_option:
            selected_options = [selected_option]

    if not selected_options:
        return False, "No options selected"

    option_type = attendance_data.get("option_type", "Option")

    redis_client = get_redis_client()
    if redis_client:
        try:
            redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
            pending_key = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
            cached = redis_client.get(redis_key)

            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                cell_group_data = data.get("cell_group_data", {})
                checked_in_list = data.get("checked_in_list", [])
                recent_checkins = data.get("recent_checkins", [])
            else:
                cell_group_data = {}
                checked_in_list = []
                recent_checkins = []

            checked_in_set = set(checked_in_list)
            for option in selected_options:
                recent_checkins.insert(0, (timestamp, option))
                if option not in checked_in_set:
                    checked_in_set.add(option)
                    checked_in_list.append(option)
                    pname, cell_group = parse_name_cell_group(option)
                    if cell_group not in cell_group_data:
                        cell_group_data[cell_group] = []
                    cell_group_data[cell_group].append(pname)

            cache_data = {
                "cell_group_data": cell_group_data,
                "checked_in_list": checked_in_list,
                "recent_checkins": recent_checkins,
            }
            redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)

            for option in selected_options:
                line = json.dumps({"opt": option, "ts": timestamp}, sort_keys=True, separators=(",", ":"))
                redis_client.rpush(pending_key, line)
            redis_client.expire(pending_key, REDIS_CACHE_TTL)

            count = len(selected_options)
            return True, f"Checked in {count} {'person' if count == 1 else 'people'} successfully!"
        except Exception as e:
            return False, f"Failed to save check-in (Redis): {str(e)}"

    # No Redis: write Google Sheet immediately
    if not client:
        return False, "Google Sheets client not available."
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
            existing_headers = attendance_sheet.row_values(1)
            if not existing_headers:
                attendance_sheet.append_row(["Timestamp", option_type])
        except gspread.exceptions.WorksheetNotFound:
            attendance_sheet = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=20)
            attendance_sheet.append_row(["Timestamp", option_type])

        rows = [[timestamp, option] for option in selected_options]
        attendance_sheet.append_rows(rows)

        count = len(selected_options)
        return True, f"Checked in {count} {'person' if count == 1 else 'people'} successfully!"
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return False, "⚠️ API quota exceeded. Please wait a moment and try again."
        return False, f"Failed to save attendance: {str(e)}"
    except Exception as e:
        return False, f"Failed to save attendance: {str(e)}"


def undo_last_checkin(client, name, tab_name):
    """Undo the most recent check-in for this name: remove from Upstash pending queue or delete a sheet row."""
    today_myt = get_today_myt_date()
    redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
    pending_key = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
    display_name = name.split(" - ")[0] if " - " in name else name

    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(redis_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                recent_checkins = data.get("recent_checkins", [])
                idx = None
                for i, pair in enumerate(recent_checkins):
                    ts_i, n = pair[0], pair[1]
                    if n == name:
                        idx = i
                        break
                if idx is not None:
                    ts, opt = recent_checkins[idx][0], recent_checkins[idx][1]
                    recent_checkins.pop(idx)
                    line = json.dumps({"opt": opt, "ts": ts}, sort_keys=True, separators=(",", ":"))
                    try:
                        removed_raw = redis_client.lrem(pending_key, 1, line)
                        removed = int(removed_raw) if removed_raw is not None else 0
                    except (TypeError, ValueError):
                        removed = 0

                    cell_group_data, checked_in_list = _rebuild_attendance_structures_from_recent(
                        recent_checkins
                    )
                    cache_data = {
                        "cell_group_data": cell_group_data,
                        "checked_in_list": checked_in_list,
                        "recent_checkins": recent_checkins,
                    }
                    redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)

                    if removed > 0:
                        return True, f"Undone! {display_name} has been removed from today's check-in."

                    if client and (SHEET_ID or "").strip():
                        try:
                            spreadsheet = client.open_by_key(SHEET_ID)
                            attendance_sheet = spreadsheet.worksheet(tab_name)
                            all_values = attendance_sheet.get_all_values()
                            row_to_delete = None
                            for i in range(len(all_values) - 1, 0, -1):
                                if len(all_values[i]) >= 2 and all_values[i][1] == name and all_values[i][0] == ts:
                                    row_to_delete = i + 1
                                    break
                            if row_to_delete is None:
                                for i in range(len(all_values) - 1, 0, -1):
                                    if len(all_values[i]) >= 2 and all_values[i][1] == name:
                                        row_to_delete = i + 1
                                        break
                            if row_to_delete is not None:
                                attendance_sheet.delete_rows(row_to_delete)
                        except Exception:
                            pass
                    return True, f"Undone! {display_name} has been removed from today's check-in."
        except Exception as e:
            return False, f"Failed to undo: {str(e)}"

    if not client:
        return False, "Could not undo: no Sheets client and no Redis cache."

    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        attendance_sheet = spreadsheet.worksheet(tab_name)
        all_values = attendance_sheet.get_all_values()
        row_to_delete = None
        for i in range(len(all_values) - 1, 0, -1):
            if len(all_values[i]) >= 2 and all_values[i][1] == name:
                row_to_delete = i + 1
                break
        if row_to_delete is None:
            return False, f"Could not find check-in record for {name}"
        attendance_sheet.delete_rows(row_to_delete)
        if redis_client:
            try:
                cached = redis_client.get(redis_key)
                if cached:
                    data = json.loads(cached) if isinstance(cached, str) else cached
                    recent_checkins = data.get("recent_checkins", [])
                    for i, pair in enumerate(recent_checkins):
                        if pair[1] == name:
                            recent_checkins.pop(i)
                            break
                    cell_group_data, checked_in_list = _rebuild_attendance_structures_from_recent(
                        recent_checkins
                    )
                    redis_client.set(
                        redis_key,
                        json.dumps({
                            "cell_group_data": cell_group_data,
                            "checked_in_list": checked_in_list,
                            "recent_checkins": recent_checkins,
                        }),
                        ex=REDIS_CACHE_TTL,
                    )
            except Exception:
                pass
        return True, f"Undone! {display_name} has been removed from today's check-in."
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return False, "⚠️ API quota exceeded. Please wait a moment and try again."
        return False, f"Failed to undo: {str(e)}"
    except Exception as e:
        return False, f"Failed to undo: {str(e)}"


# ---------- Streamlit App ----------
st.set_page_config(
    page_title="Church Check-In",
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Top anchor for scroll-to-top button
st.markdown('<div id="top-anchor"></div>', unsafe_allow_html=True)

# Initialize session state for cache invalidation
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0

# Track last refresh time for cache countdown (MYT timezone)
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = get_now_myt()
# Ensure existing session state is timezone-aware (fix for mixed tz issues)
elif st.session_state.last_refresh_time.tzinfo is None:
    st.session_state.last_refresh_time = get_now_myt()

CACHE_TTL_SECONDS = 60  # Local Streamlit cache duration (Redis handles main caching)

# Generate daily colors
daily_colors = generate_daily_colors()

# Determine current page early for color scheme
_early_query_params = st.query_params
_early_page = _early_query_params.get("page", "nwst")
is_leaders_page = _early_page == "leaders"

# Create color schemes for each page type
if is_leaders_page:
    # Leaders page: Light theme (white background, dark text)
    page_colors = {
        'primary': daily_colors['primary'],
        'light': daily_colors['light'],
        'background': '#ffffff',
        'text': '#000000',
        'text_muted': '#666666',
        'card_bg': '#f5f5f5',
        'border': daily_colors['primary']
    }
else:
    # NWST page: Dark theme (black background, light text)
    page_colors = {
        'primary': daily_colors['primary'],
        'light': daily_colors['light'],
        'background': '#000000',
        'text': '#ffffff',
        'text_muted': '#999999',
        'card_bg': '#0a0a0a',
        'border': daily_colors['primary']
    }

# Progress bar track behind daily primary (check-in loaders)
_checkin_progress_track = "rgba(0,0,0,0.11)" if is_leaders_page else "rgba(255,255,255,0.14)"

# Add CSS to reduce Streamlit default spacing and style buttons with daily color
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600&display=swap');
    /* Force instruction text to be white */
    .instruction-text {{
        color: #ffffff !important;
    }}

    /* Base theme colors */
    .stApp {{
        background-color: {page_colors['background']} !important;
    }}

    .element-container {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] {{
        gap: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] > [style*="flex-direction: column"] {{
        gap: 0rem !important;
    }}
    .stMarkdown {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="column"] {{
        padding-top: 0rem !important;
    }}

    /* Simple st.progress: fill = primary, track = subtle contrast on page bg */
    .stProgress > div > div > div {{
        background-color: {_checkin_progress_track} !important;
    }}
    .stProgress > div > div > div > div {{
        background-color: {page_colors['primary']} !important;
    }}
    .stProgress label, .stProgress [data-testid="stMarkdownContainer"] p {{
        color: {page_colors['text']} !important;
    }}

    /* Style all buttons with daily color theme */
    .stButton > button {{
        background-color: transparent !important;
        color: {page_colors['primary']} !important;
        border: 2px solid {page_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.2s ease !important;
    }}
    .stButton > button:hover {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        transform: scale(1.02) !important;
    }}

    /* Update Names link styled as button */
    .update-names-btn {{
        display: block !important;
        text-align: center !important;
        padding: 0.5rem 1rem !important;
        border: 2px solid {page_colors['primary']} !important;
        border-radius: 0px !important;
        color: {page_colors['primary']} !important;
        text-decoration: none !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 400 !important;
        letter-spacing: 0.5px !important;
        background: transparent !important;
        transition: all 0.2s ease !important;
    }}
    .update-names-btn:hover {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        transform: scale(1.02) !important;
    }}

    /* Primary buttons (Check In, Close) */
    .stButton > button[kind="primary"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        border: 2px solid {page_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {page_colors['light']} !important;
        border-color: {page_colors['light']} !important;
    }}

    /* Form submit button */
    .stFormSubmitButton > button {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        border: 2px solid {page_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
    }}
    .stFormSubmitButton > button:hover {{
        background-color: {page_colors['light']} !important;
        border-color: {page_colors['light']} !important;
        transform: scale(1.02) !important;
    }}

    /* Multiselect styling */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {page_colors['primary']} !important;
    }}

    /* Style checked-in options (starting with ✓) in multiselect dropdown */
    .stMultiSelect [data-baseweb="menu"] li[aria-disabled="false"]:has(div[title^="✓"]),
    .stMultiSelect [role="listbox"] li:has(div[title^="✓"]) {{
        opacity: 0.5 !important;
        color: #888 !important;
    }}
    /* Alternative selector for dropdown items containing tick */
    [data-baseweb="menu"] [role="option"] {{
        transition: opacity 0.2s ease;
    }}

    /* Text colors for leaders page */
    {"" if not is_leaders_page else '''
    .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown div {
        color: #000000 !important;
    }
    /* Keep instruction text white on dark overlay */
    .instruction-text {
        color: #ffffff !important;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #000000 !important;
    }
    .stRadio label {
        color: #000000 !important;
    }
    [data-testid="stSidebar"] {
        background-color: #f0f0f0 !important;
    }
    [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {
        color: #000000 !important;
    }
    /* Form labels */
    .stMultiSelect label, .stSelectbox label, .stTextInput label {
        color: #000000 !important;
    }
    '''}

    /* Collapsible cell group styles */
    .cell-collapsible {{
        cursor: pointer;
        user-select: none;
        transition: all 0.2s ease;
    }}
    .cell-collapsible:hover {{
        opacity: 0.8;
    }}
    .cell-content {{
        overflow: hidden;
        transition: max-height 0.3s ease-out, opacity 0.2s ease-out;
        max-height: 0;
        opacity: 0;
    }}
    .cell-content.expanded {{
        max-height: 2000px;
        opacity: 1;
    }}
    .cell-toggle {{
        display: inline-block;
        margin-right: 0.5rem;
        transition: transform 0.2s ease;
        font-size: 0.85rem;
    }}
    .cell-toggle.expanded {{
        transform: rotate(90deg);
    }}
    .expand-collapse-btn {{
        background: transparent;
        border: 1px solid {page_colors['primary']};
        color: {page_colors['primary']};
        padding: 0.3rem 0.8rem;
        margin-right: 0.5rem;
        border-radius: 4px;
        cursor: pointer;
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem;
        font-weight: 600;
        transition: all 0.2s ease;
    }}
    .expand-collapse-btn:hover {{
        background: {page_colors['primary']};
        color: {page_colors['background']};
    }}
</style>
""", unsafe_allow_html=True)

# GIF / banner (Theme Override filename in this folder, then BANNER_GIF_URL, then banner.gif)
_app_dir = os.path.dirname(__file__)
gif_url = os.getenv("BANNER_GIF_URL", "").strip()
theme_banner_fn = daily_colors.get("banner")


def _banner_mime_for_path(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    return {
        ".gif": "image/gif",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }.get(ext, "image/gif")


background_gif = ""
gif_src = ""
if theme_banner_fn:
    _p_theme = os.path.join(_app_dir, theme_banner_fn)
    if os.path.isfile(_p_theme):
        import base64
        with open(_p_theme, "rb") as f:
            _raw = base64.b64encode(f.read()).decode()
        _mime = _banner_mime_for_path(_p_theme)
        background_gif = f"url('data:{_mime};base64,{_raw}')"
        gif_src = f"data:{_mime};base64,{_raw}"
if not gif_src and gif_url:
    background_gif = f"url('{gif_url}')"
    gif_src = gif_url
if not gif_src:
    _default = os.path.join(_app_dir, "banner.gif")
    if os.path.isfile(_default):
        import base64
        with open(_default, "rb") as f:
            _raw = base64.b64encode(f.read()).decode()
        background_gif = f"url('data:image/gif;base64,{_raw}')"
        gif_src = f"data:image/gif;base64,{_raw}"

# Initialize Google Sheets client
client = get_gsheet_client()

if not client:
    st.stop()

# Check if sheet ID is configured
if not SHEET_ID:
    st.error("⚠️ Please configure ATTENDANCE_SHEET_ID in your .env file or Streamlit secrets.")
    st.info("""
    **Setup Instructions:**
    1. Create a Google Sheet
    2. Add a tab named "Options" with your form options
    3. Format: Column C only
    4. Example:
       - Row 1, Column C: Header name (e.g., "Name" or "Attendee")
       - Row 2+, Column C: Options listed vertically
       - Example: "Miracle Wong - Narrowstreet Core Team"
    5. Set ATTENDANCE_SHEET_ID in your environment variables
    """)
    st.stop()

# Get options from Google Sheets
with st.spinner("Loading options..."):
    options, _, error_msg = get_options_from_sheet(client, SHEET_ID)

if options is None:
    if error_msg:
        st.error(error_msg)
        st.info("Tip: If you're seeing quota errors, wait a moment and refresh the page. Data is cached to reduce API calls.")
    st.stop()

if not options:
    if error_msg:
        st.warning(error_msg)
    else:
        st.warning("""
        No options found. Please add options to your Google Sheet.

        **Format in the Options tab:**
        - Column C, Row 1: Header name (e.g., "Name" or "Attendee")
        - Column C, Row 2+: Options listed vertically
        - Example:
          |   |   | Name - Cell Group        |
          |---|---|--------------------------|
          |   |   | Miracle Wong - Narrowstreet Core Team |
          |   |   | Shaun Quek - Narrowstreet Core Team |
        """)
    st.stop()

# Get the single option type and values
option_type = list(options.keys())[0]
all_option_values = list(options.values())[0]


def render_check_in_form(tab_name, form_key, page_label="Check In"):
    """Render the check-in form for a specific tab"""
    if st.session_state.get('show_undo_success'):
        st.info(f"↩️ {st.session_state['show_undo_success']}")
        st.session_state['show_undo_success'] = None

    # Wrap form section with GIF background
    if background_gif and gif_src:
        st.markdown(f"""
        <div style="
            position: relative;
            padding: 2rem;
            margin: 0;
            border-radius: 8px;
            border: 2px solid {page_colors['primary']};
            min-height: 250px;
            overflow: hidden;
        ">
            <img src="{gif_src}"
                 style="
                     position: absolute;
                     top: 0;
                     left: 0;
                     width: 100%;
                     height: 100%;
                     object-fit: cover;
                     z-index: 0;
                     opacity: 0.8;
                 " />
            <!-- Page label badge -->
            <div style="
                position: absolute;
                top: 10px;
                left: 10px;
                background: {page_colors['primary']};
                color: {page_colors['background']};
                padding: 0.4rem 1rem;
                font-family: 'Inter', sans-serif;
                font-weight: 800;
                font-size: 0.85rem;
                letter-spacing: 1px;
                text-transform: uppercase;
                z-index: 2;
            ">{page_label}</div>
            <div style="position: relative; z-index: 1;">
        """, unsafe_allow_html=True)

    # Display form in centered column
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        render_birthdays_notice_board(page_colors)

        def _flush_checkin_success_here():
            succ = st.session_state.get('show_checkin_success')
            if succ and succ.get('form_key') == form_key:
                name_only = succ.get('name', '').split(" - ")[0] if succ.get('name') else ''
                st.success(f"✅ {name_only} checked in!")
                st.session_state['show_checkin_success'] = None

        selectbox_key = f"{form_key}_selectbox"
        if st.session_state.pop(f"{form_key}_reset_selectbox", None):
            st.session_state[selectbox_key] = ""

        checked_in_today = _with_checkin_progress(
            "Loading today's attendance…",
            lambda: get_checked_in_today(client, SHEET_ID, tab_name),
        )

        available_options = [opt for opt in all_option_values if opt not in checked_in_today]
        checked_in_options = [opt for opt in all_option_values if opt in checked_in_today]

        # Check if there are any available options
        if not available_options:
            _flush_checkin_success_here()
            st.warning("All attendees have already checked in for today!")
        else:
            # Create formatted options: available ones normal, checked-in ones with tick prefix
            # Format: "✓ Name - Cell" for checked in, "Name - Cell" for available
            # Sort by Cell Group first, then by Name within each group
            def get_sort_key(opt):
                parts = opt.split(" - ", 1)
                if len(parts) == 2:
                    name, cell = parts[0].strip(), parts[1].strip()
                    return (cell.lower(), name.lower())
                return (opt.lower(), "")

            sorted_options = sorted(all_option_values, key=get_sort_key)

            formatted_options = []
            option_mapping = {}  # Maps display name back to original name

            for opt in sorted_options:
                if opt in checked_in_today:
                    display_name = f"✓ {opt}"
                    formatted_options.append(display_name)
                    option_mapping[display_name] = opt
                else:
                    formatted_options.append(opt)
                    option_mapping[opt] = opt

            # Sort formatted options again to ensure correct order
            # (ignoring ✓ prefix for sorting)
            def get_display_sort_key(display_opt):
                opt = display_opt.lstrip("✓ ")
                parts = opt.split(" - ", 1)
                if len(parts) == 2:
                    name, cell = parts[0].strip(), parts[1].strip()
                    return (cell.lower(), name.lower())
                return (opt.lower(), "")

            formatted_options = sorted(formatted_options, key=get_display_sort_key)

            # Add placeholder at the beginning
            placeholder = ""
            options_with_placeholder = [placeholder] + formatted_options

            # Gradient instruction pill above the dropdown
            newcomer_note_html = f"""
            <div style="
                margin-bottom: 1rem;
                padding: 2px;
                background: linear-gradient(135deg, {page_colors['primary']} 0%, {page_colors['light']} 50%, {page_colors['primary']} 100%);
                border-radius: 999px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.12), 0 0 20px {page_colors['primary']}20;
            ">
                <div style="
                    padding: 0.55rem 1.2rem;
                    background: {page_colors['background']};
                    border-radius: 999px;
                    display: inline-flex;
                    align-items: center;
                    gap: 0.5rem;
                    font-family: 'Outfit', 'Inter', -apple-system, sans-serif;
                    font-size: 0.88rem;
                    font-weight: 500;
                    color: {page_colors['text']};
                    letter-spacing: 0.03em;
                ">
                    <span>Select a name from dropdown below to check in.</span>
                </div>
            </div>
            """
            st.markdown(newcomer_note_html, unsafe_allow_html=True)

            # Auto-submit selectbox
            selected_display = st.selectbox(
                "Select your name",
                options=options_with_placeholder,
                index=0,
                key=selectbox_key,
                label_visibility="hidden",
                help="Select a name to instantly check in. Names with ✓ are already checked in today.",
            )

            _flush_checkin_success_here()

            # Add JavaScript to gray out checked-in options in the dropdown
            components.html(f"""
            <script>
                // Function to style checked-in options (those starting with ✓)
                function styleCheckedInOptions() {{
                    // Find all option items in the selectbox dropdown
                    const options = document.querySelectorAll('[data-baseweb="menu"] li, [role="listbox"] li, [data-baseweb="select"] [role="option"]');
                    options.forEach(opt => {{
                        const text = opt.textContent || opt.innerText;
                        if (text && text.trim().startsWith('✓')) {{
                            opt.style.opacity = '0.5';
                            opt.style.color = '#888';
                            opt.style.fontStyle = 'italic';
                        }}
                    }});
                }}

                // Run on page load and observe for dropdown changes
                const observer = new MutationObserver(styleCheckedInOptions);
                observer.observe(document.body, {{ childList: true, subtree: true }});
                styleCheckedInOptions();
            </script>
            """, height=0)

            # Auto check-in when a valid selection is made
            if selected_display and selected_display != placeholder:
                original_name = option_mapping.get(selected_display, selected_display)

                # Prevent duplicate check-ins (selectbox remembers selection after rerun)
                last_processed = st.session_state.get('last_processed_checkin')
                if last_processed == original_name:
                    # Already processed this selection, skip
                    pass
                elif original_name in checked_in_today:
                    # Check if already checked in
                    st.warning(f"{original_name} has already checked in today.")
                else:
                    # Mark as being processed
                    st.session_state['last_processed_checkin'] = original_name

                    # Prepare attendance data
                    attendance_data = {
                        "selected_options": [original_name],
                        "option_type": option_type
                    }

                    success, message = _with_checkin_progress(
                        "Checking you in…",
                        lambda: save_attendance_to_sheet(client, attendance_data, tab_name),
                    )

                    if success:
                        # Store last check-in for potential undo
                        st.session_state['last_checkin'] = {
                            'name': original_name,
                            'tab_name': tab_name,
                            'timestamp': get_now_myt(),
                            'form_type': 'attendance'
                        }

                        st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                        st.session_state.last_refresh_time = get_now_myt()

                        name_only = original_name.split(" - ")[0] if " - " in original_name else original_name
                        toast_fn = getattr(st, "toast", None)
                        if toast_fn:
                            toast_fn(f"Checked in: {name_only}", icon="✅")

                        st.session_state['show_checkin_success'] = {
                            'name': original_name,
                            'message': message,
                            'form_key': form_key,
                        }
                        st.session_state[f'{form_key}_reset_selectbox'] = True
                        st.session_state['last_processed_checkin'] = None
                        st.rerun()
                    else:
                        # Clear the processed flag on error
                        st.session_state['last_processed_checkin'] = None
                        st.error(f"{message}")

            # Show undo button BELOW the selectbox (with spacing)
            if 'last_checkin' in st.session_state and st.session_state['last_checkin']:
                last_checkin = st.session_state['last_checkin']
                # Only show undo for this form type
                if last_checkin.get('form_type') == 'attendance':
                    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
                    checkin_display_name = last_checkin['name'].split(" - ")[0] if " - " in last_checkin['name'] else last_checkin['name']
                    if st.button(f"Undo check-in for {checkin_display_name}", key=f"{form_key}_undo", type="secondary"):
                        success, undo_message = undo_last_checkin(client, last_checkin['name'], last_checkin['tab_name'])
                        if success:
                            st.session_state['last_checkin'] = None
                            st.session_state['show_checkin_success'] = None
                            st.session_state['last_processed_checkin'] = None  # Allow re-checking in
                            st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                            st.session_state['show_undo_success'] = undo_message
                            st.rerun()
                        else:
                            st.error(undo_message)

    # Close background GIF container if it was opened
    if background_gif:
        st.markdown("</div></div>", unsafe_allow_html=True)
    else:
        # Show placeholder if no GIF
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 1rem; padding: 1rem; background: {page_colors['card_bg']}; border: 2px dashed {page_colors['primary']}; border-radius: 8px;">
            <p style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 600; margin: 0;">
                Add your banner GIF by setting BANNER_GIF_URL in .env or placing banner.gif in the CHECK IN folder
            </p>
        </div>
        """, unsafe_allow_html=True)

    return checked_in_today


def render_ministry_check_in_form(selected_ministry, form_key, page_label="Ministry Check In"):
    """Render the check-in form for ministry attendance.

    Args:
        selected_ministry: The ministry to show options for (e.g., "Worship", "Hype")
        form_key: Unique key for the Streamlit form
        page_label: Label shown on the form badge
    """
    # Get ministry options for the selected ministry
    ministry_options, ministry_error = get_ministry_options_from_sheet(client, SHEET_ID, selected_ministry)

    if ministry_options is None or ministry_error:
        if ministry_error:
            st.warning(ministry_error)
        st.info(f"No members found for {selected_ministry} ministry. Please add members to the 'Options - Ministry' tab.")
        return set()

    ministry_option_values = list(ministry_options.values())[0] if ministry_options else []

    if not ministry_option_values:
        st.warning(f"No members found for {selected_ministry} ministry.")
        return set()

    if st.session_state.get('show_undo_success'):
        st.info(f"↩️ {st.session_state['show_undo_success']}")
        st.session_state['show_undo_success'] = None

    # Wrap form section with GIF background
    if background_gif and gif_src:
        st.markdown(f"""
        <div style="
            position: relative;
            padding: 2rem;
            margin: 0;
            border-radius: 8px;
            border: 2px solid {page_colors['primary']};
            min-height: 250px;
            overflow: hidden;
        ">
            <img src="{gif_src}"
                 style="
                     position: absolute;
                     top: 0;
                     left: 0;
                     width: 100%;
                     height: 100%;
                     object-fit: cover;
                     z-index: 0;
                     opacity: 0.8;
                 " />
            <!-- Page label badge -->
            <div style="
                position: absolute;
                top: 10px;
                left: 10px;
                background: {page_colors['primary']};
                color: {page_colors['background']};
                padding: 0.4rem 1rem;
                font-family: 'Inter', sans-serif;
                font-weight: 800;
                font-size: 0.85rem;
                letter-spacing: 1px;
                text-transform: uppercase;
                z-index: 2;
            ">{page_label}</div>
            <div style="position: relative; z-index: 1;">
        """, unsafe_allow_html=True)

    # Display form in centered column
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        def _flush_ministry_checkin_success_here():
            succ = st.session_state.get('show_checkin_success')
            if succ and succ.get('form_key') == form_key:
                name_only = succ.get('name', '').split(" - ")[0] if succ.get('name') else ''
                st.success(f"✅ {name_only} checked in!")
                st.session_state['show_checkin_success'] = None

        selectbox_key = f"{form_key}_selectbox"
        if st.session_state.pop(f"{form_key}_reset_selectbox", None):
            st.session_state[selectbox_key] = ""

        checked_in_today = _with_checkin_progress(
            "Loading today's attendance…",
            lambda: get_checked_in_today(client, SHEET_ID, MINISTRY_ATTENDANCE_TAB_NAME),
        )

        available_options = [opt for opt in ministry_option_values if opt not in checked_in_today]

        # Check if there are any available options
        if not available_options:
            _flush_ministry_checkin_success_here()
            st.warning(f"All {selected_ministry} ministry members have already checked in for today!")
        else:
            # Create formatted options: available ones normal, checked-in ones with tick prefix
            # Sort by Department first, then by Name within each department
            def get_sort_key(opt):
                parts = opt.split(" - ", 1)
                if len(parts) == 2:
                    name, dept = parts[0].strip(), parts[1].strip()
                    return (dept.lower(), name.lower())
                return (opt.lower(), "")

            sorted_options = sorted(ministry_option_values, key=get_sort_key)

            formatted_options = []
            option_mapping = {}  # Maps display name back to original name

            for opt in sorted_options:
                if opt in checked_in_today:
                    display_name = f"✓ {opt}"
                    formatted_options.append(display_name)
                    option_mapping[display_name] = opt
                else:
                    formatted_options.append(opt)
                    option_mapping[opt] = opt

            # Sort formatted options again to ensure correct order
            def get_display_sort_key(display_opt):
                opt = display_opt.lstrip("✓ ")
                parts = opt.split(" - ", 1)
                if len(parts) == 2:
                    name, dept = parts[0].strip(), parts[1].strip()
                    return (dept.lower(), name.lower())
                return (opt.lower(), "")

            formatted_options = sorted(formatted_options, key=get_display_sort_key)

            # Add placeholder at the beginning
            placeholder = ""
            options_with_placeholder = [placeholder] + formatted_options

            # Gradient instruction pill above the dropdown
            newcomer_note_html = f"""
            <div style="
                margin-bottom: 1rem;
                padding: 2px;
                background: linear-gradient(135deg, {page_colors['primary']} 0%, {page_colors['light']} 50%, {page_colors['primary']} 100%);
                border-radius: 999px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.12), 0 0 20px {page_colors['primary']}20;
            ">
                <div style="
                    padding: 0.55rem 1.2rem;
                    background: {page_colors['background']};
                    border-radius: 999px;
                    display: inline-flex;
                    align-items: center;
                    gap: 0.5rem;
                    font-family: 'Outfit', 'Inter', -apple-system, sans-serif;
                    font-size: 0.88rem;
                    font-weight: 500;
                    color: {page_colors['text']};
                    letter-spacing: 0.03em;
                ">
                    <span>Select a name from dropdown below to check in.</span>
                </div>
            </div>
            """
            st.markdown(newcomer_note_html, unsafe_allow_html=True)

            # Auto-submit selectbox
            selected_display = st.selectbox(
                "Select your name",
                options=options_with_placeholder,
                index=0,
                key=selectbox_key,
                label_visibility="hidden",
                help="Select a name to instantly check in. Names with ✓ are already checked in today.",
            )

            _flush_ministry_checkin_success_here()

            # Add JavaScript to gray out checked-in options
            components.html(f"""
            <script>
                function styleCheckedInOptions() {{
                    const options = document.querySelectorAll('[data-baseweb="menu"] li, [role="listbox"] li, [data-baseweb="select"] [role="option"]');
                    options.forEach(opt => {{
                        const text = opt.textContent || opt.innerText;
                        if (text && text.trim().startsWith('✓')) {{
                            opt.style.opacity = '0.5';
                            opt.style.color = '#888';
                            opt.style.fontStyle = 'italic';
                        }}
                    }});
                }}
                const observer = new MutationObserver(styleCheckedInOptions);
                observer.observe(document.body, {{ childList: true, subtree: true }});
                styleCheckedInOptions();
            </script>
            """, height=0)

            # Auto check-in when a valid selection is made
            if selected_display and selected_display != placeholder:
                original_name = option_mapping.get(selected_display, selected_display)

                # Prevent duplicate check-ins (selectbox remembers selection after rerun)
                last_processed = st.session_state.get('last_processed_ministry_checkin')
                if last_processed == original_name:
                    # Already processed this selection, skip
                    pass
                elif original_name in checked_in_today:
                    # Check if already checked in
                    st.warning(f"{original_name} has already checked in today.")
                else:
                    # Mark as being processed
                    st.session_state['last_processed_ministry_checkin'] = original_name

                    # Prepare attendance data
                    attendance_data = {
                        "selected_options": [original_name],
                        "option_type": "Name"
                    }

                    success, message = _with_checkin_progress(
                        "Checking you in…",
                        lambda: save_attendance_to_sheet(client, attendance_data, MINISTRY_ATTENDANCE_TAB_NAME),
                    )

                    if success:
                        # Store last check-in for potential undo
                        st.session_state['last_checkin'] = {
                            'name': original_name,
                            'tab_name': MINISTRY_ATTENDANCE_TAB_NAME,
                            'timestamp': get_now_myt(),
                            'form_type': 'ministry',
                            'ministry': selected_ministry
                        }

                        st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                        st.session_state.last_refresh_time = get_now_myt()

                        name_only = original_name.split(" - ")[0] if " - " in original_name else original_name
                        toast_fn = getattr(st, "toast", None)
                        if toast_fn:
                            toast_fn(f"Checked in: {name_only}", icon="✅")

                        st.session_state['show_checkin_success'] = {
                            'name': original_name,
                            'message': message,
                            'form_key': form_key,
                        }
                        st.session_state[f'{form_key}_reset_selectbox'] = True
                        st.session_state['last_processed_ministry_checkin'] = None
                        st.rerun()
                    else:
                        # Clear the processed flag on error
                        st.session_state['last_processed_ministry_checkin'] = None
                        st.error(f"{message}")

            # Show undo button BELOW the selectbox (with spacing)
            if 'last_checkin' in st.session_state and st.session_state['last_checkin']:
                last_checkin = st.session_state['last_checkin']
                # Only show undo for ministry form type
                if last_checkin.get('form_type') == 'ministry':
                    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
                    checkin_display_name = last_checkin['name'].split(" - ")[0] if " - " in last_checkin['name'] else last_checkin['name']
                    if st.button(f"Undo check-in for {checkin_display_name}", key=f"{form_key}_undo", type="secondary"):
                        success, undo_message = undo_last_checkin(client, last_checkin['name'], last_checkin['tab_name'])
                        if success:
                            st.session_state['last_checkin'] = None
                            st.session_state['show_checkin_success'] = None
                            st.session_state['last_processed_ministry_checkin'] = None  # Allow re-checking in
                            st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                            st.session_state['show_undo_success'] = undo_message
                            st.rerun()
                        else:
                            st.error(undo_message)

    # Close background GIF container if it was opened
    if background_gif:
        st.markdown("</div></div>", unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 1rem; padding: 1rem; background: {page_colors['card_bg']}; border: 2px dashed {page_colors['primary']}; border-radius: 8px;">
            <p style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 600; margin: 0;">
                Add your banner GIF by setting BANNER_GIF_URL in .env or placing banner.gif in the CHECK IN folder
            </p>
        </div>
        """, unsafe_allow_html=True)

    return checked_in_today


def render_ministry_dashboard(selected_ministry):
    """Render the dashboard for ministry attendance, grouped by department."""
    st.markdown("---")

    # Get today's attendance data for ministry tab
    with st.spinner("Loading dashboard data..."):
        refresh_key = st.session_state.get('refresh_counter', 0)
        cell_group_data, checked_in_list, _ = get_today_attendance_data(client, SHEET_ID, refresh_key, MINISTRY_ATTENDANCE_TAB_NAME)

    # Filter checked-in list by selected ministry
    ministry_checked_in = []
    ministry_dept_data = {}
    for name_dept in checked_in_list:
        parts = name_dept.split(" - ", 1)
        if len(parts) == 2:
            dept = parts[1].strip()
            # Check if this department belongs to selected ministry
            if ":" in dept:
                ministry_part = dept.split(":")[0].strip()
            else:
                ministry_part = dept  # No colon means department IS the ministry
            if ministry_part.lower() == selected_ministry.lower():
                ministry_checked_in.append(name_dept)
                name = parts[0].strip()
                if dept not in ministry_dept_data:
                    ministry_dept_data[dept] = []
                ministry_dept_data[dept].append(name)

    total_checked_in = len(ministry_checked_in)

    # Get all ministry members grouped by department
    all_members_by_dept = get_ministry_members_by_department(client, SHEET_ID, selected_ministry)

    # Get name-to-role mapping from main OPTIONS tab (for role display in badges)
    _, name_to_role, _ = get_options_from_sheet(client, SHEET_ID)

    # Create a set of checked-in names for quick lookup
    checked_in_names_set = set()
    for name_dept in ministry_checked_in:
        name, _ = parse_name_cell_group(name_dept)
        if name:
            checked_in_names_set.add(name)

    # Convert hex color to RGB for rgba shadows
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    primary_rgb = hex_to_rgb(daily_colors['primary'])

    # KPI Card - Total Checked In
    st.markdown(f"""
    <style>
        .kpi-card {{
            background: {page_colors['card_bg']};
            padding: 2rem 2.5rem;
            border-radius: 0px;
            border-left: 6px solid {page_colors['primary']};
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.15);
            transition: all 0.3s ease;
        }}
        .kpi-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.25);
            border-left-width: 8px;
        }}
        .kpi-label {{
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 700;
            color: {page_colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .kpi-number {{
            font-family: 'Inter', sans-serif;
            font-size: 5.5rem;
            font-weight: 900;
            color: {page_colors['primary']};
            line-height: 1;
            margin: 0.5rem 0;
            text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
        }}
        .kpi-subtitle {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            color: {page_colors['text_muted']};
            margin-top: 0.5rem;
            font-weight: 500;
        }}
    </style>
    <div class="kpi-card">
        <div class="kpi-label">{selected_ministry} Ministry - Checked In Today</div>
        <div class="kpi-number">{total_checked_in}</div>
        <div class="kpi-subtitle">Ministry members checked in</div>
    </div>
    """, unsafe_allow_html=True)

    if total_checked_in > 0 and ministry_dept_data:
        # Bar Chart by Department
        st.markdown(f'<div style="font-family: \'Inter\', sans-serif; font-size: 1.8rem; font-weight: 900; color: {page_colors["primary"]}; text-transform: uppercase; letter-spacing: 3px; margin-bottom: 1.5rem; border-bottom: 3px solid {page_colors["primary"]}; padding-bottom: 0.5rem; display: inline-block;">Attendance by Department</div>', unsafe_allow_html=True)

        sorted_depts = sorted(ministry_dept_data.items(), key=lambda x: len(x[1]), reverse=True)

        chart_data = {
            'Department': [dept for dept, _ in sorted_depts],
            'Count': [len(names) for _, names in sorted_depts]
        }
        df_chart = pd.DataFrame(chart_data)

        fig = px.bar(
            df_chart,
            x='Department',
            y='Count',
            color='Count',
            color_continuous_scale=[page_colors['background'], page_colors['primary']],
            text='Count',
            height=400
        )

        fig.update_layout(
            plot_bgcolor=page_colors['background'],
            paper_bgcolor=page_colors['card_bg'],
            font=dict(family='Inter, sans-serif', size=12, color=page_colors['primary']),
            xaxis=dict(
                tickfont=dict(color=page_colors['text_muted']),
                linecolor=page_colors['primary'],
                linewidth=2
            ),
            yaxis=dict(
                tickfont=dict(color=page_colors['text_muted']),
                linecolor=page_colors['primary'],
                linewidth=2
            ),
            coloraxis_showscale=False,
            showlegend=False,
            margin=dict(l=50, r=50, t=60, b=50)
        )

        fig.update_traces(
            textfont=dict(size=14, color=page_colors['background'], family='Inter'),
            textposition='inside'
        )

        st.plotly_chart(fig, use_container_width=True)

        # Names Breakdown by Department with search and collapsible sections
        st.markdown(f'<div style="font-family: \'Inter\', sans-serif; font-size: 1.8rem; font-weight: 900; color: {page_colors["primary"]}; text-transform: uppercase; letter-spacing: 3px; margin-bottom: 1.5rem; border-bottom: 3px solid {page_colors["primary"]}; padding-bottom: 0.5rem; display: inline-block;">Attendees by Department</div>', unsafe_allow_html=True)

        # Build search options for the HTML select
        all_depts_search = sorted(all_members_by_dept.keys(), key=str.lower)
        searchable_depts = [("dept", d) for d in all_depts_search]

        # Build collapsible breakdown HTML
        ministry_breakdown_html = f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

            * {{
                font-family: 'Inter', sans-serif !important;
                box-sizing: border-box;
            }}
            .cell-collapsible {{
                cursor: pointer;
                user-select: none;
                transition: all 0.2s ease;
            }}
            .cell-collapsible:hover {{
                opacity: 0.8;
            }}
            .cell-content {{
                overflow: hidden;
                transition: max-height 0.3s ease-out, opacity 0.2s ease-out, padding 0.3s ease-out;
                max-height: 0;
                opacity: 0;
                padding: 0;
            }}
            .cell-content.expanded {{
                max-height: 2000px;
                opacity: 1;
                padding-top: 0.5rem;
            }}
            .cell-toggle {{
                display: inline-block;
                margin-right: 0.5rem;
                transition: transform 0.2s ease;
                font-size: 0.85rem;
            }}
            .cell-toggle.expanded {{
                transform: rotate(90deg);
            }}
            .expand-collapse-btn {{
                background: transparent;
                border: 1px solid {page_colors['primary']};
                color: {page_colors['primary']};
                padding: 0.4rem 1rem;
                border-radius: 4px;
                cursor: pointer;
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 600;
                transition: all 0.2s ease;
                min-width: 120px;
            }}
            .expand-collapse-btn:hover {{
                background: {page_colors['primary']};
                color: {page_colors['background']};
            }}
            .search-select {{
                padding: 0.5rem;
                border: 1px solid {page_colors['primary']};
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 500;
                background: {page_colors['background']};
                color: {page_colors['text']};
                min-width: 200px;
                cursor: pointer;
            }}
            .search-select:focus {{
                outline: none;
                border-color: {page_colors['primary']};
                box-shadow: 0 0 5px {page_colors['primary']}40;
            }}
            .controls-row {{
                display: flex;
                gap: 1rem;
                align-items: center;
                margin-bottom: 1rem;
                flex-wrap: wrap;
            }}
            .name-badge {{
                background: {page_colors['background']};
                border: 1px solid {page_colors['primary']};
                color: {page_colors['primary']};
                padding: 0.6rem 1.2rem;
                margin: 0.4rem 0.4rem 0.4rem 0;
                border-radius: 0px;
                display: inline-block;
                font-family: 'Inter', sans-serif !important;
                font-weight: 600;
                font-size: 0.9rem;
                letter-spacing: 0.5px;
                transition: all 0.2s ease;
            }}
            .name-badge:hover {{
                background: {page_colors['primary']};
                color: {page_colors['background']};
                transform: scale(1.05);
            }}
            .name-badge-pending {{
                background: {page_colors['background']};
                border: 1px solid {page_colors['text_muted']};
                color: {page_colors['text_muted']};
                padding: 0.6rem 1.2rem;
                margin: 0.4rem 0.4rem 0.4rem 0;
                border-radius: 0px;
                display: inline-block;
                font-family: 'Inter', sans-serif !important;
                font-weight: 600;
                font-size: 0.9rem;
                letter-spacing: 0.5px;
                opacity: 0.5;
            }}
            .name-badge-name {{ display: block; }}
            .name-badge-role {{ display: block; font-size: 0.8em; font-weight: 400; text-transform: none; letter-spacing: normal; opacity: 0.95; }}
            .role-row {{ margin-bottom: 0.8rem; }}
            .role-label {{ font-family: 'Inter', sans-serif; font-size: 0.85rem; font-weight: 600; color: {page_colors['text_muted']}; margin-right: 0.5rem; display: inline; }}
            .dept-header {{
                font-family: 'Inter', sans-serif;
                font-size: 1.3rem;
                font-weight: 900;
                color: {page_colors['primary']};
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 0.3rem;
            }}
            .dept-container {{
                margin-bottom: 1rem;
                padding: 0.5rem;
                border-radius: 8px;
                transition: all 0.3s ease;
            }}
            .count-label {{
                color: {page_colors['text_muted']};
                font-size: 0.85rem;
                font-weight: normal;
                text-transform: none;
                letter-spacing: normal;
            }}
            .highlight {{
                box-shadow: 0 0 20px {page_colors['primary']};
                background-color: {page_colors['light']}20;
            }}
        </style>
        <div class="controls-row">
            <select id="ministrySearchSelect" class="search-select" onchange="jumpToDept(this.value)">
                <option value="">Jump to...</option>
        """

        # Add search options to dropdown
        for _, dept_name in searchable_depts:
            target_id = f"dept-{dept_name.replace(' ', '-').replace(':', '-').replace(chr(39), '').lower()}"
            ministry_breakdown_html += f'<option value="{target_id}">{dept_name}</option>'

        ministry_breakdown_html += f"""
            </select>
            <button id="ministryToggleAllBtn" class="expand-collapse-btn" onclick="toggleAllMinistry()">Expand All</button>
        </div>
        """

        # Display departments with collapsible content
        for dept in sorted(all_members_by_dept.keys(), key=str.lower):
            checked_in_names = ministry_dept_data.get(dept, [])
            all_names_in_dept = all_members_by_dept.get(dept, [])
            checked_count = len(checked_in_names)
            total_count = len(all_names_in_dept)
            role_grouped_badges = build_role_grouped_badges(
                all_names_in_dept, checked_in_names_set, name_to_role,
                "name-badge", "name-badge-pending"
            )

            dept_id = dept.replace(" ", "-").replace(":", "-").replace("'", "").lower()
            ministry_breakdown_html += f"""
            <div id="dept-{dept_id}" class="dept-container">
                <div class="cell-collapsible dept-header" onclick="toggleDept('{dept_id}')">
                    <span id="toggle-{dept_id}" class="cell-toggle">▶</span>
                    {dept} <span class="count-label">({checked_count}/{total_count})</span>
                </div>
                <div id="content-{dept_id}" class="cell-content">
                    {role_grouped_badges}
                </div>
            </div>
            """

        # Add JavaScript
        ministry_breakdown_html += """
        <script>
            var ministryIsExpanded = false;

            function toggleAllMinistry() {
                var btn = document.getElementById('ministryToggleAllBtn');
                if (ministryIsExpanded) {
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                    btn.textContent = 'Expand All';
                    ministryIsExpanded = false;
                } else {
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                    btn.textContent = 'Collapse All';
                    ministryIsExpanded = true;
                }
            }

            function toggleDept(deptId) {
                var content = document.getElementById('content-' + deptId);
                var toggle = document.getElementById('toggle-' + deptId);
                if (content && toggle) {
                    content.classList.toggle('expanded');
                    toggle.classList.toggle('expanded');
                }
            }

            function jumpToDept(targetId) {
                if (!targetId) return;
                var el = document.getElementById(targetId);
                if (el) {
                    var deptId = targetId.replace('dept-', '');
                    var content = document.getElementById('content-' + deptId);
                    var toggle = document.getElementById('toggle-' + deptId);
                    if (content && !content.classList.contains('expanded')) {
                        content.classList.add('expanded');
                        if (toggle) toggle.classList.add('expanded');
                    }
                    el.scrollIntoView({behavior: 'smooth', block: 'center'});
                    el.classList.add('highlight');
                    setTimeout(function() { el.classList.remove('highlight'); }, 2500);
                }
                document.getElementById('ministrySearchSelect').value = '';
            }
        </script>
        """

        # Calculate height and render
        num_depts = len(all_members_by_dept)
        estimated_height = 150 + (num_depts * 80)
        components.html(ministry_breakdown_html, height=estimated_height, scrolling=True)
    else:
        # Empty state
        st.markdown(f"""
        <div style="text-align: center; padding: 4rem 2rem; background: {page_colors['card_bg']}; border: 2px dashed {page_colors['text_muted']};">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📋</div>
            <div style="font-family: 'Inter', sans-serif; font-size: 1.5rem; color: {page_colors['text_muted']}; font-weight: 700; text-transform: uppercase; letter-spacing: 2px;">
                No {selected_ministry} check-ins yet today
            </div>
            <div style="font-size: 1rem; color: {page_colors['text_muted']}; margin-top: 1rem; font-weight: 500;">
                Be the first to check in!
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Show all members greyed out with collapsible sections
        if all_members_by_dept:
            st.markdown(f'<div style="font-family: \'Inter\', sans-serif; font-size: 1.5rem; font-weight: 900; color: {page_colors["primary"]}; margin-top: 2rem; margin-bottom: 1rem;">Attendees by Department</div>', unsafe_allow_html=True)

            # Build search options
            all_depts_empty = sorted(all_members_by_dept.keys(), key=str.lower)

            # Build collapsible HTML for empty state
            empty_ministry_html = f"""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
                * {{ font-family: 'Inter', sans-serif !important; box-sizing: border-box; }}
                .cell-collapsible {{ cursor: pointer; user-select: none; transition: all 0.2s ease; }}
                .cell-collapsible:hover {{ opacity: 0.8; }}
                .cell-content {{ overflow: hidden; transition: max-height 0.3s ease-out, opacity 0.2s ease-out; max-height: 0; opacity: 0; padding: 0; }}
                .cell-content.expanded {{ max-height: 2000px; opacity: 1; padding-top: 0.5rem; }}
                .cell-toggle {{ display: inline-block; margin-right: 0.5rem; transition: transform 0.2s ease; font-size: 0.85rem; }}
                .cell-toggle.expanded {{ transform: rotate(90deg); }}
                .expand-collapse-btn {{ background: transparent; border: 1px solid {page_colors['primary']}; color: {page_colors['primary']}; padding: 0.4rem 1rem; border-radius: 4px; cursor: pointer; font-family: 'Inter', sans-serif; font-size: 0.85rem; font-weight: 600; transition: all 0.2s ease; min-width: 120px; }}
                .expand-collapse-btn:hover {{ background: {page_colors['primary']}; color: {page_colors['background']}; }}
                .search-select {{ padding: 0.5rem; border: 1px solid {page_colors['primary']}; border-radius: 4px; font-family: 'Inter', sans-serif; font-size: 0.9rem; font-weight: 500; background: {page_colors['background']}; color: {page_colors['text']}; min-width: 200px; cursor: pointer; }}
                .controls-row {{ display: flex; gap: 1rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; }}
                .name-badge-pending {{ background: {page_colors['background']}; border: 1px solid {page_colors['text_muted']}; color: {page_colors['text_muted']}; padding: 0.6rem 1.2rem; margin: 0.4rem 0.4rem 0.4rem 0; border-radius: 0px; display: inline-block; font-family: 'Inter', sans-serif; font-weight: 600; font-size: 0.9rem; letter-spacing: 0.5px; opacity: 0.5; }}
                .name-badge-name {{ display: block; }}
                .name-badge-role {{ display: block; font-size: 0.8em; font-weight: 400; text-transform: none; letter-spacing: normal; opacity: 0.95; }}
                .role-row {{ margin-bottom: 0.8rem; }}
                .role-label {{ font-family: 'Inter', sans-serif; font-size: 0.85rem; font-weight: 600; color: {page_colors['text_muted']}; margin-right: 0.5rem; display: inline; }}
                .dept-header {{ font-family: 'Inter', sans-serif; font-size: 1.3rem; font-weight: 900; color: {page_colors['primary']}; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 0.3rem; }}
                .dept-container {{ margin-bottom: 1rem; padding: 0.5rem; border-radius: 8px; transition: all 0.3s ease; }}
                .count-label {{ color: {page_colors['text_muted']}; font-size: 0.85rem; font-weight: normal; text-transform: none; letter-spacing: normal; }}
                .highlight {{ box-shadow: 0 0 20px {page_colors['primary']}; background-color: {page_colors['light']}20; }}
            </style>
            <div class="controls-row">
                <select id="emptyMinistrySearchSelect" class="search-select" onchange="jumpToDeptEmpty(this.value)">
                    <option value="">Jump to...</option>
            """

            for dept_name in all_depts_empty:
                target_id = f"empty-dept-{dept_name.replace(' ', '-').replace(':', '-').replace(chr(39), '').lower()}"
                empty_ministry_html += f'<option value="{target_id}">{dept_name}</option>'

            empty_ministry_html += f"""
                </select>
                <button id="emptyMinistryToggleAllBtn" class="expand-collapse-btn" onclick="toggleAllMinistryEmpty()">Expand All</button>
            </div>
            """

            for dept in all_depts_empty:
                all_names = all_members_by_dept.get(dept, [])
                role_grouped_badges = build_role_grouped_badges(
                    all_names, set(), name_to_role,
                    "name-badge-pending", "name-badge-pending"
                )
                dept_id = dept.replace(" ", "-").replace(":", "-").replace("'", "").lower()

                empty_ministry_html += f"""
                <div id="empty-dept-{dept_id}" class="dept-container">
                    <div class="cell-collapsible dept-header" onclick="toggleDeptEmpty('{dept_id}')">
                        <span id="empty-toggle-{dept_id}" class="cell-toggle">▶</span>
                        {dept} <span class="count-label">(0/{len(all_names)})</span>
                    </div>
                    <div id="empty-content-{dept_id}" class="cell-content">
                        {role_grouped_badges}
                    </div>
                </div>
                """

            empty_ministry_html += """
            <script>
                var emptyMinistryIsExpanded = false;
                function toggleAllMinistryEmpty() {
                    var btn = document.getElementById('emptyMinistryToggleAllBtn');
                    if (emptyMinistryIsExpanded) {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                        btn.textContent = 'Expand All';
                        emptyMinistryIsExpanded = false;
                    } else {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                        btn.textContent = 'Collapse All';
                        emptyMinistryIsExpanded = true;
                    }
                }
                function toggleDeptEmpty(deptId) {
                    var content = document.getElementById('empty-content-' + deptId);
                    var toggle = document.getElementById('empty-toggle-' + deptId);
                    if (content && toggle) {
                        content.classList.toggle('expanded');
                        toggle.classList.toggle('expanded');
                    }
                }
                function jumpToDeptEmpty(targetId) {
                    if (!targetId) return;
                    var el = document.getElementById(targetId);
                    if (el) {
                        var deptId = targetId.replace('empty-dept-', '');
                        var content = document.getElementById('empty-content-' + deptId);
                        var toggle = document.getElementById('empty-toggle-' + deptId);
                        if (content && !content.classList.contains('expanded')) {
                            content.classList.add('expanded');
                            if (toggle) toggle.classList.add('expanded');
                        }
                        el.scrollIntoView({behavior: 'smooth', block: 'center'});
                        el.classList.add('highlight');
                        setTimeout(function() { el.classList.remove('highlight'); }, 2500);
                    }
                    document.getElementById('emptyMinistrySearchSelect').value = '';
                }
            </script>
            """

            num_depts_empty = len(all_members_by_dept)
            estimated_height_empty = 150 + (num_depts_empty * 80)
            components.html(empty_ministry_html, height=estimated_height_empty, scrolling=True)


def render_recent_checkins_table(tab_name):
    """Render a scrollable table showing recent check-ins ordered by latest first"""
    # Get today's attendance data including recent check-ins
    refresh_key = st.session_state.get('refresh_counter', 0)
    _, _, recent_checkins = get_today_attendance_data(client, SHEET_ID, refresh_key, tab_name)

    last_refresh_str = st.session_state.last_refresh_time.strftime("%H:%M:%S")

    st.markdown("---")
    st.markdown(f"""
    <div style="margin-bottom: 0.5rem; display: flex; align-items: center; justify-content: space-between;
                flex-wrap: wrap; gap: 0.5rem;">
        <span style="font-family: 'Inter', sans-serif; font-weight: 700; font-size: 1rem;
                     color: {page_colors['primary']}; text-transform: uppercase; letter-spacing: 1px;">
            Recent Check-Ins
        </span>
        <span style="
            background: {page_colors['primary']}20;
            color: {page_colors['primary']};
            padding: 0.5rem 1rem;
            border-radius: 4px;
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            font-weight: 600;
            border: 1px solid {page_colors['primary']}40;
        ">
            Last refresh: {last_refresh_str}
        </span>
    </div>
    """, unsafe_allow_html=True)

    if not recent_checkins:
        return

    # Create dataframe for the table
    table_data = []
    for timestamp_str, name_cell_group in recent_checkins:
        # Extract just the time part (HH:MM:SS) from timestamp
        time_part = timestamp_str[11:19] if len(timestamp_str) >= 19 else timestamp_str
        table_data.append({
            "Time": time_part,
            "Name - Cell Group": name_cell_group
        })

    df = pd.DataFrame(table_data)

    # Calculate height for 5 rows (approximately 35px per row + header)
    row_height = 35
    header_height = 38
    max_visible_rows = 5
    table_height = header_height + (row_height * min(len(table_data), max_visible_rows))

    # Display as a scrollable dataframe
    # Calculate appropriate height - always provide a positive integer
    display_height = table_height if len(table_data) > max_visible_rows else header_height + (row_height * len(table_data))
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=max(display_height, header_height + row_height)  # Ensure minimum height for at least 1 row
    )


def render_dashboard(tab_name, group_by_zone=False):
    """Render the dashboard section for a specific tab.
    If group_by_zone=True, groups by Zone instead of Cell Group."""
    st.markdown("---")

    # Leaders (group_by_zone): Refresh toolbar above the zone dashboard.
    if group_by_zone:
        col_refresh, _col_trailing = st.columns([1, 4])
        with col_refresh:
            if st.button("Refresh", type="secondary", key=f"refresh_btn_{tab_name}", use_container_width=True):
                st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                st.session_state.last_refresh_time = get_now_myt()
                get_today_attendance_data.clear()
                get_options_from_sheet.clear()
                get_cell_to_zone_mapping.clear()
                get_newcomers_count.clear()
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        today_myt = get_today_myt_date()
                        redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
                    except Exception:
                        pass
                st.rerun()

    # Get today's attendance data for the specific tab
    with st.spinner("Loading dashboard data..."):
        refresh_key = st.session_state.get('refresh_counter', 0)
        cell_group_data, checked_in_list, _ = get_today_attendance_data(client, SHEET_ID, refresh_key, tab_name)

    total_checked_in = len(checked_in_list)
    # Fetch newcomers count (uses Redis cache on initial load, fresh pull when buttons clear cache)
    total_newcomers, newcomers_list = get_newcomers_count(client, SHEET_ID, refresh_key)

    # Get all team members from Options tab and group by cell group
    all_members_by_cell_group = {}
    name_to_role = {}
    options_data, name_to_role, _ = get_options_from_sheet(client, SHEET_ID)
    if options_data:
        for header, values in options_data.items():
            for value in values:
                name, cell_group = parse_name_cell_group(value)
                if name and cell_group:
                    if cell_group not in all_members_by_cell_group:
                        all_members_by_cell_group[cell_group] = []
                    all_members_by_cell_group[cell_group].append(name)

    # Create a set of checked-in names for quick lookup
    checked_in_names_set = set()
    for name_cell_group in checked_in_list:
        name, _ = parse_name_cell_group(name_cell_group)
        if name:
            checked_in_names_set.add(name)

    # If grouping by zone, convert cell_group_data to zone_data
    if group_by_zone:
        # Get zone mapping from Key Values tab (cell name -> zone)
        cell_to_zone, zone_error = get_cell_to_zone_mapping(client, SHEET_ID)

        # Convert cell groups to zones
        zone_data = {}
        for cell_group, names in cell_group_data.items():
            # Look up zone from Key Values (case-insensitive)
            zone = cell_to_zone.get(cell_group.lower(), cell_group)
            if zone not in zone_data:
                zone_data[zone] = []
            zone_data[zone].extend(names)
        display_data = zone_data
        group_label = "Zone"
    else:
        display_data = cell_group_data
        group_label = "Cell Group"

    # Convert hex color to RGB for rgba shadows
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    primary_rgb = hex_to_rgb(daily_colors['primary'])

    # Modern Edgy Dashboard Styling with Dynamic Colors
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@700;900&display=swap');

        .kpi-card {{
            background: {page_colors['card_bg']};
            padding: 2rem 2.5rem;
            border-radius: 0px;
            border-left: 6px solid {page_colors['primary']};
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.15);
            transition: all 0.3s ease;
        }}
        .kpi-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.25);
            border-left-width: 8px;
        }}
        .kpi-label {{
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 700;
            color: {page_colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .kpi-number {{
            font-family: 'Inter', sans-serif;
            font-size: 5.5rem;
            font-weight: 900;
            color: {page_colors['primary']};
            line-height: 1;
            margin: 0.5rem 0;
            text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
        }}
        .kpi-subtitle {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            color: {page_colors['text_muted']};
            margin-top: 0.5rem;
            font-weight: 500;
        }}
        .dashboard-section {{
            background: {page_colors['card_bg']};
            padding: 2rem;
            border-radius: 0px;
            border: 2px solid {page_colors['primary']};
            margin: 2rem 0;
        }}
        .section-title {{
            font-family: 'Inter', sans-serif;
            font-size: 1.8rem;
            font-weight: 900;
            color: {page_colors['primary']};
            text-transform: uppercase;
            letter-spacing: 3px;
            margin-bottom: 1.5rem;
            border-bottom: 3px solid {page_colors['primary']};
            padding-bottom: 0.5rem;
            display: inline-block;
        }}
        .name-badge {{
            background: {page_colors['background']};
            border: 1px solid {page_colors['primary']};
            color: {page_colors['primary']};
            padding: 0.6rem 1.2rem;
            margin: 0.4rem 0.4rem 0.4rem 0;
            border-radius: 0px;
            display: inline-block;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            font-size: 0.9rem;
            letter-spacing: 0.5px;
            transition: all 0.2s ease;
        }}
        .name-badge:hover {{
            background: {page_colors['primary']};
            color: {page_colors['background']};
            transform: scale(1.05);
        }}
        .name-badge-pending {{
            background: {page_colors['background']};
            border: 1px solid {page_colors['text_muted']};
            color: {page_colors['text_muted']};
            padding: 0.6rem 1.2rem;
            margin: 0.4rem 0.4rem 0.4rem 0;
            border-radius: 0px;
            display: inline-block;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            font-size: 0.9rem;
            letter-spacing: 0.5px;
            opacity: 0.5;
        }}
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            background: {page_colors['card_bg']};
            border: 2px dashed {page_colors['text_muted']};
            border-radius: 0px;
        }}
        .empty-state-text {{
            font-family: 'Inter', sans-serif;
            font-size: 1.5rem;
            color: {page_colors['text_muted']};
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 2px;
        }}
    </style>
    """, unsafe_allow_html=True)

    # KPI Cards - Total Checked In and Total Newcomers (side by side)
    kpi_col1, kpi_col2 = st.columns(2)
    with kpi_col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Checked In Today</div>
            <div class="kpi-number">{total_checked_in}</div>
            <div class="kpi-subtitle">People checked in as of now</div>
        </div>
        """, unsafe_allow_html=True)
    with kpi_col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Newcomers</div>
            <div class="kpi-number">{total_newcomers}</div>
        </div>
        """, unsafe_allow_html=True)

        # Display newcomers list if any
        if newcomers_list:
            st.markdown("### Newcomer Details")
            for newcomer in newcomers_list:
                name = newcomer['name'] if newcomer['name'] else "(No name)"
                cell = newcomer['cell'] if newcomer['cell'] else "(Not assigned)"
                st.markdown(f"- **{name}** → {cell}")

    # Zone tiles (only for zone grouping)
    if group_by_zone and total_checked_in > 0 and cell_group_data:
        # Get zone mapping from Key Values tab
        cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)

        # Aggregate by zone for tiles
        zone_counts = {}
        for cell_group, names in cell_group_data.items():
            # Look up zone from Key Values (case-insensitive)
            zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
            if zone not in zone_counts:
                zone_counts[zone] = 0
            zone_counts[zone] += len(names)

        sorted_zones_for_tiles = sorted(zone_counts.items(), key=lambda x: x[0].lower())

        # Create tiles in a grid
        st.markdown(f"""
        <style>
            .zone-tiles-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 1rem;
                margin-bottom: 2rem;
            }}
            .zone-tile {{
                background: {page_colors['card_bg']};
                border: 2px solid {page_colors['primary']};
                padding: 1.2rem 1.5rem;
                min-width: 140px;
                flex: 1;
                text-align: center;
                transition: all 0.2s ease;
            }}
            .zone-tile:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
            }}
            .zone-name {{
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 700;
                color: {page_colors['text_muted']};
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 0.3rem;
            }}
            .zone-count {{
                font-family: 'Inter', sans-serif;
                font-size: 2.5rem;
                font-weight: 900;
                color: {page_colors['primary']};
                line-height: 1;
            }}
        </style>
        <div class="zone-tiles-container">
            {''.join([f'<div class="zone-tile"><div class="zone-name">{zone}</div><div class="zone-count">{count}</div></div>' for zone, count in sorted_zones_for_tiles])}
        </div>
        """, unsafe_allow_html=True)

    if total_checked_in > 0:
        # Bar Chart Section
        chart_title = "Attendance by Zone" if group_by_zone else "Check-Ins by Cell Group"
        st.markdown(f'<div class="section-title">{chart_title}</div>', unsafe_allow_html=True)

        # Prepare data for bar chart - sort by count descending
        sorted_groups = sorted(display_data.items(), key=lambda x: len(x[1]), reverse=True)

        chart_data = {
            group_label: [group for group, _ in sorted_groups],
            'Count': [len(names) for _, names in sorted_groups]
        }
        df_chart = pd.DataFrame(chart_data)

        # Create bar chart with modern edgy style
        fig = px.bar(
            df_chart,
            x=group_label,
            y='Count',
            color='Count',
            color_continuous_scale=[page_colors['background'], page_colors['primary']],
            text='Count',
            labels={'Count': 'Number of People', group_label: group_label},
            height=400
        )

        # Update layout for modern edgy style
        fig.update_layout(
            plot_bgcolor=page_colors['background'],
            paper_bgcolor=page_colors['card_bg'],
            font=dict(family='Inter, sans-serif', size=12, color=page_colors['primary']),
            xaxis=dict(
                title=dict(font=dict(size=14, color=page_colors['primary'], family='Inter')),
                tickfont=dict(color=page_colors['text_muted'], family='Inter'),
                gridcolor=page_colors['text_muted'],
                linecolor=page_colors['primary'],
                linewidth=2,
                categoryorder='total descending'
            ),
            yaxis=dict(
                title=dict(font=dict(size=14, color=page_colors['primary'], family='Inter')),
                tickfont=dict(color=page_colors['text_muted'], family='Inter'),
                gridcolor=page_colors['text_muted'],
                linecolor=page_colors['primary'],
                linewidth=2
            ),
            coloraxis_showscale=False,
            showlegend=False,
            margin=dict(l=50, r=50, t=60, b=50)
        )

        # Update bar style
        fig.update_traces(
            textfont=dict(size=14, color=page_colors['background'], family='Inter', weight='bold'),
            textposition='inside',
            insidetextanchor='middle',
            marker=dict(line=dict(color=page_colors['primary'], width=2)),
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>',
            hoverlabel=dict(bgcolor=page_colors['background'], font=dict(color=page_colors['primary'], family='Inter'))
        )

        st.plotly_chart(fig, use_container_width=True)

        # Names Breakdown Section
        names_title = "Attendees by Zone" if group_by_zone else "Attendees by Cell Group"
        st.markdown(f'<div class="section-title">{names_title}</div>', unsafe_allow_html=True)

        # Build search options for the HTML select
        all_cell_groups_search = sorted(set(all_members_by_cell_group.keys()) | set(display_data.keys()), key=str.lower)

        if group_by_zone:
            cell_to_zone_map_search, _ = get_cell_to_zone_mapping(client, SHEET_ID)
            all_zones_search = set()
            for cell_group in all_cell_groups_search:
                zone = cell_to_zone_map_search.get(cell_group.lower(), cell_group)
                all_zones_search.add(zone)
            zones_list = sorted(all_zones_search, key=str.lower)
            searchable_groups = [("zone", z) for z in zones_list] + [("cell", c) for c in all_cell_groups_search]
        else:
            searchable_groups = [("group", g) for g in all_cell_groups_search]

        # Build collapsible breakdown HTML - must be in single components.html() for JS to work
        breakdown_html = f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

            * {{
                font-family: 'Inter', sans-serif !important;
                box-sizing: border-box;
            }}
            body {{
                font-family: 'Inter', sans-serif !important;
                margin: 0;
                padding: 0;
            }}
            .cell-collapsible {{
                cursor: pointer;
                user-select: none;
                transition: all 0.2s ease;
            }}
            .cell-collapsible:hover {{
                opacity: 0.8;
            }}
            .cell-content {{
                overflow: hidden;
                transition: max-height 0.3s ease-out, opacity 0.2s ease-out, padding 0.3s ease-out;
                max-height: 0;
                opacity: 0;
                padding: 0;
            }}
            .cell-content.expanded {{
                max-height: 2000px;
                opacity: 1;
                padding-top: 0.5rem;
            }}
            .cell-toggle {{
                display: inline-block;
                margin-right: 0.5rem;
                transition: transform 0.2s ease;
                font-size: 0.85rem;
            }}
            .cell-toggle.expanded {{
                transform: rotate(90deg);
            }}
            .expand-collapse-btn {{
                background: transparent;
                border: 1px solid {page_colors['primary']};
                color: {page_colors['primary']};
                padding: 0.4rem 1rem;
                border-radius: 4px;
                cursor: pointer;
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 600;
                transition: all 0.2s ease;
                min-width: 120px;
            }}
            .expand-collapse-btn:hover {{
                background: {page_colors['primary']};
                color: {page_colors['background']};
            }}
            .search-select {{
                padding: 0.5rem;
                border: 1px solid {page_colors['primary']};
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 500;
                background: {page_colors['background']};
                color: {page_colors['text']};
                min-width: 200px;
                cursor: pointer;
            }}
            .search-select:focus {{
                outline: none;
                border-color: {page_colors['primary']};
                box-shadow: 0 0 5px {page_colors['primary']}40;
            }}
            .controls-row {{
                display: flex;
                gap: 1rem;
                align-items: center;
                margin-bottom: 1rem;
                flex-wrap: wrap;
            }}
            .name-badge {{
                display: inline-block;
                background: {page_colors['primary']};
                color: {page_colors['background']};
                padding: 0.6rem 1.2rem;
                margin: 0.25rem;
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                transition: all 0.2s ease;
                cursor: default;
            }}
            .name-badge:hover {{
                transform: scale(1.05);
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            }}
            .name-badge-pending {{
                display: inline-block;
                background: transparent;
                color: {page_colors['text_muted']};
                border: 1px solid {page_colors['text_muted']};
                padding: 0.6rem 1.2rem;
                margin: 0.25rem;
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                opacity: 0.5;
                transition: all 0.2s ease;
                cursor: default;
            }}
            .name-badge-pending:hover {{
                transform: scale(1.05);
                opacity: 0.7;
            }}
            .name-badge-name {{
                display: block;
            }}
            .name-badge-role {{
                display: block;
                font-size: 0.8em;
                font-weight: 400;
                text-transform: none;
                letter-spacing: normal;
                opacity: 0.95;
            }}
            .role-row {{
                margin-bottom: 0.8rem;
            }}
            .role-label {{
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 600;
                color: {page_colors['text_muted']};
                margin-right: 0.5rem;
                display: inline;
            }}
            .zone-header {{
                font-family: 'Inter', sans-serif;
                font-size: 1.3rem;
                font-weight: 900;
                color: {page_colors['primary']};
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 1rem;
                margin-top: 1.5rem;
            }}
            .zone-header:first-of-type {{
                margin-top: 0;
            }}
            .cell-header {{
                font-family: 'Inter', sans-serif;
                font-size: 1rem;
                font-weight: 700;
                color: {page_colors['text_muted']};
                letter-spacing: 1px;
                margin-bottom: 0.3rem;
            }}
            .cell-container {{
                margin-left: 1.5rem;
                margin-bottom: 0.8rem;
                padding: 0.5rem;
                border-radius: 8px;
                transition: all 0.3s ease;
            }}
            .group-header {{
                font-family: 'Inter', sans-serif;
                font-size: 1.3rem;
                font-weight: 900;
                color: {page_colors['primary']};
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 0.3rem;
            }}
            .group-container {{
                margin-bottom: 1rem;
                padding: 0.5rem;
                border-radius: 8px;
                transition: all 0.3s ease;
            }}
            .count-label {{
                color: {page_colors['text_muted']};
                font-size: 0.85rem;
                font-weight: normal;
                text-transform: none;
                letter-spacing: normal;
            }}
            .highlight {{
                box-shadow: 0 0 20px {page_colors['primary']};
                background-color: {page_colors['light']}20;
            }}
        </style>
        <div class="controls-row">
            <select id="searchSelect" class="search-select" onchange="jumpToGroup(this.value)">
                <option value="">Jump to...</option>
        """

        # Add search options to dropdown
        for group_type, group_name in searchable_groups:
            if group_type == "zone":
                target_id = f"group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = f"Zone: {group_name}"
            elif group_type == "cell":
                target_id = f"cell-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = f"Cell: {group_name}"
            else:
                target_id = f"group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = group_name
            breakdown_html += f'<option value="{target_id}">{display_name}</option>'

        breakdown_html += f"""
            </select>
            <button id="toggleAllBtn" class="expand-collapse-btn" onclick="toggleAll()">Expand All</button>
        </div>
        """

        # Display names for each group
        if group_by_zone:
            # For zone grouping, show Zone -> Cell -> Names hierarchy
            # Build zone -> cell -> names structure (checked-in only)
            zone_cell_names = {}
            cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
            for cell_group, names in cell_group_data.items():
                zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                if zone not in zone_cell_names:
                    zone_cell_names[zone] = {}
                if cell_group not in zone_cell_names[zone]:
                    zone_cell_names[zone][cell_group] = []
                zone_cell_names[zone][cell_group].extend(names)

            # Build zone -> cell -> all members structure (for pending display)
            zone_cell_all_members = {}
            for cell_group, members in all_members_by_cell_group.items():
                zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                if zone not in zone_cell_all_members:
                    zone_cell_all_members[zone] = {}
                if cell_group not in zone_cell_all_members[zone]:
                    zone_cell_all_members[zone][cell_group] = []
                zone_cell_all_members[zone][cell_group].extend(members)

            # Get all zones (from both checked-in and all members)
            all_zones = set(zone_cell_names.keys()) | set(zone_cell_all_members.keys())

            # Display with hierarchy
            for zone in sorted(all_zones, key=str.lower):
                cells_checked_in = zone_cell_names.get(zone, {})
                cells_all = zone_cell_all_members.get(zone, {})
                all_cells_in_zone = set(cells_checked_in.keys()) | set(cells_all.keys())

                total_checked_in_zone = sum(len(names) for names in cells_checked_in.values())
                total_in_zone = sum(len(members) for members in cells_all.values())

                zone_id = zone.replace(" ", "-").replace("'", "").lower()
                breakdown_html += f"""
                <div id="group-{zone_id}" class="zone-header">
                    {zone} <span class="count-label">({total_checked_in_zone}/{total_in_zone})</span>
                </div>
                """

                # Show each cell within the zone
                for cell_group in sorted(all_cells_in_zone, key=str.lower):
                    checked_in_names = cells_checked_in.get(cell_group, [])
                    all_names_in_cell = cells_all.get(cell_group, [])

                    checked_count = len(checked_in_names)
                    total_count = len(all_names_in_cell)
                    role_grouped_badges = build_role_grouped_badges(
                        all_names_in_cell, checked_in_names_set, name_to_role,
                        "name-badge", "name-badge-pending"
                    )

                    cell_id = cell_group.replace(" ", "-").replace("'", "").lower()
                    breakdown_html += f"""
                    <div id="cell-{cell_id}" class="cell-container">
                        <div class="cell-collapsible cell-header" onclick="toggleCell('{cell_id}')">
                            <span id="toggle-{cell_id}" class="cell-toggle">▶</span>
                            {cell_group} <span class="count-label">({checked_count}/{total_count})</span>
                        </div>
                        <div id="content-{cell_id}" class="cell-content">
                            {role_grouped_badges}
                        </div>
                    </div>
                    """
        else:
            # Regular cell group display - sorted alphabetically
            # Show all cell groups from options (not just those with check-ins)
            all_cell_groups = set(all_members_by_cell_group.keys()) | set(display_data.keys())
            sorted_groups_alpha = sorted(all_cell_groups, key=str.lower)

            for group_name in sorted_groups_alpha:
                checked_in_names = display_data.get(group_name, [])
                all_names_in_group = all_members_by_cell_group.get(group_name, [])

                total_in_group = len(all_names_in_group)
                checked_count = len(checked_in_names)
                role_grouped_badges = build_role_grouped_badges(
                    all_names_in_group, checked_in_names_set, name_to_role,
                    "name-badge", "name-badge-pending"
                )

                group_id = group_name.replace(" ", "-").replace("'", "").lower()
                breakdown_html += f"""
                <div id="group-{group_id}" class="group-container">
                    <div class="cell-collapsible group-header" onclick="toggleCell('{group_id}')">
                        <span id="toggle-{group_id}" class="cell-toggle">▶</span>
                        {group_name} <span class="count-label">({checked_count}/{total_in_group})</span>
                    </div>
                    <div id="content-{group_id}" class="cell-content">
                        {role_grouped_badges}
                    </div>
                </div>
                """

        # Add JavaScript and close the HTML
        breakdown_html += """
        <script>
            var isExpanded = false;

            function toggleAll() {
                var btn = document.getElementById('toggleAllBtn');
                if (isExpanded) {
                    // Collapse all
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                    btn.textContent = 'Expand All';
                    isExpanded = false;
                } else {
                    // Expand all
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                    btn.textContent = 'Collapse All';
                    isExpanded = true;
                }
            }

            function toggleCell(cellId) {
                var content = document.getElementById('content-' + cellId);
                var toggle = document.getElementById('toggle-' + cellId);
                if (content && toggle) {
                    content.classList.toggle('expanded');
                    toggle.classList.toggle('expanded');
                }
            }

            function jumpToGroup(targetId) {
                if (!targetId) return;

                var el = document.getElementById(targetId);
                if (el) {
                    // Expand the cell content if it's collapsed
                    var content = document.getElementById('content-' + targetId.replace('group-', '').replace('cell-', ''));
                    var toggle = document.getElementById('toggle-' + targetId.replace('group-', '').replace('cell-', ''));
                    if (content && !content.classList.contains('expanded')) {
                        content.classList.add('expanded');
                        if (toggle) toggle.classList.add('expanded');
                    }

                    // Scroll to element
                    el.scrollIntoView({behavior: 'smooth', block: 'center'});

                    // Highlight effect
                    el.classList.add('highlight');
                    setTimeout(function() {
                        el.classList.remove('highlight');
                    }, 2500);
                }

                // Reset dropdown
                document.getElementById('searchSelect').value = '';
            }
        </script>
        """

        # Calculate height based on content
        num_items = len(all_members_by_cell_group) if not group_by_zone else sum(len(cells) for cells in zone_cell_all_members.values()) if 'zone_cell_all_members' in dir() else 10
        estimated_height = 150 + (num_items * 60)  # Base height + per-item height
        components.html(breakdown_html, height=estimated_height, scrolling=True)
    else:
        # Show empty state message and all pending members greyed out
        st.markdown(f"""
        <div class="empty-state">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📋</div>
            <div class="empty-state-text">No check-ins yet today</div>
            <div style="font-size: 1rem; color: {page_colors['text_muted']}; margin-top: 1rem; font-weight: 500;">
                Be the first to check in!
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Show all members greyed out even when no check-ins
        if all_members_by_cell_group:
            st.markdown(f'<div class="section-title">{"Attendees by Zone" if group_by_zone else "Attendees by Cell Group"}</div>', unsafe_allow_html=True)

            # Build search options for the HTML select (empty state)
            all_cell_groups_search = sorted(all_members_by_cell_group.keys(), key=str.lower)

            if group_by_zone:
                cell_to_zone_map_search, _ = get_cell_to_zone_mapping(client, SHEET_ID)
                all_zones_search = set()
                for cell_group in all_cell_groups_search:
                    zone = cell_to_zone_map_search.get(cell_group.lower(), cell_group)
                    all_zones_search.add(zone)
                zones_list = sorted(all_zones_search, key=str.lower)
                searchable_groups_empty = [("zone", z) for z in zones_list] + [("cell", c) for c in all_cell_groups_search]
            else:
                searchable_groups_empty = [("group", g) for g in all_cell_groups_search]

            # Build collapsible breakdown HTML for empty state
            empty_breakdown_html = f"""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

                * {{
                    font-family: 'Inter', sans-serif !important;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: 'Inter', sans-serif !important;
                    margin: 0;
                    padding: 0;
                }}
                .cell-collapsible {{
                    cursor: pointer;
                    user-select: none;
                    transition: all 0.2s ease;
                }}
                .cell-collapsible:hover {{
                    opacity: 0.8;
                }}
                .cell-content {{
                    overflow: hidden;
                    transition: max-height 0.3s ease-out, opacity 0.2s ease-out, padding 0.3s ease-out;
                    max-height: 0;
                    opacity: 0;
                    padding: 0;
                }}
                .cell-content.expanded {{
                    max-height: 2000px;
                    opacity: 1;
                    padding-top: 0.5rem;
                }}
                .cell-toggle {{
                    display: inline-block;
                    margin-right: 0.5rem;
                    transition: transform 0.2s ease;
                    font-size: 0.85rem;
                }}
                .cell-toggle.expanded {{
                    transform: rotate(90deg);
                }}
                .expand-collapse-btn {{
                    background: transparent;
                    border: 1px solid {page_colors['primary']};
                    color: {page_colors['primary']};
                    padding: 0.4rem 1rem;
                    border-radius: 4px;
                    cursor: pointer;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.85rem;
                    font-weight: 600;
                    transition: all 0.2s ease;
                    min-width: 120px;
                }}
                .expand-collapse-btn:hover {{
                    background: {page_colors['primary']};
                    color: {page_colors['background']};
                }}
                .search-select {{
                    padding: 0.5rem;
                    border: 1px solid {page_colors['primary']};
                    border-radius: 4px;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.9rem;
                    font-weight: 500;
                    background: {page_colors['background']};
                    color: {page_colors['text']};
                    min-width: 200px;
                    cursor: pointer;
                }}
                .search-select:focus {{
                    outline: none;
                    border-color: {page_colors['primary']};
                    box-shadow: 0 0 5px {page_colors['primary']}40;
                }}
                .controls-row {{
                    display: flex;
                    gap: 1rem;
                    align-items: center;
                    margin-bottom: 1rem;
                    flex-wrap: wrap;
                }}
                .name-badge-pending {{
                    display: inline-block;
                    background: transparent;
                    color: {page_colors['text_muted']};
                    border: 1px solid {page_colors['text_muted']};
                    padding: 0.6rem 1.2rem;
                    margin: 0.25rem;
                    border-radius: 4px;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.9rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    opacity: 0.5;
                    transition: all 0.2s ease;
                    cursor: default;
                }}
                .name-badge-pending:hover {{
                    transform: scale(1.05);
                    opacity: 0.7;
                }}
                .name-badge-name {{ display: block; }}
                .role-row {{ margin-bottom: 0.8rem; }}
                .role-label {{
                    font-family: 'Inter', sans-serif;
                    font-size: 0.85rem;
                    font-weight: 600;
                    color: {page_colors['text_muted']};
                    margin-right: 0.5rem;
                    display: inline;
                }}
                .zone-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1.3rem;
                    font-weight: 900;
                    color: {page_colors['primary']};
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    margin-bottom: 1rem;
                    margin-top: 1.5rem;
                }}
                .zone-header:first-of-type {{
                    margin-top: 0;
                }}
                .cell-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1rem;
                    font-weight: 700;
                    color: {page_colors['text_muted']};
                    letter-spacing: 1px;
                    margin-bottom: 0.3rem;
                }}
                .cell-container {{
                    margin-left: 1.5rem;
                    margin-bottom: 0.8rem;
                    padding: 0.5rem;
                    border-radius: 8px;
                    transition: all 0.3s ease;
                }}
                .group-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1.3rem;
                    font-weight: 900;
                    color: {page_colors['primary']};
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    margin-bottom: 0.3rem;
                }}
                .group-container {{
                    margin-bottom: 1rem;
                    padding: 0.5rem;
                    border-radius: 8px;
                    transition: all 0.3s ease;
                }}
                .count-label {{
                    color: {page_colors['text_muted']};
                    font-size: 0.85rem;
                    font-weight: normal;
                    text-transform: none;
                    letter-spacing: normal;
                }}
                .highlight {{
                    box-shadow: 0 0 20px {page_colors['primary']};
                    background-color: {page_colors['light']}20;
                }}
            </style>
            <div class="controls-row">
                <select id="searchSelectEmpty" class="search-select" onchange="jumpToGroup(this.value)">
                    <option value="">Jump to...</option>
            """

            # Add search options to dropdown
            for group_type, group_name in searchable_groups_empty:
                if group_type == "zone":
                    target_id = f"group-empty-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = f"Zone: {group_name}"
                elif group_type == "cell":
                    target_id = f"cell-empty-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = f"Cell: {group_name}"
                else:
                    target_id = f"group-empty-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = group_name
                empty_breakdown_html += f'<option value="{target_id}">{display_name}</option>'

            empty_breakdown_html += f"""
                </select>
                <button id="toggleAllBtnEmpty" class="expand-collapse-btn" onclick="toggleAll()">Expand All</button>
            </div>
            """

            if group_by_zone:
                # Build zone -> cell -> all members structure
                cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
                zone_cell_all_members = {}
                for cell_group, members in all_members_by_cell_group.items():
                    zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                    if zone not in zone_cell_all_members:
                        zone_cell_all_members[zone] = {}
                    if cell_group not in zone_cell_all_members[zone]:
                        zone_cell_all_members[zone][cell_group] = []
                    zone_cell_all_members[zone][cell_group].extend(members)

                for zone in sorted(zone_cell_all_members.keys(), key=str.lower):
                    cells_all = zone_cell_all_members[zone]
                    total_in_zone = sum(len(members) for members in cells_all.values())

                    zone_id = zone.replace(" ", "-").replace("'", "").lower()
                    empty_breakdown_html += f"""
                    <div id="group-empty-{zone_id}" class="zone-header">
                        {zone} <span class="count-label">(0/{total_in_zone})</span>
                    </div>
                    """

                    for cell_group in sorted(cells_all.keys(), key=str.lower):
                        all_names_in_cell = cells_all[cell_group]
                        role_grouped_badges = build_role_grouped_badges(
                            all_names_in_cell, set(), name_to_role,
                            "name-badge-pending", "name-badge-pending"
                        )

                        cell_id = cell_group.replace(" ", "-").replace("'", "").lower()
                        empty_breakdown_html += f"""
                        <div id="cell-empty-{cell_id}" class="cell-container">
                            <div class="cell-collapsible cell-header" onclick="toggleCell('empty-{cell_id}')">
                                <span id="toggle-empty-{cell_id}" class="cell-toggle">▶</span>
                                {cell_group} <span class="count-label">(0/{len(all_names_in_cell)})</span>
                            </div>
                            <div id="content-empty-{cell_id}" class="cell-content">
                                {role_grouped_badges}
                            </div>
                        </div>
                        """
            else:
                # Regular cell group display - all greyed out
                for group_name in sorted(all_members_by_cell_group.keys(), key=str.lower):
                    all_names_in_group = all_members_by_cell_group[group_name]
                    role_grouped_badges = build_role_grouped_badges(
                        all_names_in_group, set(), name_to_role,
                        "name-badge-pending", "name-badge-pending"
                    )
                    total_in_group = len(all_names_in_group)

                    group_id = group_name.replace(" ", "-").replace("'", "").lower()
                    empty_breakdown_html += f"""
                    <div id="group-empty-{group_id}" class="group-container">
                        <div class="cell-collapsible group-header" onclick="toggleCell('empty-{group_id}')">
                            <span id="toggle-empty-{group_id}" class="cell-toggle">▶</span>
                            {group_name} <span class="count-label">(0/{total_in_group})</span>
                        </div>
                        <div id="content-empty-{group_id}" class="cell-content">
                            {role_grouped_badges}
                        </div>
                    </div>
                    """

            # Add JavaScript
            empty_breakdown_html += """
            <script>
                var isExpanded = false;

                function toggleAll() {
                    var btn = document.getElementById('toggleAllBtnEmpty');
                    if (isExpanded) {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                        btn.textContent = 'Expand All';
                        isExpanded = false;
                    } else {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                        btn.textContent = 'Collapse All';
                        isExpanded = true;
                    }
                }

                function toggleCell(cellId) {
                    var content = document.getElementById('content-' + cellId);
                    var toggle = document.getElementById('toggle-' + cellId);
                    if (content && toggle) {
                        content.classList.toggle('expanded');
                        toggle.classList.toggle('expanded');
                    }
                }

                function jumpToGroup(targetId) {
                    if (!targetId) return;
                    var el = document.getElementById(targetId);
                    if (el) {
                        var cellId = targetId.replace('group-empty-', 'empty-').replace('cell-empty-', 'empty-');
                        var content = document.getElementById('content-' + cellId);
                        var toggle = document.getElementById('toggle-' + cellId);
                        if (content && !content.classList.contains('expanded')) {
                            content.classList.add('expanded');
                            if (toggle) toggle.classList.add('expanded');
                        }
                        el.scrollIntoView({behavior: 'smooth', block: 'center'});
                        el.classList.add('highlight');
                        setTimeout(function() { el.classList.remove('highlight'); }, 2500);
                    }
                    document.getElementById('searchSelectEmpty').value = '';
                }
            </script>
            """

            # Calculate height and render
            num_items_empty = len(all_members_by_cell_group) if not group_by_zone else sum(len(cells) for cells in zone_cell_all_members.values()) if 'zone_cell_all_members' in dir() else 10
            estimated_height_empty = 150 + (num_items_empty * 60)
            components.html(empty_breakdown_html, height=estimated_height_empty, scrolling=True)


if hasattr(st, "fragment"):
    @st.fragment
    def render_dashboard_fragment(tab_name, group_by_zone=False):
        render_dashboard(tab_name, group_by_zone)
else:
    def render_dashboard_fragment(tab_name, group_by_zone=False):
        render_dashboard(tab_name, group_by_zone)


if hasattr(st, "fragment"):
    @st.fragment
    def render_ministry_dashboard_fragment(selected_ministry):
        render_ministry_dashboard(selected_ministry)
else:
    def render_ministry_dashboard_fragment(selected_ministry):
        render_ministry_dashboard(selected_ministry)


def render_historical_dashboard(tab_name, target_date, colors, group_by_zone=False):
    """Render the dashboard for a historical date with custom colors.
    Args:
        tab_name: Tab name to read from
        target_date: Date string in 'YYYY-MM-DD' format
        colors: Color dictionary with 'primary', 'light', 'background', etc.
        group_by_zone: If True, groups by Zone instead of Cell Group
    """
    st.markdown("---")

    # Format date for display
    try:
        display_date_formatted = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d %b %Y")
    except:
        display_date_formatted = target_date

    st.markdown(f"""
    <div style="text-align: center; padding: 1rem; margin-bottom: 1rem;">
        <span style="
            background: {colors['primary']};
            color: {colors['background']};
            padding: 0.5rem 1rem;
            font-family: 'Inter', sans-serif;
            font-weight: 700;
            font-size: 1rem;
            letter-spacing: 1px;
        ">HISTORICAL DATA - {display_date_formatted}</span>
    </div>
    """, unsafe_allow_html=True)

    # Get historical attendance data for the specific tab and date
    with st.spinner(f"Loading data for {display_date_formatted}..."):
        cell_group_data, checked_in_list, recent_checkins = get_attendance_data_for_date(
            client, SHEET_ID, target_date, tab_name
        )

    total_checked_in = len(checked_in_list)

    # Get all team members from Options tab and group by cell group
    all_members_by_cell_group = {}
    name_to_role = {}
    options_data, name_to_role, _ = get_options_from_sheet(client, SHEET_ID)
    if options_data:
        for header, values in options_data.items():
            for value in values:
                name, cell_group = parse_name_cell_group(value)
                if name and cell_group:
                    if cell_group not in all_members_by_cell_group:
                        all_members_by_cell_group[cell_group] = []
                    all_members_by_cell_group[cell_group].append(name)

    # Create a set of checked-in names for quick lookup
    checked_in_names_set = set()
    for name_cell_group in checked_in_list:
        name, _ = parse_name_cell_group(name_cell_group)
        if name:
            checked_in_names_set.add(name)

    # If grouping by zone, convert cell_group_data to zone_data
    if group_by_zone:
        cell_to_zone, zone_error = get_cell_to_zone_mapping(client, SHEET_ID)
        zone_data = {}
        for cell_group, names in cell_group_data.items():
            zone = cell_to_zone.get(cell_group.lower(), cell_group)
            if zone not in zone_data:
                zone_data[zone] = []
            zone_data[zone].extend(names)
        display_data = zone_data
        group_label = "Zone"
    else:
        display_data = cell_group_data
        group_label = "Cell Group"

    # Convert hex color to RGB for rgba shadows
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    primary_rgb = hex_to_rgb(colors['primary'])

    # Modern Edgy Dashboard Styling with Historical Colors
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@700;900&display=swap');

        .historical-kpi-card {{
            background: {colors['card_bg']};
            padding: 2rem 2.5rem;
            border-radius: 0px;
            border-left: 6px solid {colors['primary']};
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.15);
            transition: all 0.3s ease;
        }}
        .historical-kpi-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.25);
            border-left-width: 8px;
        }}
        .historical-kpi-label {{
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 700;
            color: {colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .historical-kpi-number {{
            font-family: 'Inter', sans-serif;
            font-size: 5.5rem;
            font-weight: 900;
            color: {colors['primary']};
            line-height: 1;
            margin: 0.5rem 0;
            text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
        }}
        .historical-kpi-subtitle {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            color: {colors['text_muted']};
            margin-top: 0.5rem;
            font-weight: 500;
        }}
        .historical-section-title {{
            font-family: 'Inter', sans-serif;
            font-size: 1.8rem;
            font-weight: 900;
            color: {colors['primary']};
            text-transform: uppercase;
            letter-spacing: 3px;
            margin-bottom: 1.5rem;
            border-bottom: 3px solid {colors['primary']};
            padding-bottom: 0.5rem;
            display: inline-block;
        }}
        .historical-name-badge {{
            background: {colors['background']};
            border: 1px solid {colors['primary']};
            color: {colors['primary']};
            padding: 0.6rem 1.2rem;
            margin: 0.4rem 0.4rem 0.4rem 0;
            border-radius: 0px;
            display: inline-block;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            font-size: 0.9rem;
            letter-spacing: 0.5px;
            transition: all 0.2s ease;
        }}
        .historical-name-badge:hover {{
            background: {colors['primary']};
            color: {colors['background']};
            transform: scale(1.05);
        }}
        .historical-name-badge-pending {{
            background: {colors['background']};
            border: 1px solid {colors['text_muted']};
            color: {colors['text_muted']};
            padding: 0.6rem 1.2rem;
            margin: 0.4rem 0.4rem 0.4rem 0;
            border-radius: 0px;
            display: inline-block;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            font-size: 0.9rem;
            letter-spacing: 0.5px;
            opacity: 0.5;
        }}
        .historical-empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            background: {colors['card_bg']};
            border: 2px dashed {colors['text_muted']};
            border-radius: 0px;
        }}
    </style>
    """, unsafe_allow_html=True)

    # KPI Card - Total Checked In
    st.markdown(f"""
    <div class="historical-kpi-card">
        <div class="historical-kpi-label">Total Checked In on {display_date_formatted}</div>
        <div class="historical-kpi-number">{total_checked_in}</div>
        <div class="historical-kpi-subtitle">People checked in on this date</div>
    </div>
    """, unsafe_allow_html=True)

    # Zone tiles (only for zone grouping)
    if group_by_zone and total_checked_in > 0 and cell_group_data:
        cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
        zone_counts = {}
        for cell_group, names in cell_group_data.items():
            zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
            if zone not in zone_counts:
                zone_counts[zone] = 0
            zone_counts[zone] += len(names)

        sorted_zones_for_tiles = sorted(zone_counts.items(), key=lambda x: x[0].lower())

        st.markdown(f"""
        <style>
            .historical-zone-tiles-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 1rem;
                margin-bottom: 2rem;
            }}
            .historical-zone-tile {{
                background: {colors['card_bg']};
                border: 2px solid {colors['primary']};
                padding: 1.2rem 1.5rem;
                min-width: 140px;
                flex: 1;
                text-align: center;
                transition: all 0.2s ease;
            }}
            .historical-zone-tile:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
            }}
            .historical-zone-name {{
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 700;
                color: {colors['text_muted']};
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 0.3rem;
            }}
            .historical-zone-count {{
                font-family: 'Inter', sans-serif;
                font-size: 2.5rem;
                font-weight: 900;
                color: {colors['primary']};
                line-height: 1;
            }}
        </style>
        <div class="historical-zone-tiles-container">
            {''.join([f'<div class="historical-zone-tile"><div class="historical-zone-name">{zone}</div><div class="historical-zone-count">{count}</div></div>' for zone, count in sorted_zones_for_tiles])}
        </div>
        """, unsafe_allow_html=True)

    if total_checked_in > 0:
        # Recent Check-ins Table for historical date
        if recent_checkins:
            st.markdown(f"""
            <div style="margin-bottom: 0.5rem;">
                <span style="font-family: 'Inter', sans-serif; font-weight: 700; font-size: 1rem;
                             color: {colors['primary']}; text-transform: uppercase; letter-spacing: 1px;">
                    Check-Ins on {display_date_formatted}
                </span>
            </div>
            """, unsafe_allow_html=True)

            table_data = []
            for timestamp_str, name_cell_group in recent_checkins:
                time_part = timestamp_str[11:19] if len(timestamp_str) >= 19 else timestamp_str
                table_data.append({
                    "Time": time_part,
                    "Name - Cell Group": name_cell_group
                })

            df = pd.DataFrame(table_data)
            row_height = 35
            header_height = 38
            max_visible_rows = 5
            display_height = header_height + (row_height * min(len(table_data), max_visible_rows))
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                height=max(display_height, header_height + row_height)
            )

        st.markdown("---")

        # Bar Chart Section
        chart_title = f"Attendance by Zone on {display_date_formatted}" if group_by_zone else f"Check-Ins by Cell Group on {display_date_formatted}"
        st.markdown(f'<div class="historical-section-title">{chart_title}</div>', unsafe_allow_html=True)

        sorted_groups = sorted(display_data.items(), key=lambda x: len(x[1]), reverse=True)

        chart_data = {
            group_label: [group for group, _ in sorted_groups],
            'Count': [len(names) for _, names in sorted_groups]
        }
        df_chart = pd.DataFrame(chart_data)

        fig = px.bar(
            df_chart,
            x=group_label,
            y='Count',
            color='Count',
            color_continuous_scale=[colors['background'], colors['primary']],
            text='Count',
            labels={'Count': 'Number of People', group_label: group_label},
            height=400
        )

        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['card_bg'],
            font=dict(family='Inter, sans-serif', size=12, color=colors['primary']),
            xaxis=dict(
                title=dict(font=dict(size=14, color=colors['primary'], family='Inter')),
                tickfont=dict(color=colors['text_muted'], family='Inter'),
                gridcolor=colors['text_muted'],
                linecolor=colors['primary'],
                linewidth=2,
                categoryorder='total descending'
            ),
            yaxis=dict(
                title=dict(font=dict(size=14, color=colors['primary'], family='Inter')),
                tickfont=dict(color=colors['text_muted'], family='Inter'),
                gridcolor=colors['text_muted'],
                linecolor=colors['primary'],
                linewidth=2
            ),
            coloraxis_showscale=False,
            showlegend=False,
            margin=dict(l=50, r=50, t=60, b=50)
        )

        fig.update_traces(
            textfont=dict(size=14, color=colors['background'], family='Inter', weight='bold'),
            textposition='inside',
            insidetextanchor='middle',
            marker=dict(line=dict(color=colors['primary'], width=2)),
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>',
            hoverlabel=dict(bgcolor=colors['background'], font=dict(color=colors['primary'], family='Inter'))
        )

        st.plotly_chart(fig, use_container_width=True)

        # Names Breakdown Section
        names_title = f"Attendees by Zone on {display_date_formatted}" if group_by_zone else f"Attendees by Cell Group on {display_date_formatted}"
        st.markdown(f'<div class="historical-section-title">{names_title}</div>', unsafe_allow_html=True)

        # Build search options for the HTML select (historical)
        all_cell_groups_search = sorted(set(all_members_by_cell_group.keys()) | set(display_data.keys()), key=str.lower)

        if group_by_zone:
            cell_to_zone_map_search, _ = get_cell_to_zone_mapping(client, SHEET_ID)
            all_zones_search = set()
            for cell_group in all_cell_groups_search:
                zone = cell_to_zone_map_search.get(cell_group.lower(), cell_group)
                all_zones_search.add(zone)
            zones_list = sorted(all_zones_search, key=str.lower)
            searchable_groups_hist = [("zone", z) for z in zones_list] + [("cell", c) for c in all_cell_groups_search]
        else:
            searchable_groups_hist = [("group", g) for g in all_cell_groups_search]

        # Build collapsible breakdown HTML for historical view
        hist_breakdown_html = f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

            * {{
                font-family: 'Inter', sans-serif !important;
                box-sizing: border-box;
            }}
            body {{
                font-family: 'Inter', sans-serif !important;
                margin: 0;
                padding: 0;
            }}
            .cell-collapsible {{
                cursor: pointer;
                user-select: none;
                transition: all 0.2s ease;
            }}
            .cell-collapsible:hover {{
                opacity: 0.8;
            }}
            .cell-content {{
                overflow: hidden;
                transition: max-height 0.3s ease-out, opacity 0.2s ease-out, padding 0.3s ease-out;
                max-height: 0;
                opacity: 0;
                padding: 0;
            }}
            .cell-content.expanded {{
                max-height: 2000px;
                opacity: 1;
                padding-top: 0.5rem;
            }}
            .cell-toggle {{
                display: inline-block;
                margin-right: 0.5rem;
                transition: transform 0.2s ease;
                font-size: 0.85rem;
            }}
            .cell-toggle.expanded {{
                transform: rotate(90deg);
            }}
            .expand-collapse-btn {{
                background: transparent;
                border: 1px solid {colors['primary']};
                color: {colors['primary']};
                padding: 0.4rem 1rem;
                border-radius: 4px;
                cursor: pointer;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.85rem;
                font-weight: 600;
                transition: all 0.2s ease;
                min-width: 120px;
            }}
            .expand-collapse-btn:hover {{
                background: {colors['primary']};
                color: {colors['background']};
            }}
            .search-select {{
                padding: 0.5rem;
                border: 1px solid {colors['primary']};
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 500;
                background: {colors['background']};
                color: {colors['text']};
                min-width: 200px;
                cursor: pointer;
            }}
            .search-select:focus {{
                outline: none;
                border-color: {colors['primary']};
                box-shadow: 0 0 5px {colors['primary']}40;
            }}
            .controls-row {{
                display: flex;
                gap: 1rem;
                align-items: center;
                margin-bottom: 1rem;
                flex-wrap: wrap;
            }}
            .name-badge {{
                display: inline-block;
                background: {colors['primary']};
                color: {colors['background']};
                padding: 0.6rem 1.2rem;
                margin: 0.25rem;
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                transition: all 0.2s ease;
                cursor: default;
            }}
            .name-badge:hover {{
                transform: scale(1.05);
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            }}
            .name-badge-pending {{
                display: inline-block;
                background: transparent;
                color: {colors['text_muted']};
                border: 1px solid {colors['text_muted']};
                padding: 0.6rem 1.2rem;
                margin: 0.25rem;
                border-radius: 4px;
                font-family: 'Inter', sans-serif !important;
                font-size: 0.9rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                opacity: 0.5;
                transition: all 0.2s ease;
                cursor: default;
            }}
            .name-badge-pending:hover {{
                transform: scale(1.05);
                opacity: 0.7;
            }}
            .name-badge-name {{
                display: block;
            }}
            .name-badge-role {{
                display: block;
                font-size: 0.8em;
                font-weight: 400;
                text-transform: none;
                letter-spacing: normal;
                opacity: 0.95;
            }}
            .role-row {{
                margin-bottom: 0.8rem;
            }}
            .role-label {{
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 600;
                color: {page_colors['text_muted']};
                margin-right: 0.5rem;
                display: inline;
            }}
            .zone-header {{
                font-family: 'Inter', sans-serif !important;
                font-size: 1.3rem;
                font-weight: 900;
                color: {colors['primary']};
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 1rem;
                margin-top: 1.5rem;
            }}
            .zone-header:first-of-type {{
                margin-top: 0;
            }}
            .cell-header {{
                font-family: 'Inter', sans-serif !important;
                font-size: 1rem;
                font-weight: 700;
                color: {colors['text_muted']};
                letter-spacing: 1px;
                margin-bottom: 0.3rem;
            }}
            .cell-container {{
                margin-left: 1.5rem;
                margin-bottom: 0.8rem;
                padding: 0.5rem;
                border-radius: 8px;
                transition: all 0.3s ease;
            }}
            .group-header {{
                font-family: 'Inter', sans-serif !important;
                font-size: 1.3rem;
                font-weight: 900;
                color: {colors['primary']};
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 0.3rem;
            }}
            .group-container {{
                margin-bottom: 1rem;
                padding: 0.5rem;
                border-radius: 8px;
                transition: all 0.3s ease;
            }}
            .count-label {{
                color: {colors['text_muted']};
                font-size: 0.85rem;
                font-weight: normal;
                text-transform: none;
                letter-spacing: normal;
            }}
            .highlight {{
                box-shadow: 0 0 20px {colors['primary']};
                background-color: {colors['light']}20;
            }}
        </style>
        <div class="controls-row">
            <select id="searchSelectHist" class="search-select" onchange="jumpToGroup(this.value)">
                <option value="">Jump to...</option>
        """

        # Add search options to dropdown
        for group_type, group_name in searchable_groups_hist:
            if group_type == "zone":
                target_id = f"hist-group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = f"Zone: {group_name}"
            elif group_type == "cell":
                target_id = f"hist-cell-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = f"Cell: {group_name}"
            else:
                target_id = f"hist-group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                display_name = group_name
            hist_breakdown_html += f'<option value="{target_id}">{display_name}</option>'

        hist_breakdown_html += f"""
            </select>
            <button id="toggleAllBtnHist" class="expand-collapse-btn" onclick="toggleAll()">Expand All</button>
        </div>
        """

        if group_by_zone:
            # Build zone -> cell -> all members structure
            cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
            zone_cell_all_members = {}
            for cell_group, members in all_members_by_cell_group.items():
                zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                if zone not in zone_cell_all_members:
                    zone_cell_all_members[zone] = {}
                if cell_group not in zone_cell_all_members[zone]:
                    zone_cell_all_members[zone][cell_group] = []
                zone_cell_all_members[zone][cell_group].extend(members)

            for zone in sorted(zone_cell_all_members.keys(), key=str.lower):
                cells_all = zone_cell_all_members[zone]
                checked_in_zone = 0
                total_in_zone = sum(len(members) for members in cells_all.values())
                for cell_group, members in cells_all.items():
                    for member in members:
                        if member in checked_in_names_set:
                            checked_in_zone += 1

                zone_id = zone.replace(" ", "-").replace("'", "").lower()
                hist_breakdown_html += f"""
                <div id="hist-group-{zone_id}" class="zone-header">
                    {zone} <span class="count-label">({checked_in_zone}/{total_in_zone})</span>
                </div>
                """

                for cell_group in sorted(cells_all.keys(), key=str.lower):
                    all_names_in_cell = cells_all[cell_group]
                    checked_count = len([n for n in all_names_in_cell if n in checked_in_names_set])
                    total_in_cell = len(all_names_in_cell)
                    role_grouped_badges = build_role_grouped_badges(
                        all_names_in_cell, checked_in_names_set, name_to_role,
                        "name-badge", "name-badge-pending"
                    )

                    cell_id = cell_group.replace(" ", "-").replace("'", "").lower()
                    hist_breakdown_html += f"""
                    <div id="hist-cell-{cell_id}" class="cell-container">
                        <div class="cell-collapsible cell-header" onclick="toggleCell('hist-{cell_id}')">
                            <span id="toggle-hist-{cell_id}" class="cell-toggle">▶</span>
                            {cell_group} <span class="count-label">({checked_count}/{total_in_cell})</span>
                        </div>
                        <div id="content-hist-{cell_id}" class="cell-content">
                            {role_grouped_badges}
                        </div>
                    </div>
                    """
        else:
            # Get all cell groups from both checked-in and all members
            all_cell_groups = set(all_members_by_cell_group.keys()) | set(display_data.keys())

            for group_name in sorted(all_cell_groups, key=str.lower):
                all_names_in_group = all_members_by_cell_group.get(group_name, [])
                checked_count = len([n for n in all_names_in_group if n in checked_in_names_set])
                total_in_group = len(all_names_in_group)
                role_grouped_badges = build_role_grouped_badges(
                    all_names_in_group, checked_in_names_set, name_to_role,
                    "name-badge", "name-badge-pending"
                )

                group_id = group_name.replace(" ", "-").replace("'", "").lower()
                hist_breakdown_html += f"""
                <div id="hist-group-{group_id}" class="group-container">
                    <div class="cell-collapsible group-header" onclick="toggleCell('hist-{group_id}')">
                        <span id="toggle-hist-{group_id}" class="cell-toggle">▶</span>
                        {group_name} <span class="count-label">({checked_count}/{total_in_group})</span>
                    </div>
                    <div id="content-hist-{group_id}" class="cell-content">
                        {role_grouped_badges}
                    </div>
                </div>
                """

        # Add JavaScript
        hist_breakdown_html += """
        <script>
            var isExpanded = false;

            function toggleAll() {
                var btn = document.getElementById('toggleAllBtnHist');
                if (isExpanded) {
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                    btn.textContent = 'Expand All';
                    isExpanded = false;
                } else {
                    document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                    document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                    btn.textContent = 'Collapse All';
                    isExpanded = true;
                }
            }

            function toggleCell(cellId) {
                var content = document.getElementById('content-' + cellId);
                var toggle = document.getElementById('toggle-' + cellId);
                if (content && toggle) {
                    content.classList.toggle('expanded');
                    toggle.classList.toggle('expanded');
                }
            }

            function jumpToGroup(targetId) {
                if (!targetId) return;
                var el = document.getElementById(targetId);
                if (el) {
                    var cellId = targetId.replace('hist-group-', 'hist-').replace('hist-cell-', 'hist-');
                    var content = document.getElementById('content-' + cellId);
                    var toggle = document.getElementById('toggle-' + cellId);
                    if (content && !content.classList.contains('expanded')) {
                        content.classList.add('expanded');
                        if (toggle) toggle.classList.add('expanded');
                    }
                    el.scrollIntoView({behavior: 'smooth', block: 'center'});
                    el.classList.add('highlight');
                    setTimeout(function() { el.classList.remove('highlight'); }, 2500);
                }
                document.getElementById('searchSelectHist').value = '';
            }
        </script>
        """

        # Calculate height and render
        num_items_hist = len(all_members_by_cell_group) if not group_by_zone else sum(len(cells) for cells in zone_cell_all_members.values()) if 'zone_cell_all_members' in dir() else 10
        estimated_height_hist = 150 + (num_items_hist * 60)
        components.html(hist_breakdown_html, height=estimated_height_hist, scrolling=True)
    else:
        # No check-ins - show empty state and all pending members greyed out
        st.markdown(f"""
        <div class="historical-empty-state">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📋</div>
            <div style="font-family: 'Inter', sans-serif; font-size: 1.5rem; color: {colors['text_muted']}; font-weight: 700;
                        text-transform: uppercase; letter-spacing: 2px;">
                No check-ins on {display_date_formatted}
            </div>
            <div style="font-size: 1rem; color: {colors['text_muted']}; margin-top: 1rem; font-weight: 500;">
                No attendance records found for this date.
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Show all members greyed out even when no check-ins
        if all_members_by_cell_group:
            names_title = f"Attendees by Zone on {display_date_formatted}" if group_by_zone else f"Attendees by Cell Group on {display_date_formatted}"
            st.markdown(f'<div class="historical-section-title">{names_title}</div>', unsafe_allow_html=True)

            # Build search options for the HTML select (historical empty state)
            all_cell_groups_search = sorted(all_members_by_cell_group.keys(), key=str.lower)

            if group_by_zone:
                cell_to_zone_map_search, _ = get_cell_to_zone_mapping(client, SHEET_ID)
                all_zones_search = set()
                for cell_group in all_cell_groups_search:
                    zone = cell_to_zone_map_search.get(cell_group.lower(), cell_group)
                    all_zones_search.add(zone)
                zones_list = sorted(all_zones_search, key=str.lower)
                searchable_groups_hist_empty = [("zone", z) for z in zones_list] + [("cell", c) for c in all_cell_groups_search]
            else:
                searchable_groups_hist_empty = [("group", g) for g in all_cell_groups_search]

            # Build collapsible breakdown HTML for historical empty state
            hist_empty_breakdown_html = f"""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

                * {{
                    font-family: 'Inter', sans-serif !important;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: 'Inter', sans-serif !important;
                    margin: 0;
                    padding: 0;
                }}
                .cell-collapsible {{
                    cursor: pointer;
                    user-select: none;
                    transition: all 0.2s ease;
                }}
                .cell-collapsible:hover {{
                    opacity: 0.8;
                }}
                .cell-content {{
                    overflow: hidden;
                    transition: max-height 0.3s ease-out, opacity 0.2s ease-out, padding 0.3s ease-out;
                    max-height: 0;
                    opacity: 0;
                    padding: 0;
                }}
                .cell-content.expanded {{
                    max-height: 2000px;
                    opacity: 1;
                    padding-top: 0.5rem;
                }}
                .cell-toggle {{
                    display: inline-block;
                    margin-right: 0.5rem;
                    transition: transform 0.2s ease;
                    font-size: 0.85rem;
                }}
                .cell-toggle.expanded {{
                    transform: rotate(90deg);
                }}
                .expand-collapse-btn {{
                    background: transparent;
                    border: 1px solid {colors['primary']};
                    color: {colors['primary']};
                    padding: 0.4rem 1rem;
                    border-radius: 4px;
                    cursor: pointer;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.85rem;
                    font-weight: 600;
                    transition: all 0.2s ease;
                    min-width: 120px;
                }}
                .expand-collapse-btn:hover {{
                    background: {colors['primary']};
                    color: {colors['background']};
                }}
                .search-select {{
                    padding: 0.5rem;
                    border: 1px solid {colors['primary']};
                    border-radius: 4px;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.9rem;
                    font-weight: 500;
                    background: {colors['background']};
                    color: {colors['text']};
                    min-width: 200px;
                    cursor: pointer;
                }}
                .search-select:focus {{
                    outline: none;
                    border-color: {colors['primary']};
                    box-shadow: 0 0 5px {colors['primary']}40;
                }}
                .controls-row {{
                    display: flex;
                    gap: 1rem;
                    align-items: center;
                    margin-bottom: 1rem;
                    flex-wrap: wrap;
                }}
                .name-badge-pending {{
                    display: inline-block;
                    background: transparent;
                    color: {colors['text_muted']};
                    border: 1px solid {colors['text_muted']};
                    padding: 0.6rem 1.2rem;
                    margin: 0.25rem;
                    border-radius: 4px;
                    font-family: 'Inter', sans-serif !important;
                    font-size: 0.9rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    opacity: 0.5;
                    transition: all 0.2s ease;
                    cursor: default;
                }}
                .name-badge-pending:hover {{
                    transform: scale(1.05);
                    opacity: 0.7;
                }}
                .name-badge-name {{ display: block; }}
                .role-row {{ margin-bottom: 0.8rem; }}
                .role-label {{
                    font-family: 'Inter', sans-serif;
                    font-size: 0.85rem;
                    font-weight: 600;
                    color: {colors['text_muted']};
                    margin-right: 0.5rem;
                    display: inline;
                }}
                .zone-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1.3rem;
                    font-weight: 900;
                    color: {colors['primary']};
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    margin-bottom: 1rem;
                    margin-top: 1.5rem;
                }}
                .zone-header:first-of-type {{
                    margin-top: 0;
                }}
                .cell-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1rem;
                    font-weight: 700;
                    color: {colors['text_muted']};
                    letter-spacing: 1px;
                    margin-bottom: 0.3rem;
                }}
                .cell-container {{
                    margin-left: 1.5rem;
                    margin-bottom: 0.8rem;
                    padding: 0.5rem;
                    border-radius: 8px;
                    transition: all 0.3s ease;
                }}
                .group-header {{
                    font-family: 'Inter', sans-serif !important;
                    font-size: 1.3rem;
                    font-weight: 900;
                    color: {colors['primary']};
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    margin-bottom: 0.3rem;
                }}
                .group-container {{
                    margin-bottom: 1rem;
                    padding: 0.5rem;
                    border-radius: 8px;
                    transition: all 0.3s ease;
                }}
                .count-label {{
                    color: {colors['text_muted']};
                    font-size: 0.85rem;
                    font-weight: normal;
                    text-transform: none;
                    letter-spacing: normal;
                }}
                .highlight {{
                    box-shadow: 0 0 20px {colors['primary']};
                    background-color: {colors['light']}20;
                }}
            </style>
            <div class="controls-row">
                <select id="searchSelectHistEmpty" class="search-select" onchange="jumpToGroup(this.value)">
                    <option value="">Jump to...</option>
            """

            # Add search options to dropdown
            for group_type, group_name in searchable_groups_hist_empty:
                if group_type == "zone":
                    target_id = f"hist-empty-group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = f"Zone: {group_name}"
                elif group_type == "cell":
                    target_id = f"hist-cell-empty-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = f"Cell: {group_name}"
                else:
                    target_id = f"hist-empty-group-{group_name.replace(' ', '-').replace(chr(39), '').lower()}"
                    display_name = group_name
                hist_empty_breakdown_html += f'<option value="{target_id}">{display_name}</option>'

            hist_empty_breakdown_html += f"""
                </select>
                <button id="toggleAllBtnHistEmpty" class="expand-collapse-btn" onclick="toggleAll()">Expand All</button>
            </div>
            """

            if group_by_zone:
                # Build zone -> cell -> all members structure
                cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
                zone_cell_all_members = {}
                for cell_group, members in all_members_by_cell_group.items():
                    zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                    if zone not in zone_cell_all_members:
                        zone_cell_all_members[zone] = {}
                    if cell_group not in zone_cell_all_members[zone]:
                        zone_cell_all_members[zone][cell_group] = []
                    zone_cell_all_members[zone][cell_group].extend(members)

                for zone in sorted(zone_cell_all_members.keys(), key=str.lower):
                    cells_all = zone_cell_all_members[zone]
                    total_in_zone = sum(len(members) for members in cells_all.values())

                    zone_id = zone.replace(" ", "-").replace("'", "").lower()
                    hist_empty_breakdown_html += f"""
                    <div id="hist-empty-group-{zone_id}" class="zone-header">
                        {zone} <span class="count-label">(0/{total_in_zone})</span>
                    </div>
                    """

                    for cell_group in sorted(cells_all.keys(), key=str.lower):
                        all_names_in_cell = cells_all[cell_group]
                        role_grouped_badges = build_role_grouped_badges(
                            all_names_in_cell, set(), name_to_role,
                            "name-badge-pending", "name-badge-pending"
                        )

                        cell_id = cell_group.replace(" ", "-").replace("'", "").lower()
                        hist_empty_breakdown_html += f"""
                        <div id="hist-cell-empty-{cell_id}" class="cell-container">
                            <div class="cell-collapsible cell-header" onclick="toggleCell('hist-empty-{cell_id}')">
                                <span id="toggle-hist-empty-{cell_id}" class="cell-toggle">▶</span>
                                {cell_group} <span class="count-label">(0/{len(all_names_in_cell)})</span>
                            </div>
                            <div id="content-hist-empty-{cell_id}" class="cell-content">
                                {role_grouped_badges}
                            </div>
                        </div>
                        """
            else:
                # Regular cell group display - all greyed out
                for group_name in sorted(all_members_by_cell_group.keys(), key=str.lower):
                    all_names_in_group = all_members_by_cell_group[group_name]
                    total_in_group = len(all_names_in_group)
                    role_grouped_badges = build_role_grouped_badges(
                        all_names_in_group, set(), name_to_role,
                        "name-badge-pending", "name-badge-pending"
                    )

                    group_id = group_name.replace(" ", "-").replace("'", "").lower()
                    hist_empty_breakdown_html += f"""
                    <div id="hist-empty-group-{group_id}" class="group-container">
                        <div class="cell-collapsible group-header" onclick="toggleCell('hist-empty-{group_id}')">
                            <span id="toggle-hist-empty-{group_id}" class="cell-toggle">▶</span>
                            {group_name} <span class="count-label">(0/{total_in_group})</span>
                        </div>
                        <div id="content-hist-empty-{group_id}" class="cell-content">
                            {role_grouped_badges}
                        </div>
                    </div>
                    """

            # Add JavaScript
            hist_empty_breakdown_html += """
            <script>
                var isExpanded = false;

                function toggleAll() {
                    var btn = document.getElementById('toggleAllBtnHistEmpty');
                    if (isExpanded) {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.remove('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.remove('expanded'));
                        btn.textContent = 'Expand All';
                        isExpanded = false;
                    } else {
                        document.querySelectorAll('.cell-content').forEach(el => el.classList.add('expanded'));
                        document.querySelectorAll('.cell-toggle').forEach(el => el.classList.add('expanded'));
                        btn.textContent = 'Collapse All';
                        isExpanded = true;
                    }
                }

                function toggleCell(cellId) {
                    var content = document.getElementById('content-' + cellId);
                    var toggle = document.getElementById('toggle-' + cellId);
                    if (content && toggle) {
                        content.classList.toggle('expanded');
                        toggle.classList.toggle('expanded');
                    }
                }

                function jumpToGroup(targetId) {
                    if (!targetId) return;
                    var el = document.getElementById(targetId);
                    if (el) {
                        var cellId = targetId.replace('hist-empty-group-', 'hist-empty-').replace('hist-cell-empty-', 'hist-empty-');
                        var content = document.getElementById('content-' + cellId);
                        var toggle = document.getElementById('toggle-' + cellId);
                        if (content && !content.classList.contains('expanded')) {
                            content.classList.add('expanded');
                            if (toggle) toggle.classList.add('expanded');
                        }
                        el.scrollIntoView({behavior: 'smooth', block: 'center'});
                        el.classList.add('highlight');
                        setTimeout(function() { el.classList.remove('highlight'); }, 2500);
                    }
                    document.getElementById('searchSelectHistEmpty').value = '';
                }
            </script>
            """

            # Calculate height and render
            num_items_hist_empty = len(all_members_by_cell_group) if not group_by_zone else sum(len(cells) for cells in zone_cell_all_members.values()) if 'zone_cell_all_members' in dir() else 10
            estimated_height_hist_empty = 150 + (num_items_hist_empty * 60)
            components.html(hist_empty_breakdown_html, height=estimated_height_hist_empty, scrolling=True)



# ========== SIDEBAR NAVIGATION ==========
# Use query params for persistent page selection across refreshes
query_params = st.query_params
default_page = query_params.get("page", "nwst")

# Map query param to page name
page_map = {
    "nwst": "NWST Check In",
    "leaders": "Leaders Discipleship",
    "ministry": "Ministry Discipleship",
}
reverse_page_map = {v: k for k, v in page_map.items()}

# Get current page from query params (source of truth)
current_page = page_map.get(default_page, "NWST Check In")
page = current_page  # Set page variable for use later

# Initialize ministry selection in session state
if 'selected_ministry' not in st.session_state:
    st.session_state.selected_ministry = MINISTRY_LIST[0]  # Default to first ministry

with st.sidebar:
    # ========== PAGE NAVIGATION ==========
    st.markdown(f"""
    <h3 style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 700; letter-spacing: 1px; font-size: 0.9rem; text-transform: uppercase;">
        Navigate
    </h3>
    """, unsafe_allow_html=True)

    _nav_pages = [
        ("NWST Check In",       "nwst"),
        ("Leaders Discipleship","leaders"),
        ("Ministry Discipleship","ministry"),
    ]
    for _nav_label, _nav_key in _nav_pages:
        _nav_active = page == page_map.get(_nav_key, "")
        if st.button(
            _nav_label,
            type="primary" if _nav_active else "secondary",
            use_container_width=True,
            key=f"sidebar_nav_{_nav_key}",
            disabled=_nav_active,
        ):
            st.query_params["page"] = _nav_key
            st.rerun()
        st.markdown('<div style="height: 1.6rem;"></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ========== HISTORICAL VIEW SECTION ==========
    st.markdown(f"""
    <h3 style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 700; letter-spacing: 1px; font-size: 0.9rem;">
        VIEW HISTORICAL DATA
    </h3>
    """, unsafe_allow_html=True)

    # Initialize session state for historical view
    if 'historical_date' not in st.session_state:
        st.session_state.historical_date = None
    if 'viewing_historical' not in st.session_state:
        st.session_state.viewing_historical = False

    # Date picker for historical view
    today_myt_date = datetime.strptime(get_today_myt_date(), "%Y-%m-%d").date()
    selected_date = st.date_input(
        "Select date to view",
        value=today_myt_date,
        max_value=today_myt_date,
        key="historical_date_picker"
    )

    # Convert to string format
    selected_date_str = selected_date.strftime("%Y-%m-%d")

    col_view, col_reset = st.columns(2)
    with col_view:
        if st.button("View Date", type="primary", use_container_width=True, key="view_historical"):
            st.session_state.historical_date = selected_date_str
            st.session_state.viewing_historical = (selected_date_str != get_today_myt_date())
            st.rerun()

    with col_reset:
        if st.button("Back to Today", type="secondary", use_container_width=True, key="reset_to_today"):
            st.session_state.historical_date = None
            st.session_state.viewing_historical = False
            st.rerun()

    st.markdown("---")

    # ========== EMAIL REPORT TO PSQ ==========
    st.markdown(f"""
    <h3 style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 700; letter-spacing: 1px; font-size: 0.9rem;">
        EMAIL REPORT TO PSQ
    </h3>
    """, unsafe_allow_html=True)

    if st.button(
        "📋 Weekly Check In",
        type="secondary",
        use_container_width=True,
        key="psq_weekly_checkin_btn",
    ):
        st.session_state.pending_psq_email = "weekly_checkin"

    st.markdown(
        '<div aria-hidden="true" style="height: 1.6rem; min-height: 1.6rem;"></div>',
        unsafe_allow_html=True,
    )

    if st.button(
        "📤 Current Cell Health",
        type="secondary",
        use_container_width=True,
        key="psq_cell_health_btn",
    ):
        st.session_state.pending_psq_email = "cell_health"

    pending_psq = st.session_state.get("pending_psq_email")
    if pending_psq:
        confirm_msg = (
            "Send Weekly Check-In email now?"
            if pending_psq == "weekly_checkin"
            else "Send Current Cell Health email now? (PDF is the NWST cell-health table only.)"
        )
        st.warning(confirm_msg)
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("Yes, Send", type="primary", key="confirm_send_psq"):
                st.session_state.sending_psq_email = pending_psq
                st.session_state.pending_psq_email = None
                st.rerun()
        with col_no:
            if st.button("Cancel", key="cancel_send_psq"):
                st.session_state.pending_psq_email = None
                st.rerun()

    sending_psq = st.session_state.get("sending_psq_email")
    if sending_psq:
        st.session_state.sending_psq_email = None
        report_date = (
            st.session_state.get("historical_date")
            if st.session_state.get("viewing_historical", False)
            else None
        )
        kind_label = (
            "Weekly Check-In"
            if sending_psq == "weekly_checkin"
            else "Current Cell Health"
        )
        with st.spinner(
            f"Sending {kind_label} email{' for ' + report_date if report_date else ''}..."
        ):
            try:
                from weekly_email_report import (
                    send_psq_cell_health_only,
                    send_psq_weekly_checkin_only,
                )

                import io
                import sys

                old_stdout = sys.stdout
                sys.stdout = io.StringIO()

                if sending_psq == "weekly_checkin":
                    send_psq_weekly_checkin_only(target_date=report_date)
                else:
                    send_psq_cell_health_only(target_date=report_date)

                output = sys.stdout.getvalue()
                sys.stdout = old_stdout

                if "SUCCESS" in output:
                    st.success(
                        f"{kind_label} email sent successfully!"
                        f"{' (Date: ' + report_date + ')' if report_date else ''}"
                    )
                else:
                    st.error("Failed to send email. Check configuration.")
                    if output:
                        st.text(output)
            except ImportError as e:
                st.error(
                    "Could not import weekly_email_report. "
                    f"{e} Use CHECK IN/weekly_email_report.py (not a duplicate at repo root) "
                    "and install dependencies (reportlab)."
                )
            except Exception as e:
                st.error(f"Error sending email: {str(e)}")

# ========== RENDER SELECTED PAGE ==========

# Determine if viewing historical data
viewing_historical = st.session_state.get('viewing_historical', False)
historical_date = st.session_state.get('historical_date', None)

# If viewing historical, generate colors for that date
if viewing_historical and historical_date:
    historical_colors = generate_colors_for_date(historical_date)
    # Override page_colors with historical colors
    if is_leaders_page:
        display_colors = {
            'primary': historical_colors['primary'],
            'light': historical_colors['light'],
            'background': '#ffffff',
            'text': '#000000',
            'text_muted': '#666666',
            'card_bg': '#f5f5f5',
            'border': historical_colors['primary']
        }
    else:
        display_colors = {
            'primary': historical_colors['primary'],
            'light': historical_colors['light'],
            'background': '#000000',
            'text': '#ffffff',
            'text_muted': '#999999',
            'card_bg': '#0a0a0a',
            'border': historical_colors['primary']
        }
else:
    display_colors = page_colors
    historical_date = get_today_myt_date()

# Display historical sticker if viewing past data
if viewing_historical:
    # Format date for display
    try:
        display_date = datetime.strptime(historical_date, "%Y-%m-%d").strftime("%d %b %Y")
    except:
        display_date = historical_date

    st.markdown(f"""
    <div style="
        position: fixed;
        top: 70px;
        right: 20px;
        background: linear-gradient(135deg, {display_colors['primary']}, {display_colors['light']});
        color: {display_colors['background']};
        padding: 0.8rem 1.5rem;
        border-radius: 8px;
        font-family: 'Inter', sans-serif;
        font-weight: 800;
        font-size: 0.85rem;
        letter-spacing: 1px;
        text-transform: uppercase;
        z-index: 9999;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        display: flex;
        align-items: center;
        gap: 0.5rem;
    ">
        <span style="font-size: 1.2rem;">📅</span>
        <span>Viewing: {display_date}</span>
    </div>
    """, unsafe_allow_html=True)

# Display clear page header
if page == "NWST Check In":
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {display_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            NWST Check In
        </h1>
        <p style="color: {display_colors['text_muted']}; font-size: 0.9rem; margin: 0;">NWST Service Attendance</p>
    </div>
    """, unsafe_allow_html=True)

    # If viewing historical, show historical dashboard only (no check-in form)
    if viewing_historical:
        render_historical_dashboard(ATTENDANCE_TAB_NAME, historical_date, display_colors)
    else:
        render_check_in_form(ATTENDANCE_TAB_NAME, "attendance_form", "NWST Check In")
        # Refresh + Update Names toolbar
        st.markdown("<br><br>", unsafe_allow_html=True)
        col_refresh, col_update_names, _col_trailing = st.columns([1, 1, 3])
        with col_refresh:
            if st.button("Refresh", type="secondary", key=f"refresh_btn_{ATTENDANCE_TAB_NAME}", use_container_width=True):
                st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                st.session_state.last_refresh_time = get_now_myt()
                get_today_attendance_data.clear()
                get_options_from_sheet.clear()
                get_newcomers_count.clear()
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        today_myt = get_today_myt_date()
                        redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
                    except Exception:
                        pass
                st.rerun()
        with col_update_names:
            st.markdown(
                '''<a href="https://update-names.streamlit.app/" target="_blank" class="update-names-btn">Update names</a>''',
                unsafe_allow_html=True
            )
        render_recent_checkins_table(ATTENDANCE_TAB_NAME)
        render_dashboard_fragment(ATTENDANCE_TAB_NAME)

elif page == "Leaders Discipleship":
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {display_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            Leaders Discipleship
        </h1>
        <p style="color: {display_colors['text_muted']}; font-size: 0.9rem; margin: 0;">Leaders Discipleship (Grouped by Zone)</p>
    </div>
    """, unsafe_allow_html=True)

    # If viewing historical, show historical dashboard only (no check-in form)
    if viewing_historical:
        render_historical_dashboard(LEADERS_ATTENDANCE_TAB_NAME, historical_date, display_colors, group_by_zone=True)
    else:
        render_check_in_form(LEADERS_ATTENDANCE_TAB_NAME, "leaders_attendance_form", "Leaders Discipleship")
        render_recent_checkins_table(LEADERS_ATTENDANCE_TAB_NAME)
        render_dashboard_fragment(LEADERS_ATTENDANCE_TAB_NAME, group_by_zone=True)

elif page == "Ministry Discipleship":
    # Ministry page header
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {display_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            Ministry Discipleship
        </h1>
        <p style="color: {display_colors['text_muted']}; font-size: 0.9rem; margin: 0;">Ministry Discipleship (by Department)</p>
    </div>
    """, unsafe_allow_html=True)

    # Ministry selector dropdown
    col_select1, col_select2, col_select3 = st.columns([1, 2, 1])
    with col_select2:
        selected_ministry = st.selectbox(
            "Select Ministry",
            options=MINISTRY_LIST,
            index=MINISTRY_LIST.index(st.session_state.selected_ministry) if st.session_state.selected_ministry in MINISTRY_LIST else 0,
            key="ministry_selector",
            help="Select the ministry to view and check in members"
        )
        # Update session state when selection changes
        if selected_ministry != st.session_state.selected_ministry:
            st.session_state.selected_ministry = selected_ministry
            st.rerun()

    # Show check-in form and dashboard for selected ministry
    if not viewing_historical:
        render_ministry_check_in_form(st.session_state.selected_ministry, "ministry_attendance_form", f"{st.session_state.selected_ministry} Ministry")
        # Refresh + Update Names toolbar
        st.markdown("<br><br>", unsafe_allow_html=True)
        col_refresh_m, col_update_names_m, _col_trailing_m = st.columns([1, 1, 3])
        with col_refresh_m:
            if st.button("Refresh", type="secondary", key=f"refresh_btn_ministry_{st.session_state.selected_ministry}", use_container_width=True):
                st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                st.session_state.last_refresh_time = get_now_myt()
                get_today_attendance_data.clear()
                get_ministry_options_from_sheet.clear()
                get_newcomers_count.clear()
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        today_myt = get_today_myt_date()
                        redis_client.delete(f"{REDIS_NEWCOMERS_KEY_PREFIX}{today_myt}")
                    except Exception:
                        pass
                st.rerun()
        with col_update_names_m:
            st.markdown(
                '''<a href="https://update-names.streamlit.app/" target="_blank" class="update-names-btn">Update names</a>''',
                unsafe_allow_html=True
            )
        render_recent_checkins_table(MINISTRY_ATTENDANCE_TAB_NAME)
        render_ministry_dashboard_fragment(st.session_state.selected_ministry)
    else:
        st.info("Historical view is not yet available for Ministry Discipleship. Switch to NWST or Leaders Discipleship to view historical data.")

# Scroll to top button
st.markdown(f"""
<style>
    /* Scroll to top button */
    #scroll-to-top {{
        position: fixed;
        bottom: 30px;
        left: 50%;
        transform: translateX(-50%);
        width: 50px;
        height: 50px;
        background: {page_colors['primary']};
        color: {page_colors['background']};
        border: none;
        border-radius: 50%;
        cursor: pointer;
        font-size: 1.5rem;
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 9999;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        transition: all 0.3s ease;
        text-decoration: none;
    }}
    #scroll-to-top:hover {{
        transform: scale(1.1);
        box-shadow: 0 6px 20px rgba(0,0,0,0.4);
    }}
</style>
<a href="#top-anchor" id="scroll-to-top">↑</a>
""", unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown(
    f"<div style='text-align: center; color: {page_colors['text_muted']}; font-size: 0.9em;'>"
    "Church Check-In System | Powered by Streamlit"
    "</div>",
    unsafe_allow_html=True
)
