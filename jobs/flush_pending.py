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
import base64
import colorsys
import hashlib
import importlib.util
import json
import os
import random
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
SESSION_LOG_KEY = "flush_pending_session_log"

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


def _redis_client_for_theme():
    """Upstash client for Theme Override snapshot (no logging)."""
    if Redis is None:
        return None
    url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
    if not url or not token:
        try:
            import streamlit as st

            url = str(st.secrets.get("UPSTASH_REDIS_REST_URL", "") or "").strip()
            token = str(st.secrets.get("UPSTASH_REDIS_REST_TOKEN", "") or "").strip()
        except Exception:
            pass
    if not url or not token:
        return None
    try:
        return Redis(url=url, token=token)
    except Exception:
        return None


def _theme_overrides_from_redis_flush() -> dict:
    mod = _load_nwst_accent_cfg()
    r = _redis_client_for_theme()
    if not mod or not r:
        return {}
    try:
        return mod.read_theme_override_from_redis(r)
    except Exception:
        return {}


def _accent_overrides_from_json() -> dict:
    mod = _load_nwst_accent_cfg()
    if not mod:
        return {}
    return mod.get_accent_override_by_date()


def _normalize_primary_hex(hex_str: str | None) -> str | None:
    h = (hex_str or "").strip()
    if not h:
        return None
    if not h.startswith("#"):
        h = "#" + h
    if len(h) != 7:
        return None
    try:
        int(h[1:], 16)
    except ValueError:
        return None
    return h.lower()


def _theme_from_primary_hex(primary_hex: str) -> dict:
    p = _normalize_primary_hex(primary_hex)
    if not p:
        raise ValueError("Invalid primary hex")
    r = int(p[1:3], 16) / 255.0
    g = int(p[3:5], 16) / 255.0
    b = int(p[5:7], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    rgb_light = colorsys.hls_to_rgb(h, min(light + 0.2, 0.9), sat)
    light_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255),
    )
    return {
        "primary": p,
        "light": light_color,
        "background": "#000000",
        "accent": p,
    }


def _generate_colors_for_date(date_str: str) -> dict:
    seed = int(hashlib.md5(date_str.encode()).hexdigest(), 16)
    random.seed(seed)
    hue = random.random()
    saturation = random.uniform(0.7, 1.0)
    lightness = random.uniform(0.45, 0.65)
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255),
    )
    rgb_light = colorsys.hls_to_rgb(hue, min(lightness + 0.2, 0.9), saturation)
    light_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255),
    )
    return {
        "primary": primary_color,
        "light": light_color,
        "background": "#000000",
        "accent": primary_color,
    }


def _resolve_theme_override_row_for_today(from_sheet: dict | None = None) -> dict:
    mod = _load_nwst_accent_cfg()
    from_file = _accent_overrides_from_json()
    if from_sheet is None:
        from_sheet = _theme_overrides_from_redis_flush()
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


def generate_daily_colors_for_flush_ui() -> dict:
    """Match attendance_app.generate_daily_colors (weekly palette + Theme Override)."""
    today = datetime.strptime(get_today_myt_date(), "%Y-%m-%d")
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    from_sheet = _theme_overrides_from_redis_flush()
    row = _resolve_theme_override_row_for_today(from_sheet=from_sheet)
    hex_override = row.get("primary")
    base = None
    if hex_override:
        pn = _normalize_primary_hex(hex_override)
        if pn:
            base = _theme_from_primary_hex(pn)
    if base is None:
        base = _generate_colors_for_date(last_saturday.strftime("%Y-%m-%d"))
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


def _banner_mime_for_path(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    return {
        ".gif": "image/gif",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }.get(ext, "image/gif")


def resolve_banner_gif_src(daily_colors: dict) -> tuple[str, str]:
    """Same resolution order as attendance_app (theme file, BANNER_GIF_URL, banner.gif)."""
    app_dir = str(_CHECK_IN_ROOT)
    gif_url = os.getenv("BANNER_GIF_URL", "").strip()
    theme_banner_fn = daily_colors.get("banner")
    background_gif = ""
    gif_src = ""
    if theme_banner_fn:
        p = os.path.join(app_dir, theme_banner_fn)
        if os.path.isfile(p):
            with open(p, "rb") as f:
                raw = base64.b64encode(f.read()).decode()
            mime = _banner_mime_for_path(p)
            background_gif = f"url('data:{mime};base64,{raw}')"
            gif_src = f"data:{mime};base64,{raw}"
    if not gif_src and gif_url:
        background_gif = f"url('{gif_url}')"
        gif_src = gif_url
    if not gif_src:
        default_p = os.path.join(app_dir, "banner.gif")
        if os.path.isfile(default_p):
            with open(default_p, "rb") as f:
                raw = base64.b64encode(f.read()).decode()
            background_gif = f"url('data:image/gif;base64,{raw}')"
            gif_src = f"data:image/gif;base64,{raw}"
    return background_gif, gif_src


def _flush_page_css(page_colors: dict, *, is_leaders_page: bool) -> str:
    """Mirrors attendance_app injected styles (Outfit, buttons, multiselect)."""
    leaders_extra = ""
    if is_leaders_page:
        leaders_extra = """
    .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown div {
        color: #000000 !important;
    }
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
    .stMultiSelect label, .stSelectbox label, .stTextInput label {
        color: #000000 !important;
    }
"""
    nwst_text = ""
    if not is_leaders_page:
        nwst_text = """
    .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown li {
        color: #ffffff !important;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    .stMultiSelect label, .stSelectbox label, .stTextInput label, label[data-testid="stWidgetLabel"] {
        color: #e0e0e0 !important;
    }
    [data-testid="stExpander"] summary, [data-testid="stExpander"] summary p {
        color: #ffffff !important;
    }
"""
    return f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600&display=swap');
    .instruction-text {{
        color: #ffffff !important;
    }}
    .stApp {{
        background-color: {page_colors['background']} !important;
    }}
    html, body, [data-testid="stAppViewContainer"] {{
        font-family: 'Outfit', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
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
    .stButton > button[kind="primary"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        border: 2px solid {page_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {page_colors['light']} !important;
        border-color: {page_colors['light']} !important;
    }}
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {page_colors['primary']} !important;
    }}
    {leaders_extra}
    {nwst_text}
</style>
"""


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

    st.set_page_config(
        page_title="Church Check-In — Flush pending",
        page_icon="⛪",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown('<div id="top-anchor"></div>', unsafe_allow_html=True)

    daily_colors = generate_daily_colors_for_flush_ui()
    early_page = st.query_params.get("page", "nwst")
    is_leaders_page = early_page == "leaders"
    if is_leaders_page:
        page_colors = {
            "primary": daily_colors["primary"],
            "light": daily_colors["light"],
            "background": "#ffffff",
            "text": "#000000",
            "text_muted": "#666666",
            "card_bg": "#f5f5f5",
            "border": daily_colors["primary"],
        }
    else:
        page_colors = {
            "primary": daily_colors["primary"],
            "light": daily_colors["light"],
            "background": "#000000",
            "text": "#ffffff",
            "text_muted": "#999999",
            "card_bg": "#0a0a0a",
            "border": daily_colors["primary"],
        }

    st.markdown(_flush_page_css(page_colors, is_leaders_page=is_leaders_page), unsafe_allow_html=True)

    background_gif, gif_src = resolve_banner_gif_src(daily_colors)
    hero_label = "Flush pending"

    if SESSION_LOG_KEY not in st.session_state:
        st.session_state[SESSION_LOG_KEY] = []

    if background_gif and gif_src:
        st.markdown(
            f"""
        <div style="
            position: relative;
            padding: 1.5rem 1.5rem 1rem 1.5rem;
            margin: 0 0 0.75rem 0;
            border-radius: 8px;
            border: 2px solid {page_colors['primary']};
            min-height: 180px;
            overflow: hidden;
        ">
            <img src="{gif_src}"
                 alt=""
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
            ">{hero_label}</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        tab_choice = st.multiselect(
            "Tabs to flush",
            options=["attendance", "leaders", "ministry", "all"],
            default=["all"],
            help="Which sheet tabs to process. Include **all** to flush every pending queue for today (MYT).",
        )
        if not tab_choice:
            st.warning("Select at least one tab.")
            tab_choice = ["all"]

        if st.button("Run flush now", type="primary", use_container_width=True, key="flush_run"):
            run_log: list[str] = []
            st.session_state[SESSION_LOG_KEY].append("--- run started ---")

            sheet_id = _resolve_sheet_id(run_log)
            if not sheet_id:
                st.error("⚠️ ATTENDANCE_SHEET_ID missing (env or Streamlit secrets).")
                st.session_state[SESSION_LOG_KEY].extend(run_log)
            else:
                with st.spinner("Flushing…"):
                    client = _gsheet_client(run_log)
                    if not client:
                        st.session_state[SESSION_LOG_KEY].extend(run_log)
                        st.error("Could not build Google Sheets client — see session log below.")
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
            if st.button("Clear on-screen log", key="flush_clear_log"):
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
