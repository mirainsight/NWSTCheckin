"""Accent overrides shared by CHECK IN and NWST Health.

1) **Theme Override** tab on the CHECK IN workbook — snapshot in **Upstash** (see nwst_accent_redis.py).
   Refreshed from **either** app: CHECK IN **Update names** (`attendance_app.perform_hard_sheet_resync`)
   **or** NWST Health **Sync from Google Sheets**. Same key; not pulled on every page view.

2) Sibling file nwst_accent_overrides.json — re-read each resolve (Rerun picks up edits).

Redis Theme rows override JSON for the same date (per field). Env/secrets still apply for hex after both.

Banner values must be filenames only (e.g. banner.gif); place files in the app root folder
( CHECK IN / or NWST HEALTH / next to the Streamlit app)."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

# Sibling imports (nwst_accent_redis / nwst_accent_gsheet) when loaded via importlib and
# the Streamlit entrypoint is not this directory (e.g. app under .streamlit/).
_CFG_DIR = str(Path(__file__).resolve().parent)
if _CFG_DIR not in sys.path:
    sys.path.insert(0, _CFG_DIR)

_JSON = Path(__file__).resolve().parent / "nwst_accent_overrides.json"
_DATE_KEY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BANNER_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def sanitize_banner_filename(raw: str | None) -> str | None:
    """Allow only a safe basename like ``banner.gif`` (no paths)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    base = os.path.basename(s.replace("\\", "/"))
    if not base:
        return None
    if not _BANNER_SAFE.match(base):
        return None
    low = base.lower()
    if not low.endswith((".gif", ".png", ".jpg", ".jpeg", ".webp")):
        return None
    return base


def merge_theme_override_maps(
    file_map: dict[str, dict[str, str]],
    sheet_map: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Merge date -> row dicts; sheet keys win on conflict."""
    keys = set(file_map) | set(sheet_map)
    out: dict[str, dict[str, str]] = {}
    for k in keys:
        inner = {**(file_map.get(k) or {}), **(sheet_map.get(k) or {})}
        if inner:
            out[k] = inner
    return out


def _coerce_date_entry(value: object) -> dict[str, str]:
    if isinstance(value, str):
        s = value.strip()
        return {"primary": s} if s else {}
    if isinstance(value, dict):
        entry: dict[str, str] = {}
        p = value.get("primary") or value.get("hex") or value.get("color")
        if p:
            entry["primary"] = str(p).strip()
        b = value.get("banner") or value.get("banner_file") or value.get("image")
        if b:
            entry["banner"] = str(b).strip()
        return entry
    return {}


def get_accent_override_by_date() -> dict[str, dict[str, str]]:
    """MYT date -> {primary?, banner?}. Legacy string values become {primary: ...}."""
    if not _JSON.is_file():
        return {}
    try:
        with open(_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    raw = data.get("accent_override_by_date", data)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not _DATE_KEY.match(k):
            continue
        entry = _coerce_date_entry(v)
        if entry:
            out[k] = entry
    return out


def read_theme_override_from_redis(redis_client: object) -> dict[str, dict[str, str]]:
    from nwst_accent_redis import theme_overrides_from_redis

    return theme_overrides_from_redis(redis_client)


def refresh_theme_override_shared_cache(
    redis_client: object,
    gsheet_client: object,
    checkin_sheet_id: str,
) -> None:
    """Pull Theme Override from the CHECK IN workbook into Upstash (one snapshot for all apps).

    **Call from either sync path** (different files / UI buttons; same outcome):

    - ``PROJECTS/CHECK IN/attendance_app.py`` — end of ``perform_hard_sheet_resync`` (Update names).
    - ``PROJECTS/NWST HEALTH/.streamlit/app.py`` — after successful **Sync from Google Sheets** on CG Health.
    """
    if not redis_client or not gsheet_client:
        return
    sid = (checkin_sheet_id or "").strip()
    if not sid:
        return
    from nwst_accent_gsheet import fetch_accent_overrides_from_gsheet
    from nwst_accent_redis import store_theme_overrides_in_redis

    try:
        data = fetch_accent_overrides_from_gsheet(gsheet_client, sid)
    except Exception:
        return
    if not isinstance(data, dict):
        data = {}
    store_theme_overrides_in_redis(redis_client, data)


# Backward-compatible name
refresh_theme_override_cache = refresh_theme_override_shared_cache
