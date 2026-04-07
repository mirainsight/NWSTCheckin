"""Upstash cache for Theme Override rows (CHECK IN spreadsheet tab).

**Two independent write triggers** (same Redis key; either run refreshes everyone):

1. **CHECK IN** — ``attendance_app.perform_hard_sheet_resync`` after **Update names → Sync with Google Sheets**
   (congregation or ministry mode).

2. **NWST Health** — CG Health page **Sync from Google Sheets** (reads Theme tab from ``ATTENDANCE_SHEET_ID``).

Page loads **read Redis only** — no Theme-tab Google request per view.

Note: Legacy ``CHECK IN/app.py`` does not define that resync; use ``attendance_app.py`` (or run a NWST sync) to refresh."""

from __future__ import annotations

import json
import re
from typing import Any

REDIS_THEME_OVERRIDE_KEY = "nwst_theme_override_by_date_v1"
# Long TTL as a safety net; sync refreshes the value.
THEME_OVERRIDE_REDIS_TTL_SEC = 86400 * 90
_DATE_KEY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def theme_overrides_from_redis(redis_client: Any) -> dict[str, dict[str, str]]:
    """Return date -> {primary?, banner?} from Redis; empty dict if missing or error."""
    if not redis_client:
        return {}
    try:
        raw = redis_client.get(REDIS_THEME_OVERRIDE_KEY)
        if not raw:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError, OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not _DATE_KEY.match(k):
            continue
        if not isinstance(v, dict):
            continue
        inner: dict[str, str] = {}
        if v.get("primary"):
            inner["primary"] = str(v["primary"]).strip()
        if v.get("banner"):
            inner["banner"] = str(v["banner"]).strip()
        if inner:
            out[k] = inner
    return out


def store_theme_overrides_in_redis(redis_client: Any, theme_map: dict[str, dict[str, str]]) -> None:
    if not redis_client:
        return
    try:
        redis_client.set(
            REDIS_THEME_OVERRIDE_KEY,
            json.dumps(theme_map),
            ex=THEME_OVERRIDE_REDIS_TTL_SEC,
        )
    except Exception:
        pass
