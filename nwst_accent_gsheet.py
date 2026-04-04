"""Optional accent overrides from the shared CHECK IN spreadsheet (ATTENDANCE_SHEET_ID).

Add a tab named **Theme Override** with a header row, e.g.:

  | date       | primary_hex | banner      |
  |------------|-------------|-------------|
  | 2026-04-04 | #C26D4A     | banner.gif  |

Column names:
  date: date / myt_date / day
  primary: primary_hex / hex / color / primary / accent
  banner: banner / banner_file / image / gif / filename (filename only; file lives in app root)

If the header row is not recognized, columns A, B, C are treated as date, hex, and optional banner."""

from __future__ import annotations

import re
from typing import Any

THEME_OVERRIDE_TAB = "Theme Override"
_DATE_KEY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _norm_header(s: Any) -> str:
    return str(s).strip().lower().replace(" ", "_")


def fetch_accent_overrides_from_gsheet(client, sheet_id: str) -> dict[str, dict[str, str]]:
    """MYT YYYY-MM-DD -> {"primary": hex, "banner": optional filename}."""
    if not client or not sheet_id:
        return {}
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(THEME_OVERRIDE_TAB)
        rows = ws.get_all_values()
    except Exception:
        return {}
    if len(rows) < 2:
        return {}
    header = [_norm_header(c) for c in rows[0]]
    date_idx = None
    hex_idx = None
    banner_idx = None
    for i, h in enumerate(header):
        if h in ("date", "myt_date", "day"):
            date_idx = i
        elif h in ("primary_hex", "hex", "color", "primary", "accent"):
            hex_idx = i
        elif h in ("banner", "banner_file", "image", "gif", "filename"):
            banner_idx = i
    if date_idx is None and hex_idx is None and len(header) >= 2:
        date_idx, hex_idx = 0, 1
        if len(header) >= 3 and banner_idx is None:
            banner_idx = 2
    if date_idx is None or hex_idx is None:
        return {}
    if banner_idx is None:
        used = {date_idx, hex_idx}
        for i in range(len(header)):
            if i not in used:
                banner_idx = i
                break
    out: dict[str, dict[str, str]] = {}
    for row in rows[1:]:
        if date_idx >= len(row):
            continue
        dk = str(row[date_idx]).strip()
        if not _DATE_KEY.match(dk):
            continue
        entry: dict[str, str] = {}
        if hex_idx < len(row):
            hv = str(row[hex_idx]).strip()
            if hv:
                entry["primary"] = hv
        if banner_idx is not None and banner_idx < len(row):
            bv = str(row[banner_idx]).strip()
            if bv:
                entry["banner"] = bv
        if entry:
            out[dk] = entry
    return out
