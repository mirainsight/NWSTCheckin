"""
Live data context builder for the NWST chatbot.

Two-layer cache:
  1. Redis key `chatbot:data_context` (TTL 300s) — shared across all app instances/users.
  2. Streamlit @st.cache_data (TTL 300s) — in-memory, shared within one worker process.

On a normal load: Streamlit cache hit → 0 Redis reads.
On a worker cold-start with warm Redis: 1 Redis read (reads pre-built context string).
On cache miss (first load or expired): ~7 Redis reads, then re-warms both caches.
Forced refresh (cache_buster != 0): clears Redis key and Streamlit cache, full rebuild.
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta, timezone
from pathlib import Path

import streamlit as st

_CHATBOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _CHATBOT_DIR.parent
for _p in [str(_CHATBOT_DIR), str(_REPO_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

MYT = timezone(timedelta(hours=8))

REDIS_CTX_KEY = "chatbot:data_context"
REDIS_CTX_TTL = 300  # 5 minutes

ATTENDANCE_TABS = [
    ("Congregation", "Attendance"),
    ("Leaders", "Leaders Attendance"),
    ("Ministry", "Ministry Attendance"),
]


# ── helpers ────────────────────────────────────────────────────────────────────

def _decode(raw) -> str:
    return raw.decode() if isinstance(raw, bytes) else (raw or "")


def _load_json_key(r, key: str):
    try:
        raw = r.get(key)
        if not raw:
            return None
        return json.loads(_decode(raw))
    except Exception:
        return None


# ── section formatters ─────────────────────────────────────────────────────────

def _format_last_sync(r) -> str:
    try:
        val = _decode(r.get("nwst_last_sync_time") or b"")
        return f"[Data last synced: {val}]" if val else ""
    except Exception:
        return ""


def _format_cell_health(r) -> str:
    try:
        from nwst_shared.nwst_cell_health_cache import get_cell_health_from_redis
        data = get_cell_health_from_redis(r)
    except Exception:
        return ""
    if not data:
        return ""

    snapshot = data.get("snapshot_date", "?")
    lines = [f"=== CELL HEALTH (snapshot {snapshot}) ==="]

    def _bucket(row: dict, count_key: str, pct_key: str, delta_key: str | None = None) -> str:
        count = row.get(count_key, 0)
        pct = row.get(pct_key, 0.0)
        delta = row.get(delta_key, 0) if delta_key else 0
        d = f",{delta:+d}" if delta else ""
        return f"{count}({pct:.0f}%{d})"

    def _fmt_row(row: dict) -> str:
        cell = row.get("cell", "?")
        zone = row.get("zone", "")
        total = row.get("total_count", 0)
        label = f"[{cell}" + (f" | {zone}" if zone else "") + "]"
        return (
            f"{label} Total:{total} | "
            f"Reg:{_bucket(row,'regular_count','regular_pct','delta_regular')} "
            f"Irr:{_bucket(row,'irregular_count','irregular_pct','delta_irregular')} "
            f"FU:{_bucket(row,'follow_up_count','follow_up_pct','delta_follow_up')} "
            f"Red:{row.get('red_count',0)} "
            f"New:{_bucket(row,'new_count','new_pct','delta_new')} "
            f"Grad:{row.get('graduated_count',0)}"
        )

    all_row = data.get("all_row")
    if all_row:
        lines.append(_fmt_row(all_row))

    cell_rows = sorted(data.get("cell_rows", []), key=lambda r: r.get("regular_pct", 0))
    for row in cell_rows:
        lines.append(_fmt_row(row))

    return "\n".join(lines)


def _format_members(r) -> str:
    cg_data = _load_json_key(r, "nwst_cg_combined_data")
    if not cg_data:
        return ""

    columns = cg_data.get("columns", [])
    rows = cg_data.get("rows", [])
    if not columns or not rows:
        return ""

    cols_lower = [c.lower().strip() for c in columns]

    def _find_col(*candidates) -> int | None:
        for c in candidates:
            if c in cols_lower:
                return cols_lower.index(c)
        return None

    name_idx = _find_col("name", "member name", "member")
    cell_idx = _find_col("cell", "cell group", "group")
    status_idx = _find_col("status", "member status")

    if name_idx is None:
        return ""

    att_stats: dict = _load_json_key(r, "nwst_attendance_stats") or {}

    lines = ["=== MEMBERS (CG Combined) ==="]
    for row in rows:
        name = row[name_idx].strip() if name_idx is not None and name_idx < len(row) else ""
        cell = row[cell_idx].strip() if cell_idx is not None and cell_idx < len(row) else ""
        status_raw = row[status_idx].strip() if status_idx is not None and status_idx < len(row) else ""
        if not name:
            continue
        # Clean status prefix (e.g. "Regular:" → "Regular")
        status = status_raw.rstrip(":") if status_raw else "—"

        # Attendance lookup: try "Name - Cell" key
        att_key = f"{name} - {cell}" if cell else name
        att_info = att_stats.get(att_key) or att_stats.get(name, {})
        att_pct = f"{att_info.get('percentage', 0)}%" if att_info else "—"

        lines.append(f"{name} | {cell or '—'} | {status} | Att:{att_pct}")

    return "\n".join(lines) if len(lines) > 1 else ""


def _format_checkin_today(r, today_str: str) -> str:
    sections = []
    for label, tab_name in ATTENDANCE_TABS:
        data = _load_json_key(r, f"attendance:data:{today_str}:{tab_name}")
        if not data:
            continue
        cell_group_data: dict = data.get("cell_group_data", {})
        checked_in_list: list = data.get("checked_in_list", [])
        if not checked_in_list:
            continue
        lines = [f"{label} ({len(checked_in_list)} checked in):"]
        for cell, names in sorted(cell_group_data.items()):
            lines.append(f"  {cell}: {', '.join(names)}")
        sections.append("\n".join(lines))

    if not sections:
        return ""
    return "=== CHECK-IN TODAY (" + today_str + ") ===\n" + "\n".join(sections)


def _format_newcomers(r, week_start_str: str) -> str:
    data = _load_json_key(r, f"attendance:newcomers:week:{week_start_str}")
    if not data:
        return ""
    count = data.get("count", 0)
    newcomers = data.get("newcomers_list", [])
    header = "=== NEWCOMERS THIS WEEK ==="
    if count == 0 or not newcomers:
        return f"{header}\nNone this week."
    entries = []
    for n in newcomers:
        name = n.get("name", "?")
        cell = n.get("cell", "").strip()
        entries.append(f"{name} ({cell})" if cell else name)
    return f"{header}\n{', '.join(entries)}  [Total: {count}]"


def _format_ministries(r) -> str:
    data = _load_json_key(r, "nwst_ministries_combined_data")
    if not data:
        return ""
    columns = data.get("columns", [])
    rows = data.get("rows", [])
    if not columns or not rows:
        return ""

    cols_lower = [c.lower().strip() for c in columns]

    def _find_col(*candidates) -> int | None:
        for c in candidates:
            if c in cols_lower:
                return cols_lower.index(c)
        return None

    name_idx = _find_col("name", "member name", "member")
    ministry_idx = _find_col("ministry", "department", "team")

    if name_idx is None or ministry_idx is None:
        return ""

    groups: dict[str, list[str]] = {}
    for row in rows:
        name = row[name_idx].strip() if name_idx < len(row) else ""
        ministry = row[ministry_idx].strip() if ministry_idx < len(row) else ""
        if name and ministry:
            groups.setdefault(ministry, []).append(name)

    if not groups:
        return ""

    lines = ["=== MINISTRIES ==="]
    for ministry, names in sorted(groups.items()):
        lines.append(f"{ministry}: {', '.join(sorted(names))}")
    return "\n".join(lines)


# ── public API ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=REDIS_CTX_TTL, show_spinner=False)
def build_data_context(today_str: str, week_start_str: str, cache_buster: int = 0) -> str:
    """
    Returns a compact multi-section text of all live NWST data.
    Cached in Streamlit memory (layer 2) and Redis (layer 1).
    Pass cache_buster != 0 to force a full rebuild.
    """
    from chatbot_redis import get_redis_client
    r = get_redis_client()
    if not r:
        return ""

    # Layer 1: read pre-built Redis cache (skip on forced refresh)
    if cache_buster == 0:
        try:
            cached = r.get(REDIS_CTX_KEY)
            if cached:
                return _decode(cached)
        except Exception:
            pass
    else:
        try:
            r.delete(REDIS_CTX_KEY)
        except Exception:
            pass

    # Full build
    parts: list[str] = []
    for section in [
        _format_last_sync(r),
        _format_cell_health(r),
        _format_members(r),
        _format_checkin_today(r, today_str),
        _format_newcomers(r, week_start_str),
        _format_ministries(r),
    ]:
        if section and section.strip():
            parts.append(section)

    context = "\n\n".join(parts)

    # Store in Redis so other workers / users get it cheaply
    if context:
        try:
            r.set(REDIS_CTX_KEY, context, ex=REDIS_CTX_TTL)
        except Exception:
            pass

    return context
