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


_STATUS_ABBREV = {
    "regular": "Reg", "irregular": "Irr", "follow up": "FU",
    "new": "New", "red": "Red", "graduated": "Grad",
}
_ROLE_ABBREV = {
    "cg leader": "CGL", "assistant cg leader": "ACGL",
    "cg core": "CGC", "potential cg core": "PCGC",
    "ministry leader": "ML", "assistant ministry leader": "AML",
    "ministry core": "MC", "potential ministry core": "PMC",
    "zone leader": "ZL",
}
_MINISTRY_ROLE_COLS = ["hype role", "frontlines role", "vs role", "worship role"]


def _abbrev_status(raw: str) -> str:
    key = raw.rstrip(":").strip().lower()
    return _STATUS_ABBREV.get(key, raw.rstrip(":").strip() or "—")


def _abbrev_role(raw: str) -> str:
    key = raw.strip().lower()
    for prefix, abbrev in _ROLE_ABBREV.items():
        if key.startswith(prefix):
            return abbrev
    return raw.strip() or ""


def _format_members(r) -> str:
    cg_data = _load_json_key(r, "nwst_cg_combined_data")
    if not cg_data:
        return ""

    columns = cg_data.get("columns", [])
    rows = cg_data.get("rows", [])
    if not columns or not rows:
        return ""

    cols_lower = [c.lower().strip() for c in columns]

    def _find(*candidates) -> int | None:
        for c in candidates:
            if c in cols_lower:
                return cols_lower.index(c)
        return None

    name_idx   = _find("name", "member name", "member")
    cell_idx   = _find("cell", "cell group", "group")
    status_idx = _find("status", "member status")
    role_idx   = _find("role")
    gender_idx = _find("gender")
    age_idx    = _find("age")
    min_dept_idx = _find("ministry department")
    ministry_role_idxs = {label: cols_lower.index(label) for label in _MINISTRY_ROLE_COLS if label in cols_lower}

    if name_idx is None:
        return ""

    att_stats: dict = _load_json_key(r, "nwst_attendance_stats") or {}

    lines = ["=== MEMBERS (CG Combined) ===",
             "Format: Name | Cell | Status | Gender | Age | Role | Ministry | Att% | Last attended | Recent(last 8 sessions)"]

    for row in rows:
        def _get(idx) -> str:
            return row[idx].strip() if idx is not None and idx < len(row) else ""

        name   = _get(name_idx)
        if not name:
            continue
        cell   = _get(cell_idx)
        status = _abbrev_status(_get(status_idx))
        gender = _get(gender_idx) or "—"
        age    = _get(age_idx) or "—"
        role   = _abbrev_role(_get(role_idx)) if role_idx is not None else ""

        # Ministry: prefer Ministry Department col; fallback to first non-empty ministry role col
        ministry = _get(min_dept_idx) if min_dept_idx is not None else ""
        if not ministry:
            for label, idx in ministry_role_idxs.items():
                val = _get(idx)
                if val:
                    # e.g. "hype role" + "Core" → "Hype:Core"
                    ministry = label.replace(" role", "").title() + ":" + val
                    break

        # Attendance lookup keyed by "Name - Cell" (matching flush_pending format)
        att_key = f"{name} - {cell}" if cell else name
        att_info = att_stats.get(att_key) or att_stats.get(name) or {}
        att_pct  = f"{att_info.get('percentage', 0)}%" if att_info else "—"
        last_att = att_info.get("last_attended") or "—"
        rec_att  = att_info.get("recent_attended")
        rec_tot  = att_info.get("recent_total")
        recent   = f"{rec_att}/{rec_tot}" if rec_att is not None else "—"

        parts = [
            name,
            cell or "—",
            status,
            gender,
            age,
            role or "—",
            ministry or "—",
            f"Att:{att_pct}",
            f"Last:{last_att}",
            f"R:{recent}",
        ]
        lines.append(" | ".join(parts))

    return "\n".join(lines) if len(lines) > 2 else ""


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

    unique_members = len({n for names in groups.values() for n in names})
    lines = [f"=== MINISTRIES ({unique_members} unique members across all teams) ==="]
    for ministry, names in sorted(groups.items()):
        pct = round(len(names) / unique_members * 100) if unique_members else 0
        lines.append(f"{ministry}: {len(names)} members ({pct}%)")
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
