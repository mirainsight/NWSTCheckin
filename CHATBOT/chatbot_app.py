from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

_CHATBOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _CHATBOT_DIR.parent
_CHECK_IN_DIR = _REPO_ROOT / "CHECK IN"

for _p in [str(_CHATBOT_DIR), str(_REPO_ROOT), str(_CHECK_IN_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from dotenv import load_dotenv
    load_dotenv(_CHECK_IN_DIR / ".env")
    load_dotenv()
except ImportError:
    pass

import json
import re

import streamlit as st
from chatbot_redis import get_redis_client, get_chatbot_redis_client, log_qa_to_redis, submit_change_request
from chatbot_data import build_data_context
from nwst_shared.nwst_daily_palette import generate_colors_for_date, theme_from_primary_hex, normalize_primary_hex
from nwst_shared.nwst_accent_config import get_accent_override_by_date, resolve_latest_cached_theme_row
from nwst_shared.nwst_accent_redis import theme_overrides_from_redis

MYT = timezone(timedelta(hours=8))


def _get_daily_palette() -> dict:
    today_str = datetime.now(MYT).strftime("%Y-%m-%d")
    try:
        rc = get_redis_client()
        sheet_map = theme_overrides_from_redis(rc)
        file_map = get_accent_override_by_date()
        row = resolve_latest_cached_theme_row(file_map, sheet_map)
        pn = normalize_primary_hex(row.get("primary"))
        if pn:
            return theme_from_primary_hex(pn)
    except Exception:
        pass
    return generate_colors_for_date(today_str)

MAX_RESPONSE_TOKENS = 800  # increased to accommodate <thinking> block + answer
MAX_CONTEXT_MESSAGES = 6   # last 3 human + 3 assistant turns
MODEL = "gpt-4o-mini"
DATA_TTL_SECONDS = 300     # auto-refresh data every 5 minutes

_PROMPT_FILE = _CHATBOT_DIR / "prompt_instructions.md"
SYSTEM_PROMPT = (
    _PROMPT_FILE.read_text(encoding="utf-8")
    if _PROMPT_FILE.exists()
    else "You are a helpful assistant for NWST (Narrow Street), a church community in Malaysia."
)


def _get_week_start() -> str:
    today = datetime.now(MYT).date()
    days_since_saturday = (today.weekday() - 5) % 7
    return (today - timedelta(days=days_since_saturday)).isoformat()


def _should_refresh_data() -> bool:
    fetched_at = st.session_state.get("data_fetched_at")
    if fetched_at is None:
        return True
    return (datetime.now(MYT) - fetched_at).total_seconds() > DATA_TTL_SECONDS


def _load_data(cache_buster: int = 0) -> None:
    today_str = datetime.now(MYT).strftime("%Y-%m-%d")
    week_start_str = _get_week_start()
    st.session_state["data_context"] = build_data_context(today_str, week_start_str, cache_buster)
    st.session_state["data_fetched_at"] = datetime.now(MYT)


def _context_ring_html(current: int, total: int) -> str:
    """SVG circular progress showing context window usage."""
    pct = current / total if total > 0 else 0
    fill = round(pct * 100, 1)
    color = "#2ecc71" if pct < 0.5 else ("#e67e22" if pct < 1.0 else "#e74c3c")
    left = total - current
    label = f"{left} msg{'s' if left != 1 else ''} left" if left > 0 else "rolling — oldest dropped"
    return (
        f'<div style="display:flex;align-items:center;gap:8px;padding:2px 0;">'
        f'<svg width="26" height="26" viewBox="0 0 36 36" style="transform:rotate(-90deg);flex-shrink:0;">'
        f'<circle cx="18" cy="18" r="15.9" fill="none" stroke="#2a2a2a" stroke-width="3.5"/>'
        f'<circle cx="18" cy="18" r="15.9" fill="none" stroke="{color}" stroke-width="3.5"'
        f' stroke-dasharray="{fill} 100" stroke-linecap="round"/>'
        f'</svg>'
        f'<span style="color:#666;font-size:0.78rem;">Context {current}/{total} &nbsp;·&nbsp; {label}</span>'
        f'</div>'
    )


def _parse_response(content: str) -> tuple[str, str]:
    """Split model output into (thinking, answer). Returns ('', content) if no <thinking> block."""
    match = re.search(r"<thinking>(.*?)</thinking>", content, re.DOTALL)
    if match:
        thinking = match.group(1).strip()
        answer = content[match.end():].strip()
        return thinking, answer
    return "", content


def _token_stat_html(this_t: int, session_tokens: list) -> str:
    """Format the token stat line shown below an assistant reply."""
    total = sum(session_tokens)
    count = len(session_tokens)
    if total == 0 or this_t == 0:
        return ""
    pct = this_t / total * 100
    avg = total / count
    delta = this_t - avg
    pct_str = f"{pct:.0f}% of session"
    if count == 1:
        delta_str = "first reply"
    elif delta > 0:
        delta_str = f"+{delta:.0f} vs avg"
    elif delta < 0:
        delta_str = f"{delta:.0f} vs avg"
    else:
        delta_str = "= avg"
    return (
        f"<p style='font-size:0.72rem;color:#555;font-style:italic;margin:2px 0 0 0;'>"
        f"+{this_t:,} tokens &nbsp;·&nbsp; {pct_str} &nbsp;·&nbsp; {delta_str}"
        f"</p>"
    )


def _status_style(v: str) -> str:
    """Inline CSS matching NWST HEALTH status colours."""
    if v.startswith("Regular:"):   return "color:#2ecc71;font-weight:700"
    if v.startswith("Irregular:"): return "color:#e67e22;font-weight:700"
    if v.startswith("New"):        return "color:#3498db;font-weight:700"
    if v.startswith("Follow Up:"): return "color:#f39c12;font-weight:700"
    if v.startswith("Red:"):       return "color:#e74c3c;font-weight:700"
    if v.startswith("Graduated:"): return "color:#9b59b6;font-weight:700"
    return "color:#ffffff;font-weight:700"


def _member_info_html(member: dict, mcols: list, label: str, pending: list, palette: dict | None = None) -> str:
    """Render the member profile as a grouped HTML card using the daily colour palette."""
    if palette is None:
        palette = _get_daily_palette()

    primary = palette.get("primary", "#5bc0eb")
    light = palette.get("light", "#8dd4f0")
    try:
        pr, pg, pb = int(primary[1:3], 16), int(primary[3:5], 16), int(primary[5:7], 16)
    except (ValueError, IndexError):
        pr, pg, pb = 91, 192, 235

    _MR = ["Hype Role", "Frontlines Role", "VS Role", "Worship Role"]
    _LM_ALL = ["Role"] + _MR + ["Ministry Department"]

    def _hv(f):
        fi = _cr_field_col_idx(mcols, f)
        return fi != -1 and bool(str(member.get(mcols[fi], "") or "").strip())

    sorted_roles = sorted(_MR, key=lambda f: (not _hv(f), _MR.index(f)))
    ministry_fields = sorted_roles + ["Ministry Department"]
    has_lm = any(_hv(f) for f in _LM_ALL)

    lm_groups = [("LEADERSHIP", ["Role"]), ("MINISTRY", ministry_fields)]
    fixed_top = [("IDENTITY", ["Name", "Cell"]), ("HEALTH", ["Status", "New Since", "Prev Cell"])]
    fixed_bot = [
        ("PERSONAL", ["Gender", "Age", "School / Work", "Notes", "Birthday"]),
        ("CONTACT",  ["Contact No.", "Email Address", "Emergency Contact", "Emergency Relationship"]),
    ]
    groups = fixed_top + lm_groups + fixed_bot if has_lm else fixed_top + fixed_bot + lm_groups

    parts = label.split(" · ", 1)
    name_part = parts[0]
    cell_part = (
        f' <span style="color:#999999;font-size:0.88rem;">· {parts[1]}</span>'
        if len(parts) > 1 else ""
    )

    card = (
        f'<div style="background:#0a0a0a;border:1px solid rgba({pr},{pg},{pb},0.25);'
        f'border-top:3px solid {primary};border-radius:10px;overflow:hidden;margin:4px 0;'
        f'font-family:Inter,-apple-system,BlinkMacSystemFont,sans-serif;'
        f'box-shadow:0 8px 32px rgba({pr},{pg},{pb},0.15);">'
        f'<div style="background:#111111;padding:12px 16px;border-bottom:1px solid rgba({pr},{pg},{pb},0.15);">'
        f'<span style="font-size:1.0rem;font-weight:700;color:{primary};">👤 {name_part}</span>'
        f'{cell_part}</div>'
    )

    first_group = True
    for section_label, fields in groups:
        rows = ""
        for field in fields:
            fi = _cr_field_col_idx(mcols, field)
            v = ""
            if fi != -1:
                v = str(member.get(mcols[fi], "") or "").strip()
            val_style = (_status_style(v) if field == "Status" and v else ("color:#ffffff" if v else "color:#333333"))
            val_text = v if v else "—"
            rows += (
                f'<tr style="border-top:1px solid #141414;">'
                f'<td style="padding:5px 8px 5px 16px;color:#999999;font-size:0.82rem;width:38%;white-space:nowrap;">{field}</td>'
                f'<td style="padding:5px 16px 5px 0;{val_style};font-size:0.82rem;">{val_text}</td>'
                f'</tr>'
            )
        top_border = "" if first_group else f"border-top:1px solid rgba({pr},{pg},{pb},0.12);"
        card += (
            f'<div style="padding:8px 16px 2px;font-size:0.68rem;font-weight:700;color:{primary};'
            f'letter-spacing:0.12em;text-transform:uppercase;{top_border}">{section_label}</div>'
            f'<table style="width:100%;border-collapse:collapse;">{rows}</table>'
        )
        first_group = False

    if pending:
        rows = ""
        for ch in pending:
            old = ch.get("current_value", "") or "—"
            new = ch["new_value"]
            rows += (
                f'<tr style="border-top:1px solid rgba({pr},{pg},{pb},0.1);">'
                f'<td style="padding:5px 8px 5px 16px;color:{light};font-size:0.82rem;width:38%;">{ch["field"]}</td>'
                f'<td style="padding:5px 16px 5px 0;color:{light};font-size:0.82rem;">'
                f'{old} <span style="color:#555;">→</span> {new}</td>'
                f'</tr>'
            )
        n = len(pending)
        card += (
            f'<div style="padding:8px 16px 2px;font-size:0.68rem;font-weight:700;color:{light};'
            f'letter-spacing:0.12em;text-transform:uppercase;border-top:1px solid rgba({pr},{pg},{pb},0.2);">'
            f'{n} QUEUED CHANGE{"S" if n != 1 else ""}</div>'
            f'<table style="width:100%;border-collapse:collapse;">{rows}</table>'
        )

    card += '</div>'
    return card


def _get_openai_key() -> str:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        try:
            key = (st.secrets.get("OPENAI_API_KEY") or "").strip()
        except Exception:
            pass
    return key


def _get_login_config() -> tuple[str, list[str]]:
    """Return (password, allowed_emails) from Streamlit secrets or env."""
    password = ""
    raw_emails: object = ""
    try:
        password = (st.secrets.get("CHATBOT_PASSWORD") or "").strip()
        raw_emails = st.secrets.get("CHATBOT_ALLOWED_EMAILS") or ""
    except Exception:
        pass
    if not password:
        password = os.getenv("CHATBOT_PASSWORD", "").strip()
    if not raw_emails:
        raw_emails = os.getenv("CHATBOT_ALLOWED_EMAILS", "")
    if isinstance(raw_emails, (list, tuple)):
        allowed = [e.strip().lower() for e in raw_emails if str(e).strip()]
    else:
        allowed = [e.strip().lower() for e in str(raw_emails).split(",") if e.strip()]
    return password, allowed


def _check_login(email: str, password: str) -> bool:
    correct_pw, allowed = _get_login_config()
    if not correct_pw or not allowed:
        return False
    return email.strip().lower() in allowed and password == correct_pw


def _lookup_member_by_email(email: str) -> dict | None:
    """Look up a member row in nwst_cg_combined_data by email. Returns col→value dict or None."""
    if not email:
        return None
    rc = get_redis_client()
    if not rc:
        return None
    try:
        raw = rc.get("nwst_cg_combined_data")
        if not raw:
            return None
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        cols = data.get("columns", [])
        rows = data.get("rows", [])
        cols_lower = [str(c).lower().strip() for c in cols]

        email_idx = next((i for i, c in enumerate(cols_lower) if "email" in c), -1)
        if email_idx == -1:
            return None

        for row in rows:
            cell_val = row[email_idx] if email_idx < len(row) else ""
            if str(cell_val).strip().lower() == email:
                return dict(zip(cols_lower, row))
    except Exception:
        pass
    return None


def _pick(member: dict, *keywords: str) -> str:
    """Return the first non-empty value from a member dict whose key contains any keyword."""
    for kw in keywords:
        for k, v in member.items():
            if kw in k and str(v).strip():
                return str(v).strip()
    return ""


def _call_openai(messages: list[dict]) -> SimpleNamespace:
    api_key = _get_openai_key()
    if not api_key:
        return SimpleNamespace(
            content="OPENAI_API_KEY is not configured. Add it to your Streamlit secrets.",
            tokens=0,
        )
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_tokens=MAX_RESPONSE_TOKENS,
            temperature=0.4,
        )
        return SimpleNamespace(
            content=resp.choices[0].message.content,
            tokens=resp.usage.total_tokens,
        )
    except Exception as e:
        return SimpleNamespace(content=f"Error calling OpenAI: {e}", tokens=0)


# ── change-request wizard ──────────────────────────────────────────────────────

_CR_FIELDS = [
    # IDENTITY
    "Name", "Cell",
    # HEALTH
    "Status", "Prev Cell",
    # LEADERSHIP
    "Role",
    # MINISTRY
    "Hype Role", "Frontlines Role", "VS Role", "Worship Role", "Ministry Department",
    # PERSONAL
    "Gender", "Birthday", "School / Work", "Notes",
    # CONTACT
    "Contact No.", "Email Address", "Emergency Contact", "Emergency Relationship",
]

_CR_DROPDOWN_FIELDS = {
    "Cell", "Role", "Hype Role", "Frontlines Role",
    "VS Role", "Worship Role", "Ministry Department",
    "Status", "Gender", "Prev Cell",
}

_CR_INFO_ONLY_FIELDS = ["Age", "Attendance", "New Since"]

_CR_FIELD_ALIASES: dict[str, str] = {
    "phone": "Contact No.", "mobile": "Contact No.", "hp": "Contact No.",
    "handphone": "Contact No.", "number": "Contact No.",
    "bday": "Birthday", "birth": "Birthday", "born": "Birthday", "dob": "Birthday",
    "mail": "Email Address",
    "emergency": "Emergency Contact", "ec": "Emergency Contact",
    "relationship": "Emergency Relationship",
    "school": "School / Work", "work": "School / Work",
    "job": "School / Work", "occupation": "School / Work",
    "prev": "Prev Cell", "previous": "Prev Cell",
    "ministry": "Ministry Department", "dept": "Ministry Department",
    "department": "Ministry Department",
    "worship": "Worship Role", "hype": "Hype Role",
    "frontline": "Frontlines Role", "frontlines": "Frontlines Role",
    "vs": "VS Role", "volunteer": "VS Role",
    "note": "Notes", "notes": "Notes", "remark": "Notes", "remarks": "Notes",
    "sex": "Gender",
    "archive": "Cell", "transfer": "Cell", "move": "Cell",
    "duplicate": "Status",
}


def _get_health_sheet_id() -> str:
    sid = os.getenv("NWST_HEALTH_SHEET_ID", "").strip()
    if not sid:
        try:
            sid = (st.secrets.get("NWST_HEALTH_SHEET_ID") or "").strip()
        except Exception:
            pass
    return sid


@st.cache_data(ttl=86400)
def _load_key_values_dropdowns() -> dict:
    from chatbot_sync import _gsheet_client
    gc = _gsheet_client()
    sid = _get_health_sheet_id()
    if not gc or not sid:
        return {}
    try:
        ws = gc.open_by_key(sid).worksheet("Key Values")
        rows = ws.get_all_values()
        if len(rows) < 2:
            return {}

        def _col(idx):
            return [r[idx].strip() for r in rows[1:] if idx < len(r) and r[idx].strip()]

        cells = _col(0)
        return {
            "Cell":                cells,
            "Prev Cell":           cells,
            "Role":                _col(1),
            "Status":              _col(2),
            "Ministry Department": _col(6),
            "Hype Role":           _col(10),
            "Frontlines Role":     _col(10),
            "VS Role":             _col(10),
            "Worship Role":        _col(10),
            "Gender":              ["Male", "Female"],
        }
    except Exception:
        return {}


_CR_FIELD_FORMAT_HINTS = {
    "Name":              "Title case  e.g. John Tan Wei Ming",
    "Birthday":          "DD Mon YYYY  e.g. 28 Sep 2012",
    "Contact No.":       "Digits only  e.g. +60123456789 or 60123456789",
    "Email Address":     "Must contain @  e.g. name@email.com",
    "Emergency Contact": "Digits only  e.g. +60123456789",
}

_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _cr_parse_birthday(v: str) -> "str | None":
    for fmt in ("%d %b %Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %B %Y"):
        try:
            dt = datetime.strptime(v.strip(), fmt)
            return f"{dt.day:02d} {_MONTH_ABBR[dt.month - 1]} {dt.year}"
        except ValueError:
            pass
    return None


def _cr_validate_field(field: str, value: str) -> "str | None":
    v = value.strip()
    if not v:
        return None
    if field == "Email Address":
        if "@" not in v or "." not in v.split("@")[-1]:
            return "Enter a valid email address (must contain @)"
    if field in ("Contact No.", "Emergency Contact"):
        stripped = re.sub(r"[\s+\-()\.]", "", v)
        if stripped and not stripped.isdigit():
            return "Contact number should only contain digits, +, -, spaces, or parentheses"
    if field == "Birthday":
        if _cr_parse_birthday(v) is None:
            return "Use format DD Mon YYYY  (e.g. 28 Sep 2012)"
    return None


def _cr_normalize(s: str) -> str:
    return " ".join(str(s).replace(" ", " ").strip().lower().split())


def _cr_find_any(cols: list, keywords: list[str]) -> int:
    for i, c in enumerate(cols):
        cl = str(c).lower().strip()
        if any(kw in cl for kw in keywords):
            return i
    return -1


def _cr_find_all(cols: list, keywords: list[str]) -> int:
    for i, c in enumerate(cols):
        cl = str(c).lower().strip()
        if all(kw in cl for kw in keywords):
            return i
    return -1


def _cr_find_role(cols: list) -> int:
    for i, c in enumerate(cols):
        if str(c).lower().strip() in ("role", "member role"):
            return i
    return -1


def _cr_field_col_idx(cols: list, field: str) -> int:
    f = field.lower().strip()
    if f == "name":
        return _cr_find_any(cols, ["name", "member"])
    if f == "cell":
        for i, c in enumerate(cols):
            if str(c).lower().strip() == "cell":
                return i
        return _cr_find_any(cols, ["cell"])
    if f == "role":
        return _cr_find_role(cols)
    if f == "hype role":
        return _cr_find_any(cols, ["hype"])
    if f == "frontlines role":
        return _cr_find_any(cols, ["frontlines"])
    if f == "vs role":
        return _cr_find_any(cols, ["vs"])
    if f == "worship role":
        return _cr_find_any(cols, ["worship"])
    if f == "ministry department":
        return _cr_find_all(cols, ["ministry", "department"])
    if f == "gender":
        return _cr_find_any(cols, ["gender"])
    if f == "birthday":
        return _cr_find_any(cols, ["birthday"])
    if f == "contact no.":
        for i, c in enumerate(cols):
            cl = str(c).lower().strip()
            if cl in ("contact no.", "contact no", "contact number"):
                return i
        return _cr_find_any(cols, ["contact no"])
    if f == "email address":
        return _cr_find_any(cols, ["email"])
    if f == "emergency contact":
        for i, c in enumerate(cols):
            cl = str(c).lower().strip()
            if "emergency" in cl and "relationship" not in cl:
                return i
        return -1
    if f == "emergency relationship":
        return _cr_find_all(cols, ["emergency", "relationship"])
    if f == "school / work":
        return _cr_find_any(cols, ["school", "work"])
    if f == "notes":
        return _cr_find_any(cols, ["notes", "remark"])
    if f == "prev cell":
        return _cr_find_any(cols, ["prev"])
    if f == "age":
        return _cr_find_any(cols, ["age"])
    if f == "status":
        return _cr_find_any(cols, ["status"])
    if f == "attendance":
        return _cr_find_any(cols, ["attendance"])
    return -1



def _cr_fuzzy_match_fields(query: str, available_fields: list[str]) -> list[str]:
    q = query.lower().strip()
    avail = set(available_fields)
    seen: set[str] = set()
    results: list[str] = []

    # Exact alias hit — return immediately
    if q in _CR_FIELD_ALIASES:
        t = _CR_FIELD_ALIASES[q]
        if t in avail:
            return [t]

    # Alias partial match (query is substring of alias key)
    for alias, field in _CR_FIELD_ALIASES.items():
        if q in alias and field in avail and field not in seen:
            results.append(field)
            seen.add(field)

    # Field name substring match
    for field in available_fields:
        if q in field.lower() and field not in seen:
            results.append(field)
            seen.add(field)

    return results


def _cr_advance_to_field(field: str, member: dict, mcols: list, name_val: str, cell_val: str) -> None:
    fi = _cr_field_col_idx(mcols, field)
    current_val = str(member.get(mcols[fi], "") or "").strip() if fi != -1 else ""
    st.session_state.cr_data.update({
        "field": field,
        "current_value": current_val,
        "member_name": name_val,
        "member_cell": cell_val,
    })
    st.session_state.pop("cr_field_group", None)
    st.session_state["cr_field_candidates"] = []
    st.session_state["cr_field_query"] = ""
    st.session_state.cr_step = "new_value"
    st.rerun()


def _cr_infer_field_llm(query: str, available_fields: list[str], chat_history: list[dict]) -> list[str]:
    key = _get_openai_key()
    if not key:
        return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)
        fields_str = ", ".join(available_fields)
        history_lines = []
        for msg in chat_history:
            role = "User" if msg.get("role") == "user" else "Assistant"
            history_lines.append(f"{role}: {str(msg.get('content', ''))[:300]}")
        history_block = ("\n\nRecent conversation:\n" + "\n".join(history_lines)) if history_lines else ""
        content = (
            f"Fields: {fields_str}{history_block}\n\n"
            f'User said: "{query}"\n\n'
            "Which field(s) from the list is the user most likely referring to? "
            "Reply with field name(s) from the list exactly, comma-separated, max 3. "
            "If none match, reply 'none'."
        )
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": content}],
            max_tokens=40,
            temperature=0,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.lower() in ("none", "unclear", ""):
            return []
        return [
            f.strip().strip("\"'") for f in raw.split(",")
            if f.strip().strip("\"'") in available_fields
        ]
    except Exception:
        return []


def _cr_member_label(name: str, cell: str) -> str:
    return f"{name} · {cell}" if cell else name


def _cr_load_members():
    """Returns (cols, rows, name_idx, cell_idx) from nwst_cg_combined_data Redis key."""
    rc = get_redis_client()
    if not rc:
        return [], [], -1, -1
    try:
        raw = rc.get("nwst_cg_combined_data")
        if not raw:
            return [], [], -1, -1
        s = raw.decode() if isinstance(raw, bytes) else raw
        data = json.loads(s)
        cols = data.get("columns", [])
        rows = data.get("rows", [])
        name_idx = _cr_find_any(cols, ["name", "member"])
        cell_idx = _cr_find_any(cols, ["cell", "group"])
        return cols, rows, name_idx, cell_idx
    except Exception:
        return [], [], -1, -1


def _cr_reset() -> None:
    st.session_state.cr_active = False
    st.session_state.cr_step = "requester"
    st.session_state.cr_data = {}
    st.session_state.cr_member_row = None
    st.session_state.cr_matches = []
    st.session_state.pop("cr_search_error", None)
    st.session_state["cr_field_group"] = None
    st.session_state["cr_field_search"] = ""
    st.session_state["cr_field_candidates"] = []
    st.session_state["cr_field_query"] = ""
    st.session_state.pop("cr_val_error", None)


def _render_cr_wizard() -> None:
    step = st.session_state.cr_step
    data = st.session_state.cr_data

    # Step 1 — Requester identity
    if step == "requester":
        _known_name = st.session_state.get("user_name", "")
        _known_cell = st.session_state.get("user_cell", "")
        _known_role = st.session_state.get("user_role", "")
        _greeting = f"Hi {_known_name}!" if _known_name else "Hi!"
        _parts = [p for p in [_known_name, _known_cell, _known_role] if p]
        _prefill = " - ".join(_parts)
        with st.chat_message("assistant"):
            st.markdown(f"{_greeting} Who is making this request? Please confirm your name and role.")
        _go = False
        with st.form("cr_requester"):
            val = st.text_input("Your name and role", value=_prefill, placeholder="e.g. Pastor John, Zone Leader Sarah")
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Next →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go and val.strip():
            st.session_state.cr_data["requester"] = val.strip()
            st.session_state.cr_step = "member_search"
            st.rerun()

    # Step 2 — Member name search
    elif step == "member_search":
        with st.chat_message("assistant"):
            st.markdown(
                f"**Requested by:** {data.get('requester', '')}  \n"
                "Which member's information would you like to update? Type their name to search."
            )
        err = st.session_state.get("cr_search_error", "")
        if err:
            st.warning(err)
        _go = False
        with st.form("cr_member_search"):
            val = st.text_input("Member name", placeholder="e.g. John Tan, Sarah")
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Search →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go and val.strip():
            cols, rows, name_idx, cell_idx = _cr_load_members()
            if name_idx == -1:
                st.session_state.cr_search_error = "⚠️ Could not load member data. Please sync data first."
                st.rerun()
            else:
                query = _cr_normalize(val.strip())
                matches = []
                for row in rows:
                    raw_name = row[name_idx] if name_idx < len(row) else ""
                    if not raw_name:
                        continue
                    raw_cell = (row[cell_idx] if cell_idx != -1 and cell_idx < len(row) else "") or ""
                    if str(raw_cell).strip().lower() == "archive":
                        continue
                    if query in _cr_normalize(raw_name):
                        matches.append({
                            "label": _cr_member_label(str(raw_name), str(raw_cell).strip()),
                            "row": dict(zip(cols, row)),
                        })
                st.session_state.pop("cr_search_error", None)
                if not matches:
                    st.session_state.cr_search_error = f"No member found matching \"{val.strip()}\". Please try again."
                    st.rerun()
                elif len(matches) == 1:
                    st.session_state.cr_member_row = matches[0]["row"]
                    st.session_state.cr_step = "show_info"
                    st.rerun()
                else:
                    st.session_state.cr_matches = matches
                    st.session_state.cr_step = "member_select"
                    st.rerun()

    # Step 2b — Select from multiple matches
    elif step == "member_select":
        matches = st.session_state.cr_matches or []
        with st.chat_message("assistant"):
            st.markdown(f"Found **{len(matches)}** members matching that name. Please select one:")
        _go = False
        with st.form("cr_member_select"):
            labels = [m["label"] for m in matches]
            choice = st.selectbox("Select member", labels)
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Select →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go:
            idx = labels.index(choice)
            st.session_state.cr_member_row = matches[idx]["row"]
            st.session_state.cr_step = "show_info"
            st.rerun()

    # Step 3 — Show current info, pick field
    elif step == "show_info":
        member = st.session_state.cr_member_row or {}
        mcols = list(member.keys())

        name_val = ""
        cell_val = ""
        ni = _cr_find_any(mcols, ["name", "member"])
        ci = _cr_find_any(mcols, ["cell", "group"])
        if ni != -1:
            name_val = str(member.get(mcols[ni], "") or "").strip()
        if ci != -1:
            cell_val = str(member.get(mcols[ci], "") or "").strip()

        pending = data.get("pending_changes", [])
        queued_fields = {ch["field"] for ch in pending}
        available_fields = [f for f in _CR_FIELDS if f not in queued_fields]

        label = _cr_member_label(name_val, cell_val)
        html = _member_info_html(member, mcols, label, pending, _get_daily_palette())

        with st.chat_message("assistant"):
            st.markdown(html, unsafe_allow_html=True)
            if available_fields:
                st.markdown("\nWhich field would you like to change?")
            else:
                st.markdown("\nAll fields have been queued. Ready to review.")

        if available_fields:
            avail_set = set(available_fields)
            _CHIP_GROUPS = [
                ("Identity",   [f for f in ["Name", "Cell"] if f in avail_set]),
                ("Health",     [f for f in ["Status", "Prev Cell"] if f in avail_set]),
                ("Leadership", [f for f in ["Role"] if f in avail_set]),
                ("Ministry",   [f for f in ["Hype Role", "Frontlines Role", "VS Role", "Worship Role", "Ministry Department"] if f in avail_set]),
                ("Personal",   [f for f in ["Gender", "Birthday", "School / Work", "Notes"] if f in avail_set]),
                ("Contact",    [f for f in ["Contact No.", "Email Address", "Emergency Contact", "Emergency Relationship"] if f in avail_set]),
            ]
            active_groups = [(g, fs) for g, fs in _CHIP_GROUPS if fs]
            _ministry_fields = {"Hype Role", "Frontlines Role", "VS Role", "Worship Role", "Ministry Department", "Role"}

            # ── Common shortcuts ──────────────────────────────────────────
            _shortcuts = []
            if "Cell" in avail_set:
                _shortcuts.append(("Change Cell", "Cell"))
            if "Status" in avail_set:
                _shortcuts.append(("Change Status", "Status"))
            if "Cell" in avail_set:
                _shortcuts.append(("Archive", "Cell"))
            _has_ministry = any(f in avail_set for f in _ministry_fields)
            if _has_ministry:
                _shortcuts.append(("Add Role →", None))  # None = open Ministry group

            if _shortcuts:
                sc_cols = st.columns(len(_shortcuts))
                for i, (sc_label, sc_field) in enumerate(_shortcuts):
                    if sc_cols[i].button(sc_label, key=f"cr_sc_{i}", use_container_width=True):
                        if sc_field is not None:
                            _cr_advance_to_field(sc_field, member, mcols, name_val, cell_val)
                        else:
                            st.session_state.cr_field_group = "Ministry"
                            st.session_state["cr_field_candidates"] = []
                            st.rerun()

            st.divider()

            # ── LLM-suggested candidates ──────────────────────────────────
            _candidates = st.session_state.get("cr_field_candidates", [])
            _cq = st.session_state.get("cr_field_query", "")
            if _candidates:
                st.caption(f'Suggested for "{_cq}":')
                cand_cols = st.columns(min(len(_candidates), 3))
                for i, f in enumerate(_candidates):
                    fi = _cr_field_col_idx(mcols, f)
                    cv = str(member.get(mcols[fi], "") or "").strip() if fi != -1 else ""
                    if cand_cols[i % 3].button(
                        f"{f}  ({cv if cv else 'empty'})", key=f"cr_cand_{f}", use_container_width=True
                    ):
                        _cr_advance_to_field(f, member, mcols, name_val, cell_val)
                st.write("")

            # ── Group / field chips ───────────────────────────────────────
            selected_group = st.session_state.get("cr_field_group")
            if selected_group is None:
                g_cols = st.columns(len(active_groups))
                for i, (g_name, _) in enumerate(active_groups):
                    if g_cols[i].button(g_name, key=f"cr_grp_{g_name}", use_container_width=True):
                        st.session_state.cr_field_group = g_name
                        st.session_state["cr_field_candidates"] = []
                        st.rerun()
            else:
                group_fields = next((fs for g, fs in active_groups if g == selected_group), [])
                f_cols = st.columns(2)
                for i, f in enumerate(group_fields):
                    fi = _cr_field_col_idx(mcols, f)
                    cv = str(member.get(mcols[fi], "") or "").strip() if fi != -1 else ""
                    if f_cols[i % 2].button(
                        f"{f}  ({cv if cv else 'empty'})", key=f"cr_field_{f}", use_container_width=True
                    ):
                        _cr_advance_to_field(f, member, mcols, name_val, cell_val)
                if st.button("← Back", key="cr_back_grp"):
                    st.session_state.cr_field_group = None
                    st.session_state["cr_field_candidates"] = []
                    st.rerun()

            if st.button("Cancel", key="cr_cancel_show_info"):
                _cr_reset()
                st.rerun()
        else:
            with st.form("cr_show_info_done"):
                c1, c2 = st.columns([3, 1])
                _review = c1.form_submit_button("Review & Submit →", use_container_width=True)
                _cancel = c2.form_submit_button("Cancel", use_container_width=True)
            if _cancel:
                _cr_reset()
                st.rerun()
            if _review:
                st.session_state.cr_data.update({
                    "member_name": name_val,
                    "member_cell": cell_val,
                })
                st.session_state.cr_step = "reason"
                st.rerun()

    # Step 4 — New value
    elif step == "new_value":
        field = data.get("field", "")
        current = data.get("current_value", "")
        label = _cr_member_label(data.get("member_name", ""), data.get("member_cell", ""))
        with st.chat_message("assistant"):
            st.markdown(
                f"**Member:** {label}  \n"
                f"**Field:** {field}  \n"
                f"**Current value:** {current if current else '—'}  \n\n"
                "What should it be changed to?"
            )
        _go = False
        _val_error = st.session_state.pop("cr_val_error", None)
        if _val_error:
            st.error(_val_error)
        with st.form("cr_new_value"):
            if field in _CR_DROPDOWN_FIELDS:
                dropdowns = _load_key_values_dropdowns()
                options = dropdowns.get(field, [])
                if options:
                    default_idx = options.index(current) if current in options else 0
                    val = st.selectbox("New value", options, index=default_idx)
                else:
                    val = st.text_input("New value", value=current)
                    hint = _CR_FIELD_FORMAT_HINTS.get(field, "")
                    if hint:
                        st.caption(hint)
            else:
                prefill = current.title() if field == "Name" else current
                val = st.text_input("New value", value=prefill)
                hint = _CR_FIELD_FORMAT_HINTS.get(field, "")
                if hint:
                    st.caption(hint)
            c1, c2, c3 = st.columns([2, 2, 1])
            _add_more = c1.form_submit_button("+ Add another field", use_container_width=True)
            _review   = c2.form_submit_button("Review & Submit →",   use_container_width=True)
            _cancel   = c3.form_submit_button("Cancel",              use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _add_more or _review:
            str_val = val if not isinstance(val, str) else val.strip()
            if str_val:
                err = _cr_validate_field(field, str_val)
                if err:
                    st.session_state.cr_val_error = err
                    st.rerun()
                else:
                    if field == "Birthday":
                        str_val = _cr_parse_birthday(str_val) or str_val
                    st.session_state.cr_data.setdefault("pending_changes", []).append({
                        "field": field,
                        "current_value": current,
                        "new_value": str_val,
                    })
                    if _add_more:
                        st.session_state.cr_step = "show_info"
                    else:
                        st.session_state.cr_step = "reason"
                    st.rerun()

    # Step 5 — Reason (optional)
    elif step == "reason":
        with st.chat_message("assistant"):
            st.markdown("Any reason for this change? *(optional — leave blank to skip)*")
        _go = False
        with st.form("cr_reason"):
            val = st.text_input("Reason", placeholder="e.g. Member moved to a different cell group")
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Next →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go:
            st.session_state.cr_data["reason"] = val.strip()
            st.session_state.cr_step = "confirm"
            st.rerun()

    # Step 6 — Confirm and submit
    elif step == "confirm":
        label = _cr_member_label(data.get("member_name", ""), data.get("member_cell", ""))
        pending = data.get("pending_changes", [])
        rows_md = "\n".join(
            f"| **{ch['field']}** | {ch.get('current_value', '') or '—'} | {ch['new_value']} |"
            for ch in pending
        )
        summary = (
            "Please confirm these change requests:\n\n"
            f"**Requested by:** {data.get('requester', '')}  \n"
            f"**Member:** {label}  \n\n"
            "| Field | Current | New |\n|---|---|---|\n"
            + rows_md
            + f"\n\n**Reason:** {data.get('reason', '') or '—'}"
        )
        with st.chat_message("assistant"):
            st.markdown(summary)
        _submit = False
        _cancel = False
        with st.form("cr_confirm"):
            c1, c2 = st.columns([1, 1])
            _submit = c1.form_submit_button("✅ Submit all", use_container_width=True)
            _cancel = c2.form_submit_button("✗ Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.session_state.messages.append({
                "role": "assistant", "content": "Change request cancelled.", "tokens": 0,
            })
            st.rerun()
        if _submit:
            rc = get_chatbot_redis_client()
            if rc:
                for ch in pending:
                    submit_change_request(rc, {
                        "requester":     data.get("requester", ""),
                        "member_name":   data.get("member_name", ""),
                        "member_cell":   data.get("member_cell", ""),
                        "field":         ch["field"],
                        "current_value": ch["current_value"],
                        "new_value":     ch["new_value"],
                        "reason":        data.get("reason", ""),
                    })
            n = len(pending)
            _cr_reset()
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"✅ {n} change request(s) submitted! They will be reviewed and updated shortly.",
                "tokens": 0,
            })
            st.rerun()


# ── page setup ─────────────────────────────────────────────────────────────────

st.set_page_config(page_title="NWST Assistant", page_icon="💬", layout="centered")

st.markdown(
    """
    <style>
    .stApp { background-color: #0d0d0d; color: #f0f0f0; }
    section[data-testid="stSidebar"] { display: none; }
    .stChatMessage { background: transparent; }
    /* Chat message text — prose font, readable size */
    .stChatMessage .stMarkdown p,
    .stChatMessage .stMarkdown li,
    .stChatMessage .stMarkdown strong,
    .stChatMessage .stMarkdown em,
    [data-testid="stChatMessage"] .stMarkdown p,
    [data-testid="stChatMessage"] .stMarkdown li,
    [data-testid="stChatMessageContent"] p,
    [data-testid="stChatMessageContent"] li {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.95rem !important;
        line-height: 1.6 !important;
    }
    .stTextInput > div > div > input {
        background-color: #1a1a1a;
        color: #f0f0f0;
        border: 1px solid #333;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("NWST Assistant")
st.caption("Ask about cell health, check-in, members, or newcomers")

# ── identity + login gate ──────────────────────────────────────────────────────

for _k, _v in [
    ("user_name", ""), ("user_email", ""), ("user_cell", ""),
    ("user_role", ""), ("user_status", ""), ("user_profile_loaded", False),
    ("authenticated", False), ("login_email", ""),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# Wizard state
if "cr_active" not in st.session_state:
    st.session_state.cr_active = False
if "cr_step" not in st.session_state:
    st.session_state.cr_step = "requester"
if "cr_data" not in st.session_state:
    st.session_state.cr_data = {}
if "cr_member_row" not in st.session_state:
    st.session_state.cr_member_row = None
if "cr_matches" not in st.session_state:
    st.session_state.cr_matches = []
if "cr_field_group" not in st.session_state:
    st.session_state.cr_field_group = None
if "cr_field_candidates" not in st.session_state:
    st.session_state.cr_field_candidates = []
if "cr_field_query" not in st.session_state:
    st.session_state.cr_field_query = ""

# Login gate — show sign-in form and halt if not authenticated
if not st.session_state.authenticated:
    with st.form("login_form"):
        _email_input = st.text_input("Email address", placeholder="your@email.com")
        _pw_input = st.text_input("Password", type="password")
        _sign_in = st.form_submit_button("Sign in", use_container_width=True)
    if _sign_in:
        if _check_login(_email_input, _pw_input):
            st.session_state.authenticated = True
            st.session_state.login_email = _email_input.strip().lower()
            st.rerun()
        else:
            st.error("Incorrect email or password.")
    st.stop()

# Auto-populate from login email (runs once per session)
if not st.session_state.user_profile_loaded:
    _member = _lookup_member_by_email(st.session_state.login_email)
    if _member:
        st.session_state.user_name = _pick(_member, "name", "member")
        st.session_state.user_email = st.session_state.login_email
        st.session_state.user_cell = _pick(_member, "cell", "group")
        st.session_state.user_role = _pick(_member, "role")
        st.session_state.user_status = _pick(_member, "status")
    st.session_state.user_profile_loaded = True

# Identity display
if st.session_state.user_name and st.session_state.user_cell:
    st.caption(
        f"👤 **{st.session_state.user_name}** · {st.session_state.user_cell}"
        + (f" · {st.session_state.user_role}" if st.session_state.user_role else "")
    )
else:
    _id_col1, _id_col2, _id_col3 = st.columns(3)
    st.session_state.user_name = _id_col1.text_input(
        "Your name", value=st.session_state.user_name, placeholder="Name",
    )
    st.session_state.user_email = _id_col2.text_input(
        "Email", value=st.session_state.user_email, placeholder="Email",
    )
    st.session_state.user_cell = _id_col3.text_input(
        "Cell group", value=st.session_state.user_cell, placeholder="Cell group",
    )

# ── data load + refresh ────────────────────────────────────────────────────────

# Detect Update Names runs: one Redis GET per render, compare sync timestamp
_sync_changed = False
try:
    _rc_sync = get_redis_client()
    if _rc_sync:
        _raw_sync = _rc_sync.get("nwst_last_sync_time")
        if _raw_sync:
            _sync_str = _raw_sync.decode() if isinstance(_raw_sync, bytes) else _raw_sync
            if _sync_str != st.session_state.get("_last_sync_seen", ""):
                st.session_state["_last_sync_seen"] = _sync_str
                _sync_changed = True
except Exception:
    pass

if _sync_changed or _should_refresh_data():
    if _sync_changed:
        build_data_context.clear()
        _load_data(cache_buster=1)
    else:
        _load_data()

st.divider()

# ── chat ───────────────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state.messages = []

_session_tokens = [
    m["tokens"] for m in st.session_state.messages
    if m["role"] == "assistant" and m.get("tokens", 0) > 0
]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("tokens", 0) > 0:
            st.markdown(
                _token_stat_html(msg["tokens"], _session_tokens),
                unsafe_allow_html=True,
            )

if st.session_state.cr_active:
    _render_cr_wizard()

# ── context ring + new chat + info change ─────────────────────────────────────

_col_new, _col_cr, _col_ring = st.columns([1, 1.4, 2.6])
with _col_new:
    if st.button("+ New Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.pop("pending_prompt", None)
        _cr_reset()
        st.rerun()
with _col_cr:
    if st.session_state.cr_active:
        if st.button("✗ Cancel Request", use_container_width=True):
            _cr_reset()
            st.rerun()
    else:
        if st.button("📋 Info Change", use_container_width=True):
            st.session_state.cr_active = True
            st.session_state.cr_step = "requester"
            st.rerun()
with _col_ring:
    _ctx_used = min(len(st.session_state.messages), MAX_CONTEXT_MESSAGES)
    st.markdown(_context_ring_html(_ctx_used, MAX_CONTEXT_MESSAGES), unsafe_allow_html=True)

if st.session_state.cr_active and st.session_state.cr_step == "show_info":
    _typed = st.chat_input("Describe which field to change…")
elif st.session_state.cr_active:
    _typed = None
else:
    _typed = st.chat_input("Ask a question...")
prompt = _typed

# Field inference: intercept chat input during show_info step
if _typed and st.session_state.get("cr_active") and st.session_state.get("cr_step") == "show_info":
    _m = st.session_state.get("cr_member_row") or {}
    _queued = {ch["field"] for ch in (st.session_state.cr_data or {}).get("pending_changes", [])}
    _avail = [f for f in _CR_FIELDS if f not in _queued]
    _q = _typed.strip()
    _cands = _cr_fuzzy_match_fields(_q, _avail)
    if not _cands:
        _cands = _cr_infer_field_llm(_q, _avail, st.session_state.get("messages", []))
    st.session_state["cr_field_candidates"] = _cands
    st.session_state["cr_field_query"] = _q
    prompt = None

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)


    # Build system message: static behaviour + user profile + live data context
    data_context = st.session_state.get("data_context", "")
    full_system = SYSTEM_PROMPT
    if st.session_state.user_name and st.session_state.user_cell:
        full_system += (
            f"\n\nLOGGED IN USER: {st.session_state.user_name}"
            f" · Cell: {st.session_state.user_cell}"
            f" · Role: {st.session_state.user_role or '—'}"
            f" · Status: {st.session_state.user_status or '—'}"
        )
    if data_context:
        full_system += "\n\nCURRENT DATA:\n" + data_context

    context = st.session_state.messages[-MAX_CONTEXT_MESSAGES:]
    api_messages = [{"role": "system", "content": full_system}] + context

    with st.chat_message("assistant"):
        with st.status("Thinking...", expanded=True) as status:
            result = _call_openai(api_messages)
            thinking, answer = _parse_response(result.content)
            if thinking:
                st.markdown(thinking)
            status.update(label="Reasoning", state="complete", expanded=False)
        st.markdown(answer or result.content)
        if result.tokens > 0:
            _live_session_tokens = _session_tokens + [result.tokens]
            st.markdown(
                _token_stat_html(result.tokens, _live_session_tokens),
                unsafe_allow_html=True,
            )

    # Store only the clean answer in chat history (not the <thinking> block)
    stored = answer if answer else result.content
    st.session_state.messages.append({"role": "assistant", "content": stored, "tokens": result.tokens})

    if result.tokens > 0:
        rc = get_chatbot_redis_client()
        if rc:
            log_qa_to_redis(
                rc,
                st.session_state.user_name or "Anonymous",
                prompt,
                stored,
                result.tokens,
                email=st.session_state.user_email or "",
                cell=st.session_state.user_cell or "",
            )
