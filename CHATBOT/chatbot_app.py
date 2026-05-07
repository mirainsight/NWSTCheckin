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
import random
import re

import streamlit as st
import streamlit.components.v1 as _st_components
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

MAX_RESPONSE_TOKENS = 2000
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

    lm_groups = [("LEADERSHIP", ["Role", "Role Last Updated"]), ("MINISTRY", ministry_fields)]
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



# ── change-request wizard ──────────────────────────────────────────────────────

_CR_FIELDS = [
    # IDENTITY
    "Name", "Cell",
    # HEALTH
    "Status", "Prev Cell",
    # LEADERSHIP
    "Role", "Role Last Updated",
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

_CR_FIELD_DESCRIPTIONS: dict[str, str] = {
    "Name":                   "member or individual or youth's full name (could be someone filing on behalf of another, typically a mentor like leader or pastor or zone leader)",
    "Cell":                   "cell group/cell that the individual currently belongs to",
    "Status":                 "attendance health — tracks how regularly a member attends (Regular 75% or more attendance for past 12 services, Irregular less than 75% attendance but attends for past 12 services, New Member, Follow Up 0% for past 12 services, Red as no longer comes to church, Graduated as moved on to another ministry)",
    "Prev Cell":              "previous cell group before a transfer",
    "Role":                   "leadership or ministry serving role, includes: 1. CG Leader, 2. Assistant CG Leader, 3. CG Core, 4. Potential CG Core, 5. Ministry Leader, 6. Assistant Ministry Leader, 7. Ministry Core, 8. Potential Ministry Core, 9. Zone Leader",
    "Role Last Updated":      "date the role was last changed",
    "Hype Role":              "role within Hype ministry team can include: 1. Ministry Leader, 2. Assistant Ministry Leader, 3. Ministry Core, 4. Potential Ministry Core, 5. Member, 6. Advisor",
    "Frontlines Role":        "role within Frontlines ministry team can include: 1. Ministry Leader, 2. Assistant Ministry Leader, 3. Ministry Core, 4. Potential Ministry Core, 5. Member, 6. Advisor",
    "VS Role":                "role within VS (visual storyteller; media team) ministry team can include: 1. Ministry Leader, 2. Assistant Ministry Leader, 3. Ministry Core, 4. Potential Ministry Core, 5. Member, 6. Advisor",
    "Worship Role":           "role within Worship ministry team can include: 1. Ministry Leader, 2. Assistant Ministry Leader, 3. Ministry Core, 4. Potential Ministry Core, 5. Member, 6. Advisor",
    "Ministry Department":    "ministry or department the member serves in: mainly related to worship such as Band, LCD, Dance, Sound, Vocals, Lights",
    "Gender":                 "Male or Female",
    "Birthday":               "date of birth",
    "School / Work":          "school name or current workplace",
    "Notes":                  "general notes or remarks about the member",
    "Contact No.":            "phone / mobile number",
    "Email Address":          "email address",
    "Emergency Contact":      "emergency contact phone number",
    "Emergency Relationship": "relationship of emergency contact to the member",
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
    "Role Last Updated": "DD Mon YYYY  e.g. 01 Jan 2024",
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
    if field in ("Birthday", "Role Last Updated"):
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
    if f == "role last updated":
        i = _cr_find_all(cols, ["role", "updated"])
        return i if i != -1 else _cr_find_any(cols, ["role last", "last updated"])
    return -1



def _cr_fuzzy_match_fields(query: str, available_fields: list[str]) -> list[str]:
    q = query.lower().strip()
    avail = set(available_fields)
    seen: set[str] = set()
    results: list[str] = []
    if q in _CR_FIELD_ALIASES:
        t = _CR_FIELD_ALIASES[q]
        if t in avail:
            return [t]
    for alias, field in _CR_FIELD_ALIASES.items():
        if q in alias and field in avail and field not in seen:
            results.append(field)
            seen.add(field)
    for field in available_fields:
        if q in field.lower() and field not in seen:
            results.append(field)
            seen.add(field)
    return results


def _cr_infer_field_llm(query: str, available_fields: list[str]) -> list[str]:
    key = _get_openai_key()
    if not key:
        return []
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)
        fields_block = "\n".join(
            f"- {f}: {_CR_FIELD_DESCRIPTIONS[f]}" if f in _CR_FIELD_DESCRIPTIONS else f"- {f}"
            for f in available_fields
        )
        content = (
            f"Available fields:\n{fields_block}\n\n"
            f'User said: "{query}"\n\n'
            "Which field(s) from the list is the user most likely referring to? "
            "Include ALL fields that are a reasonable match — if the query is broad (e.g. 'serve', 'ministry', 'role'), list every relevant field, not just the top 3. "
            "Reply in this exact format: REASON: <one short sentence, written in second person as the chatbot speaking directly to the user, e.g. 'Sounds like you want to update your role!' or 'You're probably looking for their contact info.'> | FIELDS: <field1>, <field2>, ... (exact names from the list, as many as relevant). "
            "If nothing matches, reply: REASON: Hmm, not sure what you mean — try again? | FIELDS: none"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": content}],
            max_tokens=150,
            temperature=0,
        )
        raw = resp.choices[0].message.content.strip()
        reason = ""
        fields_part = raw
        if "|" in raw:
            left, right = raw.split("|", 1)
            if left.upper().startswith("REASON:"):
                reason = left[7:].strip()
            if right.upper().strip().startswith("FIELDS:"):
                fields_part = right.strip()[7:].strip()
        st.session_state["cr_field_reason"] = reason
        if fields_part.lower() in ("none", "unclear", ""):
            return []
        return [
            f.strip().strip("\"'") for f in fields_part.split(",")
            if f.strip().strip("\"'") in available_fields
        ]
    except Exception:
        return []


_CARD_QUIPS = [
    "",
    "",
    "did you even read the identity card? 👀",
    "make sure to read through ah~",
    "scroll up a bit, it's all there 😌",
    "just a lil peek before you edit, yeah?",
    "__llm__",
]


def _get_card_llm_quip(name: str) -> str:
    key = _get_openai_key()
    if not key:
        return ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": (
                f"You're a sassy, youthful church app assistant. "
                f"A user just pulled up {name}'s identity card. "
                "Give ONE short playful nudge (max 10 words) telling them to actually read it "
                "before making changes. Be fun and warm, not rude. You may use one emoji."
            )}],
            max_tokens=30,
            temperature=1.1,
        )
        return resp.choices[0].message.content.strip().strip('"')
    except Exception:
        return ""


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

    # Step 1 — Requester identity (auto-populated from login, no form shown)
    if step == "requester":
        _known_name = st.session_state.get("user_name", "")
        _known_cell = st.session_state.get("user_cell", "")
        _known_role = st.session_state.get("user_role", "")
        _parts = [p for p in [_known_name, _known_cell, _known_role] if p]
        st.session_state.cr_data["requester"] = " - ".join(_parts) if _parts else "Unknown"
        st.session_state.cr_step = "member_search"
        st.rerun()

    # Step 2 — Member name search
    elif step == "member_search":
        with st.chat_message("assistant", avatar="🤖"):
            st.markdown(
                f"Got it — filing this one from **{data.get('requester', 'you')}** ✨  \n"
                "Who are we looking up today? Drop their name and I'll track them down! 👀"
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
        with st.chat_message("assistant", avatar="🤖"):
            st.markdown(f"Ooh, I found **{len(matches)}** people with that name — which one are we talking about?")
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
        _st_components.html(
            "<script>var el=window.parent.document.querySelector('section[data-testid=\"stMain\"]');"
            "if(!el)el=window.parent.document.querySelector('.main');if(el)el.scrollTop=0;</script>",
            height=0,
        )
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

        # Pick a quip once per member and cache it so rerenders stay stable
        _quip_cache_key = f"_card_quip__{name_val}"
        if st.session_state.get("_card_quip_member") != name_val:
            st.session_state["_card_quip_member"] = name_val
            st.session_state[_quip_cache_key] = random.choice(_CARD_QUIPS)
            st.session_state.pop("_card_llm_quip", None)
        _chosen_quip = st.session_state.get(_quip_cache_key, "")

        if _chosen_quip == "__llm__" and "_card_llm_quip" not in st.session_state:
            st.session_state["_card_llm_quip"] = _get_card_llm_quip(name_val)

        with st.chat_message("assistant", avatar="🤖"):
            st.markdown(html, unsafe_allow_html=True)
            if available_fields:
                st.markdown(f"\nHere's everything I've got on **{name_val}**! So, what are we changing today?")
            else:
                st.markdown("\nAll fields have been queued up — we're good to go! Ready to review.")
            if _chosen_quip == "__llm__":
                _llm_quip = st.session_state.get("_card_llm_quip", "")
                if _llm_quip:
                    st.caption(f"*{_llm_quip}*")
            elif _chosen_quip:
                st.caption(f"*{_chosen_quip}*")

        if available_fields:
            avail_set = set(available_fields)
            _CHIP_GROUPS = [
                ("Identity",   [f for f in ["Name", "Cell"] if f in avail_set]),
                ("Health",     [f for f in ["Status", "Prev Cell"] if f in avail_set]),
                ("Leadership", [f for f in ["Role", "Role Last Updated"] if f in avail_set]),
                ("Ministry",   [f for f in ["Hype Role", "Frontlines Role", "VS Role", "Worship Role", "Ministry Department"] if f in avail_set]),
                ("Personal",   [f for f in ["Gender", "Birthday", "School / Work", "Notes"] if f in avail_set]),
                ("Contact",    [f for f in ["Contact No.", "Email Address", "Emergency Contact", "Emergency Relationship"] if f in avail_set]),
            ]
            active_groups = [(g, fs) for g, fs in _CHIP_GROUPS if fs]
            _ministry_fields = {"Hype Role", "Frontlines Role", "VS Role", "Worship Role", "Ministry Department", "Role"}
            selected_group = st.session_state.get("cr_field_group")

            if selected_group == "__browse__":
                # ── Level 1: group chips ──────────────────────────────────
                st.markdown("**Browse by group:**")
                g_cols = st.columns(min(len(active_groups), 3))
                for i, (g_name, _) in enumerate(active_groups):
                    if g_cols[i % 3].button(g_name, key=f"cr_grp_{g_name}", use_container_width=True):
                        st.session_state.cr_field_group = g_name
                        st.session_state["cr_field_candidates"] = []
                        st.rerun()
                col_back, col_cancel = st.columns([1, 1])
                if col_back.button("← Back", key="cr_back_to_l0"):
                    st.session_state.cr_field_group = None
                    st.rerun()
                if col_cancel.button("Cancel", key="cr_cancel_l1"):
                    _cr_reset()
                    st.rerun()

            elif selected_group is not None:
                # ── Level 2: field chips for selected group ───────────────
                group_fields = next((fs for g, fs in active_groups if g == selected_group), [])
                f_cols = st.columns(2)
                for i, f in enumerate(group_fields):
                    fi = _cr_field_col_idx(mcols, f)
                    cv = str(member.get(mcols[fi], "") or "").strip() if fi != -1 else ""
                    if f_cols[i % 2].button(
                        f"{f}  ({cv if cv else 'empty'})", key=f"cr_field_{f}", use_container_width=True
                    ):
                        _cr_advance_to_field(f, member, mcols, name_val, cell_val)
                col_back, col_cancel = st.columns([1, 1])
                if col_back.button("← Back", key="cr_back_to_groups"):
                    st.session_state.cr_field_group = "__browse__"
                    st.rerun()
                if col_cancel.button("Cancel", key="cr_cancel_l2"):
                    _cr_reset()
                    st.rerun()

            else:
                # ── Level 0: shortcuts + Browse all (hidden once user has searched) ─────
                if not st.session_state.get("cr_field_query", ""):
                    _shortcuts = []
                    if "Cell" in avail_set:
                        _shortcuts.append(("Change Cell", "Cell"))
                    if "Status" in avail_set:
                        _shortcuts.append(("Change Status", "Status"))
                    if "Notes" in avail_set:
                        _shortcuts.append(("Add Notes", "Notes"))
                    _has_ministry = any(f in avail_set for f in _ministry_fields)
                    if _has_ministry:
                        _shortcuts.append(("Change Role", None))

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

                    col_browse, col_cancel = st.columns([3, 1])
                    if col_browse.button("Browse all →", key="cr_browse_all", use_container_width=True):
                        st.session_state.cr_field_group = "__browse__"
                        st.session_state["cr_field_candidates"] = []
                        st.rerun()
                    if col_cancel.button("Cancel", key="cr_cancel_l0"):
                        _cr_reset()
                        st.rerun()
                else:
                    if st.button("Cancel", key="cr_cancel_searched"):
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
        with st.chat_message("assistant", avatar="🤖"):
            _member_name = data.get("member_name", "") or label
            st.markdown(
                f"A **{field}** for {_member_name}? Let's see — "
                f"currently sitting at **{current if current else 'empty'}**. "
                f"What should I change it to?"
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
            c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
            _add_more = c1.form_submit_button("+ Add another field", use_container_width=True)
            _review   = c2.form_submit_button("Review & Submit →",   use_container_width=True)
            _back     = c3.form_submit_button("← Back",              use_container_width=True)
            _cancel   = c4.form_submit_button("Cancel",              use_container_width=True)
        if _back:
            st.session_state.cr_step = "show_info"
            st.rerun()
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
                    if field in ("Birthday", "Role Last Updated"):
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
        with st.chat_message("assistant", avatar="🤖"):
            st.markdown("Any reason behind this change? No pressure — spill the tea or just skip it! *(optional)*")
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
            "Okay! Here's the rundown — double-check and let's make it happen:\n\n"
            f"**Requested by:** {data.get('requester', '')}  \n"
            f"**Member:** {label}  \n\n"
            "| Field | Current | New |\n|---|---|---|\n"
            + rows_md
            + f"\n\n**Reason:** {data.get('reason', '') or '—'}"
        )
        with st.chat_message("assistant", avatar="🤖"):
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

# ── greeting + info change wizard ─────────────────────────────────────────────

if st.session_state.cr_active:
    _render_cr_wizard()
else:
    _hour = datetime.now(MYT).hour
    if 5 <= _hour < 12:
        _greeting = "Good morning"
    elif 12 <= _hour < 17:
        _greeting = "Good afternoon"
    elif 17 <= _hour < 21:
        _greeting = "Good evening"
    else:
        _greeting = "Good night"

    _display_name = st.session_state.user_name or st.session_state.login_email or "there"

    st.markdown(
        f"## {_greeting}, {_display_name}  \nReady to change your or someone's info?"
    )
    if st.button("Yes", use_container_width=False):
        st.session_state.cr_active = True
        st.session_state.cr_step = "requester"
        st.rerun()

# ── field inference suggestions + chat input (show_info step only) ────────────

if st.session_state.cr_active and st.session_state.cr_step == "show_info":
    _candidates = st.session_state.get("cr_field_candidates", [])
    _cq = st.session_state.get("cr_field_query", "")
    _cr_reason = st.session_state.get("cr_field_reason", "")
    if _candidates:
        _pal = _get_daily_palette()
        _pc = _pal.get("primary", "#5bc0eb")
        try:
            _pr, _pg, _pb = int(_pc[1:3], 16), int(_pc[3:5], 16), int(_pc[5:7], 16)
        except (ValueError, IndexError):
            _pr, _pg, _pb = 91, 192, 235
        _reason_html = f'<span style="color:#aaaaaa;font-size:0.82rem;"> — {_cr_reason}</span>' if _cr_reason else ""
        st.markdown(
            f'<style>'
            f'.cr-cand-marker ~ div[data-testid="stHorizontalBlock"] .stButton > button {{'
            f'  border: 1px solid {_pc} !important;'
            f'  color: {_pc} !important;'
            f'  background: rgba({_pr},{_pg},{_pb},0.08) !important;'
            f'}}'
            f'.cr-cand-marker ~ div[data-testid="stHorizontalBlock"] .stButton > button:hover {{'
            f'  background: rgba({_pr},{_pg},{_pb},0.2) !important;'
            f'}}'
            f'</style>'
            f'<div style="border-left:3px solid {_pc};padding:6px 14px;margin:10px 0 6px;'
            f'background:rgba({_pr},{_pg},{_pb},0.08);border-radius:0 6px 6px 0;">'
            f'<span style="color:{_pc};font-size:0.85rem;font-weight:700;">Suggested for &ldquo;{_cq}&rdquo;</span>'
            f'{_reason_html}</div>'
            f'<span class="cr-cand-marker"></span>',
            unsafe_allow_html=True,
        )
        _m_row = st.session_state.get("cr_member_row") or {}
        _mcols_s = list(_m_row.keys())
        _ncols = min(len(_candidates), 3)
        _cand_cols = st.columns(_ncols)
        for _i, _f in enumerate(_candidates):
            _fi = _cr_field_col_idx(_mcols_s, _f)
            _cv = str(_m_row.get(_mcols_s[_fi], "") or "").strip() if _fi != -1 else ""
            if _cand_cols[_i % _ncols].button(
                f"{_f}  ({_cv if _cv else 'empty'})", key=f"cr_cand_{_f}", use_container_width=True
            ):
                _ni = _cr_find_any(_mcols_s, ["name", "member"])
                _ci = _cr_find_any(_mcols_s, ["cell", "group"])
                _nv = str(_m_row.get(_mcols_s[_ni], "") or "").strip() if _ni != -1 else ""
                _cv2 = str(_m_row.get(_mcols_s[_ci], "") or "").strip() if _ci != -1 else ""
                _cr_advance_to_field(_f, _m_row, _mcols_s, _nv, _cv2)

    elif _cq:
        _no_match_reason = _cr_reason if _cr_reason and _cr_reason.lower() not in ("no clear match",) else ""
        _no_match_msg = (
            f"Hmm, **\"{_cq}\"** doesn't match any field 🤔"
            + (f" — {_no_match_reason}" if _no_match_reason else " — try something like \"Name\", \"Cell\", or \"Status\"!")
        )
        st.markdown(f"*{_no_match_msg}*")

    _typed = st.chat_input('Not what you need? Try "Change Name" or "School"…')
    if _typed:
        _m = st.session_state.get("cr_member_row") or {}
        _queued = {ch["field"] for ch in (st.session_state.cr_data or {}).get("pending_changes", [])}
        _avail = [f for f in _CR_FIELDS if f not in _queued]
        _q = _typed.strip()
        _cands = _cr_fuzzy_match_fields(_q, _avail)
        if len(_cands) != 1:
            _llm_cands = _cr_infer_field_llm(_q, _avail)
            if _llm_cands:
                _cands = _llm_cands
        if len(_cands) == 1:
            _mcols = list(_m.keys())
            _ni = _cr_find_any(_mcols, ["name", "member"])
            _ci = _cr_find_any(_mcols, ["cell", "group"])
            _name_val = str(_m.get(_mcols[_ni], "") or "").strip() if _ni != -1 else ""
            _cell_val = str(_m.get(_mcols[_ci], "") or "").strip() if _ci != -1 else ""
            _cr_advance_to_field(_cands[0], _m, _mcols, _name_val, _cell_val)
        st.session_state["cr_field_candidates"] = _cands
        st.session_state["cr_field_query"] = _q
        _resolved = _cands[0] if len(_cands) == 1 else (", ".join(_cands) if _cands else "no match")
        _rc_log = get_chatbot_redis_client()
        if _rc_log:
            log_qa_to_redis(
                _rc_log,
                st.session_state.user_name or "Anonymous",
                _q,
                f"[field search] → {_resolved}",
                0,
                email=st.session_state.user_email or "",
                cell=st.session_state.user_cell or "",
            )
        st.rerun()
