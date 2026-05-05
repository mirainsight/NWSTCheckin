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

MYT = timezone(timedelta(hours=8))

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


def _get_openai_key() -> str:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        try:
            key = (st.secrets.get("OPENAI_API_KEY") or "").strip()
        except Exception:
            pass
    return key


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
    "Name", "Cell Group", "Role", "Ministry Department",
    "Gender", "Birthday", "Phone / Contact", "School / Work", "Notes",
]


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
    if field == "Name":
        return _cr_find_any(cols, ["name", "member"])
    if field == "Cell Group":
        return _cr_find_any(cols, ["cell", "group"])
    if field == "Role":
        return _cr_find_role(cols)
    if field == "Ministry Department":
        return _cr_find_all(cols, ["ministry", "department"])
    if field == "Gender":
        return _cr_find_any(cols, ["gender"])
    if field == "Birthday":
        return _cr_find_any(cols, ["birthday"])
    if field == "Phone / Contact":
        return _cr_find_any(cols, ["phone", "contact", "mobile"])
    if field == "School / Work":
        return _cr_find_any(cols, ["school", "work"])
    if field == "Notes":
        return _cr_find_any(cols, ["notes", "remark"])
    return -1


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


def _render_cr_wizard() -> None:
    step = st.session_state.cr_step
    data = st.session_state.cr_data

    # Step 1 — Requester identity
    if step == "requester":
        with st.chat_message("assistant"):
            st.markdown("Hi! Who is making this request? Please enter your name and role.")
        _go = False
        with st.form("cr_requester"):
            val = st.text_input("Your name and role", placeholder="e.g. Pastor John, Zone Leader Sarah")
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

        label = _cr_member_label(name_val, cell_val)
        info_lines = [f"**Member found: {label}**", ""]
        for field in _CR_FIELDS:
            fi = _cr_field_col_idx(mcols, field)
            if fi != -1:
                v = str(member.get(mcols[fi], "") or "").strip()
                info_lines.append(f"**{field}:** {v if v else '—'}")

        with st.chat_message("assistant"):
            st.markdown("\n".join(info_lines))
            st.markdown("\nWhich field would you like to request a change for?")

        field_options = []
        for field in _CR_FIELDS:
            fi = _cr_field_col_idx(mcols, field)
            v = ""
            if fi != -1:
                v = str(member.get(mcols[fi], "") or "").strip()
            field_options.append(f"{field}  (currently: {v if v else 'empty'})")

        _go = False
        with st.form("cr_show_info"):
            choice = st.selectbox("Field to change", field_options)
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Next →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go:
            field_name = choice.split("  (currently:")[0]
            fi = _cr_field_col_idx(mcols, field_name)
            current_val = ""
            if fi != -1:
                current_val = str(member.get(mcols[fi], "") or "").strip()
            st.session_state.cr_data.update({
                "field": field_name,
                "current_value": current_val,
                "member_name": name_val,
                "member_cell": cell_val,
            })
            st.session_state.cr_step = "new_value"
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
        with st.form("cr_new_value"):
            val = st.text_input("New value", value=current)
            c1, c2 = st.columns([3, 1])
            _go = c1.form_submit_button("Next →", use_container_width=True)
            _cancel = c2.form_submit_button("Cancel", use_container_width=True)
        if _cancel:
            _cr_reset()
            st.rerun()
        if _go and val.strip():
            st.session_state.cr_data["new_value"] = val.strip()
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
        summary = (
            "Please confirm this change request:\n\n"
            "| | |\n|---|---|\n"
            f"| **Requested by** | {data.get('requester', '')} |\n"
            f"| **Member** | {label} |\n"
            f"| **Field** | {data.get('field', '')} |\n"
            f"| **Current value** | {data.get('current_value', '') or '—'} |\n"
            f"| **New value** | {data.get('new_value', '')} |\n"
            f"| **Reason** | {data.get('reason', '') or '—'} |"
        )
        with st.chat_message("assistant"):
            st.markdown(summary)
        _submit = False
        _cancel = False
        with st.form("cr_confirm"):
            c1, c2 = st.columns([1, 1])
            _submit = c1.form_submit_button("✅ Submit", use_container_width=True)
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
                submit_change_request(rc, {
                    "requester": data.get("requester", ""),
                    "member_name": data.get("member_name", ""),
                    "member_cell": data.get("member_cell", ""),
                    "field": data.get("field", ""),
                    "current_value": data.get("current_value", ""),
                    "new_value": data.get("new_value", ""),
                    "reason": data.get("reason", ""),
                })
            _cr_reset()
            st.session_state.messages.append({
                "role": "assistant",
                "content": "✅ Change request submitted! It will be reviewed and updated shortly.",
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

# ── name input ─────────────────────────────────────────────────────────────────

if "user_name" not in st.session_state:
    st.session_state.user_name = ""
if "user_email" not in st.session_state:
    st.session_state.user_email = ""
if "user_cell" not in st.session_state:
    st.session_state.user_cell = ""

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

_id_col1, _id_col2, _id_col3 = st.columns(3)
st.session_state.user_name = _id_col1.text_input(
    "Your name",
    value=st.session_state.user_name,
    placeholder="Name",
)
st.session_state.user_email = _id_col2.text_input(
    "Email",
    value=st.session_state.user_email,
    placeholder="Email",
)
st.session_state.user_cell = _id_col3.text_input(
    "Cell group",
    value=st.session_state.user_cell,
    placeholder="Cell group",
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

col_info, col_btn = st.columns([5, 1])
with col_info:
    fetched_at = st.session_state.get("data_fetched_at")
    if fetched_at:
        has_data = bool(st.session_state.get("data_context", "").strip())
        label = f"Data as of {fetched_at.strftime('%H:%M')} MYT"
        label += "" if has_data else " · no data (run Update Names first)"
        st.caption(label)
with col_btn:
    if st.button("↺", help="Refresh live data", use_container_width=True):
        build_data_context.clear()
        _load_data(cache_buster=1)
        st.rerun()

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

_typed = None if st.session_state.cr_active else st.chat_input("Ask a question...")
prompt = _typed

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)


    # Build system message: static behaviour + live data context
    data_context = st.session_state.get("data_context", "")
    full_system = SYSTEM_PROMPT
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
