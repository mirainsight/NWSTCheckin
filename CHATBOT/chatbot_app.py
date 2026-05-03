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

import re

import streamlit as st
from chatbot_redis import get_redis_client, log_qa_to_redis
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


# ── page setup ─────────────────────────────────────────────────────────────────

st.set_page_config(page_title="NWST Assistant", page_icon="💬", layout="centered")

st.markdown(
    """
    <style>
    .stApp { background-color: #0d0d0d; color: #f0f0f0; }
    section[data-testid="stSidebar"] { display: none; }
    .stChatMessage { background: transparent; }
    /* Chat message text — prose font, readable size */
    [data-testid="stChatMessage"] p,
    [data-testid="stChatMessage"] li,
    [data-testid="stChatMessage"] span:not([data-testid]),
    [data-testid="stChatMessage"] .stMarkdown p {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 0.95rem !important;
        line-height: 1.6 !important;
    }
    .stTextInput > div > div > input {
        background-color: #1a1a1a;
        color: #f0f0f0;
        border: 1px solid #333;
    }
    /* Suggestion bubble buttons */
    div[data-testid="stHorizontalBlock"] button[kind="secondary"] {
        background-color: #1a1a1a !important;
        color: #d0d0d0 !important;
        border: 1px solid #333 !important;
        border-radius: 20px !important;
        font-size: 0.82rem !important;
        padding: 0.3rem 0.8rem !important;
        white-space: normal !important;
        text-align: left !important;
        line-height: 1.3 !important;
    }
    div[data-testid="stHorizontalBlock"] button[kind="secondary"]:hover {
        border-color: #666 !important;
        color: #fff !important;
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

st.session_state.user_name = st.text_input(
    "Your name",
    value=st.session_state.user_name,
    placeholder="Enter your name before chatting",
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

_SUGGESTIONS = [
    "How is overall cell health?",
    "Which cells need the most attention?",
    "Who checked in today?",
    "Any newcomers this week?",
    "Who are the CG Leaders?",
    "Which members haven't attended recently?",
    "Show ministry distribution",
    "Summarise this week's check-in",
]

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("tokens", 0) > 0:
            st.markdown(
                f"<p style='font-size:0.72rem;color:#444;font-style:italic;margin:2px 0 0 0;'>{msg['tokens']} tokens</p>",
                unsafe_allow_html=True,
            )

# Suggestion bubbles — only shown when chat is empty
if not st.session_state.messages:
    st.markdown("<p style='color:#555;font-size:0.85rem;margin-bottom:0.4rem;'>Try asking:</p>", unsafe_allow_html=True)
    cols = st.columns(2)
    for i, suggestion in enumerate(_SUGGESTIONS):
        if cols[i % 2].button(suggestion, key=f"suggestion_{i}", use_container_width=True):
            st.session_state["pending_prompt"] = suggestion
            st.rerun()

# ── context ring + new chat ────────────────────────────────────────────────────

_col_new, _col_ring = st.columns([1, 3])
with _col_new:
    if st.button("+ New Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.pop("pending_prompt", None)
        st.rerun()
with _col_ring:
    _ctx_used = min(len(st.session_state.messages), MAX_CONTEXT_MESSAGES)
    st.markdown(_context_ring_html(_ctx_used, MAX_CONTEXT_MESSAGES), unsafe_allow_html=True)

# Consume any suggestion-button prompt queued from the previous rerun
_pending = st.session_state.pop("pending_prompt", None)
_typed = st.chat_input("Ask a question...")
prompt = _pending or _typed

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
            st.markdown(
                f"<p style='font-size:0.72rem;color:#444;font-style:italic;margin:2px 0 0 0;'>{result.tokens} tokens</p>",
                unsafe_allow_html=True,
            )

    # Store only the clean answer in chat history (not the <thinking> block)
    stored = answer if answer else result.content
    st.session_state.messages.append({"role": "assistant", "content": stored, "tokens": result.tokens})

    if result.tokens > 0:
        rc = get_redis_client()
        if rc:
            log_qa_to_redis(
                rc,
                st.session_state.user_name or "Anonymous",
                prompt,
                stored,
                result.tokens,
            )
