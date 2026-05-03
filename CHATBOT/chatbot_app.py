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

import streamlit as st
from chatbot_redis import get_redis_client, log_qa_to_redis
from chatbot_data import build_data_context

MYT = timezone(timedelta(hours=8))

MAX_RESPONSE_TOKENS = 500
MAX_CONTEXT_MESSAGES = 6   # last 3 human + 3 assistant turns
MODEL = "gpt-4o-mini"
DATA_TTL_SECONDS = 300     # auto-refresh data every 5 minutes

SYSTEM_PROMPT = """You are an assistant for NWST (Narrow Street), a church community in Malaysia.
You help members and leaders understand two internal tools: cell health tracking and weekly check-in.

CELL HEALTH — member status categories:
- New: recently joined, still being integrated into the cell
- Regular: attending consistently
- Irregular: inconsistent attendance, needs follow-up
- Follow Up: requires specific pastoral attention
- Red: at serious risk of leaving or already disengaged
- Graduated: completed the cell journey (moved on positively)
Health % is roughly Regular count divided by total active members. Week-over-week (WoW) deltas show +/- changes from the prior snapshot.

CHECK-IN — weekly cell group attendance tracking:
- Members select their name from a dropdown and click check in.
- Tabs: Congregation (main service), Leaders (leaders discipleship), Ministry (Worship, Hype, VS, Frontlines).
- Newcomers submit via a separate form (captured in Form Responses tab).
- Birthdays are displayed on the check-in screen, pulled from CG Combined data.
- "Update Names" flushes pending check-ins from Redis to Google Sheets and refreshes all caches.

You have access to live NWST data injected below. Use it to answer specific questions accurately.
When summarising health, lead with the weakest cells (lowest Regular %). Be concise."""


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

st.session_state.user_name = st.text_input(
    "Your name",
    value=st.session_state.user_name,
    placeholder="Enter your name before chatting",
)

# ── data load + refresh ────────────────────────────────────────────────────────

if _should_refresh_data():
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

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask a question..."):
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
        with st.spinner(""):
            result = _call_openai(api_messages)
        st.markdown(result.content)

    st.session_state.messages.append({"role": "assistant", "content": result.content})

    if result.tokens > 0:
        rc = get_redis_client()
        if rc:
            log_qa_to_redis(
                rc,
                st.session_state.user_name or "Anonymous",
                prompt,
                result.content,
                result.tokens,
            )
