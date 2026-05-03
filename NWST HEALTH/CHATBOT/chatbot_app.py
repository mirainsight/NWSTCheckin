from __future__ import annotations

import os
import sys
from datetime import timedelta, timezone
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

MYT = timezone(timedelta(hours=8))

MAX_RESPONSE_TOKENS = 500
MAX_CONTEXT_MESSAGES = 6   # last 3 human + 3 assistant turns
MODEL = "gpt-4o-mini"

SYSTEM_PROMPT = """You are an assistant for NWST (Narrow Street), a church community in Malaysia.
You help members and leaders understand two internal tools: cell health tracking and weekly check-in.

CELL HEALTH — member status categories:
- New: recently joined, still being integrated into the cell
- Regular: attending consistently
- Irregular: inconsistent attendance, needs follow-up
- Follow Up: requires specific pastoral attention
- Red: at serious risk of leaving or already disengaged
- Graduated: completed the cell journey (moved on positively)
Health % is roughly Regular count divided by total active members. Week-over-week (WoW) deltas show changes from the prior snapshot. The NWST Health dashboard shows KPI cards and per-cell breakdowns by zone.

CHECK-IN — weekly cell group attendance tracking:
- Members select their name from a dropdown and click check in.
- Tabs: Congregation (main service), Leaders (leaders discipleship), Ministry (Worship, Hype, VS, Frontlines).
- Newcomers submit via a separate form (captured in Form Responses tab).
- Birthdays are displayed on the check-in screen, pulled from CG Combined data.
- "Update Names" flushes pending check-ins from Redis to Google Sheets and refreshes all caches.

Answer concisely. If you don't know something specific, say so rather than guessing.
Do not discuss internal credentials, sheet IDs, or Redis keys."""


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
st.caption("Ask about cell health or check-in")

if "user_name" not in st.session_state:
    st.session_state.user_name = ""

st.session_state.user_name = st.text_input(
    "Your name",
    value=st.session_state.user_name,
    placeholder="Enter your name before chatting",
)

st.divider()

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask a question..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    context = st.session_state.messages[-MAX_CONTEXT_MESSAGES:]
    api_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + context

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
