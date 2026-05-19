"""
Temporary login test page — DO NOT deploy to production.
Run via: streamlit run NWSTCheckin/CHATBOT/test_login.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_CHECK_IN = _ROOT / "CHECK IN"

for _p in [str(_HERE), str(_ROOT), str(_CHECK_IN)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from dotenv import load_dotenv
    load_dotenv(_CHECK_IN / ".env")
    load_dotenv()
except ImportError:
    pass

import streamlit as st

st.set_page_config(page_title="NWST Assistant", page_icon="💬", layout="centered")

st.markdown(
    """
    <style>
    .stApp { background-color: #0d0d0d; color: #f0f0f0; }
    section[data-testid="stSidebar"] { display: none; }
    .stTextInput > div > div > input {
        background-color: #1a1a1a;
        color: #f0f0f0;
        border: 1px solid #333;
    }
    .stTextInput > div > div > input::placeholder { color: #555; }
    .stTextInput label { color: #aaaaaa !important; font-size: 0.85rem; }
    .login-divider {
        display: flex; align-items: center; gap: 12px;
        margin: 18px 0; color: #444; font-size: 0.85rem;
    }
    .login-divider::before, .login-divider::after {
        content: ""; flex: 1; height: 1px; background: #2a2a2a;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── session state init ────────────────────────────────────────────────────────

for _k, _v in [
    ("authenticated", False), ("login_email", ""),
    ("user_name", ""), ("user_email", ""), ("user_cell", ""),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── helpers ───────────────────────────────────────────────────────────────────

def _allowed_emails() -> list[str]:
    try:
        raw = st.secrets.get("CHATBOT_ALLOWED_EMAILS") or []
    except Exception:
        raw = os.getenv("CHATBOT_ALLOWED_EMAILS", "")
    if isinstance(raw, (list, tuple)):
        return [e.strip().lower() for e in raw if str(e).strip()]
    return [e.strip().lower() for e in str(raw).split(",") if e.strip()]


def _check_login(email: str, password: str) -> bool:
    try:
        correct_pw = (st.secrets.get("CHATBOT_PASSWORD") or "").strip()
    except Exception:
        correct_pw = ""
    if not correct_pw:
        correct_pw = os.getenv("CHATBOT_PASSWORD", "").strip()
    allowed = _allowed_emails()
    if not correct_pw or not allowed:
        return False
    return email.strip().lower() in allowed and password == correct_pw


def _oauth_available() -> bool:
    try:
        _ = st.user
        return True
    except AttributeError:
        return False


# ── check if already authenticated via either path ───────────────────────────

_oauth_ok = _oauth_available() and st.user.is_logged_in
_pw_ok    = st.session_state.authenticated

# ── title ─────────────────────────────────────────────────────────────────────

st.title("NWST Assistant")

# ── logged-in state ───────────────────────────────────────────────────────────

if _oauth_ok or _pw_ok:
    if _oauth_ok:
        _email = getattr(st.user, "email", "") or ""
        _name  = getattr(st.user, "name",  "") or ""
        _method = "Google"
    else:
        _email  = st.session_state.login_email
        _name   = st.session_state.user_name or _email
        _method = "email + password"

    _permitted = _email.strip().lower() in _allowed_emails() if _email else False

    st.caption(f"👤 **{_name or _email}**" + (" · " + _email if _name and _name != _email else ""))

    st.success(f"Signed in via {_method}.")

    if not _permitted:
        st.warning(
            f"**{_email}** is not in the allowed-email list.  \n"
            "Add it to `CHATBOT_ALLOWED_EMAILS` in Secrets to grant full access."
        )

    if st.button("Sign out", use_container_width=False):
        st.session_state.authenticated = False
        st.session_state.login_email   = ""
        st.session_state.user_name     = ""
        if _oauth_ok:
            st.logout()
        else:
            st.rerun()

    st.stop()

# ── login page ────────────────────────────────────────────────────────────────

st.caption("Sign in to continue")
st.write("")

# — Google / Auth0 button —
if _oauth_available():
    if st.button("Sign in with Google", use_container_width=True, type="primary"):
        st.login("auth0")
else:
    st.warning(
        "Google sign-in requires Streamlit ≥ 1.41. "
        "Run `pip install -U streamlit` to enable it."
    )

# — divider —
st.markdown('<div class="login-divider">or</div>', unsafe_allow_html=True)

# — email + password form —
with st.form("login_form"):
    _email_input = st.text_input("Email address", placeholder="your@email.com")
    _pw_input    = st.text_input("Password", type="password")
    _sign_in     = st.form_submit_button("Sign in", use_container_width=True)

if _sign_in:
    if _check_login(_email_input, _pw_input):
        st.session_state.authenticated = True
        st.session_state.login_email   = _email_input.strip().lower()
        st.rerun()
    else:
        st.error("Incorrect email or password.")
