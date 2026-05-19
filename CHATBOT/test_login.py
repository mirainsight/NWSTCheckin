"""
Temporary login test page — DO NOT deploy to production.
Manual OAuth flow (no st.login): state stored in Redis, callback via st.query_params.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import urllib.parse
import urllib.request
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
        background-color: #1a1a1a; color: #f0f0f0; border: 1px solid #333;
    }
    .stTextInput > div > div > input::placeholder { color: #555; }
    .stTextInput label { color: #aaaaaa !important; font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Auth0 config (read from secrets) ─────────────────────────────────────────

try:
    _a0 = st.secrets.get("auth", {}).get("auth0", {})
    _DOMAIN        = _a0.get("server_metadata_url", "").replace("https://", "").split("/")[0]
    _CLIENT_ID     = _a0.get("client_id", "")
    _CLIENT_SECRET = _a0.get("client_secret", "")
except Exception:
    _DOMAIN        = "nwst-chatbot.us.auth0.com"
    _CLIENT_ID     = os.getenv("AUTH0_CLIENT_ID", "")
    _CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET", "")

_REDIRECT_URI = "https://nwst-test.streamlit.app"

# ── Redis state store ─────────────────────────────────────────────────────────

_rc = None
try:
    from chatbot_redis import get_redis_client
    _rc = get_redis_client()
except Exception:
    pass

def _store_state(state: str) -> None:
    if _rc:
        try:
            _rc.setex(f"oauth_state:{state}", 300, "1")
        except Exception:
            pass

def _consume_state(state: str) -> bool:
    if not _rc:
        return True  # fail open if Redis unavailable
    try:
        key = f"oauth_state:{state}"
        val = _rc.get(key)
        if val:
            _rc.delete(key)
            return True
    except Exception:
        return True
    return False

# ── OAuth helpers ─────────────────────────────────────────────────────────────

def _build_auth_url() -> str:
    state = secrets.token_urlsafe(32)
    _store_state(state)
    qs = urllib.parse.urlencode({
        "response_type": "code",
        "client_id":     _CLIENT_ID,
        "redirect_uri":  _REDIRECT_URI,
        "scope":         "openid email profile",
        "state":         state,
    })
    return f"https://{_DOMAIN}/authorize?{qs}"

def _exchange_code(code: str) -> dict | None:
    try:
        body = urllib.parse.urlencode({
            "grant_type":    "authorization_code",
            "client_id":     _CLIENT_ID,
            "client_secret": _CLIENT_SECRET,
            "code":          code,
            "redirect_uri":  _REDIRECT_URI,
        }).encode()
        req = urllib.request.Request(
            f"https://{_DOMAIN}/oauth/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception:
        return None

def _get_userinfo(access_token: str) -> dict | None:
    try:
        req = urllib.request.Request(
            f"https://{_DOMAIN}/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception:
        return None

# ── password reset ───────────────────────────────────────────────────────────

def _send_password_reset(email: str) -> bool:
    try:
        body = json.dumps({
            "client_id":  _CLIENT_ID,
            "email":      email,
            "connection": "Username-Password-Authentication",
        }).encode()
        req = urllib.request.Request(
            f"https://{_DOMAIN}/dbconnections/change_password",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False

# ── allowed emails ────────────────────────────────────────────────────────────

def _allowed_emails() -> list[str]:
    try:
        raw = st.secrets.get("CHATBOT_ALLOWED_EMAILS") or []
    except Exception:
        raw = os.getenv("CHATBOT_ALLOWED_EMAILS", "")
    if isinstance(raw, (list, tuple)):
        return [e.strip().lower() for e in raw if str(e).strip()]
    return [e.strip().lower() for e in str(raw).split(",") if e.strip()]

# ── session init ──────────────────────────────────────────────────────────────

for _k, _v in [
    ("authenticated", False), ("login_email", ""),
    ("user_name", ""), ("auth_method", ""),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── handle OAuth callback ─────────────────────────────────────────────────────

_qp = st.query_params
if "code" in _qp and "state" in _qp and not st.session_state.authenticated:
    _code, _state = _qp["code"], _qp["state"]
    st.query_params.clear()

    if not _consume_state(_state):
        st.error("Invalid or expired sign-in link — please try again.")
    else:
        with st.spinner("Signing you in..."):
            _tokens = _exchange_code(_code)
            if _tokens and "access_token" in _tokens:
                _info = _get_userinfo(_tokens["access_token"])
                if _info:
                    _sub = _info.get("sub", "")
                    st.session_state.authenticated = True
                    st.session_state.login_email   = _info.get("email", "")
                    st.session_state.user_name     = _info.get("name", "")
                    st.session_state.auth_method   = "Google" if _sub.startswith("google-oauth2") else "email + password"
                    st.rerun()
            st.error("Token exchange failed — check Auth0 config.")

# ── title ─────────────────────────────────────────────────────────────────────

st.title("NWST Assistant")

# ── logged-in view ────────────────────────────────────────────────────────────

if st.session_state.authenticated:
    _email     = st.session_state.login_email
    _name      = st.session_state.user_name or _email
    _method    = st.session_state.auth_method or "email + password"
    _permitted = _email.strip().lower() in _allowed_emails() if _email else False

    st.caption(
        f"👤 **{_name}**" + (f" · {_email}" if _name != _email else "")
    )
    st.success(f"Signed in via {_method}.")

    if not _permitted:
        st.warning(
            f"**{_email}** is not in the allowed-email list.  \n"
            "Add it to `CHATBOT_ALLOWED_EMAILS` in Secrets to grant access."
        )

    col_pw, col_out = st.columns(2)

    if _method == "email + password":
        if col_pw.button("Change password", use_container_width=True):
            if _send_password_reset(_email):
                st.info(f"Password reset email sent to **{_email}**. Check your inbox.")
            else:
                st.error("Could not send reset email — try again or contact an admin.")

    if col_out.button("Sign out", use_container_width=True):
        st.session_state.update({
            "authenticated": False,
            "login_email": "", "user_name": "", "auth_method": "",
        })
        st.rerun()

    st.stop()

# ── login page ────────────────────────────────────────────────────────────────

st.caption("Sign in to continue")
st.write("")

st.link_button(
    "Sign in",
    _build_auth_url(),
    use_container_width=True,
    type="primary",
)
