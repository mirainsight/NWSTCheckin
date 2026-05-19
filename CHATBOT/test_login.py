"""
Temporary login test page — DO NOT deploy.

Tests Streamlit's built-in Auth0/Google OAuth against the existing
[auth] config in .streamlit/secrets.toml. Run with:

    streamlit run NWSTCheckin/CHATBOT/test_login.py

This never modifies chatbot_app.py.
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

st.set_page_config(page_title="Login Test", page_icon="🔐", layout="centered")

# ── helpers ──────────────────────────────────────────────────────────────────

def _allowed_emails() -> list[str]:
    try:
        raw = st.secrets.get("CHATBOT_ALLOWED_EMAILS") or []
    except Exception:
        raw = os.getenv("CHATBOT_ALLOWED_EMAILS", "")
    if isinstance(raw, (list, tuple)):
        return [e.strip().lower() for e in raw if str(e).strip()]
    return [e.strip().lower() for e in str(raw).split(",") if e.strip()]


def _section(title: str) -> None:
    st.markdown(f"---\n### {title}")


# ── page header ──────────────────────────────────────────────────────────────

st.title("🔐 Login Test Page")
st.caption("Temporary sandbox — not linked to chatbot_app.py")

# ── SECTION 1: Streamlit built-in Auth0 / Google OAuth ───────────────────────

_section("1 · Streamlit OAuth (Auth0 → Google)")

_auth_available = False
try:
    # st.user is available in Streamlit ≥ 1.41 when [auth] is configured
    _u = st.user  # noqa: F841
    _auth_available = True
except AttributeError:
    pass

if not _auth_available:
    st.warning(
        "st.user is not available — upgrade Streamlit: `pip install -U streamlit`  \n"
        "Requires ≥ 1.41 for built-in OAuth support."
    )
else:
    col_login, col_logout = st.columns(2)
    with col_login:
        if st.button("Sign in with Auth0 / Google", use_container_width=True, type="primary"):
            st.login("auth0")
    with col_logout:
        if st.button("Sign out", use_container_width=True):
            st.logout()

    if st.user.is_logged_in:
        st.success("✅ Authenticated via OAuth")

        # Raw user object fields
        _email = getattr(st.user, "email", None) or ""
        _name  = getattr(st.user, "name",  None) or ""
        _sub   = getattr(st.user, "sub",   None) or ""

        with st.expander("Raw st.user fields", expanded=True):
            st.json({
                "email": _email,
                "name":  _name,
                "sub":   _sub,
                "is_logged_in": st.user.is_logged_in,
                # dump any extra attributes
                **{k: getattr(st.user, k) for k in dir(st.user)
                   if not k.startswith("_") and k not in ("email", "name", "sub", "is_logged_in")},
            })

        _allowed = _allowed_emails()
        _permitted = _email.strip().lower() in _allowed if _email else False
        if _permitted:
            st.success(f"✅ {_email} is in the allowed-email list — would pass the login gate.")
        else:
            st.error(
                f"❌ {_email or '(no email)'} is NOT in the allowed-email list.  \n"
                f"Add it to `CHATBOT_ALLOWED_EMAILS` in secrets.toml to grant access."
            )
    else:
        st.info("Not logged in via OAuth — click the button above to try.")


# ── SECTION 2: Existing email + password gate (for comparison) ───────────────

_section("2 · Current email + password gate (reference)")

st.caption(
    "This mirrors the exact logic in `chatbot_app.py::_check_login()`. "
    "No Redis / member lookup is performed here."
)

def _check_pw(email: str, password: str) -> bool:
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

with st.form("pw_login_form"):
    _test_email = st.text_input("Email", placeholder="your@email.com")
    _test_pw    = st.text_input("Password", type="password")
    _submit     = st.form_submit_button("Test sign-in")

if _submit:
    if _check_pw(_test_email, _test_pw):
        st.success(f"✅ Password gate: PASS — {_test_email} would be authenticated.")
    else:
        _ok_email  = _test_email.strip().lower() in _allowed_emails()
        _has_pw    = bool(os.getenv("CHATBOT_PASSWORD") or
                         (st.secrets.get("CHATBOT_PASSWORD") if True else ""))
        st.error("❌ Password gate: FAIL")
        st.write({
            "email_in_allowlist": _ok_email,
            "CHATBOT_PASSWORD set": _has_pw,
        })


# ── SECTION 3: Config diagnostics ────────────────────────────────────────────

_section("3 · Config diagnostics")

with st.expander("Secrets / env snapshot (sensitive values redacted)"):
    _diag: dict = {}

    # Auth0 config
    try:
        _auth_sec = st.secrets.get("auth") or {}
        _diag["auth.redirect_uri"]          = _auth_sec.get("redirect_uri", "(not set)")
        _diag["auth.cookie_secret"]         = "*** (set)" if _auth_sec.get("cookie_secret") else "(not set)"
        _auth0 = _auth_sec.get("auth0") or {}
        _diag["auth.auth0.client_id"]       = _auth0.get("client_id", "(not set)")
        _diag["auth.auth0.client_secret"]   = "*** (set)" if _auth0.get("client_secret") else "(not set)"
        _diag["auth.auth0.server_metadata"] = _auth0.get("server_metadata_url", "(not set)")
    except Exception as exc:
        _diag["auth_secrets_error"] = str(exc)

    # Allowed emails
    _diag["CHATBOT_ALLOWED_EMAILS"] = _allowed_emails() or "(none)"

    # Password
    _pw_set = bool(
        (st.secrets.get("CHATBOT_PASSWORD") if True else "") or
        os.getenv("CHATBOT_PASSWORD", "")
    )
    _diag["CHATBOT_PASSWORD"] = "*** (set)" if _pw_set else "(not set)"

    st.json(_diag)


# ── SECTION 4: Session state dump ────────────────────────────────────────────

_section("4 · Session state")

with st.expander("Full st.session_state"):
    st.json({k: str(v) for k, v in st.session_state.items()})
