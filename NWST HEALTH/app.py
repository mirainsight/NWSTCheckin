import html
import importlib.util
import os
import re
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from nwst_shared.paths import resolved_nwst_accent_config_path
from nwst_shared.nwst_daily_palette import (
    generate_colors_for_date,
    normalize_primary_hex as _normalize_primary_hex,
    theme_from_primary_hex,
)
from nwst_shared.nwst_cell_health_cache import (
    get_cell_health_from_redis,
    store_cell_health_in_redis,
    build_cell_health_row,
)
from nwst_shared.nwst_cell_health_report import load_cg_combined_df

import streamlit as st
import streamlit.components.v1 as components
from datetime import date, datetime, timedelta, timezone
import colorsys
import hashlib
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
import pandas as pd
import json
from collections import defaultdict
import plotly.express as px
import plotly.graph_objects as go
from upstash_redis import Redis

# Same spreadsheet as CG Combined / Attendance (NWST Health); override via env or secrets.
_DEFAULT_NWST_HEALTH_SHEET_ID = "1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY"
NWST_HEALTH_SHEET_ID = os.getenv("NWST_HEALTH_SHEET_ID", "").strip()
# Processed Attendance grid for Cell Attendance charts (shared across instances via Upstash)
NWST_REDIS_ATTENDANCE_CHART_GRID_KEY = "nwst_attendance_chart_grid"
REDIS_BIRTHDAYS_KEY = "attendance:birthdays_data"  # CG Combined cached for birthday display

# Monthly matrix shown in st.components.v1.html (iframe) — must be self-contained (no Streamlit theme CSS).
_MONTHLY_ATTENDANCE_IFRAME_CSS = r"""
.monthly-attendance-table-wrap {
    overflow-x: auto;
    margin: 0.35rem 0 1.25rem 0;
    width: 100%;
}
.monthly-attendance-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Inter', sans-serif;
    font-size: 0.9rem;
}
.monthly-attendance-table th {
    text-align: left;
    padding: 0.65rem 0.75rem;
    border-bottom: 2px solid rgba(255, 255, 255, 0.12);
    color: #999;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-size: 0.72rem;
    white-space: nowrap;
}
.monthly-attendance-table td {
    padding: 0.55rem 0.75rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    color: #e8e8e8;
}
.monthly-attendance-table th:nth-child(1),
.monthly-attendance-table td:nth-child(1) {
    max-width: 6rem;
    width: 1%;
    overflow: hidden;
    vertical-align: top;
    position: sticky;
    left: 0;
    background: #0e1117;
    z-index: 1;
}
.monthly-attendance-table th:nth-child(1) {
    z-index: 2;
}
.monthly-attendance-table th:nth-child(2),
.monthly-attendance-table td:nth-child(2) {
    max-width: 6rem;
    width: 1%;
    overflow: hidden;
    vertical-align: top;
}
.monthly-attendance-table .monthly-trunc-details {
    max-width: 100%;
}
.monthly-attendance-table .monthly-trunc-summary {
    cursor: pointer;
    list-style: none;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
    color: #e0e0e0;
    font-weight: 400;
    font-size: inherit;
    text-transform: none;
    border-bottom: none;
    letter-spacing: normal;
}
.monthly-attendance-table .monthly-trunc-summary::-webkit-details-marker {
    display: none;
}
.monthly-attendance-table .monthly-trunc-full {
    display: block;
    margin-top: 0.35rem;
    padding-top: 0.35rem;
    border-top: 1px solid rgba(255, 255, 255, 0.1);
    color: #e0e0e0;
    font-weight: 400;
    white-space: normal;
    word-break: break-word;
    line-height: 1.3;
    text-transform: none;
}
.monthly-attendance-table th:nth-child(3),
.monthly-attendance-table td:nth-child(3) {
    max-width: 5.75rem;
    width: 1%;
    white-space: nowrap;
    padding-left: 0.45rem;
    padding-right: 0.45rem;
    font-size: 0.82rem;
}
.monthly-attendance-table th:nth-child(n+4),
.monthly-attendance-table td:nth-child(n+4) {
    max-width: 3.75rem;
    width: 1%;
    white-space: nowrap;
    text-align: center;
    padding: 0.45rem 0.35rem;
    font-size: 0.82rem;
}
.monthly-attendance-table .monthly-attendance-rate-cell span {
    font-weight: 700;
}
.monthly-status-regular {
    color: #2ecc71;
    font-weight: 700;
}
.monthly-status-irregular {
    color: #e67e22;
    font-weight: 700;
}
.monthly-status-followup {
    color: #f39c12;
    font-weight: 700;
}
.monthly-health-tile-new {
    color: #3498db;
    font-weight: 700;
}
.monthly-health-tile-red {
    color: #e74c3c;
    font-weight: 700;
}
.monthly-health-tile-graduated {
    color: #9b59b6;
    font-weight: 700;
}
.monthly-sort-th {
    user-select: none;
    cursor: pointer;
}
.monthly-sort-th:hover {
    color: #cccccc !important;
}
"""

# Detailed Members roster: same base table as Individual Attendance, without IA’s narrow month-column rules.
_DETAILED_MEMBERS_IFRAME_CSS_EXTRA = r"""
.detailed-members-table.monthly-attendance-table th:nth-child(n+2) {
    max-width: 11rem;
    width: 1%;
    text-align: left;
    font-size: 0.72rem;
    vertical-align: bottom;
    white-space: normal;
}
.detailed-members-table.monthly-attendance-table td:nth-child(n+2) {
    max-width: 8rem;
    width: 1%;
    overflow: hidden;
    text-align: left;
    font-size: 0.88rem;
    vertical-align: top;
}
.detailed-members-table.monthly-attendance-table th:first-child,
.detailed-members-table.monthly-attendance-table td:first-child {
    width: 1%;
    max-width: 5rem;
}
"""

# CHECK IN attendance spreadsheet — used only for the Analytics tab (Attendance Analytics, Options, Key Values)
CHECKIN_ATTENDANCE_SHEET_ID = os.getenv("ATTENDANCE_SHEET_ID", "").strip()
if not CHECKIN_ATTENDANCE_SHEET_ID:
    try:
        if hasattr(st, "secrets") and "ATTENDANCE_SHEET_ID" in st.secrets:
            CHECKIN_ATTENDANCE_SHEET_ID = str(st.secrets["ATTENDANCE_SHEET_ID"]).strip()
    except FileNotFoundError:
        pass

if not NWST_HEALTH_SHEET_ID:
    try:
        if hasattr(st, "secrets") and "NWST_HEALTH_SHEET_ID" in st.secrets:
            NWST_HEALTH_SHEET_ID = str(st.secrets["NWST_HEALTH_SHEET_ID"]).strip()
    except FileNotFoundError:
        pass
if not NWST_HEALTH_SHEET_ID:
    NWST_HEALTH_SHEET_ID = _DEFAULT_NWST_HEALTH_SHEET_ID

NWST_KEY_VALUES_TAB = "Key Values"
NWST_ATTENDANCE_TAB = "Attendance"
NWST_OPTIONS_TAB = "Options"
NWST_ATTENDANCE_ANALYTICS_TAB = "Attendance Analytics"
NWST_STATUS_HISTORICAL_TAB = "Status Historical"
NWST_HISTORICAL_CELL_STATUS_TAB = "Historical Cell Status"

# Individual Attendance monthly matrix: trailing window of month columns (most recent in data).
MONTHLY_MEMBER_MATRIX_MAX_MONTHS = 4

# Map CG Combined **Cell** dropdown value → short tab name (matches Apps Script `tabNameToDisplayCellForHistory_` inverse).
_NWST_CELL_DISPLAY_TO_TAB = {
    "Anchor Street": "Anchor",
    "Aster Street": "Aster",
    "Crown Street": "Crown",
    "Street Fire": "Fire",
    "Fishers Street": "Fishers",
    "Street Forth": "Forth",
    "HIS Street": "HIS",
    "Home Street": "Home",
    "King Street": "King",
    "Life Street": "Life",
    "Meta Street": "Meta",
    "Royal Street": "Royal",
    "Street Runners": "Runners",
    "Shepherds Street": "Shepherds",
    "Street Lights": "Lights",
    "Via Dolorosa Street": "Via Dolorosa",
    "Narrowstreet Core Team": "Core Team",
}

# Shared by “Attendance rate by cell” (per-zone tabs) and “Zone Attendance Trend” in Analytics
NWST_ANALYTICS_MULTILINE_PALETTE = [
    "#FF2D95",
    "#00F0FF",
    "#FFE14A",
    "#B388FF",
    "#00FF94",
    "#FF6B2C",
    "#5EB8FF",
    "#FF4081",
]


def _nwst_analytics_palette_for_n(n_categories):
    """Repeat/cycle the analytics multiline palette so every series gets a color."""
    if n_categories <= 0:
        return []
    base = NWST_ANALYTICS_MULTILINE_PALETTE
    k = len(base)
    return [base[i % k] for i in range(n_categories)]


def _nwst_collapsible_section_css(primary_hex: str) -> str:
    """Style ``st.expander`` summary like CELL HEALTH section headers (green, uppercase, rule)."""
    c = html.escape(str(primary_hex or "#00ff00"), quote=True)
    return f"""<style>
div[data-testid="stExpander"] details {{
    background: transparent;
    border: none;
}}
/* Only the expander title row — not <summary> inside Cell/Member table trunc widgets */
div[data-testid="stExpander"] summary:not(.monthly-trunc-summary) {{
    font-family: 'Inter', sans-serif !important;
    font-weight: 900 !important;
    font-size: 1.2rem !important;
    color: {c} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    list-style: none !important;
    cursor: pointer;
    padding: 0.5rem 0.75rem 0.6rem 0.75rem !important;
    margin: 0 0 0.35rem 0 !important;
    border-bottom: 3px solid {c} !important;
    background: #000000 !important;
}}
div[data-testid="stExpander"] summary:not(.monthly-trunc-summary)::-webkit-details-marker {{
    display: none !important;
}}
</style>"""


def _render_cg_leadership_section(display_df, cell_filter, cell_columns, daily_colors):
    """Content for CG Health > Leadership collapsible."""
    if not display_df.empty:
        leadership_data = get_leadership_by_role(display_df)

        if leadership_data:
            total_leaders = sum(len(members) for members in leadership_data.values())

            if cell_filter != "All" and cell_columns:
                total_in_cell = len(display_df[display_df[cell_columns[0]] == cell_filter])
            else:
                total_in_cell = len(display_df)

            leader_pct = (total_leaders / total_in_cell * 100) if total_in_cell > 0 else 0

            leader_kpi_col1, leader_kpi_col2 = st.columns(2)

            with leader_kpi_col1:
                st.markdown(
                    f"""
                <div class="kpi-card">
                    <div class="kpi-label">Total Leaders</div>
                    <div class="kpi-number">{total_leaders}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            with leader_kpi_col2:
                st.markdown(
                    f"""
                <div class="kpi-card">
                    <div class="kpi-label">Leaders %</div>
                    <div class="kpi-number" style="color: {daily_colors['primary']};">{leader_pct:.0f}%</div>
                    <div class="kpi-subtitle">{total_leaders} of {total_in_cell}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            st.markdown("")

            for role_name, members in leadership_data.items():
                st.markdown(
                    f"<h3 style='color: {daily_colors['primary']}; font-size: 1.1rem;'>{role_name}</h3>",
                    unsafe_allow_html=True,
                )

                for leader in members:
                    since_text = f"Since: {leader['since']}" if leader["since"] else "Since: Not available"
                    st.markdown(
                        f"""
                    <div style='padding: 1rem; background: #1a1a1a; border-left: 3px solid {daily_colors['primary']}; margin-bottom: 0.75rem;'>
                        <p style='font-weight: 600; margin: 0;'>{leader['name']}</p>
                        <p style='font-size: 0.85rem; color: #999; margin: 0.25rem 0 0 0;'>{since_text}</p>
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )

                st.markdown("")
        else:
            st.info("No leadership roles assigned yet.")
    else:
        st.info("No leadership data available.")


def _render_cg_ministry_section(display_df, daily_colors):
    """Content for CG Health > Ministry collapsible."""
    if display_df is None or display_df.empty:
        st.info("No ministry data available.")
        return

    ministry_data = get_members_by_ministry(display_df)
    if not ministry_data:
        st.info("No ministry roles assigned yet.")
        return

    total_ministry = sum(len(m) for m in ministry_data.values())
    total_in_cell = len(display_df)
    ministry_pct = (total_ministry / total_in_cell * 100) if total_in_cell > 0 else 0

    kpi_col1, kpi_col2 = st.columns(2)
    with kpi_col1:
        st.markdown(
            f"""
        <div class="kpi-card">
            <div class="kpi-label">Total in Ministry</div>
            <div class="kpi-number">{total_ministry}</div>
        </div>""",
            unsafe_allow_html=True,
        )
    with kpi_col2:
        st.markdown(
            f"""
        <div class="kpi-card">
            <div class="kpi-label">Ministry %</div>
            <div class="kpi-number" style="color: {daily_colors['primary']};">{ministry_pct:.0f}%</div>
            <div class="kpi-subtitle">{total_ministry} of {total_in_cell}</div>
        </div>""",
            unsafe_allow_html=True,
        )

    st.markdown("")

    for ministry_name, members in ministry_data.items():
        st.markdown(
            f"<h3 style='color: {daily_colors['primary']}; font-size: 1.1rem;'>"
            f"{ministry_name} ({len(members)})</h3>",
            unsafe_allow_html=True,
        )
        for member in members:
            st.markdown(
                f"""
            <div style='padding: 1rem; background: #1a1a1a;
                        border-left: 3px solid {daily_colors['primary']};
                        margin-bottom: 0.75rem;'>
                <p style='font-weight: 600; margin: 0;'>{member['name']}</p>
                <p style='font-size: 0.85rem; color: #999; margin: 0.25rem 0 0 0;'>{member['role']}</p>
            </div>""",
                unsafe_allow_html=True,
            )
        st.markdown("")


@st.fragment
def _nwst_cell_health_fragment(ch_ctx: dict):
    """Only this block reruns when Cell health category buttons toggle name lists."""
    _nwst_cell_health_render_interactive(ch_ctx)


def _nwst_cell_health_render_interactive(ch_ctx: dict):
    work_df = ch_ctx["work_df"]
    status_col = ch_ctx["status_col"]
    attendance_stats = ch_ctx["attendance_stats"]
    _cell_scoped = ch_ctx["_cell_scoped"]
    new_count = ch_ctx["new_count"]
    new_pct = ch_ctx["new_pct"]
    regular_count = ch_ctx["regular_count"]
    regular_pct = ch_ctx["regular_pct"]
    irregular_count = ch_ctx["irregular_count"]
    irregular_pct = ch_ctx["irregular_pct"]
    follow_up_count = ch_ctx["follow_up_count"]
    follow_up_pct = ch_ctx["follow_up_pct"]
    red_count = ch_ctx["red_count"]
    red_pct = ch_ctx["red_pct"]
    graduated_count = ch_ctx["graduated_count"]
    graduated_pct = ch_ctx["graduated_pct"]
    wow_new = ch_ctx["wow_new"]
    wow_regular = ch_ctx["wow_regular"]
    wow_irregular = ch_ctx["wow_irregular"]
    wow_follow_up = ch_ctx["wow_follow_up"]
    wow_red = ch_ctx["wow_red"]
    wow_graduated = ch_ctx["wow_graduated"]

    def _member_tiles(data_df, border_color):
        if data_df.empty:
            st.caption("No members in this bucket.")
            return
        if "name" in data_df.columns or "Name" in data_df.columns:
            name_col = "name" if "name" in data_df.columns else "Name"
            cell_col = None
            for col in data_df.columns:
                if col.lower().strip() in ["cell", "group"]:
                    cell_col = col
                    break
            bc = html.escape(border_color, quote=True)
            names = sorted(data_df[name_col].astype(str).unique().tolist())
            parts = []
            for name in names:
                person_cell = ""
                if cell_col:
                    person_row = data_df[data_df[name_col] == name]
                    if not person_row.empty:
                        person_cell = str(person_row[cell_col].iloc[0]).strip()
                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)
                tip_e = html.escape(tooltip_text, quote=True)
                name_e = html.escape(str(name), quote=True)
                parts.append(
                    f'<span class="member-tile" style="border-color: {bc};" data-tooltip="{tip_e}">{name_e}</span> '
                )
            st.markdown("".join(parts), unsafe_allow_html=True)
        else:
            st.dataframe(data_df, use_container_width=True)

    def _cell_health_mix_card_html(accent_hex, kpi_label, pct_val, n_members, wow_fragment):
        ae = html.escape(accent_hex, quote=True)
        kl = html.escape(kpi_label, quote=True)
        return f"""
            <div class="kpi-card ch-kpi-card-embed" style="cursor: pointer;">
                <div class="kpi-label">{kl}</div>
                <div class="ch-kpi-wow-row">
                    <div class="kpi-number" style="color: {ae};">{pct_val:.0f}%</div>
                    {wow_fragment}
                </div>
                <div class="kpi-subtitle">{n_members} members</div>
            </div>
            """

    for _sk in (
        "expand_new",
        "expand_regular",
        "expand_irregular",
        "expand_follow_up",
        "expand_red",
        "expand_graduated",
    ):
        if _sk not in st.session_state:
            st.session_state[_sk] = False

    if _cell_scoped:
        col1, col2, col3, col4 = st.columns(4)
    else:
        col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔵 New", key="btn_new", use_container_width=True):
            st.session_state.expand_new = not st.session_state.expand_new
        st.markdown(
            _cell_health_mix_card_html("#3498db", "New Members", new_pct, new_count, wow_new),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_new:
            st.markdown(
                "<p style='color: #3498db; font-weight: 600;'>New Members</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                new_data = work_df[work_df["status_type"] == "New"].copy()
            else:
                new_data = work_df.head(new_count).copy()
            _member_tiles(new_data, "#3498db")

    with col2:
        if st.button("🟢 Regular", key="btn_regular", use_container_width=True):
            st.session_state.expand_regular = not st.session_state.expand_regular
        st.markdown(
            _cell_health_mix_card_html(
                "#2ecc71", "Regular Members", regular_pct, regular_count, wow_regular
            ),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_regular:
            st.markdown(
                "<p style='color: #2ecc71; font-weight: 600;'>Regular Members (75% and above attendance)</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                regular_data = work_df[work_df["status_type"] == "Regular"].copy()
            else:
                regular_data = work_df.iloc[new_count : new_count + regular_count].copy()
            _member_tiles(regular_data, "#2ecc71")

    with col3:
        if st.button("🟠 Irregular", key="btn_irregular", use_container_width=True):
            st.session_state.expand_irregular = not st.session_state.expand_irregular
        st.markdown(
            _cell_health_mix_card_html(
                "#e67e22",
                "Irregular Members",
                irregular_pct,
                irregular_count,
                wow_irregular,
            ),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_irregular:
            st.markdown(
                "<p style='color: #e67e22; font-weight: 600;'>Irregular Members (Below 75% attendance)</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                irregular_data = work_df[work_df["status_type"] == "Irregular"].copy()
            else:
                irregular_data = work_df.iloc[
                    new_count + regular_count : new_count + regular_count + irregular_count
                ].copy()
            _member_tiles(irregular_data, "#e67e22")

    if _cell_scoped:
        with col4:
            if st.button("🟡 Follow Up", key="btn_follow_up", use_container_width=True):
                st.session_state.expand_follow_up = not st.session_state.expand_follow_up
            st.markdown(
                _cell_health_mix_card_html(
                    "#f39c12", "Follow Up", follow_up_pct, follow_up_count, wow_follow_up
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_follow_up:
                st.markdown(
                    "<p style='color: #f39c12; font-weight: 600;'>Follow Up (0% attendance - past 2 months)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    follow_up_data = work_df[work_df["status_type"] == "Follow Up"].copy()
                else:
                    follow_up_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                    ].copy()
                _member_tiles(follow_up_data, "#f39c12")
    else:
        st.markdown("")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🟡 Follow Up", key="btn_follow_up", use_container_width=True):
                st.session_state.expand_follow_up = not st.session_state.expand_follow_up
            st.markdown(
                _cell_health_mix_card_html(
                    "#f39c12", "Follow Up", follow_up_pct, follow_up_count, wow_follow_up
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_follow_up:
                st.markdown(
                    "<p style='color: #f39c12; font-weight: 600;'>Follow Up (0% attendance - past 2 months)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    follow_up_data = work_df[work_df["status_type"] == "Follow Up"].copy()
                else:
                    follow_up_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                    ].copy()
                _member_tiles(follow_up_data, "#f39c12")

        with col2:
            if st.button("🔴 Red", key="btn_red", use_container_width=True):
                st.session_state.expand_red = not st.session_state.expand_red
            st.markdown(
                _cell_health_mix_card_html("#e74c3c", "Red", red_pct, red_count, wow_red),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_red:
                st.markdown(
                    "<p style='color: #e74c3c; font-weight: 600;'>Red (Won't come to church anymore)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    red_data = work_df[work_df["status_type"] == "Red"].copy()
                else:
                    red_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                        + red_count
                    ].copy()
                _member_tiles(red_data, "#e74c3c")

        with col3:
            if st.button("⭐ Graduated", key="btn_graduated", use_container_width=True):
                st.session_state.expand_graduated = not st.session_state.expand_graduated
            st.markdown(
                _cell_health_mix_card_html(
                    "#9b59b6", "Graduated", graduated_pct, graduated_count, wow_graduated
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_graduated:
                st.markdown(
                    "<p style='color: #9b59b6; font-weight: 600;'>Graduated (Moved to leadership roles)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    graduated_data = work_df[work_df["status_type"] == "Graduated"].copy()
                else:
                    graduated_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                        + red_count :
                    ].copy()
                _member_tiles(graduated_data, "#9b59b6")

    st.markdown("")


def _render_cg_cell_health_section(
    display_df,
    daily_colors,
    cell_filter="All",
    attendance_stats=None,
    hist_df_override=None,
    redis_cache_key_override=None,
):
    """Cell health — KPI column layout + Historical Cell Status WoW pills + expandable name tiles.

    Reads from Upstash cache (single source of truth) when available, falls back to live calculation.
    Pass ``hist_df_override`` to use a different historical snapshot DataFrame (e.g. Historical Ministry
    Status) instead of Historical Cell Status. Pass ``redis_cache_key_override`` to read/write a
    separate Redis key (e.g. ``REDIS_MINISTRY_HEALTH_KEY``) so ministry WoW is cached independently.
    """
    if attendance_stats is None:
        attendance_stats = {}

    prim_hex = str(daily_colors.get("primary", "#00ff00"))
    prim = html.escape(prim_hex, quote=True)

    if display_df.empty:
        st.info("No cell health data available.")
        return

    # Try reading from Upstash cache first (single source of truth)
    cache_hit = False
    redis = get_redis_client()
    if redis:
        if redis_cache_key_override:
            try:
                raw = redis.get(redis_cache_key_override)
                if raw:
                    cached_data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                else:
                    cached_data = None
            except Exception:
                cached_data = None
        else:
            cached_data = get_cell_health_from_redis(redis)
        if cached_data:
            _cell_scoped = (
                cell_filter is not None
                and str(cell_filter).strip()
                and str(cell_filter).strip().lower() != "all"
            )

            # Find the right row from cache
            target_row = None
            if _cell_scoped:
                # Look for specific cell in cell_rows
                cell_filter_lower = str(cell_filter).strip().lower()
                for row in cached_data.get("cell_rows", []):
                    if str(row.get("cell", "")).strip().lower() == cell_filter_lower:
                        target_row = row
                        break
            else:
                # Use "All" row
                target_row = cached_data.get("all_row")

            if target_row:
                cache_hit = True
                new_count = target_row.get("new_count", 0)
                regular_count = target_row.get("regular_count", 0)
                irregular_count = target_row.get("irregular_count", 0)
                follow_up_count = target_row.get("follow_up_count", 0)
                red_count = target_row.get("red_count", 0)
                graduated_count = target_row.get("graduated_count", 0)
                total_members = target_row.get("total_count", 0)

                new_pct = target_row.get("new_pct", 0)
                regular_pct = target_row.get("regular_pct", 0)
                irregular_pct = target_row.get("irregular_pct", 0)
                follow_up_pct = target_row.get("follow_up_pct", 0)
                red_pct = target_row.get("red_pct", 0)
                graduated_pct = target_row.get("graduated_pct", 0)

                # Build curr_agg and prev_agg from cached deltas for WoW pills
                delta_new = target_row.get("delta_new", 0)
                delta_regular = target_row.get("delta_regular", 0)
                delta_irregular = target_row.get("delta_irregular", 0)
                delta_follow_up = target_row.get("delta_follow_up", 0)

                # Create synthetic agg dicts for WoW pill rendering
                curr_agg = {
                    "new": new_count,
                    "regular": regular_count,
                    "irregular": irregular_count,
                    "follow up": follow_up_count,
                    "red": red_count,
                    "graduated": graduated_count,
                    "total": total_members,
                }
                prev_agg = {
                    "new": new_count - delta_new,
                    "regular": regular_count - delta_regular,
                    "irregular": irregular_count - delta_irregular,
                    "follow up": follow_up_count - delta_follow_up,
                    "red": red_count,
                    "graduated": graduated_count,
                    "total": total_members - delta_new - delta_regular - delta_irregular - delta_follow_up,
                }

                # Still need work_df and status_col for member tiles even with cached aggregates
                work_df = display_df.copy()
                status_columns = [col for col in work_df.columns if "status" in col.lower()]
                status_col = status_columns[0] if status_columns else None
                if status_col:
                    work_df["status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)

    # Fallback: calculate from live display_df if cache miss
    if not cache_hit:
        work_df = display_df.copy()
        status_columns = [col for col in work_df.columns if "status" in col.lower()]
        status_col = status_columns[0] if status_columns else None

        if status_col:
            work_df["status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)
            new_count = len(work_df[work_df["status_type"] == "New"])
            regular_count = len(work_df[work_df["status_type"] == "Regular"])
            irregular_count = len(work_df[work_df["status_type"] == "Irregular"])
            follow_up_count = len(work_df[work_df["status_type"] == "Follow Up"])
            red_count = len(work_df[work_df["status_type"] == "Red"])
            graduated_count = len(work_df[work_df["status_type"] == "Graduated"])
        else:
            total_members_fb = len(work_df)
            new_count = max(1, int(total_members_fb * 0.20))
            regular_count = max(1, int(total_members_fb * 0.40))
            irregular_count = max(1, int(total_members_fb * 0.20))
            follow_up_count = max(1, int(total_members_fb * 0.10))
            red_count = max(1, int(total_members_fb * 0.05))
            graduated_count = (
                total_members_fb - new_count - regular_count - irregular_count - follow_up_count - red_count
            )

        total_members = new_count + regular_count + irregular_count + follow_up_count + red_count + graduated_count

        _cell_scoped = (
            cell_filter is not None
            and str(cell_filter).strip()
            and str(cell_filter).strip().lower() != "all"
        )
        if _cell_scoped:
            mix_denom = new_count + regular_count + irregular_count + follow_up_count
            if mix_denom > 0:
                new_pct = new_count / mix_denom * 100
                regular_pct = regular_count / mix_denom * 100
                irregular_pct = irregular_count / mix_denom * 100
                follow_up_pct = follow_up_count / mix_denom * 100
            else:
                new_pct = regular_pct = irregular_pct = follow_up_pct = 0.0
            red_pct = 0.0
            graduated_pct = 0.0
        else:
            new_pct = (new_count / total_members * 100) if total_members > 0 else 0
            regular_pct = (regular_count / total_members * 100) if total_members > 0 else 0
            irregular_pct = (irregular_count / total_members * 100) if total_members > 0 else 0
            follow_up_pct = (follow_up_count / total_members * 100) if total_members > 0 else 0
            red_pct = (red_count / total_members * 100) if total_members > 0 else 0
            graduated_pct = (graduated_count / total_members * 100) if total_members > 0 else 0

        hist_df = hist_df_override if hist_df_override is not None else load_historical_cell_status_dataframe()
        curr_agg, prev_agg = None, None
        if hist_df is not None and not hist_df.empty:
            curr_agg, prev_agg, _, _ = _nwst_hist_cell_wow_for_scope(hist_df, cell_filter)

    wow_new = _nwst_cell_health_wow_pill_html("new", curr_agg, prev_agg)
    wow_regular = _nwst_cell_health_wow_pill_html("regular", curr_agg, prev_agg)
    wow_irregular = _nwst_cell_health_wow_pill_html("irregular", curr_agg, prev_agg)
    wow_follow_up = _nwst_cell_health_wow_pill_html("follow_up", curr_agg, prev_agg)
    wow_red = _nwst_cell_health_wow_pill_html("red", curr_agg, prev_agg)
    wow_graduated = _nwst_cell_health_wow_pill_html("graduated", curr_agg, prev_agg)

    st.markdown(
        f"""
<style>
  .ch-head-nwst {{
    font-family: 'Inter', sans-serif;
    font-weight: 700;
    font-size: 0.82rem;
    color: {prim};
    text-transform: uppercase;
    letter-spacing: 0.16em;
    margin: 0 0 1.35rem 0;
    display: block;
  }}
  /* Streamlit often draws a grey frame around markdown HTML — strip it for cell-health KPI cards */
  [data-testid="stMarkdownContainer"]:has(.ch-kpi-card-embed),
  [data-testid="stMarkdownContainer"]:has(.ch-kpi-card-embed) > div,
  [data-testid="element-container"]:has(.ch-kpi-card-embed) {{
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    outline: none !important;
  }}
  .ch-kpi-wow-row {{
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-start;
    flex-wrap: wrap;
    gap: 0.45rem 0.85rem;
    margin: 0.35rem 0 0.2rem 0;
  }}
  .ch-kpi-wow-row .kpi-number {{
    margin: 0 !important;
    line-height: 1;
    flex: 0 1 auto;
  }}
  .ch-kpi-wow-row .ch-pill-wrap {{
    display: inline-flex !important;
    width: fit-content !important;
    max-width: 100%;
    margin: 0 !important;
    padding: 0 !important;
    flex: 0 0 auto;
    min-width: 0;
    align-self: center;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
  }}
  .ch-pill.ch-pill--hero {{
    font-size: clamp(0.88rem, 1.55vw, 1.12rem);
    padding: 0.3rem 0.78rem 0.32rem;
    gap: 0.34rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    border: none;
    outline: none;
  }}
  /* Hero WoW: glow only — no 1px ring (reads as a second “frame”) */
  .ch-pill.ch-pill--hero.ch-pill-good {{
    box-shadow: 0 0 28px rgba(94, 234, 212, 0.38);
    text-shadow: 0 0 12px rgba(94, 234, 212, 0.45);
  }}
  .ch-pill.ch-pill--hero.ch-pill-bad {{
    box-shadow: 0 0 24px rgba(253, 164, 175, 0.28);
    text-shadow: 0 0 10px rgba(253, 164, 175, 0.35);
  }}
  .ch-pill.ch-pill--hero.ch-pill-flat {{
    box-shadow: none;
    background: rgba(42, 42, 42, 0.95);
  }}
  .ch-pill.ch-pill--hero.ch-pill-na {{
    box-shadow: none;
    background: #2a2a2a;
  }}
  .ch-pill--hero .ch-pill-arrow {{
    font-size: 0.95em;
  }}
  .ch-pill-wrap {{ margin-top: 0.35rem; line-height: 1; max-width: 100%; }}
  .ch-pill {{
    display: inline-flex;
    align-items: center;
    gap: 0.2rem;
    padding: 0.14rem 0.42rem 0.15rem;
    border-radius: 9999px;
    font-family: 'Inter', sans-serif;
    font-size: 0.52rem;
    font-weight: 600;
    letter-spacing: 0.02em;
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .ch-pill-good {{
    background: #153729;
    color: #5eead4;
    box-shadow: 0 0 14px rgba(94, 234, 212, 0.22);
    text-shadow: 0 0 10px rgba(94, 234, 212, 0.35);
  }}
  .ch-pill-bad {{
    background: #351a22;
    color: #fda4af;
    box-shadow: 0 0 12px rgba(253, 164, 175, 0.18);
    text-shadow: 0 0 8px rgba(253, 164, 175, 0.3);
  }}
  .ch-pill-flat {{
    background: #2a2a2a;
    color: #c6c6c6;
    font-weight: 500;
    box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
  }}
  .ch-pill-na {{
    background: #252525;
    color: #888;
    font-weight: 400;
    white-space: normal;
  }}
  .ch-pill-arrow {{
    font-size: 0.68em;
    font-weight: 700;
    line-height: 1;
    opacity: 0.95;
  }}
</style>
<p class="ch-head-nwst">Cell health</p>
""",
        unsafe_allow_html=True,
    )

    ch_ctx = {
        "work_df": work_df,
        "status_col": status_col,
        "_cell_scoped": _cell_scoped,
        "attendance_stats": attendance_stats,
        "new_count": new_count,
        "new_pct": new_pct,
        "regular_count": regular_count,
        "regular_pct": regular_pct,
        "irregular_count": irregular_count,
        "irregular_pct": irregular_pct,
        "follow_up_count": follow_up_count,
        "follow_up_pct": follow_up_pct,
        "red_count": red_count,
        "red_pct": red_pct,
        "graduated_count": graduated_count,
        "graduated_pct": graduated_pct,
        "wow_new": wow_new,
        "wow_regular": wow_regular,
        "wow_irregular": wow_irregular,
        "wow_follow_up": wow_follow_up,
        "wow_red": wow_red,
        "wow_graduated": wow_graduated,
    }
    _nwst_cell_health_fragment(ch_ctx)


def _nwst_normalize_gender_value(val):
    """Map sheet gender text to Male / Female, or None if unknown."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    if s in ("m", "male", "man", "men", "boy", "boys") or s.startswith("male"):
        return "Male"
    if s in ("f", "female", "woman", "women", "girl", "girls") or s.startswith("female"):
        return "Female"
    return None


def _nwst_age_bucket_label(val):
    """Return age bucket label: '<13', '13', '14', …, or None if unparseable."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        try:
            n = int(float(val))
        except (TypeError, ValueError):
            n = None
        if n is None:
            return None
        return "<13" if n < 13 else str(n)
    s = str(val).strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+)", s)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except ValueError:
        return None
    return "<13" if n < 13 else str(n)


def _nwst_age_bucket_sort_key(label: str):
    if label == "<13":
        return (-1, 0)
    if label in ("Unknown", "—"):
        return (10_000, 0)
    try:
        return (0, int(label))
    except ValueError:
        return (5000, hash(label) % 10_000)


def _nwst_status_type_vectorized(col: pd.Series) -> pd.Series:
    """Same categories as extract_cell_sheet_status_type, without per-row Python calls."""
    s = col.fillna("").astype(str)
    out = pd.Series(None, index=col.index, dtype=object)
    assigned = pd.Series(False, index=col.index)
    for label, mk in (
        ("Regular", s.str.startswith("Regular:", na=False)),
        ("Irregular", s.str.startswith("Irregular:", na=False)),
        ("New", s.str.startswith("New", na=False)),
        ("Follow Up", s.str.startswith("Follow Up:", na=False)),
        ("Red", s.str.startswith("Red:", na=False)),
        ("Graduated", s.str.startswith("Graduated:", na=False)),
    ):
        take = mk & ~assigned
        out.loc[take] = label
        assigned |= take
    return out


def _nwst_normalize_gender_series(col: pd.Series) -> pd.Series:
    """Vectorized _nwst_normalize_gender_value (Male / Female / None)."""
    s = col.fillna("").astype(str).str.strip().str.lower()
    out = pd.Series(None, index=col.index, dtype=object)
    male_set = {"m", "male", "man", "men", "boy", "boys"}
    female_set = {"f", "female", "woman", "women", "girl", "girls", "lady", "ladies"}
    m_male = s.isin(male_set) | s.str.startswith("male", na=False)
    m_female = s.isin(female_set) | s.str.startswith("female", na=False)
    out.loc[m_male] = "Male"
    out.loc[m_female & ~m_male] = "Female"
    return out


def _nwst_age_bucket_series(series: pd.Series) -> pd.Series:
    """Vectorized age buckets: '<13', year strings, or 'Unknown'."""
    num = pd.to_numeric(series, errors="coerce")
    first_digits = series.astype(str).str.extract(r"(\d+)", expand=False)
    num_from_str = pd.to_numeric(first_digits, errors="coerce")
    num_f = num.where(num.notna(), num_from_str)
    out = pd.Series("Unknown", index=series.index, dtype=object)
    v = num_f.notna()
    out.loc[v & (num_f < 13)] = "<13"
    ge = v & (num_f >= 13)
    out.loc[ge] = num_f.loc[ge].astype("int64", copy=False).astype(str)
    return out


def _render_cell_breakdown_section(display_df, daily_colors, filter_scope: str = "all"):
    """Demographics for current roster (after Cell filter): age bars + segment chips + gender bars."""
    if display_df is None or display_df.empty:
        st.info("No member data for cell breakdown.")
        return

    work = display_df.copy()
    actual_cols, display_labels = _resolve_member_table_columns(work)
    age_col = None
    gender_col = None
    for a, lab in zip(actual_cols, display_labels):
        if lab == "Age":
            age_col = a
        if lab == "Gender":
            gender_col = a

    role_col = None
    for col in work.columns:
        if str(col).strip().lower() == "role":
            role_col = col
            break

    status_col = None
    for col in work.columns:
        if "status" in str(col).lower():
            status_col = col
            break

    if status_col:
        work["_cb_status"] = _nwst_status_type_vectorized(work[status_col])
    else:
        work["_cb_status"] = pd.Series(None, index=work.index, dtype=object)

    if gender_col:
        work["_cb_gender"] = _nwst_normalize_gender_series(work[gender_col])
    else:
        work["_cb_gender"] = pd.Series(None, index=work.index, dtype=object)

    if role_col:
        work["_cb_role_ne"] = work[role_col].notna() & (
            work[role_col].astype(str).str.strip() != ""
        )
    else:
        work["_cb_role_ne"] = pd.Series(False, index=work.index)

    # Ministry role columns
    _ministry_col_map = {}  # ministry_id -> actual column name
    for _min_id, _min_label in _MINISTRY_ROLE_COLS.items():
        for col in work.columns:
            if col.lower().strip() == _min_label.lower():
                _ministry_col_map[_min_id] = col
                break
    for _min_id, _col in _ministry_col_map.items():
        work[f"_cb_{_min_id.lower()}_ne"] = work[_col].notna() & (
            work[_col].astype(str).str.strip() != ""
        )
    # _cb_ministry_ne: True if member has ANY ministry role
    _min_ne_cols = [f"_cb_{k.lower()}_ne" for k in _ministry_col_map]
    if _min_ne_cols:
        work["_cb_ministry_ne"] = work[_min_ne_cols].any(axis=1)
    else:
        work["_cb_ministry_ne"] = pd.Series(False, index=work.index)

    # Segment filter options: parallel lists for st.radio
    seg_labels = ["All"]
    seg_ids = ["all"]

    gser = work["_cb_gender"]
    if gender_col:
        if (gser == "Male").any():
            seg_labels.append("Male")
            seg_ids.append("male")
        if (gser == "Female").any():
            seg_labels.append("Female")
            seg_ids.append("female")

    if role_col and bool(work["_cb_role_ne"].any()):
        seg_labels.append("Leader")
        seg_ids.append("leader")

    if bool(work["_cb_ministry_ne"].any()):
        seg_labels.append("Ministry")
        seg_ids.append("ministry")

    if status_col:
        for st_lab in ("New", "Regular", "Irregular", "Follow Up"):
            if (work["_cb_status"] == st_lab).any():
                seg_labels.append(st_lab)
                seg_ids.append(f"status:{st_lab}")

    def _apply_segment_id(df: pd.DataFrame, seg: str) -> pd.DataFrame:
        if seg == "all":
            return df
        if seg == "male" and gender_col:
            return df[df["_cb_gender"] == "Male"]
        if seg == "female" and gender_col:
            return df[df["_cb_gender"] == "Female"]
        if seg == "leader" and role_col:
            return df[df["_cb_role_ne"]]
        if seg == "ministry":
            return df[df["_cb_ministry_ne"]]
        if seg.startswith("min:"):
            _min_id = seg.split(":", 1)[1]
            _ne_col = f"_cb_{_min_id}_ne"
            if _ne_col in df.columns:
                return df[df[_ne_col]]
        if seg.startswith("status:") and status_col:
            want = seg.split(":", 1)[1]
            return df[df["_cb_status"] == want]
        return df

    prim = html.escape(str(daily_colors.get("primary", "#00ff00")), quote=True)
    text = html.escape(str(daily_colors.get("text", "#ffffff")), quote=True)
    muted = html.escape(str(daily_colors.get("text_muted", "#999999")), quote=True)
    track = "#262626"
    bar_female = "#7E3FF2"

    # Fragment reruns replace this block's outputs — always emit CSS each run or flex/layout is lost.
    st.markdown(
        f"""
<style>
  .nwst-cb-wrap {{
    font-family: 'Inter', sans-serif;
    color: {text};
    margin: 0.25rem 0 1rem 0;
  }}
  .nwst-cb-h2 {{
    font-weight: 800;
    font-size: 1rem;
    letter-spacing: 0.04em;
    margin: 1.25rem 0 0.65rem 0;
    color: {text};
    display: block;
    clear: both;
  }}
  .nwst-cb-row {{
    display: flex;
    align-items: center;
    gap: 0.65rem;
    margin: 0.4rem 0;
    min-height: 1.65rem;
    width: 100%;
    box-sizing: border-box;
  }}
  .nwst-cb-lbl {{
    flex: 0 0 4.25rem;
    font-size: 0.88rem;
    color: {text};
    white-space: nowrap;
  }}
  .nwst-cb-track {{
    flex: 1 1 auto;
    height: 0.55rem;
    border-radius: 999px;
    background: {track};
    overflow: hidden;
    min-width: 0;
  }}
  .nwst-cb-fill {{
    height: 100%;
    border-radius: 999px;
    min-width: 2px;
    transition: width 0.35s ease;
  }}
  .nwst-cb-pct {{
    flex: 0 0 3.2rem;
    text-align: right;
    font-size: 0.88rem;
    font-weight: 600;
    color: {text};
  }}
  .nwst-cb-note {{
    font-size: 0.82rem;
    color: {muted};
    margin: 0.35rem 0 0 0;
  }}
  .nwst-cb-between {{
    margin: 1.65rem 0 0.95rem 0;
    display: block;
    clear: both;
  }}
  .nwst-cb-between-line {{
    height: 1px;
    background: rgba(255, 255, 255, 0.1);
    margin-bottom: 0;
  }}
  .nwst-cb-h2--after-split {{
    margin-top: 0.35rem !important;
  }}
</style>
""",
        unsafe_allow_html=True,
    )

    # Inline backup: fragment DOM updates can leave class-only rows stacking as plain blocks.
    _cb_row_inline = (
        "display:flex;flex-direction:row;align-items:center;gap:0.65rem;"
        "margin:0.4rem 0;min-height:1.65rem;width:100%;box-sizing:border-box;"
    )
    _cb_lbl_inline = (
        f"flex:0 0 4.25rem;font-size:0.88rem;color:{text};white-space:nowrap;"
    )
    _cb_track_inline = (
        f"flex:1 1 auto;min-width:0;height:0.55rem;border-radius:999px;background:{track};overflow:hidden;"
    )
    _cb_pct_inline = (
        f"flex:0 0 3.2rem;text-align:right;font-size:0.88rem;font-weight:600;color:{text};"
    )

    # Tab-style control (full width); avoids uneven radio + dial layout on mobile.
    _cb_key_scope = re.sub(r"[^a-zA-Z0-9]+", "_", str(filter_scope))[:48] or "all"
    pick = st.segmented_control(
        "Filter",
        seg_labels,
        selection_mode="single",
        default=seg_labels[0],
        key=f"nwst_cb_seg_{_cb_key_scope}",
        label_visibility="collapsed",
        width="stretch",
    )
    if pick is None or pick not in seg_labels:
        pick = seg_labels[0]
    sid = seg_ids[seg_labels.index(pick)]
    filt = _apply_segment_id(work, sid)
    if filt.empty:
        st.info("No members match this filter.")
        return

    st.markdown(
        f'<p class="nwst-cb-h2" style="margin-top:0.15rem;">Age</p>',
        unsafe_allow_html=True,
    )

    if not age_col:
        st.markdown(
            f'<p class="nwst-cb-note">No <b>Age</b> column in CG Combined.</p>',
            unsafe_allow_html=True,
        )
    else:
        bucket_counts = _nwst_age_bucket_series(filt[age_col]).value_counts().to_dict()
        n_age = int(sum(bucket_counts.values()))
        if n_age == 0:
            st.caption("No age values to chart.")
        else:
            ordered = sorted(bucket_counts.keys(), key=_nwst_age_bucket_sort_key)
            parts = ['<div class="nwst-cb-wrap">']
            for lab in ordered:
                cnt = int(bucket_counts[lab])
                pct = 100.0 * cnt / n_age
                label_e = html.escape(str(lab), quote=True)
                w_e = html.escape(f"{pct:.1f}", quote=True)
                parts.append(
                    f'<div class="nwst-cb-row" style="{html.escape(_cb_row_inline, quote=True)}">'
                    f'<span class="nwst-cb-lbl" style="{html.escape(_cb_lbl_inline, quote=True)}">{label_e}</span>'
                    f'<div class="nwst-cb-track" style="{html.escape(_cb_track_inline, quote=True)}">'
                    f'<div class="nwst-cb-fill" style="width:{pct:.2f}%;background:{prim};"></div>'
                    f"</div>"
                    f'<span class="nwst-cb-pct" style="{html.escape(_cb_pct_inline, quote=True)}">{w_e}%</span>'
                    f"</div>"
                )
            parts.append("</div>")
            st.markdown("".join(parts), unsafe_allow_html=True)

    st.markdown(
        f'<div class="nwst-cb-between" role="presentation">'
        f'<div class="nwst-cb-between-line"></div>'
        f"</div>",
        unsafe_allow_html=True,
    )

    n_total = len(filt)

    # ── Gender card content ──────────────────────────────────────────────────
    if not gender_col:
        gender_card_html = '<p class="nwst-swipe-note">No <b>Gender</b> column in CG Combined.</p>'
    else:
        sub_g = filt
        n_m = int((sub_g["_cb_gender"] == "Male").sum())
        n_f = int((sub_g["_cb_gender"] == "Female").sum())
        denom = n_m + n_f
        if denom == 0:
            gender_card_html = '<p class="nwst-swipe-note">No Male/Female values in this group.</p>'
        else:
            pm = 100.0 * n_m / denom
            pf = 100.0 * n_f / denom
            unk = len(sub_g) - n_m - n_f
            unk_note = (
                f'<p class="nwst-swipe-note">{unk} member(s) with other/blank gender excluded.</p>'
                if unk > 0 else ""
            )
            gender_card_html = (
                f'<div class="nwst-cb-row" style="{_cb_row_inline}">'
                f'<span class="nwst-cb-lbl" style="{_cb_lbl_inline}">Male</span>'
                f'<div class="nwst-cb-track" style="{_cb_track_inline}">'
                f'<div class="nwst-cb-fill" style="width:{pm:.2f}%;background:{prim};"></div>'
                f'</div>'
                f'<span class="nwst-cb-pct" style="{_cb_pct_inline}">{pm:.1f}%</span>'
                f'</div>'
                f'<div class="nwst-cb-row" style="{_cb_row_inline}">'
                f'<span class="nwst-cb-lbl" style="{_cb_lbl_inline}">Female</span>'
                f'<div class="nwst-cb-track" style="{_cb_track_inline}">'
                f'<div class="nwst-cb-fill" style="width:{pf:.2f}%;background:{bar_female};"></div>'
                f'</div>'
                f'<span class="nwst-cb-pct" style="{_cb_pct_inline}">{pf:.1f}%</span>'
                f'</div>'
                + unk_note
            )

    # ── Leader card content ──────────────────────────────────────────────────
    n_leaders = int(filt["_cb_role_ne"].sum()) if "_cb_role_ne" in filt.columns else 0
    if not role_col:
        leader_card_html = '<p class="nwst-swipe-note">No <b>Role</b> column in CG Combined.</p>'
    elif n_leaders == 0:
        leader_card_html = '<p class="nwst-swipe-note">No leaders in this group.</p>'
    else:
        leader_pct = 100.0 * n_leaders / n_total if n_total > 0 else 0
        role_counts = (
            filt[filt["_cb_role_ne"]][role_col]
            .astype(str).str.strip()
            .value_counts()
        )
        def _role_sort_key(name):
            import re
            m = re.match(r"^(\d+)", str(name))
            return int(m.group(1)) if m else 9999
        role_counts = role_counts.iloc[
            sorted(range(len(role_counts)), key=lambda i: _role_sort_key(role_counts.index[i]))
        ]
        leader_rows = ""
        for role_val, cnt in role_counts.items():
            rp = 100.0 * cnt / n_total if n_total > 0 else 0
            leader_rows += (
                f'<div class="nwst-cb-row" style="{_cb_row_inline}">'
                f'<span class="nwst-cb-lbl" style="{_cb_lbl_inline}">{html.escape(str(role_val))}</span>'
                f'<div class="nwst-cb-track" style="{_cb_track_inline}">'
                f'<div class="nwst-cb-fill" style="width:{rp:.2f}%;background:{prim};"></div>'
                f'</div>'
                f'<span class="nwst-cb-pct" style="{_cb_pct_inline}">{rp:.1f}%</span>'
                f'</div>'
            )
        leader_card_html = (
            f'<p class="nwst-swipe-summary">{n_leaders} of {n_total} members &nbsp;·&nbsp; {leader_pct:.1f}%</p>'
            + leader_rows
        )

    # ── Ministry card content ────────────────────────────────────────────────
    n_ministry = int(filt["_cb_ministry_ne"].sum()) if "_cb_ministry_ne" in filt.columns else 0
    if n_ministry == 0:
        ministry_card_html = '<p class="nwst-swipe-note">No ministry members in this group.</p>'
    else:
        min_pct = 100.0 * n_ministry / n_total if n_total > 0 else 0
        ministry_rows = ""
        for _min_id in _MINISTRY_ROLE_COLS:
            _ne_col = f"_cb_{_min_id.lower()}_ne"
            if _ne_col not in filt.columns:
                continue
            cnt = int(filt[_ne_col].sum())
            if cnt == 0:
                continue
            rp = 100.0 * cnt / n_total if n_total > 0 else 0
            ministry_rows += (
                f'<div class="nwst-cb-row" style="{_cb_row_inline}">'
                f'<span class="nwst-cb-lbl" style="{_cb_lbl_inline}">{html.escape(_min_id)}</span>'
                f'<div class="nwst-cb-track" style="{_cb_track_inline}">'
                f'<div class="nwst-cb-fill" style="width:{rp:.2f}%;background:{prim};"></div>'
                f'</div>'
                f'<span class="nwst-cb-pct" style="{_cb_pct_inline}">{rp:.1f}%</span>'
                f'</div>'
            )
        ministry_card_html = (
            f'<p class="nwst-swipe-summary">{n_ministry} of {n_total} members &nbsp;·&nbsp; {min_pct:.1f}%</p>'
            + ministry_rows
        )

    # ── Swipable card strip ──────────────────────────────────────────────────
    _sid = _cb_key_scope
    cards_html = (
        f"<style>"
        f".nwst-swipe-track-{_sid}{{"
        f"display:flex;overflow-x:auto;scroll-snap-type:x mandatory;"
        f"-webkit-overflow-scrolling:touch;scrollbar-width:none;gap:0.75rem;"
        f"padding-bottom:0.5rem;"
        f"}}"
        f".nwst-swipe-track-{_sid}::-webkit-scrollbar{{display:none;}}"
        f".nwst-swipe-card-{_sid}{{"
        f"flex:0 0 100%;scroll-snap-align:start;"
        f"background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);"
        f"border-radius:12px;padding:1rem 1.1rem 0.85rem 1.1rem;box-sizing:border-box;"
        f"}}"
        f".nwst-swipe-card-title{{font-weight:800;font-size:0.9rem;letter-spacing:0.04em;"
        f"color:{text};margin:0 0 0.75rem 0;display:block;}}"
        f".nwst-swipe-summary{{font-size:0.82rem;color:{muted};margin:0 0 0.65rem 0;}}"
        f".nwst-swipe-note{{font-size:0.82rem;color:{muted};margin:0.25rem 0;}}"
        f".nwst-swipe-dots-{_sid}{{display:flex;justify-content:center;gap:0.4rem;margin-top:0.55rem;}}"
        f".nwst-swipe-dots-{_sid} .nwst-sdot{{width:6px;height:6px;border-radius:50%;"
        f"background:rgba(255,255,255,0.2);cursor:pointer;transition:background 0.2s;}}"
        f".nwst-swipe-dots-{_sid} .nwst-sdot.active{{background:{prim};}}"
        f"</style>"
        f'<div class="nwst-swipe-track-{_sid}" id="nwst-st-{_sid}">'
        f'  <div class="nwst-swipe-card-{_sid}">'
        f'    <span class="nwst-swipe-card-title">Gender</span>'
        f'    {gender_card_html}'
        f'  </div>'
        f'  <div class="nwst-swipe-card-{_sid}">'
        f'    <span class="nwst-swipe-card-title">Leader</span>'
        f'    {leader_card_html}'
        f'  </div>'
        f'  <div class="nwst-swipe-card-{_sid}">'
        f'    <span class="nwst-swipe-card-title">Ministry</span>'
        f'    {ministry_card_html}'
        f'  </div>'
        f'</div>'
        f'<div class="nwst-swipe-dots-{_sid}" id="nwst-sd-{_sid}">'
        f'  <div class="nwst-sdot active" onclick="nwstSwipe(\'{_sid}\',0)"></div>'
        f'  <div class="nwst-sdot" onclick="nwstSwipe(\'{_sid}\',1)"></div>'
        f'  <div class="nwst-sdot" onclick="nwstSwipe(\'{_sid}\',2)"></div>'
        f'</div>'
        f"<script>"
        f"(function(){{"
        f"  var t=document.getElementById('nwst-st-{_sid}');"
        f"  var ds=document.querySelectorAll('#nwst-sd-{_sid} .nwst-sdot');"
        f"  if(!t)return;"
        f"  function upd(){{"
        f"    var i=Math.round(t.scrollLeft/t.offsetWidth);"
        f"    ds.forEach(function(d,j){{d.classList.toggle('active',i===j);}});"
        f"  }}"
        f"  t.addEventListener('scroll',upd,{{passive:true}});"
        f"  window.nwstSwipe=window.nwstSwipe||function(s,i){{"
        f"    var el=document.getElementById('nwst-st-'+s);"
        f"    if(el)el.scrollTo({{left:i*el.offsetWidth,behavior:'smooth'}});"
        f"  }};"
        f"}})();"
        f"</script>"
    )
    st.markdown(cards_html, unsafe_allow_html=True)


@st.fragment
def _nwst_cell_breakdown_fragment(display_df, daily_colors, filter_scope: str):
    """Only this block reruns when the quick filter changes (not the whole CG Health page)."""
    _render_cell_breakdown_section(display_df, daily_colors, filter_scope)


_DESIRED_MEMBER_TABLE_COLUMNS = [
    "Name",
    "Role",
    "Hype Role",
    "Frontlines Role",
    "VS Role",
    "Worship Role",
    "Ministry Department",
    "Age",
    "Gender",
    "Birthday",
    "School / Work",
    "Notes",
]


def _norm_header_key(s: str) -> str:
    t = (s or "").lower().strip()
    t = re.sub(r"\s*/\s*", "/", t)
    return re.sub(r"\s+", " ", t).strip()


def _compact_header_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _resolve_member_table_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Map fixed display labels to sheet columns. Returns (actual_columns, display_labels)."""
    cols = list(df.columns)
    by_lower = {c.lower().strip(): c for c in cols}
    by_norm = {}
    by_compact = {}
    for c in cols:
        by_norm.setdefault(_norm_header_key(c), c)
        by_compact.setdefault(_compact_header_key(c), c)

    resolved_actual = []
    resolved_labels = []
    used = set()

    for label in _DESIRED_MEMBER_TABLE_COLUMNS:
        actual = None
        for cand in (
            label.lower().strip(),
            _norm_header_key(label),
            _compact_header_key(label),
        ):
            if not cand:
                continue
            if cand in by_lower:
                actual = by_lower[cand]
                break
            if cand in by_norm:
                actual = by_norm[cand]
                break
            if cand in by_compact:
                actual = by_compact[cand]
                break

        lid = label.lower().strip()
        if actual is None and lid == "name":
            for c in cols:
                if c in used:
                    continue
                cl = c.lower()
                if (("name" in cl) or ("member" in cl)) and "last" not in cl:
                    actual = c
                    break
        if actual is None and lid in ("school / work", "school/work"):
            for c in cols:
                if c in used:
                    continue
                cl = c.lower()
                if "school" in cl and "work" in cl:
                    actual = c
                    break
        if actual is None and lid == "notes":
            for c in cols:
                if c in used:
                    continue
                if "note" in c.lower():
                    actual = c
                    break

        if actual and actual not in used:
            used.add(actual)
            resolved_actual.append(actual)
            resolved_labels.append(label)

    return resolved_actual, resolved_labels


@st.fragment
def _cg_detailed_members_fragment(table_df: pd.DataFrame, tile_statuses: list):
    """Fragment for Detailed Members — name search matches Individual Attendance (CG Health)."""
    if "cg_dm_name_filter" not in st.session_state:
        st.session_state.cg_dm_name_filter = ""

    _name_filter = st.text_input(
        "Search by Name...",
        value=st.session_state.cg_dm_name_filter,
        key="cg_dm_name_input",
        placeholder="Type to filter by name...",
        label_visibility="collapsed",
    )
    st.session_state.cg_dm_name_filter = _name_filter

    _filtered_df = table_df.copy()
    _tile_f = list(tile_statuses)
    name_col = (
        "Name"
        if "Name" in _filtered_df.columns
        else (_filtered_df.columns[0] if len(_filtered_df.columns) else None)
    )
    if name_col is not None and _name_filter.strip():
        _filter_lower = _name_filter.strip().lower()
        _mem_f = _filtered_df[name_col].fillna("").astype(str).str.strip().str.lower()
        mask = _mem_f.str.contains(_filter_lower, regex=False)
        _filtered_df = _filtered_df[mask].reset_index(drop=True)
        _tile_f = [t for t, m in zip(_tile_f, mask) if m]

    if _filtered_df.empty:
        st.info("No members match the current filter.")
        return

    display_detailed_members_interactive(_filtered_df, _tile_f)


def _render_cg_detailed_members_section(df, _daily_colors):
    """Content for CG Health > Detailed Members collapsible (roster ``df``, respects cell filter)."""
    if df is None or df.empty:
        st.info("No members found.")
        return

    actual_cols, display_labels = _resolve_member_table_columns(df)
    if not actual_cols:
        st.warning(
            "Could not match any of the member table columns to your sheet headers. "
            "Expected labels like Name, Age, Gender, Birthday, School / Work, Notes."
        )
        return

    table_df = df[actual_cols].copy()
    table_df.columns = display_labels

    status_col = None
    for col in df.columns:
        if "status" in col.lower():
            status_col = col
            break
    tile_statuses = []
    for _, dr in df.iterrows():
        tile_statuses.append(
            extract_cell_sheet_status_type(dr.get(status_col)) if status_col else None
        )

    st.markdown("#### All Members")
    _cg_detailed_members_fragment(table_df, tile_statuses)

    _miss_set = set(_DESIRED_MEMBER_TABLE_COLUMNS) - set(display_labels)
    if _miss_set:
        st.caption(
            "Columns omitted (not found in sheet): " + ", ".join(sorted(_miss_set, key=str.lower))
        )


@st.cache_resource
def get_redis_client():
    """Initialize Upstash Redis client from Streamlit secrets."""
    try:
        redis_url = st.secrets.get("upstash_redis_url")
        redis_token = st.secrets.get("upstash_redis_token")

        if redis_url and redis_token:
            return Redis(url=redis_url, token=redis_token)
        return None
    except Exception:
        return None

@st.cache_resource
def get_google_sheet_client():
    """Initialize Google Sheets client using Streamlit secrets."""
    try:
        creds_dict = st.secrets["google"]
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Birthday dashboard helpers (duplicated from CHECK IN/attendance_app.py)
# ---------------------------------------------------------------------------

def _valid_month_day(month: int, day: int) -> bool:
    """True if month/day is a valid calendar day (handles leap day against 2004)."""
    for y in (2004, 2005):
        try:
            datetime(y, month, day)
            return True
        except ValueError:
            continue
    return False


def _month_day_from_sheets_serial(n: float) -> tuple[int, int] | None:
    """Google Sheets / Excel-style serial (days after 1899-12-30) → (month, day)."""
    if n != n or n < 200 or n > 800000:  # NaN or implausible
        return None
    base = date(1899, 12, 30)
    try:
        d = base + timedelta(days=int(round(n)))
    except (OverflowError, ValueError):
        return None
    return (d.month, d.day)


def _parse_en_dd_mmm_yyyy(s: str) -> tuple[int, int] | None:
    """
    CG Combined column G style: ``09 Oct 2026`` (DD + space + 3-letter English month + space + YYYY).
    Locale-independent; year is ignored for recurring birthday.
    """
    mon_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    m = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", (s or "").strip())
    if not m:
        return None
    try:
        d = int(m.group(1))
    except ValueError:
        return None
    key = m.group(2).strip().lower()[:3]
    month = mon_map.get(key)
    if month is None or not _valid_month_day(month, d):
        return None
    return (month, d)


def _parse_birthday_month_day(val) -> tuple[int, int] | None:
    """Parse a sheet Birthday value to (month, day). Supports dates, serials, and common text formats."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, date):
        return (val.month, val.day)
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        md = _month_day_from_sheets_serial(float(val))
        if md:
            return md
    if hasattr(val, "month") and hasattr(val, "day") and not isinstance(val, str):
        try:
            return (int(val.month), int(val.day))
        except (TypeError, ValueError):
            pass
    s_raw = str(val).strip()
    if not s_raw:
        return None
    s = re.sub(r"[\u00a0\u202f]", " ", s_raw)
    s = re.sub(r"\s+", " ", s).strip()
    dd_mmm = _parse_en_dd_mmm_yyyy(s)
    if dd_mmm:
        return dd_mmm
    parts = re.split(r"[/.\-]", s)
    nums: list[int] = []
    for p in parts:
        p = p.strip()
        if not p or not p.isdigit():
            if nums:
                return None
            continue
        nums.append(int(p))
    if len(nums) == 2:
        a, b = nums
        if a > 12:
            month, day = b, a
        elif b > 12:
            month, day = a, b
        else:
            month, day = b, a
        if _valid_month_day(month, day):
            return (month, day)
    if len(nums) >= 3:
        a, b, c = nums[0], nums[1], nums[2]
        month, day = 0, 0
        if a > 1000:
            month, day = b, c
        elif c > 1000:
            if a > 12:
                month, day = b, a
            elif b > 12:
                month, day = a, b
            else:
                month, day = b, a
        if month > 0 and _valid_month_day(month, day):
            return (month, day)
    for fmt in (
        "%d/%m/%Y",
        "%d/%m/%y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.month, dt.day)
        except ValueError:
            continue
    try:
        md = _month_day_from_sheets_serial(float(s.replace(",", ".")))
        if md:
            return md
    except ValueError:
        pass
    return None


_BIRTHDAY_HEADER_MARKERS = (
    "birthday",
    "birth day",
    "date of birth",
    "birthdate",
    "dob",
    "b'day",
    "bday",
)


def _find_cg_birthday_column(cg_df: pd.DataFrame) -> str | None:
    for c in cg_df.columns:
        low = str(c).lower().strip()
        if any(m in low for m in _BIRTHDAY_HEADER_MARKERS):
            return c
    return None


def _birthday_md_to_date_in_window(
    month: int, day: int, center: date, delta_days: int
) -> date | None:
    md_to_date: dict[tuple[int, int], date] = {}
    d = center - timedelta(days=delta_days)
    end = center + timedelta(days=delta_days)
    while d <= end:
        md_to_date[(d.month, d.day)] = d
        d += timedelta(days=1)
    return md_to_date.get((month, day))


@st.cache_data(ttl=300, show_spinner=False)
def _cg_combined_df_for_birthdays(_health_sheet_id: str):
    """
    Load CG Combined from Upstash (populated by flush_pending sync), with Sheets fallback.
    @st.cache_data provides 5-min local cache on top of Upstash for same-instance users.
    """
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_BIRTHDAYS_KEY)
            if cached:
                raw = cached.decode() if isinstance(cached, bytes) else cached
                return pd.read_json(raw)
        except Exception:
            pass
    # Fallback: direct Sheets read (first load before any sync, or Upstash miss)
    return load_cg_combined_df(get_google_sheet_client(), _health_sheet_id)


def _group_birthdays_near_date(
    cg: pd.DataFrame,
    bcol: str,
    ncol: str,
    ccol: str | None,
    center: date,
    delta_days: int,
) -> list[tuple[date, list[tuple[str, str]]]]:
    by_date: dict[date, list[tuple[str, str]]] = defaultdict(list)
    for _, row in cg.iterrows():
        md = _parse_birthday_month_day(row.get(bcol))
        if not md:
            continue
        m, d = md
        occ = _birthday_md_to_date_in_window(m, d, center, delta_days)
        if not occ:
            continue
        name = str(row.get(ncol) or "").strip()
        if not name:
            continue
        cell = str(row.get(ccol) or "").strip() if ccol else ""
        if not cell:
            cell = "—"
        by_date[occ].append((name, cell))

    out: list[tuple[date, list[tuple[str, str]]]] = []
    for dt in sorted(by_date.keys()):
        lines = sorted(by_date[dt], key=lambda t: (t[0].lower(), t[1].lower()))
        out.append((dt, lines))
    return out


def _chunk_birthday_days_into_cards(
    grouped: list[tuple[date, list[tuple[str, str]]]],
) -> list[list[tuple[date, list[tuple[str, str]]]]]:
    """Merge up to **two consecutive calendar days** (that have birthdays) into one horizontal card."""
    if not grouped:
        return []
    cards: list[list[tuple[date, list[tuple[str, str]]]]] = []
    i = 0
    n = len(grouped)
    while i < n:
        d0, p0 = grouped[i]
        chunk: list[tuple[date, list[tuple[str, str]]]] = [(d0, p0)]
        if i + 1 < n:
            d1, p1 = grouped[i + 1]
            if d1 == d0 + timedelta(days=1):
                chunk.append((d1, p1))
                i += 2
                cards.append(chunk)
                continue
        i += 1
        cards.append(chunk)
    return cards


def _hex_to_rgb_for_css(h: str) -> tuple[int, int, int]:
    try:
        hx = h.lstrip("#")
        if len(hx) == 6:
            return (int(hx[0:2], 16), int(hx[2:4], 16), int(hx[4:6], 16))
    except ValueError:
        pass
    return (91, 192, 235)


def _contrasting_gradient_rgb_stops(primary_hex: str, light_hex: str) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """
    Complementary-hue accent versus the daily theme (HLS hue + 0.5), keeping a strong + light leg
    so the 135° primary→light→primary card texture matches non-today cards.
    """
    rp, gp, bp = _hex_to_rgb_for_css(primary_hex)
    rl0, gl0, bl0 = _hex_to_rgb_for_css(light_hex)
    r = (rp + rl0) / 510.0
    g = (gp + gl0) / 510.0
    b = (bp + bl0) / 510.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    h2 = (h + 0.5) % 1.0
    if s < 0.08:
        s2 = 0.72
    else:
        s2 = min(1.0, s * 1.08)
    l2 = min(0.7, max(0.36, l))
    r2, g2, b2 = colorsys.hls_to_rgb(h2, l2, s2)
    s3 = max(0.12, s2 * 0.65)
    l3 = min(0.9, l2 + 0.2)
    r3, g3, b3 = colorsys.hls_to_rgb(h2, l3, s3)
    return (
        (int(r2 * 255), int(g2 * 255), int(b2 * 255)),
        (int(r3 * 255), int(g3 * 255), int(b3 * 255)),
    )


def _relative_luminance_srgb(rc: int, gc: int, bc: int) -> float:
    """WCAG relative luminance for sRGB 0–255 channels."""

    def _lin(c: int) -> float:
        x = c / 255.0
        return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4

    R, G, B = _lin(rc), _lin(gc), _lin(bc)
    return 0.2126 * R + 0.7152 * G + 0.0722 * B


def _hex_accent_readable_on_dark_card(ar: int, ag: int, ab: int) -> str:
    """
    Keep saturated hue for "today" / date labels but lift HLS lightness if the accent
    is too dark to read on charcoal + translucent gradient (≈ WCAG-minded).
    """
    lum = _relative_luminance_srgb(ar, ag, ab)
    if lum >= 0.58:
        return f"#{ar:02x}{ag:02x}{ab:02x}"
    r, g, b = ar / 255.0, ag / 255.0, ab / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    target = 0.62
    l = min(0.9, max(l, target - 0.15 * (1.0 - s)))
    if lum < 0.35:
        l = min(0.9, l + 0.18)
    s = max(s, 0.38)
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r2 * 255):02x}{int(g2 * 255):02x}{int(b2 * 255):02x}"


def _card_body_text_hex(theme_text: str) -> str:
    """Prefer theme body colour; fall back to light gray if theme text would be too dark on cards."""
    t = (theme_text or "").strip()
    if t.startswith("#") and len(t) >= 7:
        tr, tg, tb = _hex_to_rgb_for_css(t[:7])
        if _relative_luminance_srgb(tr, tg, tb) < 0.48:
            return "#e8eaed"
        return t[:7]
    return "#e8eaed"


def birthdays_notice_payload(
    health_sheet_id: str, center_myt_iso: str, delta_days: int = 5
) -> tuple[str, list[tuple[date, list[tuple[str, str]]]], str | None]:
    """
    (status, grouped, user_hint).

    status: ``ok`` | ``empty_window`` | ``load_failed`` | ``empty_sheet`` | ``no_birthday_col``
    | ``no_name_col`` | ``no_sid``
    """
    sid = (health_sheet_id or "").strip()
    if not sid:
        return "no_sid", [], "Configure **NWST_HEALTH_SHEET_ID** (or rely on the built-in default) for the Health workbook."
    cg = _cg_combined_df_for_birthdays(sid)
    if cg is None:
        return "load_failed", [], (
            "Could not read **CG Combined** from the NWST Health spreadsheet. "
            "Share that workbook with the **same Google service account** as Check In, then refresh."
        )
    if cg.empty:
        return "empty_sheet", [], "CG Combined is empty—NWST Health has no roster rows to read."
    bcol = _find_cg_birthday_column(cg)
    ncol, ccol = _resolve_cg_name_cell_columns(cg)
    if not bcol:
        return "no_birthday_col", [], "Add a column whose header mentions Birthday, DOB, or Date of Birth."
    if not ncol:
        return "no_name_col", [], "CG Combined needs a Name column so birthdays can be listed."
    try:
        center = datetime.strptime(center_myt_iso, "%Y-%m-%d").date()
    except ValueError:
        return "empty_window", [], None

    grouped = _group_birthdays_near_date(cg, bcol, ncol, ccol, center, delta_days)
    if not grouped:
        return "empty_window", [], None
    return "ok", grouped, None


def render_birthdays_notice_board(page_colors: dict) -> None:
    """Notice-board block: under banner, above instruction pill; uses CG Combined + NWST_HEALTH_SHEET_ID."""
    sid = (NWST_HEALTH_SHEET_ID or "").strip()
    today_s = get_today_myt_date()
    status, grouped, hint = birthdays_notice_payload(sid, today_s, delta_days=5)

    if status in ("no_sid", "load_failed", "empty_sheet", "no_birthday_col", "no_name_col"):
        if hint:
            st.info(hint)
        return

    prim = page_colors.get("primary", "#5BC0EB")
    bg = page_colors.get("background", "#0b1020")
    text_main = page_colors.get("text", "#e8eaed")

    def _fmt_day(d: date) -> str:
        return f"{d.strftime('%a')}, {d.day} {d.strftime('%b')}"

    prim_e = html.escape(prim, quote=True)
    body_hex = _card_body_text_hex(text_main)
    text_e = html.escape(body_hex, quote=True)
    light = page_colors.get("light", prim)
    r, g, b = _hex_to_rgb_for_css(prim)
    rl, gl, bl = _hex_to_rgb_for_css(light)
    try:
        today_d = datetime.strptime(today_s, "%Y-%m-%d").date()
    except ValueError:
        today_d = date.today()

    (crx, cgy, cbz), (crlx, cgly, cblz) = _contrasting_gradient_rgb_stops(prim, light)
    cards_html: list[str] = []

    if grouped:
        for chunk in _chunk_birthday_days_into_cards(grouped):
            days_only = [d for d, _ in chunk]
            if len(chunk) == 1:
                card_title = html.escape(_fmt_day(days_only[0]), quote=True)
            else:
                t = f"{_fmt_day(days_only[0])} – {_fmt_day(days_only[1])}"
                card_title = html.escape(t, quote=True)

            has_today = any(dt == today_d for dt, _ in chunk)
            ar, ag, ab = (crx, cgy, cbz) if has_today else (r, g, b)
            arl, agl, abl = (crlx, cgly, cblz) if has_today else (rl, gl, bl)
            card_bg_layers = (
                f"linear-gradient(135deg, rgba({ar},{ag},{ab},0.48) 0%, rgba({arl},{agl},{abl},0.32) 50%, rgba({ar},{ag},{ab},0.42) 100%), "
                f"linear-gradient(180deg, #26262a 0%, #18181c 100%)"
            )

            txt_sh = "0 1px 3px rgba(0,0,0,0.75),0 0 1px rgba(0,0,0,0.55)"
            sub_today_e = html.escape(_hex_accent_readable_on_dark_card(ar, ag, ab), quote=True)
            sub_other_e = html.escape(_hex_accent_readable_on_dark_card(r, g, b), quote=True)

            body_parts: list[str] = []
            for dt, pairs in chunk:
                if len(chunk) > 1:
                    sub_l = html.escape(_fmt_day(dt), quote=True)
                    sub_col_e = sub_today_e if dt == today_d else sub_other_e
                    body_parts.append(
                        f'<div style="margin-top:0.55rem;font-family:Inter,sans-serif;font-size:0.72rem;'
                        f"font-weight:600;color:{sub_col_e};letter-spacing:0.02em;text-shadow:{txt_sh};\">{sub_l}</div>"
                    )
                for name, cell in pairs:
                    line = html.escape(f"{name} - {cell}", quote=True)
                    body_parts.append(
                        f'<div style="margin-top:0.35rem;font-family:Inter,sans-serif;font-size:0.8rem;'
                        f"line-height:1.35;color:{text_e};text-shadow:{txt_sh};\">{line}</div>"
                    )

            n_b = sum(len(p) for _, p in chunk)
            foot_n = html.escape(str(n_b), quote=True)
            foot_label = html.escape("birthday" if n_b == 1 else "birthdays", quote=True)
            cards_html.append(
                f'<div class="nwst-bday-card" style="'
                f"flex:0 0 auto;width:min(300px,85vw);scroll-snap-align:start;"
                f"background:{card_bg_layers};border-radius:18px;padding:14px 14px 12px 14px;"
                f"border:1px solid rgba({ar},{ag},{ab},0.42);"
                f"box-shadow:0 8px 24px rgba(0,0,0,0.4),0 4px 18px rgba({ar},{ag},{ab},0.16);"
                f'">'
                f'<div style="font-family:Inter,sans-serif;font-weight:700;font-size:0.95rem;'
                f"color:#f5f5f7;line-height:1.25;text-shadow:{txt_sh};\">{card_title}</div>"
                f"{''.join(body_parts)}"
                f'<div style="margin-top:12px;font-family:Inter,sans-serif;font-size:0.74rem;'
                f"color:rgba(220,220,225,0.95);text-shadow:{txt_sh};\">🎂 {foot_n} {foot_label}</div>"
                f"</div>"
            )
    else:
        empty_txt = html.escape(
            "No birthdays in this ±5 day window (MYT), or Birthday cells are empty / not recognised.",
            quote=True,
        )
        cards_html.append(
            f'<div style="flex:1 1 auto;min-width:min(300px,100%);font-family:Inter,sans-serif;'
            f"font-size:0.82rem;color:{text_e};padding:12px 4px;\">{empty_txt}</div>"
        )

    scroll_row = "".join(cards_html)
    title = html.escape("Birthdays this week", quote=True)
    board = f"""
<div class="nwst-birthday-board" style="
    margin-bottom:2.5rem;
    padding:0.85rem 1rem 1rem 1rem;
    border-radius:12px;
    border:none;
    background:
        linear-gradient(180deg, rgba(139,90,43,0.15) 0%, rgba(24,24,26,0.92) 100%),
        rgba(0,0,0,0.5);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 4px 14px rgba(0,0,0,0.35);
">
  <div style="margin-bottom:0.75rem;">
    <span style="font-family:'Inter',sans-serif;font-weight:800;font-size:0.72rem;
                 letter-spacing:0.12em;text-transform:uppercase;color:{bg};
                 background:{prim};padding:0.25rem 0.55rem;">📌 {title}</span>
  </div>
  <div class="nwst-bday-scroll" style="
      display:flex;
      flex-direction:row;
      gap:12px;
      overflow-x:auto;
      overflow-y:hidden;
      padding:4px 2px 12px 2px;
      scroll-snap-type:x mandatory;
      -webkit-overflow-scrolling:touch;
      scrollbar-color:rgba({r},{g},{b},0.5) transparent;
  ">
    {scroll_row}
  </div>
</div>
"""
    st.markdown(board, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# End birthday dashboard helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_sheet_data():
    """Load data from Google Sheet 'CG Combined' tab or from Redis cache."""
    # Try Redis first
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_cg_combined_data")
            if cached_data:
                data = json.loads(cached_data)
                df = pd.DataFrame(data["rows"], columns=data["columns"])
                return df
        except Exception:
            pass

    # Fall back to Google Sheets
    client = get_google_sheet_client()
    if not client:
        return pd.DataFrame()

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        worksheet = spreadsheet.worksheet("CG Combined")
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        # First row is headers
        df = pd.DataFrame(data[1:], columns=data[0])

        # Cache in Redis
        redis = get_redis_client()
        if redis:
            try:
                cache_data = {
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist()
                }
                redis.set("nwst_cg_combined_data", json.dumps(cache_data), ex=300)
            except Exception:
                pass

        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_ministries_sheet_data():
    """Load data from Google Sheet 'Ministries Combined' tab or from Redis cache."""
    # Try Redis first
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_ministries_combined_data")
            if cached_data:
                data = json.loads(cached_data)
                df = pd.DataFrame(data["rows"], columns=data["columns"])
                return df
        except Exception:
            pass

    # Fall back to Google Sheets
    client = get_google_sheet_client()
    if not client:
        return pd.DataFrame()

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        worksheet = spreadsheet.worksheet("Ministries Combined")
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        # First row is headers
        df = pd.DataFrame(data[1:], columns=data[0])

        # Cache in Redis
        redis = get_redis_client()
        if redis:
            try:
                cache_data = {
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist()
                }
                redis.set("nwst_ministries_combined_data", json.dumps(cache_data), ex=300)
            except Exception:
                pass

        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_newcomers_data():
    """Load newcomers data from Google Sheet."""
    df = load_sheet_data()
    if df.empty:
        return pd.DataFrame()

    newcomers_df = df.copy()
    return newcomers_df

@st.cache_data(ttl=300)
def get_ministries_data():
    """Load ministries data from Google Sheet."""
    df = load_ministries_sheet_data()
    if df.empty:
        return pd.DataFrame()

    ministries_df = df.copy()
    return ministries_df

@st.cache_data(ttl=300)
def load_attendance_and_cg_dataframes():
    """Load Attendance + CG Combined sheets as DataFrames. Returns (att_df, cg_df) or (None, None)."""
    client = get_google_sheet_client()
    if not client:
        return None, None

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        att_worksheet = spreadsheet.worksheet("Attendance")
        att_data = att_worksheet.get_all_values()
        cg_worksheet = spreadsheet.worksheet("CG Combined")
        cg_data = cg_worksheet.get_all_values()

        if not att_data or len(att_data) < 2:
            return None, None
        if not cg_data or len(cg_data) < 2:
            return None, None

        att_df = pd.DataFrame(att_data[1:], columns=att_data[0])
        cg_df = pd.DataFrame(cg_data[1:], columns=cg_data[0])
        return att_df, cg_df
    except Exception:
        return None, None


@st.cache_data(ttl=300)
def load_status_historical_dataframe():
    """Load **Status Historical** tab (monthly Regular/Irregular/Follow Up per member)."""
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        ws = spreadsheet.worksheet(NWST_STATUS_HISTORICAL_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except WorksheetNotFound:
        return None
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_historical_cell_status_dataframe():
    """Load **Historical Cell Status** tab (per-cell snapshots + WoW columns from NWST script)."""
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        ws = spreadsheet.worksheet(NWST_HISTORICAL_CELL_STATUS_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except WorksheetNotFound:
        return None
    except Exception:
        return None


def _nwst_hist_cell_col_lookup(df):
    """Map normalized header → actual column name (strip/lower)."""
    return {str(c).strip().lower(): c for c in df.columns}


def _nwst_hist_cell_get_col(lk, *names):
    for n in names:
        k = n.strip().lower()
        if k in lk:
            return lk[k]
    return None


def _nwst_hist_cell_parse_snapshot_dates(df):
    lk = _nwst_hist_cell_col_lookup(df)
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return None
    s = pd.to_datetime(df[snap_c], errors="coerce")
    dates = sorted(s.dropna().unique(), reverse=True)
    return [pd.Timestamp(d).date() if hasattr(d, "date") else d for d in dates]


def _nwst_hist_cell_rows_for_scope(df, cell_filter):
    """Filter log rows: All = every row; else match Cell (long) or Tab (short) fallback."""
    if df is None or df.empty:
        return df
    lk = _nwst_hist_cell_col_lookup(df)
    cell_c = _nwst_hist_cell_get_col(lk, "cell")
    tab_c = _nwst_hist_cell_get_col(lk, "tab")
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return df.iloc[0:0]

    out = df.copy()
    out["_snap_parsed"] = pd.to_datetime(out[snap_c], errors="coerce")
    out = out[out["_snap_parsed"].notna()]

    if cell_filter and str(cell_filter).strip() and str(cell_filter).strip().lower() != "all":
        cf = str(cell_filter).strip()
        mask = pd.Series(False, index=out.index)
        if cell_c:
            mask = mask | (out[cell_c].astype(str).str.strip() == cf)
        short = _NWST_CELL_DISPLAY_TO_TAB.get(cf)
        if short and tab_c:
            mask = mask | (out[tab_c].astype(str).str.strip() == short)
        out = out[mask]
    return out


def _nwst_hist_cell_aggregate_counts(sub_df):
    """Sum numeric bucket columns for snapshot rows in `sub_df` (already scoped + same snapshot)."""
    if sub_df is None or sub_df.empty:
        return None
    lk = _nwst_hist_cell_col_lookup(sub_df)
    buckets = [
        ("total", ["total"]),
        ("new", ["new"]),
        ("regular", ["regular"]),
        ("irregular", ["irregular"]),
        ("follow up", ["follow up", "follow_up", "followup"]),
        ("red", ["red"]),
        ("graduated", ["graduated"]),
        ("duplicate", ["duplicate"]),
        ("other", ["other"]),
    ]
    agg = {}
    for canon, aliases in buckets:
        coln = None
        for a in aliases:
            coln = _nwst_hist_cell_get_col(lk, a)
            if coln:
                break
        if not coln:
            agg[canon] = 0
        else:
            agg[canon] = int(pd.to_numeric(sub_df[coln], errors="coerce").fillna(0).sum())
    return agg


def _nwst_hist_cell_wow_for_scope(hist_df, cell_filter):
    """Return (curr_agg, prev_agg, snap_curr, snap_prev) from latest two snapshot dates; may be partial."""
    if hist_df is None or hist_df.empty:
        return None, None, None, None
    scoped = _nwst_hist_cell_rows_for_scope(hist_df, cell_filter)
    if scoped is None or scoped.empty:
        return None, None, None, None
    lk = _nwst_hist_cell_col_lookup(scoped)
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return None, None, None, None

    dates = _nwst_hist_cell_parse_snapshot_dates(scoped)
    if not dates:
        return None, None, None, None

    snap_curr = dates[0]
    snap_prev = dates[1] if len(dates) > 1 else None

    def _norm_snap(val):
        t = pd.to_datetime(val, errors="coerce")
        if pd.isna(t):
            return None
        return t.date()

    scoped = scoped.copy()
    scoped["_d"] = scoped[snap_c].map(_norm_snap)
    curr_sub = scoped[scoped["_d"] == snap_curr]
    curr = _nwst_hist_cell_aggregate_counts(curr_sub)
    prev = None
    if snap_prev is not None:
        prev_sub = scoped[scoped["_d"] == snap_prev]
        prev = _nwst_hist_cell_aggregate_counts(prev_sub)
    return curr, prev, snap_curr, snap_prev


def _nwst_cell_health_wow_color_for_delta(bucket_key, delta_n):
    """Regular, graduated: more members = good (green). New: any non‑zero change = good (green); 0 = grey.
    Risk-style buckets: fewer = good (green). Graduated is always grey."""
    if delta_n is None or (isinstance(delta_n, float) and pd.isna(delta_n)):
        return "#aaaaaa"
    if bucket_key == "graduated":
        return "#aaaaaa"
    if bucket_key == "new":
        return "#2ecc71" if delta_n != 0 else "#aaaaaa"
    if delta_n == 0:
        return "#aaaaaa"
    if bucket_key == "regular":
        return "#2ecc71" if delta_n > 0 else "#e74c3c"
    return "#2ecc71" if delta_n < 0 else "#e74c3c"


def _nwst_cell_health_wow_pill_html(bucket_key, curr_agg, prev_agg):
    """WoW delta pill HTML (arrow + member delta + pp delta) for one cell-health bucket."""

    def _agg_n(agg, key):
        if not agg:
            return 0
        if key == "follow_up":
            return int(agg.get("follow up", 0) or 0)
        return int(agg.get(key, 0) or 0)

    d_mem = None
    d_pp = None
    if curr_agg and prev_agg:
        c = _agg_n(curr_agg, bucket_key)
        p = _agg_n(prev_agg, bucket_key)
        d_mem = c - p
        tot_c = _agg_n(curr_agg, "total")
        tot_p = _agg_n(prev_agg, "total")
        if tot_p > 0 and tot_c > 0:
            d_pp = (100.0 * c / tot_c) - (100.0 * p / tot_p)

    if curr_agg and prev_agg and d_mem is not None and d_pp is not None:
        pp_sh = float(d_pp)
        pp_str = f"{pp_sh:+.1f}%"
        mem_str = f"{d_mem:+d}"
        bubble_txt = html.escape(f"{mem_str} ({pp_str})", quote=True)
        flat = d_mem == 0 and abs(pp_sh) < 0.05
        if bucket_key == "new":
            if d_mem == 0:
                arrow = "·"
                pill_cls = "ch-pill-flat"
            else:
                arrow = "↑" if d_mem > 0 else "↓"
                pill_cls = "ch-pill-good"
        elif flat:
            arrow = "·"
            pill_cls = "ch-pill-flat"
        elif d_mem == 0:
            arrow = "·"
            tone = _nwst_cell_health_wow_color_for_delta(bucket_key, pp_sh)
            if tone == "#2ecc71":
                pill_cls = "ch-pill-good"
            elif tone == "#e74c3c":
                pill_cls = "ch-pill-bad"
            else:
                pill_cls = "ch-pill-flat"
        else:
            arrow = "↑" if d_mem > 0 else "↓"
            tone = _nwst_cell_health_wow_color_for_delta(bucket_key, d_mem)
            if tone == "#2ecc71":
                pill_cls = "ch-pill-good"
            elif tone == "#e74c3c":
                pill_cls = "ch-pill-bad"
            else:
                pill_cls = "ch-pill-flat"
        return (
            f'<div class="ch-pill-wrap"><span class="ch-pill ch-pill--hero {pill_cls}">'
            f'<span class="ch-pill-arrow">{html.escape(arrow, quote=True)}</span>'
            f"<span>{bubble_txt}</span>"
            f"</span></div>"
        )

    return (
        '<div class="ch-pill-wrap"><span class="ch-pill ch-pill--hero ch-pill-na">'
        "Need 2 log snapshots</span></div>"
    )


def calculate_and_cache_cell_health(redis_client, cg_df, hist_df, cell_to_zone_map):
    """
    Calculate cell health data from CG Combined + Historical Cell Status and cache in Redis.

    This is the single source of truth for cell health metrics, used by both:
    - KPI cards in app.py
    - PDF reports in nwst_cell_health_report.py

    Args:
        redis_client: Upstash Redis client
        cg_df: DataFrame from CG Combined tab
        hist_df: DataFrame from Historical Cell Status tab (may be None)
        cell_to_zone_map: dict mapping cell name (lowercase) -> zone string
    """
    from datetime import date

    if cg_df is None or cg_df.empty:
        return

    # Find status column in CG Combined
    status_columns = [col for col in cg_df.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None

    # Find cell column in CG Combined
    cg_cell_col = None
    for col in cg_df.columns:
        if col.lower().strip() in ("cell", "group"):
            cg_cell_col = col
            break

    if not cg_cell_col:
        return

    work_df = cg_df.copy()
    if status_col:
        work_df["_status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)

    # Get WoW deltas from Historical Cell Status
    wow_by_cell = {}  # cell_name -> (delta_new, delta_regular, delta_irregular, delta_follow_up)
    if hist_df is not None and not hist_df.empty:
        # Get all unique cells from hist_df
        lk = _nwst_hist_cell_col_lookup(hist_df)
        cell_c = _nwst_hist_cell_get_col(lk, "cell")
        if cell_c:
            unique_cells = hist_df[cell_c].dropna().unique()
            for cell_name in unique_cells:
                cell_s = str(cell_name).strip()
                if not cell_s or cell_s.lower() in ("all", "archive"):
                    continue
                curr_agg, prev_agg, _, _ = _nwst_hist_cell_wow_for_scope(hist_df, cell_s)
                if curr_agg and prev_agg:
                    d_new = int(curr_agg.get("new", 0)) - int(prev_agg.get("new", 0))
                    d_reg = int(curr_agg.get("regular", 0)) - int(prev_agg.get("regular", 0))
                    d_irr = int(curr_agg.get("irregular", 0)) - int(prev_agg.get("irregular", 0))
                    d_fu = int(curr_agg.get("follow up", 0) or curr_agg.get("follow_up", 0) or 0) - \
                           int(prev_agg.get("follow up", 0) or prev_agg.get("follow_up", 0) or 0)
                    wow_by_cell[cell_s.lower()] = (d_new, d_reg, d_irr, d_fu)

        # Get "All" WoW deltas
        curr_all, prev_all, _, _ = _nwst_hist_cell_wow_for_scope(hist_df, "All")
        if curr_all and prev_all:
            d_new = int(curr_all.get("new", 0)) - int(prev_all.get("new", 0))
            d_reg = int(curr_all.get("regular", 0)) - int(prev_all.get("regular", 0))
            d_irr = int(curr_all.get("irregular", 0)) - int(prev_all.get("irregular", 0))
            d_fu = int(curr_all.get("follow up", 0) or curr_all.get("follow_up", 0) or 0) - \
                   int(prev_all.get("follow up", 0) or prev_all.get("follow_up", 0) or 0)
            wow_by_cell["all"] = (d_new, d_reg, d_irr, d_fu)

    # Calculate "All" counts directly from the full work_df so members with blank/archive
    # cell assignments (common for Red and Graduated) are not missed.
    if status_col:
        all_counts = {
            "new": len(work_df[work_df["_status_type"] == "New"]),
            "regular": len(work_df[work_df["_status_type"] == "Regular"]),
            "irregular": len(work_df[work_df["_status_type"] == "Irregular"]),
            "follow_up": len(work_df[work_df["_status_type"] == "Follow Up"]),
            "red": len(work_df[work_df["_status_type"] == "Red"]),
            "graduated": len(work_df[work_df["_status_type"] == "Graduated"]),
        }
    else:
        all_counts = {"new": 0, "regular": 0, "irregular": 0, "follow_up": 0, "red": 0, "graduated": 0}

    # Calculate counts per cell from CG Combined (live data)
    cell_rows = []

    for cell_name, group in work_df.groupby(cg_cell_col):
        cell_s = str(cell_name).strip()
        if not cell_s or cell_s.lower() in ("all", "archive"):
            continue

        if status_col:
            new_c = len(group[group["_status_type"] == "New"])
            reg_c = len(group[group["_status_type"] == "Regular"])
            irr_c = len(group[group["_status_type"] == "Irregular"])
            fu_c = len(group[group["_status_type"] == "Follow Up"])
            red_c = len(group[group["_status_type"] == "Red"])
            grad_c = len(group[group["_status_type"] == "Graduated"])
        else:
            # Fallback: estimate distribution
            n = len(group)
            new_c = max(1, int(n * 0.20))
            reg_c = max(1, int(n * 0.40))
            irr_c = max(1, int(n * 0.20))
            fu_c = max(1, int(n * 0.10))
            red_c = max(1, int(n * 0.05))
            grad_c = max(0, n - new_c - reg_c - irr_c - fu_c - red_c)
            # Accumulate fallback estimates for "All" row (status_col missing case)
            all_counts["new"] += new_c
            all_counts["regular"] += reg_c
            all_counts["irregular"] += irr_c
            all_counts["follow_up"] += fu_c
            all_counts["red"] += red_c
            all_counts["graduated"] += grad_c

        # Get WoW deltas for this cell
        deltas = wow_by_cell.get(cell_s.lower(), (0, 0, 0, 0))

        # Get zone for this cell
        zone = cell_to_zone_map.get(cell_s.lower(), "")

        cell_rows.append(build_cell_health_row(
            cell_name=cell_s,
            zone=zone,
            new_count=new_c,
            regular_count=reg_c,
            irregular_count=irr_c,
            follow_up_count=fu_c,
            red_count=red_c,
            graduated_count=grad_c,
            delta_new=deltas[0],
            delta_regular=deltas[1],
            delta_irregular=deltas[2],
            delta_follow_up=deltas[3],
        ))

    # Build "All" row with WoW deltas from Historical Cell Status
    all_deltas = wow_by_cell.get("all", (0, 0, 0, 0))
    all_row = build_cell_health_row(
        cell_name="All",
        zone="PSQ",
        new_count=all_counts["new"],
        regular_count=all_counts["regular"],
        irregular_count=all_counts["irregular"],
        follow_up_count=all_counts["follow_up"],
        red_count=all_counts["red"],
        graduated_count=all_counts["graduated"],
        delta_new=all_deltas[0],
        delta_regular=all_deltas[1],
        delta_irregular=all_deltas[2],
        delta_follow_up=all_deltas[3],
    )

    # Build the cache payload
    cell_health_data = {
        "snapshot_date": date.today().isoformat(),
        "all_row": all_row,
        "cell_rows": cell_rows,
        "source": "CG Combined + Historical Cell Status",
    }

    # Store in Redis
    store_cell_health_in_redis(redis_client, cell_health_data)


NWST_HISTORICAL_MINISTRY_STATUS_TAB = "Historical Ministry Status"
REDIS_MINISTRY_HEALTH_KEY = "nwst_ministry_health_data"

# The four ministry tab names written by the Apps Script (MINISTRY TABS.txt).
_MINISTRY_TAB_NAMES = ["Hype", "Frontlines", "VS", "Worship"]


@st.cache_data(ttl=300)
def load_historical_ministry_status_dataframe():
    """Load **Historical Ministry Status** tab (per-ministry snapshots + WoW).

    Same column structure as Historical Cell Status; column C ("Cell") stores the ministry
    name so all shared WoW helpers work without modification.
    """
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        ws = spreadsheet.worksheet(NWST_HISTORICAL_MINISTRY_STATUS_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except WorksheetNotFound:
        return None
    except Exception:
        return None


def calculate_and_cache_ministry_health(redis_client, ministries_df, ministry_hist_df):
    """Calculate ministry health WoW from Ministries Combined + Historical Ministry Status,
    store under ``REDIS_MINISTRY_HEALTH_KEY`` using the same payload shape as cell health.

    One row per ministry (Hype / Frontlines / VS / Worship) plus an "All" aggregate.
    """
    from datetime import date as _date

    if ministries_df is None or ministries_df.empty:
        return

    # Find status column
    status_columns = [col for col in ministries_df.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None

    # Find ministry column
    ministry_col = None
    for col in ministries_df.columns:
        if "ministry" in col.lower() or "department" in col.lower():
            ministry_col = col
            break

    if not ministry_col:
        return

    work_df = ministries_df.copy()
    if status_col:
        work_df["_status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)

    # WoW deltas from Historical Ministry Status (same helpers; col C stores ministry name)
    wow_by_ministry: dict = {}
    if ministry_hist_df is not None and not ministry_hist_df.empty:
        lk = _nwst_hist_cell_col_lookup(ministry_hist_df)
        cell_c = _nwst_hist_cell_get_col(lk, "cell")
        if cell_c:
            for ministry_name in ministry_hist_df[cell_c].dropna().unique():
                ms = str(ministry_name).strip()
                if not ms or ms.lower() == "all":
                    continue
                curr_a, prev_a, _, _ = _nwst_hist_cell_wow_for_scope(ministry_hist_df, ms)
                if curr_a and prev_a:
                    wow_by_ministry[ms.lower()] = (
                        int(curr_a.get("new", 0)) - int(prev_a.get("new", 0)),
                        int(curr_a.get("regular", 0)) - int(prev_a.get("regular", 0)),
                        int(curr_a.get("irregular", 0)) - int(prev_a.get("irregular", 0)),
                        int(curr_a.get("follow up", 0) or curr_a.get("follow_up", 0) or 0)
                        - int(prev_a.get("follow up", 0) or prev_a.get("follow_up", 0) or 0),
                    )
        # "All" aggregate WoW
        curr_all, prev_all, _, _ = _nwst_hist_cell_wow_for_scope(ministry_hist_df, "All")
        if curr_all and prev_all:
            wow_by_ministry["all"] = (
                int(curr_all.get("new", 0)) - int(prev_all.get("new", 0)),
                int(curr_all.get("regular", 0)) - int(prev_all.get("regular", 0)),
                int(curr_all.get("irregular", 0)) - int(prev_all.get("irregular", 0)),
                int(curr_all.get("follow up", 0) or curr_all.get("follow_up", 0) or 0)
                - int(prev_all.get("follow up", 0) or prev_all.get("follow_up", 0) or 0),
            )

    # Per-ministry counts — group by base ministry name (part before the first colon)
    ministry_rows = []
    all_counts: dict = {"new": 0, "regular": 0, "irregular": 0, "follow_up": 0, "red": 0, "graduated": 0}

    # Derive base ministry label for each row
    work_df["_base_ministry"] = (
        work_df[ministry_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.split(":", n=1)
        .str[0]
        .str.strip()
    )

    for ministry_name, group in work_df.groupby("_base_ministry"):
        ms = str(ministry_name).strip()
        if not ms:
            continue

        if status_col:
            new_c = len(group[group["_status_type"] == "New"])
            reg_c = len(group[group["_status_type"] == "Regular"])
            irr_c = len(group[group["_status_type"] == "Irregular"])
            fu_c = len(group[group["_status_type"] == "Follow Up"])
            red_c = len(group[group["_status_type"] == "Red"])
            grad_c = len(group[group["_status_type"] == "Graduated"])
        else:
            n = len(group)
            new_c = max(1, int(n * 0.20))
            reg_c = max(1, int(n * 0.40))
            irr_c = max(1, int(n * 0.20))
            fu_c = max(1, int(n * 0.10))
            red_c = max(1, int(n * 0.05))
            grad_c = max(0, n - new_c - reg_c - irr_c - fu_c - red_c)

        all_counts["new"] += new_c
        all_counts["regular"] += reg_c
        all_counts["irregular"] += irr_c
        all_counts["follow_up"] += fu_c
        all_counts["red"] += red_c
        all_counts["graduated"] += grad_c

        deltas = wow_by_ministry.get(ms.lower(), (0, 0, 0, 0))
        ministry_rows.append(
            build_cell_health_row(
                cell_name=ms,
                zone="",
                new_count=new_c,
                regular_count=reg_c,
                irregular_count=irr_c,
                follow_up_count=fu_c,
                red_count=red_c,
                graduated_count=grad_c,
                delta_new=deltas[0],
                delta_regular=deltas[1],
                delta_irregular=deltas[2],
                delta_follow_up=deltas[3],
            )
        )

    all_deltas = wow_by_ministry.get("all", (0, 0, 0, 0))
    all_row = build_cell_health_row(
        cell_name="All",
        zone="",
        new_count=all_counts["new"],
        regular_count=all_counts["regular"],
        irregular_count=all_counts["irregular"],
        follow_up_count=all_counts["follow_up"],
        red_count=all_counts["red"],
        graduated_count=all_counts["graduated"],
        delta_new=all_deltas[0],
        delta_regular=all_deltas[1],
        delta_irregular=all_deltas[2],
        delta_follow_up=all_deltas[3],
    )

    payload = {
        "snapshot_date": _date.today().isoformat(),
        "all_row": all_row,
        "cell_rows": ministry_rows,  # keyed by "cell" field (holds ministry name)
        "source": "Ministries Combined + Historical Ministry Status",
    }

    try:
        redis_client.set(
            REDIS_MINISTRY_HEALTH_KEY,
            json.dumps(payload),
            ex=86400 * 7,
        )
    except Exception:
        pass


def _nwst_normalize_member_name(s):
    """Strip, lowercase, collapse spaces (and NBSP) for matching Attendance ↔ CG Combined."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).replace("\u00a0", " ").strip().lower()
    return " ".join(t.split())


def _nwst_detect_name_cell_columns_for_grid(header_row, sample_row):
    """Return (name_col_idx, sheet_cell_col_idx_or_None). Mirrors wide grids where Name is in B not A."""
    hr = [str(x).strip() if x is not None else "" for x in (header_row or [])]
    sr = []
    if sample_row:
        for i in range(max(len(header_row or []), len(sample_row))):
            v = sample_row[i] if i < len(sample_row) else ""
            sr.append(str(v).strip() if v is not None else "")

    h0s = hr[0].lower() if hr else ""
    h1s = hr[1].lower() if len(hr) > 1 else ""
    h2s = hr[2].lower() if len(hr) > 2 else ""

    # Snapshot layout: A="Date" (combined Name - Cell), B="Name", C="Cell", D+= week columns
    if h0s == "date" and "name" in h1s and ("cell" in h2s or "group" in h2s):
        return 1, 2

    # Prefer explicit "Name" / "Member" header (any column)
    for i, c in enumerate(hr):
        csl = c.lower()
        if "timestamp" in csl:
            continue
        if (
            ("name" in csl or csl in ("member", "full name"))
            and "last" not in csl
            and "leader" not in csl
        ):
            if i < len(header_row) and parse_attendance_column_date(header_row[i]) is None:
                cell_i = None
                for j in range(i + 1, min(len(hr), i + 5)):
                    if parse_attendance_column_date(header_row[j]) is not None:
                        break
                    jl = hr[j].lower()
                    if jl and any(k in jl for k in ("cell", "group", "cg")):
                        cell_i = j
                        break
                return i, cell_i

    h0 = h0s

    if h0 and "timestamp" in h0:
        # A = Timestamp, expect B = Name; C may be Cell before date columns
        name_i = 1
        cell_i = None
        if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
            h2 = hr[2].lower()
            if any(k in h2 for k in ("cell", "group", "cg", "zone")):
                cell_i = 2
        return name_i, cell_i

    h1 = hr[1].lower() if len(hr) > 1 else ""
    samp0 = sr[0] if sr else ""
    samp1 = sr[1] if len(sr) > 1 else ""

    # Empty col A but populated B — classic "Name" in column B
    if not samp0 and samp1:
        if any(x in h1 for x in ("name", "member", "full")) or (len(hr) > 1 and hr[1] and not hr[0]):
            cell_i = None
            if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
                h2 = hr[2].lower()
                if any(k in h2 for k in ("cell", "group", "cg")):
                    cell_i = 2
            return 1, cell_i

    # Header row says "Name" in second column
    if (
        h1
        and any(x in h1 for x in ("name", "member"))
        and h0 not in ("name", "member", "full name")
    ):
        cell_i = None
        if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
            h2 = hr[2].lower()
            if any(k in h2 for k in ("cell", "group", "cg")):
                cell_i = 2
        return 1, cell_i

    return 0, None


def _nwst_sheet_api_transient(api_err):
    """True when retrying the same read may succeed (rate limit / Google blips)."""
    code = getattr(api_err.response, "status_code", None) or getattr(api_err, "code", None)
    return code in {429, 500, 502, 503, 504}


@st.cache_data(ttl=300)
def nwst_get_attendance_grid_for_charts(sheet_id):
    """Load **Attendance** tab — Saturday columns only; cell from sheet or **CG Combined** name lookup."""
    redis = get_redis_client()
    if redis:
        try:
            cached_raw = redis.get(NWST_REDIS_ATTENDANCE_CHART_GRID_KEY)
            if cached_raw:
                payload = json.loads(cached_raw)
                df = pd.DataFrame(payload["rows"], columns=payload["columns"])
                dates = payload.get("saturday_dates_short") or []
                return df, dates, None
        except Exception:
            pass

    client = get_google_sheet_client()
    if not client:
        return None, [], "Could not connect to Google Sheets."

    transient_attempts = 3
    for attempt in range(transient_attempts):
        try:
            spreadsheet = client.open_by_key(sheet_id)
            try:
                att_sheet = spreadsheet.worksheet(NWST_ATTENDANCE_TAB)
            except WorksheetNotFound:
                return None, [], f"Tab '{NWST_ATTENDANCE_TAB}' not found."

            try:
                cg_sheet = spreadsheet.worksheet("CG Combined")
            except WorksheetNotFound:
                return None, [], "Tab 'CG Combined' not found."

            all_values = att_sheet.get_all_values()
            cg_vals = cg_sheet.get_all_values()
            if len(all_values) < 2:
                return None, [], "No data in Attendance."
            if len(cg_vals) < 2:
                return None, [], "No data in CG Combined."

            cg_df = pd.DataFrame(cg_vals[1:], columns=cg_vals[0])
            cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
            if cg_cell_col is None:
                for col in cg_df.columns:
                    cl = str(col).lower().strip()
                    if ("cell" in cl or "group" in cl) and "leader" not in cl:
                        cg_cell_col = col
                        break

            header_row = all_values[0]
            sample_row = all_values[1] if len(all_values) > 1 else []
            name_col_idx, sheet_cell_col_idx = _nwst_detect_name_cell_columns_for_grid(
                header_row, sample_row
            )

            saturday_entries = []
            for col_idx in range(len(header_row)):
                if col_idx in (name_col_idx, sheet_cell_col_idx):
                    continue
                h = header_row[col_idx]
                d = parse_attendance_column_date(h)
                if d is None or d.weekday() != 5:
                    continue
                saturday_entries.append((d, col_idx))

            if not saturday_entries:
                return None, [], (
                    "No Saturday columns found in **Attendance** row 1. "
                    "Headers must be parseable dates (same as Monthly Health)."
                )

            saturday_entries.sort(key=lambda x: x[0])
            saturday_dates_short = [d.strftime("%d %b %Y") for d, _ in saturday_entries]
            col_indices = [idx for _, idx in saturday_entries]

            def _nwst_attendance_present(cell_val):
                if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
                    return False
                s = str(cell_val).strip().lower()
                if s in ("1", "yes", "y", "true", "x"):
                    return True
                try:
                    return int(float(str(cell_val).strip())) == 1
                except (TypeError, ValueError):
                    return False

            def _cell_from_cg(person_name):
                k = _nwst_normalize_member_name(person_name)
                if not k or not cg_name_col or not cg_cell_col:
                    return ""
                cg_match = cg_df[
                    cg_df[cg_name_col]
                    .astype(str)
                    .map(_nwst_normalize_member_name)
                    == k
                ]
                if cg_match.empty:
                    return ""
                return str(cg_match.iloc[0][cg_cell_col]).strip()

            data_rows = []

            for row in all_values[1:]:
                if not row:
                    continue
                if name_col_idx >= len(row):
                    continue
                name = str(row[name_col_idx]).strip() if row[name_col_idx] else ""
                if name.lower() == "name":
                    continue

                cell_group = ""
                if sheet_cell_col_idx is not None and sheet_cell_col_idx < len(row):
                    cell_group = str(row[sheet_cell_col_idx]).strip()

                combined_a = str(row[0]).strip() if row and len(row) > 0 and row[0] else ""
                if (" - " in combined_a) and (not name or not cell_group):
                    left, right = combined_a.split(" - ", 1)
                    if not name:
                        name = left.strip()
                    if not cell_group:
                        cell_group = right.strip()

                if not name:
                    continue

                if not cell_group:
                    cell_group = _cell_from_cg(name)
                if not cell_group:
                    continue

                attendance = {
                    saturday_dates_short[j]: (
                        1
                        if (
                            col_indices[j] < len(row)
                            and _nwst_attendance_present(row[col_indices[j]])
                        )
                        else 0
                    )
                    for j in range(len(col_indices))
                }
                data_rows.append(
                    {
                        "Name": name,
                        "Cell Group": cell_group,
                        "Name - Cell Group": f"{name} - {cell_group}",
                        **attendance,
                    }
                )

            if not data_rows:
                return None, [], (
                    "No attendance rows matched **CG Combined** (or a Cell column on Attendance). "
                    "Check Name is in column A or B, cell on sheet or same spellings as CG Combined."
                )

            df = pd.DataFrame(data_rows)
            df = df.drop_duplicates(subset=["Name - Cell Group"], keep="first")
            redis = get_redis_client()
            if redis:
                try:
                    payload = {
                        "columns": df.columns.tolist(),
                        "rows": df.values.tolist(),
                        "saturday_dates_short": saturday_dates_short,
                    }
                    redis.set(
                        NWST_REDIS_ATTENDANCE_CHART_GRID_KEY,
                        json.dumps(payload, default=str),
                        ex=300,
                    )
                except Exception:
                    pass
            return df, saturday_dates_short, None
        except APIError as e:
            if attempt < transient_attempts - 1 and _nwst_sheet_api_transient(e):
                time.sleep(1.0 * (2**attempt))
                continue
            hint = (
                " (Google Sheets often returns this briefly; wait a minute and use **Sync**, "
                "or check https://www.google.com/appsstatus )"
                if _nwst_sheet_api_transient(e)
                else ""
            )
            return None, [], f"Error loading Attendance for charts: {str(e)}{hint}"
        except Exception as e:
            return None, [], f"Error loading Attendance for charts: {str(e)}"


@st.cache_data(ttl=300)
def nwst_get_cell_zone_mapping(sheet_id):
    """Cell (col A) → zone (col C) from Key Values."""
    client = get_google_sheet_client()
    if not client:
        return {}
    try:
        spreadsheet = client.open_by_key(sheet_id)
        try:
            key_values_sheet = spreadsheet.worksheet(NWST_KEY_VALUES_TAB)
        except WorksheetNotFound:
            return {}
        all_values = key_values_sheet.get_all_values()
        if len(all_values) <= 1:
            return {}
        cell_to_zone = {}
        for row in all_values[1:]:
            if len(row) >= 3:
                cn = row[0].strip()
                zn = row[2].strip()
                if cn and zn:
                    cell_to_zone[cn.lower()] = zn
        return cell_to_zone
    except Exception:
        return {}


def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)."""
    if not name_cell_group_str:
        return None, None
    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return parts[0].strip(), "Unknown"


@st.cache_data(ttl=300)
def nwst_get_options_roster_members(_sheet_id):
    """Cell roster sizes from Options tab column C (same format as CHECK IN attendance app)."""
    client = get_google_sheet_client()
    if not client:
        return {}, "Could not connect to Google Sheets."
    try:
        spreadsheet = client.open_by_key(_sheet_id)
        try:
            options_sheet = spreadsheet.worksheet(NWST_OPTIONS_TAB)
        except WorksheetNotFound:
            return {}, f"Tab '{NWST_OPTIONS_TAB}' not found."
        column_c_values = options_sheet.col_values(3)
        if not column_c_values:
            return {}, "Column C in Options sheet is empty."
        members_per_cell = {}
        for value in column_c_values[1:]:
            value = (value or "").strip()
            if not value:
                continue
            m_name, m_cell = parse_name_cell_group(value)
            if m_name and m_cell:
                members_per_cell.setdefault(m_cell, set()).add(m_name)
        if not members_per_cell:
            return {}, "No roster entries found in column C (from row 2)."
        return {k: len(v) for k, v in members_per_cell.items()}, None
    except Exception as e:
        return {}, str(e)


@st.cache_data(ttl=300)
def nwst_get_attendance_analytics_data(_sheet_id):
    """Fetch Saturday-only attendance matrix from the 'Attendance Analytics' tab (CHECK IN format)."""
    client = get_google_sheet_client()
    if not client:
        return None, [], "Could not connect to Google Sheets."
    try:
        spreadsheet = client.open_by_key(_sheet_id)
        try:
            analytics_sheet = spreadsheet.worksheet(NWST_ATTENDANCE_ANALYTICS_TAB)
        except WorksheetNotFound:
            return None, [], f"Tab '{NWST_ATTENDANCE_ANALYTICS_TAB}' not found in the Google Sheet."

        all_values = analytics_sheet.get_all_values()
        if len(all_values) < 2:
            return None, [], "No data found in the Attendance Analytics sheet."

        header_row = all_values[0]
        dates = []
        saturday_col_indices = []

        for col_idx, cell in enumerate(header_row[3:], start=3):
            if not cell or not str(cell).strip():
                continue
            cell_s = str(cell).strip()
            try:
                date_obj = datetime.strptime(cell_s, "%m/%d/%Y")
                if date_obj.weekday() == 5:
                    dates.append(date_obj)
                    saturday_col_indices.append(col_idx)
            except ValueError:
                try:
                    date_obj = datetime.strptime(cell_s, "%d/%m/%Y")
                    if date_obj.weekday() == 5:
                        dates.append(date_obj)
                        saturday_col_indices.append(col_idx)
                except ValueError:
                    continue

        if not dates:
            return None, [], "No Saturday dates found in the analytics data."

        sorted_pairs = sorted(zip(dates, saturday_col_indices), key=lambda x: x[0])
        dates = [pair[0] for pair in sorted_pairs]
        saturday_col_indices = [pair[1] for pair in sorted_pairs]
        saturday_dates_short = [d.strftime("%b %d") for d in dates]

        data_rows = []
        for row in all_values[1:]:
            if len(row) < 3:
                continue
            name = row[1].strip() if len(row) > 1 and row[1] else ""
            cell_group = row[2].strip() if len(row) > 2 and row[2] else ""
            if not name or name.lower() == "name":
                continue
            attendance = []
            for col_idx in saturday_col_indices:
                if col_idx < len(row):
                    val = row[col_idx].strip()
                    attendance.append(1 if val == "1" else 0)
                else:
                    attendance.append(0)
            data_rows.append({
                "Name": name,
                "Cell Group": cell_group,
                "Name - Cell Group": f"{name} - {cell_group}" if cell_group else name,
                **{saturday_dates_short[i]: attendance[i] for i in range(len(attendance))},
            })

        if not data_rows:
            return None, [], "No attendance records found."

        df = pd.DataFrame(data_rows)
        df = df.drop_duplicates(subset=["Name - Cell Group"], keep="first")
        return df, saturday_dates_short, None
    except Exception as e:
        return None, [], f"Error fetching analytics data: {str(e)}"


def _nwst_resolve_display_name_cell_cols(display_df):
    disp_name_col = None
    disp_cell_col = None
    for col in display_df.columns:
        col_lower = col.lower().strip()
        if col_lower in ["cell", "group"]:
            disp_cell_col = col
        if col_lower in ["name", "member name", "member"] or (
            any(x in col_lower for x in ["name", "member"]) and "last" not in col_lower
        ):
            if disp_name_col is None:
                disp_name_col = col
    if not disp_name_col:
        disp_name_col = display_df.columns[0]
    return disp_name_col, disp_cell_col


def _nwst_zone_for_cell_map(cg, cell_to_zone_map):
    return cell_to_zone_map.get(str(cg).lower(), cg) if cg else "Unknown"


def _nwst_exclude_rate_chart_cell(cg, zone_name):
    if not str(cg).strip():
        return True
    if str(zone_name).strip().lower() == "archive":
        return True
    n = str(cg).strip().lower().lstrip("*").strip()
    if n == "not sure yet" or n.startswith("not sure yet"):
        return True
    return False


def _nwst_weekly_contrasting_line_colors(primary_hex, n_series):
    """Distinct line colors anchored on the hue **opposite** the current accent primary.

    Multiple series step around the wheel (golden‑ratio hue steps) so lines stay separable on dark UI.
    """
    if n_series < 1:
        n_series = 1
    ph = str(primary_hex or "#888888").lstrip("#")
    if len(ph) != 6 or not all(c in "0123456789abcdefABCDEF" for c in ph):
        ph = "888888"
    r = int(ph[0:2], 16) / 255.0
    g = int(ph[2:4], 16) / 255.0
    b = int(ph[4:6], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    h_comp = (h + 0.5) % 1.0
    phi = 0.618033988749895
    out = []
    for i in range(n_series):
        hi = (h_comp + i * phi) % 1.0
        li = min(0.78, max(0.48, 0.52 + (i % 4) * 0.05))
        si = min(1.0, max(0.72, 0.78 + (1.0 - sat) * 0.15))
        r2, g2, b2 = colorsys.hls_to_rgb(hi, li, si)
        out.append(
            "#{:02x}{:02x}{:02x}".format(
                int(max(0, min(255, round(r2 * 255)))),
                int(max(0, min(255, round(g2 * 255)))),
                int(max(0, min(255, round(b2 * 255)))),
            )
        )
    return out


def _nwst_ui_line_palette(primary_hex, n_series):
    """Distinct lines in the same hue family as the app primary (matches Streamlit accent, not complement)."""
    if n_series < 1:
        n_series = 1
    ph = str(primary_hex or "#888888").lstrip("#")
    if len(ph) != 6 or not all(c in "0123456789abcdefABCDEF" for c in ph):
        ph = "888888"
    r = int(ph[0:2], 16) / 255.0
    g = int(ph[2:4], 16) / 255.0
    b = int(ph[4:6], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    out = []
    for i in range(n_series):
        if n_series == 1:
            li = min(0.82, max(0.42, light))
            si = min(1.0, max(0.62, sat))
        else:
            t = i / max(1, n_series - 1)
            li = 0.40 + t * 0.36
            si = min(1.0, max(0.55, sat * (0.9 + 0.1 * (i % 3))))
        r2, g2, b2 = colorsys.hls_to_rgb(h, li, si)
        out.append(
            "#{:02x}{:02x}{:02x}".format(
                int(max(0, min(255, round(r2 * 255)))),
                int(max(0, min(255, round(g2 * 255)))),
                int(max(0, min(255, round(b2 * 255)))),
            )
        )
    return out


NWST_ATTENDED_CELL_MEMBERS_COL = "Attended cell members"
# 0 = use every Saturday column from Attendance (widest time window on the chart).
NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS = 0


def _nwst_count_y_axis_range(plot_df):
    """Vertical span from padded data min/max so counts use most of the chart (less empty space below)."""
    col = NWST_ATTENDED_CELL_MEMBERS_COL
    if plot_df.empty or col not in plot_df.columns:
        return 0.0, 5.0
    s = plot_df[col].astype(float)
    data_min = float(s.min())
    data_max = float(s.max())
    if data_max <= 0:
        return 0.0, 5.0
    span = data_max - data_min
    if span <= 1e-9:
        span = max(2.0, max(1.0, data_max) * 0.25)
    pad_below = max(0.5, span * 0.1)
    pad_above = max(0.75, span * 0.12)
    y_lo = max(0.0, data_min - pad_below)
    y_hi = data_max + pad_above
    return y_lo, y_hi


def _nwst_attendance_data_min_max_int(plot_df):
    col = NWST_ATTENDED_CELL_MEMBERS_COL
    if plot_df.empty or col not in plot_df.columns:
        return 0, 0
    s = plot_df[col].astype(float)
    return int(s.min()), int(s.max())


def _nwst_attendance_y_tick_labels(tickvals):
    """Whole-number y-axis tick text (no min/max annotations)."""
    return [str(int(v)) for v in tickvals]


def _nwst_make_attendance_rate_fig(
    plot_df, date_cols, colors, daily_colors, y_axis_range=None
):
    """Minimal line chart: attended cell members per Saturday (one line per cell group).

    ``y_axis_range`` — optional ``(y_lo, y_hi)`` for fixed vertical span (e.g. same scale
    across per-cell tabs when Cell filter is All). Tick labels stay derived from ``plot_df``
    (that cell's min, max, mean).
    """
    plot_df = plot_df.copy()
    if NWST_ATTENDED_CELL_MEMBERS_COL not in plot_df.columns:
        plot_df[NWST_ATTENDED_CELL_MEMBERS_COL] = 0

    n_lines = int(plot_df["Cell Group"].nunique())
    line_colors = _nwst_ui_line_palette(daily_colors["primary"], max(n_lines, 1))
    cell_order = sorted(plot_df["Cell Group"].unique(), key=str.lower)
    cg_to_color = {cg: line_colors[i] for i, cg in enumerate(cell_order)}

    if y_axis_range is not None:
        y_lo, y_hi = y_axis_range
    else:
        y_lo, y_hi = _nwst_count_y_axis_range(plot_df)
    y_mean = float(plot_df[NWST_ATTENDED_CELL_MEMBERS_COL].mean())
    fig = go.Figure()
    plot_bg = colors["background"]
    paper_bg = colors["card_bg"]

    for cg in cell_order:
        sub = plot_df[plot_df["Cell Group"] == cg]
        c = cg_to_color[cg]
        fig.add_trace(
            go.Scatter(
                x=sub["Saturday"],
                y=sub[NWST_ATTENDED_CELL_MEMBERS_COL],
                name=str(cg),
                legendgroup=str(cg),
                mode="lines",
                line=dict(width=2, color=c, shape="spline", smoothing=1.2),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>%{x}<br>"
                    "<b>%{y:.0f}</b> attended<extra></extra>"
                ),
            )
        )

    fig.add_hline(
        y=y_mean,
        line_width=1,
        line_dash="dash",
        line_color=colors["text_muted"],
        opacity=0.85,
        layer="below",
    )

    if len(date_cols) > 1:
        x_tickvals = [date_cols[0], date_cols[-1]]
        x_ticktext = [date_cols[0], date_cols[-1]]
    else:
        x_tickvals = list(date_cols)
        x_ticktext = list(date_cols)

    line_muted = "rgba(255,255,255,0.22)"
    fig.update_layout(
        height=260,
        legend_title_text="",
        plot_bgcolor=plot_bg,
        paper_bgcolor=paper_bg,
        font=dict(family="Inter, sans-serif", size=11, color=colors["text"]),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.22,
            xanchor="center",
            x=0.5,
            font=dict(size=10, color=colors["text_muted"], family="Inter"),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
        ),
        hovermode="x unified",
        hoverdistance=24,
        spikedistance=-1,
        hoverlabel=dict(
            bgcolor="#2a2a2a",
            font=dict(size=12, color=colors["text"], family="Inter"),
            bordercolor="rgba(255,255,255,0.2)",
            align="left",
        ),
        margin=dict(l=48, r=12, t=6, b=56),
    )
    fig.update_xaxes(
        title=dict(text=""),
        tickfont=dict(color=colors["text_muted"], family="Inter", size=10),
        showgrid=False,
        zeroline=False,
        linecolor=line_muted,
        linewidth=1,
        mirror=False,
        tickmode="array",
        tickvals=x_tickvals,
        ticktext=x_ticktext,
        categoryorder="array",
        categoryarray=date_cols,
        tickangle=0,
        showspikes=True,
        spikecolor="rgba(255,255,255,0.9)",
        spikesnap="cursor",
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
    )
    data_min_i, data_max_i = _nwst_attendance_data_min_max_int(plot_df)
    lo_i, hi_i = max(0, data_min_i), max(0, data_max_i)
    mean_i = max(0, int(round(y_mean)))
    y_tickvals = sorted({lo_i, hi_i, mean_i})
    fig.update_yaxes(
        title=dict(
            text="Attended cell members",
            font=dict(size=10, color=colors["text_muted"]),
        ),
        tickfont=dict(color=colors["text_muted"], family="Inter", size=10),
        showgrid=False,
        zeroline=False,
        linecolor=line_muted,
        linewidth=1,
        range=[y_lo, y_hi],
        tickmode="array",
        tickvals=y_tickvals,
        ticktext=_nwst_attendance_y_tick_labels(y_tickvals),
    )
    return fig


def render_nwst_service_attendance_rate_charts(display_df, daily_colors, tab_each_cell_when_all=False, aggregate_label=None):
    """Per-zone Saturday attendance (headcount lines) — filtered by current display_df (global Cell / Status).

    When ``tab_each_cell_when_all`` is True and multiple cell groups are shown, each cell gets its own tab
    instead of stacking tall charts.

    When ``aggregate_label`` is set (e.g. a ministry name), all matched members are collapsed into a
    single trend line with that label instead of being split by Cell Group. Use this for Ministry Health.
    """
    if display_df is None or display_df.empty:
        return

    disp_name_col, disp_cell_col = _nwst_resolve_display_name_cell_cols(display_df)
    if not disp_cell_col:
        st.info("Add a Cell / Group column to CG data to show attendance rate by cell charts.")
        return

    ana_df, date_cols, err = nwst_get_attendance_grid_for_charts(NWST_HEALTH_SHEET_ID)
    if err:
        st.warning(err)
        return
    if ana_df is None or ana_df.empty or not date_cols:
        st.info("No Attendance sheet data to chart (need Saturday date columns from column D).")
        return

    cell_to_zone_map = nwst_get_cell_zone_mapping(NWST_HEALTH_SHEET_ID)

    keys = display_df[[disp_name_col, disp_cell_col]].copy()
    keys["_n"] = keys[disp_name_col].astype(str).str.strip()
    keys["_c"] = keys[disp_cell_col].astype(str).str.strip()
    keys = keys[["_n", "_c"]].drop_duplicates()

    work = ana_df.copy()
    work["_n"] = work["Name"].astype(str).str.strip()
    work["_c"] = work["Cell Group"].astype(str).str.strip()
    work_df = work.merge(keys, on=["_n", "_c"], how="inner").drop(columns=["_n", "_c"])

    if work_df.empty:
        st.info(
            "No matching rows between **Attendance** (Saturday 0/1) and the filtered member list. "
            "Check names and cells align with CG Combined."
        )
        return

    # Ministry mode: collapse all Cell Groups into one aggregate line so the chart shows
    # a single ministry-level trend instead of a per-cell breakdown.
    if aggregate_label:
        work_df = work_df.copy()
        work_df["Cell Group"] = aggregate_label

    _mdf = display_df.dropna(subset=[disp_cell_col, disp_name_col]).copy()
    _mdf["_c"] = _mdf[disp_cell_col].astype(str).str.strip()
    members_per_cell = _mdf.groupby("_c")[disp_name_col].nunique().to_dict()

    colors = {
        "primary": daily_colors["primary"],
        "background": daily_colors["background"],
        "card_bg": "#1a1a1a",
        "text": "#ffffff",
        "text_muted": "#999999",
    }

    zone_to_cells = defaultdict(list)
    for cg in sorted(work_df["Cell Group"].dropna().unique(), key=str.lower):
        z = _nwst_zone_for_cell_map(cg, cell_to_zone_map)
        if _nwst_exclude_rate_chart_cell(cg, z):
            continue
        zone_to_cells[z].append(cg)

    chart_date_cols = list(date_cols)
    if (
        NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS
        and len(chart_date_cols) > NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS
    ):
        chart_date_cols = chart_date_cols[-NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS :]

    zone_plots = {}
    for zone in sorted(zone_to_cells.keys(), key=str.lower):
        cells = sorted(zone_to_cells[zone], key=str.lower)
        long_rows = []
        for cg in cells:
            sub = work_df[work_df["Cell Group"] == cg]
            mc = members_per_cell.get(str(cg).strip(), 0)
            if mc == 0 and not sub.empty:
                mc = sub["Name"].nunique()
            if mc == 0:
                continue
            for dc in chart_date_cols:
                attended = int(sub[dc].sum()) if dc in sub.columns else 0
                pct = 100.0 * attended / mc
                long_rows.append(
                    {
                        "Saturday": dc,
                        "Cell Group": cg,
                        "Attendance rate %": round(pct, 1),
                        NWST_ATTENDED_CELL_MEMBERS_COL: attended,
                    }
                )
        plot_df = pd.DataFrame(long_rows)
        if plot_df.empty:
            continue
        zone_plots[zone] = plot_df

    if not zone_plots:
        st.info("No cells to chart after filters.")
        return

    if tab_each_cell_when_all:
        cell_entries = []
        for zone in sorted(zone_to_cells.keys(), key=str.lower):
            for cg in sorted(zone_to_cells[zone], key=str.lower):
                sub = work_df[work_df["Cell Group"] == cg]
                mc = members_per_cell.get(str(cg).strip(), 0)
                if mc == 0 and not sub.empty:
                    mc = sub["Name"].nunique()
                if mc == 0:
                    continue
                long_rows = []
                for dc in chart_date_cols:
                    attended = int(sub[dc].sum()) if dc in sub.columns else 0
                    pct = 100.0 * attended / mc
                    long_rows.append(
                        {
                            "Saturday": dc,
                            "Cell Group": cg,
                            "Attendance rate %": round(pct, 1),
                            NWST_ATTENDED_CELL_MEMBERS_COL: attended,
                        }
                    )
                plot_df_one = pd.DataFrame(long_rows)
                if plot_df_one.empty:
                    continue
                cell_entries.append((cg, plot_df_one))
        cell_entries.sort(key=lambda x: str(x[0]).lower())
        if len(cell_entries) > 1:
            _combined_tab_range = pd.concat(
                [df for _, df in cell_entries], ignore_index=True
            )
            _y_shared_lo, _y_shared_hi = _nwst_count_y_axis_range(_combined_tab_range)
            _shared_range = (_y_shared_lo, _y_shared_hi)
            _tab_labels = [str(cg) for cg, _ in cell_entries]
            _cg_tabs = st.tabs(_tab_labels)
            for _i, (_, plot_df_one) in enumerate(cell_entries):
                with _cg_tabs[_i]:
                    fig = _nwst_make_attendance_rate_fig(
                        plot_df_one,
                        chart_date_cols,
                        colors,
                        daily_colors,
                        y_axis_range=_shared_range,
                    )
                    st.plotly_chart(fig, use_container_width=True)
            return

    zone_tab_names = sorted(zone_plots.keys(), key=str.lower)
    for zone in zone_tab_names:
        plot_df = zone_plots[zone]
        if len(zone_tab_names) > 1:
            st.markdown(
                f"<p style='color: {daily_colors['primary']}; font-weight: 700; font-size: 1rem; margin: 0.75rem 0 0.35rem 0;'>"
                f"{zone}</p>",
                unsafe_allow_html=True,
            )
        fig = _nwst_make_attendance_rate_fig(plot_df, chart_date_cols, colors, daily_colors)
        st.plotly_chart(fig, use_container_width=True)


def _resolve_cg_name_cell_columns(cg_df):
    cg_name_col = None
    cg_cell_col = None
    for col in cg_df.columns:
        if col.lower().strip() in ['name', 'member name', 'member']:
            cg_name_col = col
        if col.lower().strip() in ['cell', 'group']:
            cg_cell_col = col
    if not cg_name_col:
        cg_name_col = cg_df.columns[0]
    return cg_name_col, cg_cell_col


def _compute_attendance_stats_from_frames(att_df, cg_df):
    """Build attendance_stats dict (Name + Cell key) from raw sheet frames."""
    attendance_stats = {}
    cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None

    if not att_name_col:
        return attendance_stats

    for att_name in att_df[att_name_col].unique():
        if pd.isna(att_name) or att_name == "":
            continue

        att_name_str = str(att_name).strip()
        member_att_data = att_df[att_df[att_name_col] == att_name]

        attendance_count = 0
        total_services = 0

        for col_idx, col in enumerate(att_df.columns):
            if col_idx >= 3:
                total_services += 1
                values = member_att_data[col].values
                if len(values) > 0 and str(values[0]).strip() == "1":
                    attendance_count += 1

        cell_info = ""
        if cg_name_col and cg_cell_col:
            cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
            if not cg_match.empty:
                cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

        if total_services > 0:
            key = att_name_str + cell_info
            attendance_stats[key] = {
                "attendance": attendance_count,
                "total": total_services,
                "percentage": round(attendance_count / total_services * 100) if total_services > 0 else 0,
            }

    return attendance_stats


@st.cache_data(ttl=300)
def get_attendance_data():
    """Load attendance rollup from Redis cache or recompute from Attendance + CG Combined."""
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_attendance_stats")
            if cached_data:
                return json.loads(cached_data)
        except Exception:
            pass

    att_df, cg_df = load_attendance_and_cg_dataframes()
    if att_df is None or cg_df is None:
        return {}

    attendance_stats = _compute_attendance_stats_from_frames(att_df, cg_df)

    if redis:
        try:
            redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
        except Exception:
            pass

    return attendance_stats


def get_attendance_text(name, cell, attendance_stats):
    """Attendance summary for tooltips (Name + Cell key), or name only if unknown."""
    if not attendance_stats:
        return name

    name_stripped = str(name).strip()
    cell_stripped = str(cell).strip() if cell else ""

    if cell_stripped:
        key = f"{name_stripped} - {cell_stripped}"
    else:
        key = name_stripped

    if key in attendance_stats:
        stats = attendance_stats[key]
        return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    key_lower = key.lower()
    for dict_key, stats in attendance_stats.items():
        if dict_key.lower() == key_lower:
            return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    return name


def categorize_member_status(attendance_count, total_possible):
    """Categorize member as Regular, Irregular, or Follow Up based on attendance."""
    if attendance_count >= (total_possible * 0.75):  # 75% and above attendance = Regular
        return "Regular"
    elif attendance_count > 0:  # Below 75% = Irregular
        return "Irregular"
    else:  # 0% attendance = Follow Up
        return "Follow Up"


def _qp_first(val, default="cg"):
    """Normalize ``st.query_params`` values (string or single-element list)."""
    if val is None:
        return default
    if isinstance(val, list):
        return val[0] if val else default
    return str(val)


def extract_cell_sheet_status_type(status_val):
    """Same labels as CELL HEALTH member tiles (Status column prefixes on the sheet)."""
    if isinstance(status_val, str):
        if status_val.startswith("Regular:"):
            return "Regular"
        if status_val.startswith("Irregular:"):
            return "Irregular"
        if status_val.startswith("New"):
            return "New"
        if status_val.startswith("Follow Up:"):
            return "Follow Up"
        if status_val.startswith("Red:"):
            return "Red"
        if status_val.startswith("Graduated:"):
            return "Graduated"
    return None


def parse_attendance_column_date(cell_val):
    """Parse a single Attendance sheet header cell into a date, or None."""
    if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
        return None
    s = str(cell_val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_status_historical_month_header(cell_val):
    """Parse **Status Historical** column headers like 'Jan 2026' into (year, month), or None."""
    if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
        return None
    s = str(cell_val).strip()
    if not s:
        return None
    for fmt in ("%b %Y", "%B %Y", "%b %y", "%B %y", "%Y-%m", "%m/%Y", "%Y/%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.year, dt.month)
        except ValueError:
            continue
    return None


def _resolve_status_historical_name_columns(df):
    """Detect composite (Name - Cell), Name, and Cell columns on Status Historical."""
    composite_col = name_col = cell_col = None
    for c in df.columns:
        s = str(c).strip()
        sl = s.lower().replace("-", " ")
        if composite_col is None and "name" in sl and ("cell" in sl or "group" in sl):
            composite_col = c
        elif name_col is None and sl in ("name", "member", "member name", "full name"):
            name_col = c
        elif cell_col is None and sl in ("cell name", "cell", "group", "cg", "cell/group"):
            cell_col = c
        elif cell_col is None and "cell" in sl and "name" not in sl:
            cell_col = c
    if name_col is None and len(df.columns) >= 2:
        c0 = df.columns[0]
        if composite_col == c0 and len(df.columns) >= 3:
            name_col, cell_col = df.columns[1], df.columns[2]
        elif composite_col is None:
            name_col = df.columns[0]
            if len(df.columns) >= 2:
                cell_col = df.columns[1]
    return composite_col, name_col, cell_col


def _status_historical_row_norm_keys(row, composite_col, name_col, cell_col):
    keys = []
    if composite_col is not None and composite_col in row.index:
        v = row.get(composite_col)
        if v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip():
            keys.append(_nwst_normalize_member_name(str(v).strip()))
    n, c = "", ""
    if name_col is not None and name_col in row.index:
        nv = row.get(name_col)
        if nv is not None and not (isinstance(nv, float) and pd.isna(nv)):
            n = str(nv).strip()
    if cell_col is not None and cell_col in row.index:
        cv = row.get(cell_col)
        if cv is not None and not (isinstance(cv, float) and pd.isna(cv)):
            c = str(cv).strip()
    if n and c:
        keys.append(_nwst_normalize_member_name(f"{n} - {c}"))
    if n:
        keys.append(_nwst_normalize_member_name(n))
    return keys


def _parse_status_historical_for_monthly(status_hist_df):
    """Build lookup and month axis from Status Historical, or None if unusable."""
    if status_hist_df is None or status_hist_df.empty:
        return None
    composite_col, name_col, cell_col = _resolve_status_historical_name_columns(status_hist_df)
    if not name_col:
        return None
    myt_today = datetime.now(timezone(timedelta(hours=8))).date()
    cur_ym = (myt_today.year, myt_today.month)
    ym_to_col = {}
    for col in status_hist_df.columns:
        ym = parse_status_historical_month_header(col)
        if ym and ym <= cur_ym and ym not in ym_to_col:
            ym_to_col[ym] = col
    if not ym_to_col:
        return None
    month_keys = sorted(ym_to_col.keys())
    lookup = {}
    for _, row in status_hist_df.iterrows():
        for nk in _status_historical_row_norm_keys(row, composite_col, name_col, cell_col):
            if nk:
                lookup[nk] = row
    return {
        "lookup": lookup,
        "month_keys": month_keys,
        "ym_to_col": ym_to_col,
    }


def _month_status_from_historical_cell(raw):
    """Map sheet cell text to Regular / Irregular / Follow Up for the matrix."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None
    st = extract_cell_sheet_status_type(s)
    if st:
        return st
    sl = s.lower()
    if sl.startswith("regular"):
        return "Regular"
    if sl.startswith("irregular"):
        return "Irregular"
    if sl.startswith("follow"):
        return "Follow Up"
    return None


def _attendance_row_lookup_key(row, att_name_col, cg_df, cg_name_col, cg_cell_col):
    att_name_str = str(row[att_name_col]).strip()
    cell_info = ""
    if cg_name_col and cg_cell_col:
        cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
        if not cg_match.empty:
            cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()
    return att_name_str + cell_info


def build_monthly_member_status_table(display_df, att_df, cg_df, status_hist_df=None):
    """
    One row per member in display_df; columns Cell, Member, Health (present/total + rate %),
    then each Month (MMM YY) with Regular / Irregular / Follow Up.
    Month labels come from **Status Historical** when that tab loads; missing cells show "—".
    If Status Historical is missing or has no month columns, months are derived from **Attendance**
    (75% rule on weekly 1/0 columns).
    Health always uses **Attendance** weekly marks aggregated over the same month keys shown.
    Only the latest MONTHLY_MEMBER_MATRIX_MAX_MONTHS month keys (within data, not after current month)
    are included so the table stays a fixed rolling window.
    Internal column _tile_status stores CG Combined status for Health cell coloring.
    Rows are sorted alphabetically by member name (case-insensitive), then by cell for stable ties.
    """
    if display_df is None or display_df.empty:
        return pd.DataFrame()

    hist_ctx = _parse_status_historical_for_monthly(status_hist_df)

    att_df = att_df if att_df is not None else pd.DataFrame()
    cg_df = cg_df if cg_df is not None else pd.DataFrame()

    month_to_colnames = {}
    att_name_col = None
    if not att_df.empty:
        att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None
        if att_name_col:
            for col_idx, col in enumerate(att_df.columns):
                if col_idx < 3:
                    continue
                d = parse_attendance_column_date(col)
                if d is None:
                    continue
                ym = (d.year, d.month)
                month_to_colnames.setdefault(ym, []).append(col)

    myt_today = datetime.now(timezone(timedelta(hours=8))).date()
    cur_ym = (myt_today.year, myt_today.month)

    if hist_ctx:
        month_keys = [ym for ym in hist_ctx["month_keys"] if ym <= cur_ym]
        ym_to_hist_col = hist_ctx["ym_to_col"]
        hist_lookup = hist_ctx["lookup"]
    else:
        month_keys = sorted(ym for ym in month_to_colnames if ym <= cur_ym)
        ym_to_hist_col = {}
        hist_lookup = {}

    if not month_keys:
        return pd.DataFrame()

    if len(month_keys) > MONTHLY_MEMBER_MATRIX_MAX_MONTHS:
        month_keys = month_keys[-MONTHLY_MEMBER_MATRIX_MAX_MONTHS:]

    month_labels = [datetime(y, m, 1).strftime("%b %y") for y, m in month_keys]

    cg_name_col, cg_cell_col = (None, None)
    if not cg_df.empty:
        cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)

    key_to_row = {}
    if att_name_col and not att_df.empty and cg_name_col is not None:
        for _, row in att_df.iterrows():
            if pd.isna(row[att_name_col]) or str(row[att_name_col]).strip() == '':
                continue
            k = _attendance_row_lookup_key(row, att_name_col, cg_df, cg_name_col, cg_cell_col)
            key_to_row[k] = row

    disp_name_col = None
    disp_cell_col = None
    for col in display_df.columns:
        col_lower = col.lower().strip()
        if col_lower in ['cell', 'group']:
            disp_cell_col = col
        if col_lower in ['name', 'member name', 'member'] or (
            any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower
        ):
            if disp_name_col is None:
                disp_name_col = col
    if not disp_name_col:
        disp_name_col = display_df.columns[0]

    def display_row_key(nm, cl):
        ns = str(nm).strip() if pd.notna(nm) else ""
        cs = str(cl).strip() if cl is not None and pd.notna(cl) else ""
        if cs:
            return f"{ns} - {cs}"
        return ns

    status_col = None
    for col in display_df.columns:
        if "status" in col.lower():
            status_col = col
            break

    rows_out = []
    seen = set()
    for _, dr in display_df.iterrows():
        nm = dr.get(disp_name_col)
        cl = dr.get(disp_cell_col) if disp_cell_col else ""
        mk = display_row_key(nm, cl)
        if mk in seen:
            continue
        seen.add(mk)

        tile_status = extract_cell_sheet_status_type(dr.get(status_col)) if status_col else None

        att_row = key_to_row.get(mk)
        if att_row is None and disp_cell_col:
            att_row = key_to_row.get(str(nm).strip() if pd.notna(nm) else "")

        cl_str = str(cl).strip() if cl is not None and pd.notna(cl) else ""
        nm_str = str(nm).strip() if pd.notna(nm) else ""
        out = {"Cell": cl_str, "Member": nm_str, "_tile_status": tile_status}

        nk_full = _nwst_normalize_member_name(mk)
        nk_name = _nwst_normalize_member_name(nm_str)
        hist_row = None
        if hist_lookup:
            if nk_full in hist_lookup:
                hist_row = hist_lookup[nk_full]
            elif nk_name in hist_lookup:
                hist_row = hist_lookup[nk_name]

        use_hist_months = hist_ctx is not None and hist_row is not None

        for ym, lbl in zip(month_keys, month_labels):
            if use_hist_months:
                hcol = ym_to_hist_col.get(ym)
                raw = hist_row.get(hcol) if hcol is not None else None
                mapped = _month_status_from_historical_cell(raw)
                out[lbl] = mapped if mapped else "—"
            elif att_row is not None:
                cols_m = month_to_colnames.get(ym, [])
                present = 0
                total = 0
                for c in cols_m:
                    total += 1
                    v = att_row.get(c)
                    if v is not None and str(v).strip() == '1':
                        present += 1
                if total == 0:
                    out[lbl] = "—"
                else:
                    out[lbl] = categorize_member_status(present, total)
            else:
                out[lbl] = "—"

        if att_row is not None:
            all_present = 0
            all_total = 0
            for ym in month_keys:
                for c in month_to_colnames.get(ym, []):
                    all_total += 1
                    v = att_row.get(c)
                    if v is not None and str(v).strip() == '1':
                        all_present += 1
            if all_total == 0:
                out["Health"] = "—"
            else:
                att_pct = round(100.0 * all_present / all_total, 1)
                out["Health"] = f"{all_present}/{all_total} ({att_pct}%)"
        else:
            out["Health"] = "—"
        rows_out.append(out)

    result = pd.DataFrame(rows_out)
    if result.empty:
        return result
    col_order = ["Cell", "Member", "Health"] + month_labels
    build_cols = [c for c in col_order if c in result.columns]
    if "_tile_status" in result.columns:
        build_cols.append("_tile_status")
    result = result[build_cols]

    result["_member_key"] = result["Member"].fillna("").astype(str).str.strip().str.lower()
    result["_cell_key"] = result["Cell"].fillna("").astype(str).str.strip().str.lower()
    return (
        result.sort_values(["_member_key", "_cell_key"])
        .drop(columns=["_member_key", "_cell_key"])
        .reset_index(drop=True)
    )


def _monthly_table_month_columns(df):
    """Columns after Cell / Member / Health (chronological month labels)."""
    fixed = {"Cell", "Member", "Health", "_tile_status"}
    return [c for c in df.columns if c not in fixed]


_MONTHLY_STATUS_SORT_RANK = {
    "Follow Up": 0,
    "Irregular": 1,
    "Regular": 2,
    "—": 9,
}


def _health_string_sort_key(val):
    """Parse Health like '3/12 (25.0%)' for numeric sort; missing → NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return float("nan")
    t = str(val).strip()
    if t in ("", "—"):
        return float("nan")
    m = re.search(r"\(([\d.]+)%\)\s*$", t)
    if m:
        return float(m.group(1))
    m2 = re.search(r"^(\d+)\s*/\s*(\d+)", t)
    if m2:
        a, b = int(m2.group(1)), int(m2.group(2))
        if b > 0:
            return 100.0 * a / b
    return float("nan")


def _monthly_month_cell_sort_key(val):
    """Severity order for month status cells (matches worst-status heuristic)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 99
    t = str(val).strip()
    return _MONTHLY_STATUS_SORT_RANK.get(t, 50)


def _worst_status_last_three_months(row, month_cols):
    """
    Fallback Health coloring when sheet tile status is missing: worst of the last
    3 month columns (Follow Up > Irregular > Regular). Ignores '—' and unknown values.
    """
    if not month_cols:
        return None
    lookback = month_cols[-3:]
    rank = {"Follow Up": 0, "Irregular": 1, "Regular": 2}
    worst_label = None
    worst_r = 99
    for c in lookback:
        raw = row.get(c)
        s = "" if pd.isna(raw) else str(raw).strip()
        if s not in rank:
            continue
        r = rank[s]
        if r < worst_r:
            worst_r = r
            worst_label = s
    return worst_label


def _monthly_trunc_expand_cell(value: str, extra_attrs: str = "") -> str:
    """Narrow Cell/Member columns: summary truncates with CSS ellipsis; click opens full text below."""
    full = (value or "").strip()
    esc_full = html.escape(full)
    if not full:
        return f'<td class="monthly-trunc-cell"{extra_attrs}></td>'
    inner = (
        f'<details class="monthly-trunc-details">'
        f'<summary class="monthly-trunc-summary" title="Click to show full text">{esc_full}</summary>'
        f'<span class="monthly-trunc-full">{esc_full}</span>'
        f"</details>"
    )
    return f'<td class="monthly-trunc-cell"{extra_attrs}>{inner}</td>'


def _monthly_td_sort_attrs(col: str, sval: str) -> str:
    """data-* attributes for client-side column sort (lexographic or numeric)."""
    if col == "Health":
        k = _health_string_sort_key(sval)
        if k != k:  # NaN
            return ' data-sort-t="num" data-sort-v=""'
        return f' data-sort-t="num" data-sort-v="{html.escape(f"{k:.6g}", quote=True)}"'
    if col in ("Cell", "Member"):
        v = (sval or "").strip().lower()
        return f' data-sort-t="lex" data-sort-v="{html.escape(v, quote=True)}"'
    r = _monthly_month_cell_sort_key(sval)
    return f' data-sort-t="num" data-sort-v="{int(r)}"'


def render_monthly_status_html_table(df):
    """Render monthly status matrix as HTML with bold status labels (tile-matching colors)."""
    if df is None or df.empty:
        return ""

    status_span = {
        "Regular": "monthly-status-regular",
        "Irregular": "monthly-status-irregular",
        "Follow Up": "monthly-status-followup",
    }
    health_tile_classes = {
        "Regular": "monthly-status-regular",
        "Irregular": "monthly-status-irregular",
        "Follow Up": "monthly-status-followup",
        "New": "monthly-health-tile-new",
        "Red": "monthly-health-tile-red",
        "Graduated": "monthly-health-tile-graduated",
    }

    month_cols = _monthly_table_month_columns(df)
    # Reorder columns: Member first, then Cell, Health, then month columns
    all_cols = [c for c in df.columns if c != "_tile_status"]
    ordered_cols = []
    if "Member" in all_cols:
        ordered_cols.append("Member")
    if "Cell" in all_cols:
        ordered_cols.append("Cell")
    if "Health" in all_cols:
        ordered_cols.append("Health")
    for c in all_cols:
        if c not in ordered_cols:
            ordered_cols.append(c)
    display_columns = ordered_cols

    header_parts = []
    for c in display_columns:
        lab = "Name" if c == "Member" else str(c)
        header_parts.append(
            "<th class=\"monthly-sort-th\" "
            f"title=\"Click to sort; click again to reverse\" "
            f'data-label="{html.escape(lab, quote=True)}">'
            f"{html.escape(lab)}</th>"
        )
    header_cells = "".join(header_parts)
    body_rows = []
    has_tile_col = "_tile_status" in df.columns
    for _, row in df.iterrows():
        cells = []
        eff_health_status = None
        if has_tile_col:
            tile_raw = row.get("_tile_status")
            if tile_raw is not None and not (isinstance(tile_raw, float) and pd.isna(tile_raw)):
                ts = str(tile_raw).strip()
                if ts in health_tile_classes:
                    eff_health_status = ts
        if eff_health_status is None:
            eff_health_status = _worst_status_last_three_months(row, month_cols)

        for col in display_columns:
            raw = row[col]
            sval = "" if pd.isna(raw) else str(raw).strip()
            sa = _monthly_td_sort_attrs(col, sval)
            if col == "Health":
                att_cls = health_tile_classes.get(eff_health_status, "")
                if att_cls:
                    cells.append(
                        f"<td class=\"monthly-attendance-rate-cell\"{sa}>"
                        f"<span class=\"{att_cls}\">{html.escape(sval)}</span></td>"
                    )
                else:
                    cells.append(
                        f"<td class=\"monthly-attendance-rate-cell\"{sa}>{html.escape(sval)}</td>"
                    )
            elif col == "Member":
                cells.append(_monthly_trunc_expand_cell(sval, sa))
            elif col == "Cell":
                cells.append(_monthly_trunc_expand_cell(sval, sa))
            else:
                mo_span = status_span.get(sval, "")
                if mo_span:
                    cells.append(
                        f"<td{sa}><span class=\"{mo_span}\">{html.escape(sval)}</span></td>"
                    )
                else:
                    cells.append(f"<td{sa}>{html.escape(sval)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    return (
        '<div class="monthly-attendance-table-wrap">'
        '<table class="monthly-attendance-table">'
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )


def display_monthly_status_interactive(df: pd.DataFrame) -> None:
    """
    Same matrix as render_monthly_status_html_table, with click-to-sort headers (client-side).
    Uses an iframe so JavaScript runs; includes embedded CSS to match the main app.
    """
    table_html = render_monthly_status_html_table(df)
    if not table_html:
        return
    # Unique per rendered table so Streamlit/React do not reuse a stale iframe when
    # tabs or filters swap data but row count/columns match.
    wrap_id = "mw_" + hashlib.sha256(table_html.encode("utf-8")).hexdigest()[:16]
    n = len(df)
    iframe_h = int(max(420, min(1000, 120 + n * 30)))
    js_wrap = json.dumps(wrap_id)
    full = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
body {{ margin: 0; background: #0e1117; }}
{_MONTHLY_ATTENDANCE_IFRAME_CSS}
</style></head><body>
<div id="{wrap_id}">{table_html}</div>
<script>
(function() {{
  const root = document.getElementById({js_wrap});
  if (!root) return;
  const table = root.querySelector('table.monthly-attendance-table');
  if (!table) return;
  const tbody = table.querySelector('tbody');
  const thead = table.querySelector('thead');
  const ths = Array.from(table.querySelectorAll('thead th.monthly-sort-th'));
  if (!tbody || !thead || ths.length === 0) return;
  let sortCol = -1;
  let sortAsc = true;
  function numFromTd(td) {{
    const v = td.getAttribute('data-sort-v');
    if (v === null || v === '') return NaN;
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : NaN;
  }}
  function cmpRows(r1, r2, colIdx) {{
    const ta = r1.children[colIdx];
    const tb = r2.children[colIdx];
    if (!ta || !tb) return 0;
    const typ = ta.getAttribute('data-sort-t') || 'lex';
    if (typ === 'num') {{
      const na = numFromTd(ta), nb = numFromTd(tb);
      const aNa = Number.isNaN(na), bNa = Number.isNaN(nb);
      if (aNa && bNa) return 0;
      if (aNa) return 1;
      if (bNa) return -1;
      return na - nb;
    }}
    const va = (ta.getAttribute('data-sort-v') || '');
    const vb = (tb.getAttribute('data-sort-v') || '');
    return va.localeCompare(vb);
  }}
  function redrawIndicators(activeIdx) {{
    ths.forEach((th, i) => {{
      const base = th.getAttribute('data-label') || '';
      const arrow = (i === activeIdx) ? (sortAsc ? ' \\u25B2' : ' \\u25BC') : '';
      th.textContent = base + arrow;
    }});
  }}
  function applySort(idx) {{
    if (sortCol === idx) sortAsc = !sortAsc;
    else {{ sortCol = idx; sortAsc = true; }}
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a, b) => {{
      const c = cmpRows(a, b, idx);
      return sortAsc ? c : -c;
    }});
    rows.forEach(row => tbody.appendChild(row));
    redrawIndicators(idx);
  }}
  thead.addEventListener('click', function(ev) {{
    let t = ev.target;
    while (t && t.nodeType !== 1) t = t.parentNode;
    const th = t && typeof t.closest === 'function' ? t.closest('th.monthly-sort-th') : null;
    if (!th || !table.contains(th)) return;
    ev.preventDefault();
    ev.stopPropagation();
    const idx = ths.indexOf(th);
    if (idx < 0) return;
    applySort(idx);
  }}, true);
}})();
</script>
</body></html>"""
    try:
        components.html(full, height=iframe_h, scrolling=True, tab_index=0)
    except TypeError:
        components.html(full, height=iframe_h, scrolling=True)


def _detailed_members_health_span_class(tile_status):
    """Same class mapping as Individual Attendance Health column (sheet tile statuses)."""
    health_tile_classes = {
        "Regular": "monthly-status-regular",
        "Irregular": "monthly-status-irregular",
        "Follow Up": "monthly-status-followup",
        "New": "monthly-health-tile-new",
        "Red": "monthly-health-tile-red",
        "Graduated": "monthly-health-tile-graduated",
    }
    return health_tile_classes.get(tile_status or "")


def _detailed_member_name_cell(sval, tile_status, sort_attrs):
    """First column: expandable name + Health color (tile status), matching IA table patterns."""
    full = (sval or "").strip()
    esc_full = html.escape(full)
    if not full:
        return f'<td class="monthly-trunc-cell"{sort_attrs}></td>'
    cls = _detailed_members_health_span_class(tile_status)
    colored = f'<span class="{cls}">{esc_full}</span>' if cls else esc_full
    inner = (
        f'<details class="monthly-trunc-details">'
        f'<summary class="monthly-trunc-summary" title="Click to show full text">{colored}</summary>'
        f'<span class="monthly-trunc-full">{colored}</span>'
        f"</details>"
    )
    return f'<td class="monthly-trunc-cell"{sort_attrs}>{inner}</td>'


def _detailed_members_name_sort_attrs(sval):
    v = (sval or "").strip().lower()
    return f' data-sort-t="lex" data-sort-v="{html.escape(v, quote=True)}"'


def _detailed_members_col_sort_attrs(col_name, sval):
    if str(col_name).strip().lower() == "age":
        t = str(sval).strip()
        try:
            n = float(t) if t else float("nan")
        except ValueError:
            n = float("nan")
        if n == n:
            return f' data-sort-t="num" data-sort-v="{html.escape(f"{n:.6g}", quote=True)}"'
    v = (sval or "").strip().lower()
    return f' data-sort-t="lex" data-sort-v="{html.escape(v, quote=True)}"'


def render_detailed_members_html_table(table_df: pd.DataFrame, tile_statuses: list) -> str:
    """Roster as HTML: same structure/classes as Individual Attendance; first column header **Names**."""
    if table_df is None or table_df.empty:
        return ""
    cols = list(table_df.columns)
    header_parts = []
    for c in cols:
        lab = "Names" if str(c).strip() == "Name" else str(c)
        header_parts.append(
            '<th class="monthly-sort-th" '
            'title="Click to sort; click again to reverse" '
            f'data-label="{html.escape(lab, quote=True)}">'
            f"{html.escape(lab)}</th>"
        )
    header_cells = "".join(header_parts)
    body_rows = []
    for i, (_, row) in enumerate(table_df.iterrows()):
        ts = tile_statuses[i] if i < len(tile_statuses) else None
        cells = []
        for j, col in enumerate(cols):
            raw = row[col]
            sval = "" if pd.isna(raw) else str(raw).strip()
            if j == 0:
                cells.append(_detailed_member_name_cell(sval, ts, _detailed_members_name_sort_attrs(sval)))
            else:
                sa = _detailed_members_col_sort_attrs(col, sval)
                cells.append(_monthly_trunc_expand_cell(sval, sa))
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        '<div class="monthly-attendance-table-wrap">'
        '<table class="monthly-attendance-table detailed-members-table">'
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )


def display_detailed_members_interactive(table_df: pd.DataFrame, tile_statuses: list) -> None:
    """Detailed Members table in an iframe with the same sort UX as Individual Attendance."""
    table_html = render_detailed_members_html_table(table_df, tile_statuses)
    if not table_html:
        return
    wrap_id = "dm_" + hashlib.sha256(table_html.encode("utf-8")).hexdigest()[:16]
    n = len(table_df)
    iframe_h = int(max(420, min(1000, 120 + n * 30)))
    js_wrap = json.dumps(wrap_id)
    full = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
body {{ margin: 0; background: #0e1117; }}
{_MONTHLY_ATTENDANCE_IFRAME_CSS}
{_DETAILED_MEMBERS_IFRAME_CSS_EXTRA}
</style></head><body>
<div id="{wrap_id}">{table_html}</div>
<script>
(function() {{
  const root = document.getElementById({js_wrap});
  if (!root) return;
  const table = root.querySelector('table.monthly-attendance-table');
  if (!table) return;
  const tbody = table.querySelector('tbody');
  const thead = table.querySelector('thead');
  const ths = Array.from(table.querySelectorAll('thead th.monthly-sort-th'));
  if (!tbody || !thead || ths.length === 0) return;
  let sortCol = -1;
  let sortAsc = true;
  function numFromTd(td) {{
    const v = td.getAttribute('data-sort-v');
    if (v === null || v === '') return NaN;
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : NaN;
  }}
  function cmpRows(r1, r2, colIdx) {{
    const ta = r1.children[colIdx];
    const tb = r2.children[colIdx];
    if (!ta || !tb) return 0;
    const typ = ta.getAttribute('data-sort-t') || 'lex';
    if (typ === 'num') {{
      const na = numFromTd(ta), nb = numFromTd(tb);
      const aNa = Number.isNaN(na), bNa = Number.isNaN(nb);
      if (aNa && bNa) return 0;
      if (aNa) return 1;
      if (bNa) return -1;
      return na - nb;
    }}
    const va = (ta.getAttribute('data-sort-v') || '');
    const vb = (tb.getAttribute('data-sort-v') || '');
    return va.localeCompare(vb);
  }}
  function redrawIndicators(activeIdx) {{
    ths.forEach((th, i) => {{
      const base = th.getAttribute('data-label') || '';
      const arrow = (i === activeIdx) ? (sortAsc ? ' \\u25B2' : ' \\u25BC') : '';
      th.textContent = base + arrow;
    }});
  }}
  function applySort(idx) {{
    if (sortCol === idx) sortAsc = !sortAsc;
    else {{ sortCol = idx; sortAsc = true; }}
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a, b) => {{
      const c = cmpRows(a, b, idx);
      return sortAsc ? c : -c;
    }});
    rows.forEach(row => tbody.appendChild(row));
    redrawIndicators(idx);
  }}
  thead.addEventListener('click', function(ev) {{
    let t = ev.target;
    while (t && t.nodeType !== 1) t = t.parentNode;
    const th = t && typeof t.closest === 'function' ? t.closest('th.monthly-sort-th') : null;
    if (!th || !table.contains(th)) return;
    ev.preventDefault();
    ev.stopPropagation();
    const idx = ths.indexOf(th);
    if (idx < 0) return;
    applySort(idx);
  }}, true);
}})();
</script>
</body></html>"""
    try:
        components.html(full, height=iframe_h, scrolling=True, tab_index=0)
    except TypeError:
        components.html(full, height=iframe_h, scrolling=True)


def get_member_category_color(category):
    """Return color based on member category."""
    colors = {
        "New": "#3498db",      # Blue
        "Regular": "#2ecc71",   # Green
        "Irregular": "#e67e22"  # Orange
    }
    return colors.get(category, "#95a5a6")

def get_leadership_by_role(df):
    """
    Extract leadership members grouped by role hierarchy.
    Returns a dict with role display names as keys and list of members as values.
    """
    # Define the role hierarchy with exact values and display order
    role_hierarchy = {
        1: "1. CG Leader",
        2: "2. Assistant CG Leader",
        3: "3. CG Core",
        4: "4. Potential CG Core",
        5: "5. Ministry Leader",
        6: "6. Assistant Ministry Leader",
        7: "7. Ministry Core",
        8: "8. Potential Ministry Core",
        9: "9. Zone Leader"
    }

    # Find the Role column (case-insensitive)
    role_col = None
    for col in df.columns:
        if col.lower().strip() == 'role':
            role_col = col
            break

    if not role_col:
        return {}

    # Find the "Since" column (case-insensitive)
    since_col = None
    for col in df.columns:
        if 'since' in col.lower():
            since_col = col
            break

    # Find the Name column
    name_col = None
    for col in df.columns:
        col_lower = col.lower()
        if (any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower):
            name_col = col
            break

    if not name_col:
        name_col = df.columns[0]

    # Group members by role
    leadership_groups = {}
    for _, row in df.iterrows():
        role_val = str(row[role_col]).strip() if pd.notna(row[role_col]) else ""

        # Check if this role matches any in our hierarchy
        matching_role = None
        for order, role_name in role_hierarchy.items():
            if role_val == role_name:
                matching_role = role_name
                break

        if matching_role:
            if matching_role not in leadership_groups:
                leadership_groups[matching_role] = []

            # Get member info
            member_name = str(row[name_col]).strip() if pd.notna(row[name_col]) else "Unknown"
            since_info = ""
            if since_col and pd.notna(row[since_col]):
                since_val = str(row[since_col]).strip()
                if since_val:
                    since_info = since_val

            leadership_groups[matching_role].append({
                "name": member_name,
                "since": since_info
            })

    # Sort by hierarchy order and return (removes numbers from display)
    sorted_leadership = {}
    for order, role_name in role_hierarchy.items():
        if role_name in leadership_groups:
            # Remove the number prefix for display (e.g., "1. " becomes "")
            display_role = role_name[role_name.find('. ') + 2:] if '. ' in role_name else role_name
            sorted_leadership[display_role] = leadership_groups[role_name]

    return sorted_leadership


_MINISTRY_ROLE_COLS = {
    "Hype":       "Hype Role",
    "Frontlines": "Frontlines Role",
    "VS":         "VS Role",
    "Worship":    "Worship Role",
}


def get_members_by_ministry(df):
    """
    Group members by ministry based on the 4 ministry-role columns.
    Returns dict {ministry_name: [{"name": ..., "role": ...}, ...]}
    Only ministries with at least one assigned member are included.
    """
    name_col = None
    for col in df.columns:
        cl = col.lower()
        if ("name" in cl or "member" in cl) and "last" not in cl:
            name_col = col
            break
    if not name_col:
        name_col = df.columns[0]

    result = {}
    for ministry, role_label in _MINISTRY_ROLE_COLS.items():
        role_col = next(
            (c for c in df.columns if c.lower().strip() == role_label.lower()),
            None,
        )
        if not role_col:
            continue
        members = []
        for _, row in df.iterrows():
            role_val = str(row[role_col]).strip() if pd.notna(row[role_col]) else ""
            if role_val:
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else "Unknown"
                members.append({"name": name, "role": role_val})
        if members:
            result[ministry] = members
    return result


def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


_nwst_accent_cfg_mod = None


def _accent_overrides_from_project_config():
    """Load shared ``nwst_accent_config.py`` (see ``nwst_shared.paths``)."""
    global _nwst_accent_cfg_mod
    if _nwst_accent_cfg_mod is None:
        cfg = resolved_nwst_accent_config_path()
        if cfg is not None:
            spec = importlib.util.spec_from_file_location("_nwst_accent_cfg", cfg)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _nwst_accent_cfg_mod = mod
    if _nwst_accent_cfg_mod is None:
        return {}
    return _nwst_accent_cfg_mod.get_accent_override_by_date()


def _theme_overrides_from_redis():
    """Theme Override rows from Upstash (refreshed on Sync from Google Sheets or CHECK IN Update names)."""
    redis_client = get_redis_client()
    if _nwst_accent_cfg_mod is None:
        _accent_overrides_from_project_config()
    if _nwst_accent_cfg_mod:
        try:
            return _nwst_accent_cfg_mod.read_theme_override_from_redis(redis_client)
        except Exception:
            return {}
    return {}


def resolve_theme_override_row_for_today(from_sheet=None):
    """Latest-dated row from the Theme Override Upstash snapshot (+ JSON merge for that date).

    If the snapshot is empty, returns ``{}`` so callers use ``banner.gif`` and generated colors.
    """
    from_file = _accent_overrides_from_project_config()
    if from_sheet is None:
        from_sheet = _theme_overrides_from_redis()
    if not from_sheet:
        return {}
    if _nwst_accent_cfg_mod:
        row = _nwst_accent_cfg_mod.resolve_latest_cached_theme_row(from_file, from_sheet)
    else:
        latest = max(from_sheet.keys())
        keys = set(from_file) | set(from_sheet)
        merged = {
            k: {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
            for k in keys
            if {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
        }
        row = dict(merged.get(latest) or {})
    today = get_today_myt_date()
    if not row.get("primary"):
        env_d = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_DATE", "").strip()
        env_h = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_HEX", "").strip()
        if env_d == today and env_h:
            row["primary"] = env_h.strip()
        else:
            try:
                if hasattr(st, "secrets"):
                    sd = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_DATE", "")).strip()
                    sh = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_HEX", "")).strip()
                    if sd == today and sh:
                        row["primary"] = sh.strip()
            except Exception:
                pass
    return row


def generate_daily_colors():
    """Daily MYT generated palette unless Theme Override supplies primary/banner (Upstash / JSON / env)."""
    today_str = get_today_myt_date()
    from_sheet = _theme_overrides_from_redis()
    row = resolve_theme_override_row_for_today(from_sheet=from_sheet)
    hex_override = row.get("primary")
    base = None
    if hex_override:
        pn = _normalize_primary_hex(hex_override)
        if pn:
            base = theme_from_primary_hex(pn)
    if base is None:
        base = generate_colors_for_date(today_str)
    b_raw = row.get("banner")
    if b_raw:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename(b_raw)
            if safe:
                base = {**base, "banner": safe}
    if not from_sheet:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename("banner.gif")
            if safe:
                base = {**base, "banner": safe}
    return base


def _render_nwst_analytics_individual_attendance(colors, cell_to_zone_map):
    """NWST monthly member matrix (same data as CG Individual Attendance), tabs grouped by zone from Key Values."""
    display_df = get_newcomers_data()
    if display_df is None or display_df.empty:
        st.info("No **CG Combined** roster — sync NWST Health to load members.")
        return

    att_df_m, cg_df_m = load_attendance_and_cg_dataframes()
    if cg_df_m is None:
        st.info("Could not load NWST **CG Combined** for this table.")
        return
    if att_df_m is None:
        att_df_m = pd.DataFrame()
    status_hist_df = load_status_historical_dataframe()
    monthly_status_df = build_monthly_member_status_table(
        display_df, att_df_m, cg_df_m, status_hist_df
    )
    if monthly_status_df is None or monthly_status_df.empty:
        st.info(
            "No individual attendance breakdown yet. Check NWST **Status Historical** month headers "
            "(e.g. Jan 2026) or **Attendance** row 1 from column D for parseable dates."
        )
        return

    def _zone_for_cell(cell_val):
        c = str(cell_val).strip() if cell_val is not None and pd.notna(cell_val) else ""
        if not c:
            return "Unknown"
        return cell_to_zone_map.get(c.lower(), c)

    monthly_status_df = monthly_status_df.copy()
    monthly_status_df["_zone"] = monthly_status_df["Cell"].apply(_zone_for_cell)

    # Render the fragment for interactive filtering
    _nwst_individual_attendance_fragment(monthly_status_df, colors)


@st.fragment
def _nwst_individual_attendance_fragment(monthly_status_df: pd.DataFrame, colors: dict):
    """Fragment for Individual Attendance section - only this block reruns when filter changes."""
    if "analytics_ia_name_filter" not in st.session_state:
        st.session_state.analytics_ia_name_filter = ""

    _name_filter = st.text_input(
        "Search by Name...",
        value=st.session_state.analytics_ia_name_filter,
        key="analytics_ia_name_input",
        placeholder="Type to filter by name...",
        label_visibility="collapsed",
    )
    st.session_state.analytics_ia_name_filter = _name_filter

    _filtered = monthly_status_df.copy()
    if _name_filter.strip():
        _filter_lower = _name_filter.strip().lower()
        _mem_f = _filtered["Member"].fillna("").astype(str).str.strip().str.lower()
        _filtered = _filtered[_mem_f.str.contains(_filter_lower, regex=False)]

    if _filtered.empty:
        st.info("No members match the current filter.")
        return

    _mwf = _filtered.copy()
    if _name_filter.strip():
        _show = _mwf.drop(columns=["_zone"], errors="ignore")
        display_monthly_status_interactive(_show)
    else:
        _zones = sorted(_mwf["_zone"].unique().tolist(), key=str.lower)
        if len(_zones) > 1:
            _ztabs = st.tabs(_zones)
            for _ti, zname in enumerate(_zones):
                with _ztabs[_ti]:
                    _sub = _mwf[_mwf["_zone"] == zname].drop(columns=["_zone"])
                    display_monthly_status_interactive(_sub)
        else:
            _show = _mwf.drop(columns=["_zone"])
            display_monthly_status_interactive(_show)


@st.fragment
def _cg_individual_attendance_fragment(monthly_status_df: pd.DataFrame, colors: dict, cell_filter: str):
    """Fragment for CG Health Individual Attendance section - only this block reruns when filter changes."""
    if "cg_ia_name_filter" not in st.session_state:
        st.session_state.cg_ia_name_filter = ""

    _name_filter = st.text_input(
        "Search by Name...",
        value=st.session_state.cg_ia_name_filter,
        key="cg_ia_name_input",
        placeholder="Type to filter by name...",
        label_visibility="collapsed",
    )
    st.session_state.cg_ia_name_filter = _name_filter

    _filtered_monthly = monthly_status_df.copy()
    if _name_filter.strip():
        _filter_lower = _name_filter.strip().lower()
        _mem_f = _filtered_monthly["Member"].fillna("").astype(str).str.strip().str.lower()
        _filtered_monthly = _filtered_monthly[_mem_f.str.contains(_filter_lower, regex=False)]

    _ch_tile_f = st.session_state.get("cg_cell_health_tile_filter")
    if _ch_tile_f and "_tile_status" in _filtered_monthly.columns:
        _filtered_monthly = _filtered_monthly[
            _filtered_monthly["_tile_status"] == _ch_tile_f
        ]

    if _filtered_monthly.empty:
        st.info("No members match the current filter.")
        return

    _mwf = _filtered_monthly.copy()
    _mwf["_monthly_tab_cell"] = (
        _mwf["Cell"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "(no cell)")
    )
    _cells_for_tabs = sorted(
        _mwf["_monthly_tab_cell"].unique().tolist(),
        key=str.lower,
    )
    # Name or Cell Health tile filter: one table so matches are not split across tabs.
    if _name_filter.strip() or _ch_tile_f:
        display_monthly_status_interactive(_filtered_monthly)
    elif cell_filter == "All" and len(_cells_for_tabs) > 1:
        _mh_cell_tabs = st.tabs(_cells_for_tabs)
        for _ti, _cell_name in enumerate(_cells_for_tabs):
            with _mh_cell_tabs[_ti]:
                _sub = _mwf[_mwf["_monthly_tab_cell"] == _cell_name].drop(
                    columns=["_monthly_tab_cell"]
                )
                display_monthly_status_interactive(_sub)
    else:
        display_monthly_status_interactive(_filtered_monthly)


def render_nwst_analytics_page(colors):
    """Saturday attendance trends from the Attendance Analytics sheet (same as CHECK IN attendance_app)."""
    analytics_sheet_id = (CHECKIN_ATTENDANCE_SHEET_ID or "").strip()
    if not analytics_sheet_id:
        st.error(
            "Analytics uses the CHECK IN spreadsheet. Set **ATTENDANCE_SHEET_ID** in your environment "
            "or in `.streamlit/secrets.toml` (same as the CHECK IN app)."
        )
        return

    df, saturday_dates, error = nwst_get_attendance_analytics_data(analytics_sheet_id)

    if error:
        st.error(error)
        return

    if df is None or df.empty:
        st.info("No analytics data available.")
        return

    cell_to_zone_map = nwst_get_cell_zone_mapping(analytics_sheet_id)
    df["Zone"] = df["Cell Group"].apply(
        lambda x: cell_to_zone_map.get(x.lower(), x) if x else "Unknown"
    )

    date_cols = [col for col in df.columns if col not in ["Name", "Cell Group", "Name - Cell Group", "Zone"]]

    def _analytics_zone_for_cell(cg):
        return cell_to_zone_map.get(cg.lower(), cg) if cg else "Unknown"

    def _analytics_exclude_from_rate_charts(cg):
        if not str(cg).strip():
            return True
        if str(_analytics_zone_for_cell(cg)).strip().lower() == "archive":
            return True
        n = str(cg).strip().lower().lstrip("*").strip()
        if n == "not sure yet" or n.startswith("not sure yet"):
            return True
        return False

    st.markdown(
        f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

        .analytics-container * {{
            font-family: 'Inter', sans-serif !important;
        }}

        [data-testid="stDataFrame"] {{
            font-family: 'Inter', sans-serif !important;
        }}
        [data-testid="stDataFrame"] * {{
            font-family: 'Inter', sans-serif !important;
        }}
        .stDataFrame th {{
            font-family: 'Inter', sans-serif !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 1px !important;
        }}
        .stDataFrame td {{
            font-family: 'Inter', sans-serif !important;
        }}

        .analytics-kpi-container {{
            display: flex;
            justify-content: center;
            gap: 2rem;
            margin: 2rem 0;
            flex-wrap: wrap;
        }}
        .analytics-kpi-card {{
            background: {colors['card_bg']};
            border: 2px solid {colors['primary']};
            padding: 1.5rem 2rem;
            text-align: center;
            min-width: 180px;
        }}
        .analytics-kpi-label {{
            font-family: 'Inter', sans-serif !important;
            font-size: 0.8rem;
            font-weight: 700;
            color: {colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .analytics-kpi-number {{
            font-family: 'Inter', sans-serif !important;
            font-size: 3rem;
            font-weight: 900;
            color: {colors['primary']};
            line-height: 1;
        }}
        .analytics-section-title {{
            font-family: 'Inter', sans-serif !important;
            font-size: 1.5rem;
            font-weight: 900;
            color: {colors['primary']};
            text-transform: uppercase;
            letter-spacing: 3px;
            margin: 2rem 0 1rem 0;
            border-bottom: 3px solid {colors['primary']};
            padding-bottom: 0.5rem;
            display: inline-block;
        }}
    </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(_nwst_collapsible_section_css(colors["primary"]), unsafe_allow_html=True)

    total_unique_attendees = len(df)
    total_saturdays = len(date_cols)
    if total_saturdays > 0:
        avg_attendance = df[date_cols].sum().mean()
        latest_attendance = df[date_cols[-1]].sum() if date_cols else 0
    else:
        avg_attendance = 0
        latest_attendance = 0

    st.markdown(
        f"""
    <div class="analytics-kpi-container">
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Total Saturdays</div>
            <div class="analytics-kpi-number">{total_saturdays}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Unique Attendees</div>
            <div class="analytics-kpi-number">{total_unique_attendees}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Avg Attendance</div>
            <div class="analytics-kpi-number">{avg_attendance:.0f}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Latest ({date_cols[-1] if date_cols else 'N/A'})</div>
            <div class="analytics-kpi-number">{latest_attendance}</div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    with st.expander("📈 ATTENDANCE TREND (SATURDAYS)", expanded=False):
        attendance_per_date = df[date_cols].sum()
        trend_df = pd.DataFrame({"Date": date_cols, "Attendance": attendance_per_date.values})

        fig_trend = px.line(
            trend_df,
            x="Date",
            y="Attendance",
            markers=True,
            title="",
            labels={"Attendance": "Total Attendance", "Date": "Saturday Date"},
            height=350,
        )

        fig_trend.update_traces(
            line=dict(color=colors["primary"], width=3),
            marker=dict(color=colors["primary"], size=10, line=dict(color=colors["background"], width=2)),
            hovertemplate="<b>%{x}</b><br>Attendance: %{y}<extra></extra>",
        )

        fig_trend.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                showgrid=True,
                gridwidth=1,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                showgrid=True,
                gridwidth=1,
            ),
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_trend, use_container_width=True)

    with st.expander("📊 AVERAGE ATTENDANCE BY ZONE", expanded=False):
        zone_attendance = (
            df.groupby("Zone")[date_cols].sum().sum(axis=1) / len(date_cols) if date_cols else pd.Series()
        )
        zone_df = pd.DataFrame({"Zone": zone_attendance.index, "Avg Attendance": zone_attendance.values}).sort_values(
            "Avg Attendance", ascending=False
        )

        fig_zone = px.bar(
            zone_df,
            x="Zone",
            y="Avg Attendance",
            color="Avg Attendance",
            color_continuous_scale=[colors["background"], colors["primary"]],
            text="Avg Attendance",
            height=350,
        )

        fig_zone.update_traces(
            texttemplate="%{text:.0f}",
            textfont=dict(size=12, color=colors["background"], family="Inter", weight="bold"),
            textposition="inside",
            marker=dict(line=dict(color=colors["primary"], width=2)),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:.1f}<extra></extra>",
        )

        fig_zone.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                categoryorder="total descending",
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            coloraxis_showscale=False,
            showlegend=False,
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_zone, use_container_width=True)

    with st.expander("👤 INDIVIDUAL ATTENDANCE", expanded=False):
        st.markdown(
            f"<p style='color: {colors['text_muted']}; font-family: Inter, sans-serif; "
            f"font-size: 0.85rem; margin: 0 0 1rem 0;'>"
            f"Monthly status columns mirror NWST <b>Status Historical</b>; "
            f"<b>Health</b> uses <b>Attendance</b> + <b>CG Combined</b>. "
            f"Each tab is one <b>zone</b> (Key Values on this spreadsheet).</p>",
            unsafe_allow_html=True,
        )
        _render_nwst_analytics_individual_attendance(colors, cell_to_zone_map)

    with st.expander("📉 ATTENDANCE RATE BY CELL", expanded=False):
        st.markdown(
            f"<p style='color: {colors['text_muted']}; font-family: Inter, sans-serif; "
            f"font-size: 0.85rem; margin: 0 0 1rem 0;'>"
            f"<b style=\"color: {colors['primary']}\">How to read:</b> pick a <b>zone tab</b> — one big chart, no endless scroll. "
            f"Saturdays run left → right; <b>Y</b> = that week&apos;s check-ins ÷ cell roster (Options), as %. "
            f"Bright line colors so each cell group is obvious.</p>",
            unsafe_allow_html=True,
        )

        members_per_cell, options_err = nwst_get_options_roster_members(analytics_sheet_id)
        if not members_per_cell and options_err:
            st.warning(
                f"Could not load Options tab for roster sizes ({options_err}). "
                f"Denominator falls back to unique names seen in analytics per cell."
            )

        st.markdown(
            f"""
        <style>
            [data-testid="stMultiSelect"] {{
                font-family: 'Inter', sans-serif !important;
            }}
            [data-testid="stMultiSelect"] > div {{
                border: 2px solid {colors['primary']} !important;
                border-radius: 0px !important;
                background: {colors['background']} !important;
            }}
            [data-testid="stMultiSelect"] span {{
                font-family: 'Inter', sans-serif !important;
                color: {colors['text']} !important;
            }}
            [data-testid="stMultiSelect"] svg {{
                fill: {colors['primary']} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
                background: {colors['primary']} !important;
                border-radius: 0px !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
                color: {colors['background']} !important;
                font-weight: 600 !important;
            }}
        </style>
        """,
            unsafe_allow_html=True,
        )

        if "clear_filter_counter" not in st.session_state:
            st.session_state.clear_filter_counter = 0

        cell_groups = sorted(
            [c for c in df["Cell Group"].unique() if c and not _analytics_exclude_from_rate_charts(c)]
        )
        filter_col1, filter_col2 = st.columns([3, 1])
        with filter_col1:
            selected_cell_groups = st.multiselect(
                "Filter by Cell Group...",
                options=cell_groups,
                default=[],
                key=f"analytics_cell_multiselect_{st.session_state.clear_filter_counter}",
                placeholder="Select cell groups...",
                label_visibility="collapsed",
            )
        with filter_col2:
            if st.button("Clear All", type="secondary", use_container_width=True, key="nwst_clear_cell_filter"):
                st.session_state.clear_filter_counter += 1
                st.rerun()

        work_df = df.copy()
        if selected_cell_groups:
            work_df = work_df[work_df["Cell Group"].isin(selected_cell_groups)]

        roster_cells = set(members_per_cell.keys())
        analytics_cells = set(work_df["Cell Group"].dropna().unique())
        all_cells = roster_cells | analytics_cells
        if selected_cell_groups:
            all_cells = set(selected_cell_groups) & all_cells

        zone_to_cells = defaultdict(list)
        for cg in all_cells:
            if not str(cg).strip() or _analytics_exclude_from_rate_charts(cg):
                continue
            zone_to_cells[_analytics_zone_for_cell(cg)].append(cg)

        zone_plots = {}
        for zone in sorted(zone_to_cells.keys(), key=str.lower):
            cells = sorted(zone_to_cells[zone], key=str.lower)
            long_rows = []
            for cg in cells:
                sub = work_df[work_df["Cell Group"] == cg]
                mc = members_per_cell.get(cg, 0)
                if mc == 0 and not sub.empty:
                    mc = sub["Name"].nunique()
                if mc == 0:
                    continue
                for dc in date_cols:
                    attended = int(sub[dc].sum()) if dc in sub.columns else 0
                    pct = 100.0 * attended / mc
                    long_rows.append({"Saturday": dc, "Cell Group": cg, "Attendance rate %": round(pct, 1)})

            plot_df = pd.DataFrame(long_rows)
            if plot_df.empty:
                continue
            ymax = max(105.0, plot_df["Attendance rate %"].max() * 1.08)
            zone_plots[zone] = (plot_df, ymax)

        if zone_plots:
            zone_tab_names = sorted(zone_plots.keys(), key=str.lower)
            zone_tabs = st.tabs(zone_tab_names)
            for i, zone in enumerate(zone_tab_names):
                plot_df, ymax = zone_plots[zone]
                with zone_tabs[i]:
                    fig_zone_cells = px.line(
                        plot_df,
                        x="Saturday",
                        y="Attendance rate %",
                        color="Cell Group",
                        markers=True,
                        title="",
                        height=460,
                        color_discrete_sequence=NWST_ANALYTICS_MULTILINE_PALETTE,
                    )
                    fig_zone_cells.update_traces(
                        line=dict(width=3.5),
                        marker=dict(
                            size=5,
                            line=dict(width=1, color="#FFFFFF"),
                            opacity=1,
                        ),
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "%{x}<br>"
                            "<b>%{y:.1f}%</b> of cell showed up<extra></extra>"
                        ),
                    )
                    fig_zone_cells.add_hline(
                        y=50,
                        line_dash="dot",
                        line_color=colors["text_muted"],
                        line_width=1,
                        opacity=0.55,
                        annotation_text="50%",
                        annotation_position="right",
                        annotation_font_color=colors["text_muted"],
                        annotation_font_size=11,
                    )
                    fig_zone_cells.update_layout(
                        plot_bgcolor=colors["background"],
                        paper_bgcolor=colors["card_bg"],
                        font=dict(family="Inter, sans-serif", size=13, color=colors["primary"]),
                        xaxis=dict(
                            title=dict(text="Saturday service", font=dict(size=12)),
                            tickfont=dict(color=colors["text"], family="Inter", size=11),
                            gridcolor=colors["text_muted"],
                            gridwidth=1,
                            linecolor=colors["primary"],
                            linewidth=2,
                            tickangle=-30,
                            categoryorder="array",
                            categoryarray=date_cols,
                        ),
                        yaxis=dict(
                            title=dict(text="How much of the cell came?", font=dict(size=12)),
                            tickfont=dict(color=colors["text"], family="Inter", size=11),
                            ticksuffix="%",
                            gridcolor=colors["text_muted"],
                            gridwidth=1,
                            linecolor=colors["primary"],
                            linewidth=2,
                            range=[0, ymax],
                        ),
                        legend=dict(
                            title=dict(text="Cell groups", font=dict(size=11, color=colors["primary"])),
                            orientation="h",
                            yanchor="top",
                            y=-0.28,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=12, color=colors["text"], family="Inter"),
                            bgcolor="rgba(0,0,0,0)",
                            borderwidth=0,
                        ),
                        hoverlabel=dict(
                            bgcolor=colors["card_bg"],
                            font=dict(size=13, color=colors["primary"], family="Inter"),
                            bordercolor=colors["primary"],
                        ),
                        margin=dict(l=55, r=50, t=28, b=150),
                    )
                    st.plotly_chart(fig_zone_cells, use_container_width=True)
                    st.caption("Tip: follow **one color** across the weeks — rightmost dot is the latest Saturday.")

    with st.expander("📊 ATTENDANCE BY CELL GROUP", expanded=False):
        cell_group_attendance = (
            df.groupby("Cell Group")[date_cols].sum().sum(axis=1) / len(date_cols) if date_cols else pd.Series()
        )
        cell_group_df = pd.DataFrame(
            {"Cell Group": cell_group_attendance.index, "Avg Attendance": cell_group_attendance.values}
        ).sort_values("Avg Attendance", ascending=False).head(20)

        fig_cell = px.bar(
            cell_group_df,
            x="Cell Group",
            y="Avg Attendance",
            color="Avg Attendance",
            color_continuous_scale=[colors["background"], colors["primary"]],
            text="Avg Attendance",
            height=400,
        )

        fig_cell.update_traces(
            texttemplate="%{text:.1f}",
            textfont=dict(size=11, color=colors["background"], family="Inter", weight="bold"),
            textposition="inside",
            marker=dict(line=dict(color=colors["primary"], width=2)),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:.1f}<extra></extra>",
        )

        fig_cell.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter", size=9),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                categoryorder="total descending",
                tickangle=-45,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            coloraxis_showscale=False,
            showlegend=False,
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=100),
        )

        st.plotly_chart(fig_cell, use_container_width=True)

    with st.expander("📈 ZONE ATTENDANCE TREND", expanded=False):
        zones = df["Zone"].dropna().unique()
        zones = [z for z in zones if str(z).strip()]
        zone_order = sorted(zones, key=lambda z: str(z).lower())
        zone_palette = _nwst_analytics_palette_for_n(len(zone_order))

        zone_trend_data = []
        for date_col in date_cols:
            for zone in zones:
                zone_attendance_on_date = df[df["Zone"] == zone][date_col].sum()
                zone_trend_data.append({"Date": date_col, "Zone": zone, "Attendance": zone_attendance_on_date})

        zone_trend_df = pd.DataFrame(zone_trend_data)

        fig_zone_trend = px.line(
            zone_trend_df,
            x="Date",
            y="Attendance",
            color="Zone",
            markers=True,
            height=400,
            category_orders={"Zone": zone_order},
            color_discrete_sequence=zone_palette,
        )

        fig_zone_trend.update_traces(
            line=dict(width=3.5),
            marker=dict(size=8, line=dict(width=1, color="#FFFFFF"), opacity=1),
            hovertemplate="<b>%{fullData.name}</b><br>%{x}: %{y}<extra></extra>",
        )

        fig_zone_trend.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            legend=dict(
                font=dict(color=colors["text_muted"], family="Inter"),
                bgcolor=colors["card_bg"],
                bordercolor=colors["primary"],
                borderwidth=1,
            ),
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_zone_trend, use_container_width=True)


# Page configuration
st.set_page_config(
    page_title="NWST Health",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Weekly accent theme (locked to most recent Saturday MYT, same as CHECK IN attendance app)
daily_colors = generate_daily_colors()

# Optional banner image from Theme Override (file in NWST HEALTH folder, not .streamlit/)
_nwst_banner = daily_colors.get("banner")
if _nwst_banner:
    _nwst_banner_path = Path(__file__).resolve().parent / _nwst_banner
    if _nwst_banner_path.is_file():
        st.image(str(_nwst_banner_path), use_container_width=True)

# Convert hex color to RGB for rgba shadows
def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

primary_rgb = hex_to_rgb(daily_colors['primary'])

# Add CSS to reduce Streamlit default spacing and style with daily color theme
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Base theme colors */
    .stApp {{
        background-color: {daily_colors['background']} !important;
    }}

    .element-container {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] {{
        gap: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] > [style*="flex-direction: column"] {{
        gap: 0rem !important;
    }}
    .stMarkdown {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="column"] {{
        padding-top: 0rem !important;
    }}

    /* Equal height columns */
    [data-testid="column"] > div {{
        height: 100%;
    }}

    /* Style all buttons with daily color theme */
    .stButton > button {{
        background-color: transparent !important;
        color: {daily_colors['primary']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.2s ease !important;
    }}
    .stButton > button:hover {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        transform: scale(1.02) !important;
    }}

    /* Primary buttons */
    .stButton > button[kind="primary"] {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        border: 2px solid {daily_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
    }}

    /* Form submit button */
    .stFormSubmitButton > button {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
    }}
    .stFormSubmitButton > button:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
        transform: scale(1.02) !important;
    }}

    /* Multiselect styling */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {daily_colors['primary']} !important;
    }}

    /* KPI Card styling */
    .kpi-card {{
        background: #1a1a1a !important;
        padding: 2rem 2.5rem;
        border-radius: 0px !important;
        border-left: 6px solid {daily_colors['primary']};
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 180px;
    }}
    .kpi-card:hover {{
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.5);
        border-left-width: 8px;
    }}
    .kpi-label {{
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
        font-weight: 700;
        color: #999999;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 0.5rem;
    }}
    .kpi-number {{
        font-family: 'Inter', sans-serif;
        font-size: 5.5rem;
        font-weight: 900;
        color: {daily_colors['primary']};
        line-height: 1;
        margin: 0.5rem 0;
        text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
    }}
    .kpi-subtitle {{
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        color: #cccccc;
        margin-top: 0.5rem;
    }}

    @media (max-width: 768px) {{
        .kpi-card {{
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            min-height: 140px;
        }}
        .kpi-label {{
            font-size: 0.75rem;
            letter-spacing: 1px;
            margin-bottom: 0.25rem;
        }}
        .kpi-number {{
            font-size: 2.5rem;
            margin: 0.25rem 0;
        }}
        .kpi-subtitle {{
            font-size: 0.7rem;
            margin-top: 0.25rem;
        }}
    }}

    /* Member tile styling with CSS tooltip */
    .member-tile {{
        display: inline-block;
        padding: 0.5rem 1rem;
        margin: 0.25rem;
        border: 1px solid;
        border-radius: 4px;
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        cursor: pointer;
        position: relative;
        transition: all 0.2s ease;
    }}

    .member-tile:hover {{
        transform: scale(1.05);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    }}

    /* Tooltip styling */
    .member-tile::after {{
        content: attr(data-tooltip);
        position: absolute;
        bottom: 125%;
        left: 50%;
        transform: translateX(-50%);
        background-color: #2a2a2a;
        color: #ffffff;
        padding: 0.5rem 0.75rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 400;
        text-transform: none;
        letter-spacing: normal;
        white-space: nowrap;
        border: 1px solid #444;
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.2s ease, visibility 0.2s ease;
        pointer-events: none;
        z-index: 1000;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.5);
    }}

    .member-tile::before {{
        content: '';
        position: absolute;
        bottom: 115%;
        left: 50%;
        transform: translateX(-50%);
        border: 5px solid transparent;
        border-top-color: #2a2a2a;
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.2s ease, visibility 0.2s ease;
        pointer-events: none;
        z-index: 1000;
    }}

    .member-tile:hover::after,
    .member-tile:hover::before {{
        opacity: 1;
        visibility: visible;
    }}

    /* Monthly attendance matrix — status colors match KPI / member-tile accents */
    .monthly-attendance-table-wrap {{
        overflow-x: auto;
        margin: 0.35rem 0 1.25rem 0;
        width: 100%;
    }}
    .monthly-attendance-table {{
        width: 100%;
        border-collapse: collapse;
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
    }}
    .monthly-attendance-table th {{
        text-align: left;
        padding: 0.65rem 0.75rem;
        border-bottom: 2px solid rgba(255, 255, 255, 0.12);
        color: #999;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 0.72rem;
        white-space: nowrap;
    }}
    .monthly-attendance-table td {{
        padding: 0.55rem 0.75rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        color: #e8e8e8;
    }}
    .monthly-attendance-table th:nth-child(1),
    .monthly-attendance-table td:nth-child(1),
    .monthly-attendance-table th:nth-child(2),
    .monthly-attendance-table td:nth-child(2) {{
        max-width: 7.5rem;
        width: 1%;
        overflow: hidden;
        vertical-align: top;
    }}
    .monthly-attendance-table .monthly-trunc-details {{
        max-width: 100%;
    }}
    .monthly-attendance-table .monthly-trunc-summary {{
        cursor: pointer;
        list-style: none;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100%;
        color: #e0e0e0;
        font-weight: 400;
        font-size: inherit;
        text-transform: none;
        border-bottom: none;
        letter-spacing: normal;
    }}
    .monthly-attendance-table .monthly-trunc-summary::-webkit-details-marker {{
        display: none;
    }}
    .monthly-attendance-table .monthly-trunc-full {{
        display: block;
        margin-top: 0.35rem;
        padding-top: 0.35rem;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        color: #e0e0e0;
        font-weight: 400;
        white-space: normal;
        word-break: break-word;
        line-height: 1.3;
        text-transform: none;
    }}
    .monthly-attendance-table th:nth-child(3),
    .monthly-attendance-table td:nth-child(3) {{
        max-width: 5.75rem;
        width: 1%;
        white-space: nowrap;
        padding-left: 0.45rem;
        padding-right: 0.45rem;
        font-size: 0.82rem;
    }}
    .monthly-attendance-table th:nth-child(n+4),
    .monthly-attendance-table td:nth-child(n+4) {{
        max-width: 3.75rem;
        width: 1%;
        white-space: nowrap;
        text-align: center;
        padding: 0.45rem 0.35rem;
        font-size: 0.82rem;
    }}
    .monthly-attendance-table .monthly-attendance-rate-cell span {{
        font-weight: 700;
    }}
    .monthly-status-regular {{
        color: #2ecc71;
        font-weight: 700;
    }}
    .monthly-status-irregular {{
        color: #e67e22;
        font-weight: 700;
    }}
    .monthly-status-followup {{
        color: #f39c12;
        font-weight: 700;
    }}
    /* Health column: sheet / tile statuses (match member-tile border colors) */
    .monthly-health-tile-new {{
        color: #3498db;
        font-weight: 700;
    }}
    .monthly-health-tile-red {{
        color: #e74c3c;
        font-weight: 700;
    }}
    .monthly-health-tile-graduated {{
        color: #9b59b6;
        font-weight: 700;
    }}

</style>
""", unsafe_allow_html=True)

# Main app content
st.title("🏥 NWST Health")

# Get page from query parameters
query_params = st.query_params
current_page = _qp_first(query_params.get("page"), "cg")

# Page navigation — sidebar buttons (styled to match CHECK IN attendance_app)
with st.sidebar:
    st.markdown(f"""
<style>
    [data-testid="stSidebar"] .stButton > button {{
        background-color: transparent !important;
        color: {daily_colors['primary']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.2s ease !important;
    }}
    [data-testid="stSidebar"] .stButton > button:hover {{
        background-color: {daily_colors['primary']} !important;
        color: #000000 !important;
        transform: scale(1.02) !important;
    }}
    [data-testid="stSidebar"] .stButton > button[kind="primary"] {{
        background-color: {daily_colors['primary']} !important;
        color: #000000 !important;
        border: 2px solid {daily_colors['primary']} !important;
    }}
    [data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
    }}
</style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <h3 style="color: {daily_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 700; letter-spacing: 1px; font-size: 0.9rem; text-transform: uppercase;">
        Navigate
    </h3>
    """, unsafe_allow_html=True)

    _PAGE_LABELS = ["CG Health", "Ministry Health", "Analytics"]
    _PAGE_KEYS   = ["cg",        "ministry",         "analytics"]
    for _label, _key in zip(_PAGE_LABELS, _PAGE_KEYS):
        _is_active = current_page == _key
        if st.button(
            _label,
            type="primary" if _is_active else "secondary",
            use_container_width=True,
            key=f"sidebar_nav_{_key}",
            disabled=_is_active,
        ):
            st.query_params["page"] = _key
            st.rerun()
        st.markdown('<div style="height: 1.6rem;"></div>', unsafe_allow_html=True)

# ========== CG HEALTH PAGE ==========
if current_page == "cg":
    # Sync button and status
    sync_col1, sync_col2, sync_col3 = st.columns([1, 2, 1])
    with sync_col2:
        if st.button("🔄 Sync from Google Sheets", use_container_width=True):
            client = get_google_sheet_client()
            if not client:
                st.error("❌ Google credentials not configured. Please add 'google' to your Streamlit secrets.")
            else:
                try:
                    spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")

                    # Sync CG Combined data
                    worksheet = spreadsheet.worksheet("CG Combined")
                    data = worksheet.get_all_values()

                    if data:
                        df = pd.DataFrame(data[1:], columns=data[0])

                        # Cache in Redis
                        redis = get_redis_client()
                        if redis:
                            cache_data = {
                                "columns": df.columns.tolist(),
                                "rows": df.values.tolist()
                            }
                            redis.set("nwst_cg_combined_data", json.dumps(cache_data), ex=300)

                        # Sync Ministries Combined data
                        try:
                            ministries_worksheet = spreadsheet.worksheet("Ministries Combined")
                            ministries_data = ministries_worksheet.get_all_values()

                            if ministries_data:
                                ministries_df = pd.DataFrame(ministries_data[1:], columns=ministries_data[0])

                                # Cache in Redis
                                redis = get_redis_client()
                                if redis:
                                    cache_data = {
                                        "columns": ministries_df.columns.tolist(),
                                        "rows": ministries_df.values.tolist()
                                    }
                                    redis.set("nwst_ministries_combined_data", json.dumps(cache_data), ex=300)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Ministries data: {e}")

                        # Sync Attendance data
                        try:
                            att_worksheet = spreadsheet.worksheet("Attendance")
                            att_data = att_worksheet.get_all_values()

                            if att_data and len(att_data) >= 2:
                                att_headers = att_data[0]
                                att_df = pd.DataFrame(att_data[1:], columns=att_headers)

                                # Load CG Combined to get Name and Cell mapping
                                cg_worksheet = spreadsheet.worksheet("CG Combined")
                                cg_data = cg_worksheet.get_all_values()
                                if cg_data and len(cg_data) >= 2:
                                    cg_headers = cg_data[0]
                                    cg_df = pd.DataFrame(cg_data[1:], columns=cg_headers)

                                    # Find name and cell columns in CG Combined
                                    cg_name_col = None
                                    cg_cell_col = None
                                    for col in cg_df.columns:
                                        if col.lower().strip() in ['name', 'member name', 'member']:
                                            cg_name_col = col
                                        if col.lower().strip() in ['cell', 'group']:
                                            cg_cell_col = col

                                    if not cg_name_col:
                                        cg_name_col = cg_df.columns[0]

                                    # Calculate attendance stats using Name + Cell key
                                    attendance_stats = {}

                                    # Find name column in attendance (usually column A)
                                    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None

                                    # Create a mapping of attendance names from column A only
                                    if att_name_col:
                                        for att_name in att_df[att_name_col].unique():
                                            if pd.isna(att_name) or att_name == '':
                                                continue

                                            att_name_str = str(att_name).strip()
                                            member_att_data = att_df[att_df[att_name_col] == att_name]

                                            # Count attendance only from columns D onwards (skip A, B, C)
                                            attendance_count = 0
                                            total_services = 0

                                            for col_idx, col in enumerate(att_df.columns):
                                                if col_idx >= 3:  # Skip columns A (0), B (1), C (2)
                                                    total_services += 1
                                                    values = member_att_data[col].values
                                                    if len(values) > 0 and str(values[0]).strip() == '1':
                                                        attendance_count += 1

                                            # Find the cell for this person from CG Combined
                                            cell_info = ""
                                            if cg_name_col and cg_cell_col:
                                                cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
                                                if not cg_match.empty:
                                                    cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

                                            # Use Name + Cell as key
                                            if total_services > 0:
                                                key = att_name_str + cell_info
                                                attendance_stats[key] = {
                                                    'attendance': attendance_count,
                                                    'total': total_services,
                                                    'percentage': round(attendance_count / total_services * 100) if total_services > 0 else 0
                                                }

                                # Cache attendance stats in Redis
                                if redis:
                                    redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Attendance data: {e}")

                        # Sync Cell Health data (single source of truth for KPI cards and PDF reports)
                        try:
                            # Load Historical Cell Status for WoW deltas
                            hist_df = None
                            try:
                                hist_ws = spreadsheet.worksheet(NWST_HISTORICAL_CELL_STATUS_TAB)
                                hist_data = hist_ws.get_all_values()
                                if hist_data and len(hist_data) >= 2:
                                    hist_df = pd.DataFrame(hist_data[1:], columns=hist_data[0])
                            except WorksheetNotFound:
                                pass

                            # Load cell-to-zone map from Attendance sheet Key Values tab
                            cell_to_zone_map = {"all": "PSQ"}
                            try:
                                att_sheet_id = os.getenv("NWST_ATTENDANCE_SHEET_ID", "").strip() or "1o647tyrjusQmfoj3ZQITWL3LkcMIwMEilwaQoxyfrNc"
                                att_spreadsheet = client.open_by_key(att_sheet_id)
                                kv_ws = att_spreadsheet.worksheet("Key Values")
                                kv_data = kv_ws.get_all_values()
                                if kv_data and len(kv_data) > 1:
                                    for row in kv_data[1:]:
                                        if len(row) >= 3:
                                            cn = row[0].strip()
                                            zn = row[2].strip()
                                            if cn and zn:
                                                cell_to_zone_map[cn.lower()] = zn
                            except Exception:
                                pass

                            # Calculate and cache cell health
                            if redis:
                                calculate_and_cache_cell_health(redis, df, hist_df, cell_to_zone_map)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Cell Health data: {e}")

                        # Sync Ministry Health data (Historical Ministry Status → Redis)
                        try:
                            ministry_hist_df_sync = None
                            try:
                                mh_ws = spreadsheet.worksheet(NWST_HISTORICAL_MINISTRY_STATUS_TAB)
                                mh_data = mh_ws.get_all_values()
                                if mh_data and len(mh_data) >= 2:
                                    ministry_hist_df_sync = pd.DataFrame(mh_data[1:], columns=mh_data[0])
                            except WorksheetNotFound:
                                pass

                            _min_df_for_sync = locals().get("ministries_df")
                            if _min_df_for_sync is None or (hasattr(_min_df_for_sync, "empty") and _min_df_for_sync.empty):
                                _min_df_for_sync = get_ministries_data()
                            if redis and _min_df_for_sync is not None and not _min_df_for_sync.empty:
                                calculate_and_cache_ministry_health(redis, _min_df_for_sync, ministry_hist_df_sync)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Ministry Health data: {e}")

                        if redis:
                            st.success("✅ Attendance updated successfully!")

                            # Store last sync time in Malaysian time
                            myt = timezone(timedelta(hours=8))
                            sync_time_myt = datetime.now(myt)
                            sync_time_str = sync_time_myt.strftime("%Y-%m-%d %H:%M:%S MYT")
                            redis.set("nwst_last_sync_time", sync_time_str)
                            checkin_sid = (CHECKIN_ATTENDANCE_SHEET_ID or "").strip()
                            if checkin_sid and client:
                                try:
                                    if _nwst_accent_cfg_mod is None:
                                        _accent_overrides_from_project_config()
                                    if _nwst_accent_cfg_mod:
                                        _nwst_accent_cfg_mod.refresh_theme_override_shared_cache(
                                            redis, client, checkin_sid
                                        )
                                except Exception:
                                    pass
                        else:
                            st.warning("⚠️ Redis not configured, but data loaded from Google Sheets.")

                        if redis:
                            try:
                                redis.delete(NWST_REDIS_ATTENDANCE_CHART_GRID_KEY)
                            except Exception:
                                pass

                        # Clear cache to force reload
                        st.cache_data.clear()
                    else:
                        st.error(
                            "No data found in the **CG Combined** tab. "
                            "Check that: (1) the tab exists and is named exactly 'CG Combined', "
                            "(2) it has at least a header row and data rows, "
                            "(3) the service account has Editor access to the spreadsheet."
                        )
                except Exception as e:
                    st.error(f"Error syncing data: {e}")

    # Display last sync time
    redis = get_redis_client()
    if redis:
        try:
            last_sync = redis.get("nwst_last_sync_time")
            if last_sync:
                st.markdown(f"""
<style>
@media (min-width: 768px) {{ .nwst-last-synced {{ margin-top: 0.25rem !important; }} }}
@media (max-width: 767px) {{ .nwst-last-synced {{ margin-top: -0.5rem !important; }} }}
</style>
<p class='nwst-last-synced' style='text-align: center; color: #999; font-size: 0.85rem;'>Last synced: {last_sync}</p>
""", unsafe_allow_html=True)
        except Exception:
            pass

    st.markdown("")
    render_birthdays_notice_board(daily_colors)
    try:
        newcomers_df = get_newcomers_data()
        attendance_stats = get_attendance_data()

        if not newcomers_df.empty:
            # Get unique cell names for filtering
            cell_columns = [col for col in newcomers_df.columns if 'cell' in col.lower() or 'group' in col.lower()]

            # Build cell filter options
            cell_options = ["All"]
            if cell_columns:
                unique_cells = sorted(newcomers_df[cell_columns[0]].unique().tolist())
                cell_options.extend(unique_cells)

            # Filter section with dynamic options
            cell_filter = st.selectbox(
                "Cell",
                options=cell_options,
                key="global_cell_filter",
            )

            st.markdown("---")

            # Apply filters
            display_df = newcomers_df.copy()

            # Apply cell filter
            if cell_filter != "All" and cell_columns:
                display_df = display_df[display_df[cell_columns[0]] == cell_filter]

            # CELL HEALTH — quick view (Historical Cell Status WoW + live CG Combined mix)
            _render_cg_cell_health_section(display_df, daily_colors, cell_filter, attendance_stats)

            with st.expander("👤 INDIVIDUAL ATTENDANCE", expanded=False):
                if not display_df.empty:
                    st.markdown("")
                    att_df_m, cg_df_m = load_attendance_and_cg_dataframes()
                    if cg_df_m is not None:
                        if att_df_m is None:
                            att_df_m = pd.DataFrame()
                        status_hist_df = load_status_historical_dataframe()
                        monthly_status_df = build_monthly_member_status_table(
                            display_df, att_df_m, cg_df_m, status_hist_df
                        )
                        if monthly_status_df is not None and not monthly_status_df.empty:
                            _cg_individual_attendance_fragment(monthly_status_df, daily_colors, cell_filter)
                        else:
                            st.info(
                                "No individual attendance breakdown yet. Check that Attendance row 1 from column D has parseable dates "
                                "(e.g. DD/MM/YYYY or MM/DD/YYYY)."
                            )
                    else:
                        st.info("Could not load the Attendance sheet for the individual attendance table.")
                else:
                    st.info("No member data to show individual attendance.")

            with st.expander("📊 CELL BREAKDOWN & ATTENDANCE", expanded=False):
                _nwst_cell_breakdown_fragment(display_df, daily_colors, cell_filter)
                st.markdown("---")
                st.markdown("")
                if display_df is None or display_df.empty:
                    st.info("No member data to show cell attendance charts.")
                else:
                    render_nwst_service_attendance_rate_charts(
                        display_df,
                        daily_colors,
                        tab_each_cell_when_all=(cell_filter == "All"),
                    )

            with st.expander("📋 DETAILED MEMBERS", expanded=False):
                _render_cg_detailed_members_section(display_df, daily_colors)

            with st.expander("👔 LEADERSHIP", expanded=False):
                _render_cg_leadership_section(display_df, cell_filter, cell_columns, daily_colors)

            with st.expander("⛪ MINISTRY", expanded=False):
                _render_cg_ministry_section(display_df, daily_colors)

        else:
            st.warning("No data found. Click 'Sync from Google Sheets' to load data.")

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ========== MINISTRY HEALTH PAGE ==========
elif current_page == "ministry":
    st.markdown("")
    try:
        ministries_df = get_ministries_data()
        attendance_stats = get_attendance_data()

        if not ministries_df.empty:
            # Detect ministry column for the filter
            ministry_col_options = [
                col for col in ministries_df.columns
                if "ministry" in col.lower() or "department" in col.lower()
            ]
            # Also detect cell columns (same structure as CG Combined — used by leadership section)
            mc_cell_columns = [
                col for col in ministries_df.columns
                if "cell" in col.lower() or "group" in col.lower()
            ]

            # Build ministry filter options from the role columns (Hype Role, Frontlines Role, etc.).
            # The "Ministry Department" column exists in the sheet but is unused/empty;
            # actual membership is determined by whether the role column is non-empty.
            mc_ministry_options = ["All"]
            for _min_name, _min_col in _MINISTRY_ROLE_COLS.items():
                if _min_col in ministries_df.columns:
                    has_members = ministries_df[_min_col].notna() & (
                        ministries_df[_min_col].astype(str).str.strip() != ""
                    )
                    if has_members.any():
                        mc_ministry_options.append(_min_name)

            mc_ministry_filter = st.selectbox(
                "Ministry",
                options=mc_ministry_options,
                key="mc_ministry_filter",
            )

            st.markdown("---")

            # Apply ministry filter — filter rows where the selected ministry's role column is non-empty.
            display_df = ministries_df.copy()
            if mc_ministry_filter != "All":
                role_col_name = _MINISTRY_ROLE_COLS.get(mc_ministry_filter, f"{mc_ministry_filter} Role")
                if role_col_name in display_df.columns:
                    display_df = display_df[
                        display_df[role_col_name].notna()
                        & (display_df[role_col_name].astype(str).str.strip() != "")
                    ]

            # STATUS KPI CARDS — same layout as CG Health (New / Regular / Irregular / Follow Up / Red / Graduated)
            # Pass mc_ministry_filter as the cell_filter arg so _cell_scoped layout is applied when a
            # specific ministry is selected (4-column row).
            # hist_df_override: use Historical Ministry Status instead of Historical Cell Status for WoW.
            # redis_cache_key_override: read/write ministry health cache independently from cell health.
            _mc_hist_df = load_historical_ministry_status_dataframe()
            _render_cg_cell_health_section(
                display_df,
                daily_colors,
                mc_ministry_filter,
                attendance_stats,
                hist_df_override=_mc_hist_df,
                redis_cache_key_override=REDIS_MINISTRY_HEALTH_KEY,
            )

            with st.expander("👤 INDIVIDUAL ATTENDANCE", expanded=False):
                if not display_df.empty:
                    st.markdown("")
                    att_df_m, _ = load_attendance_and_cg_dataframes()
                    # Use ministries_df as the roster (cg_df) so name lookups resolve correctly
                    if not ministries_df.empty:
                        if att_df_m is None:
                            att_df_m = pd.DataFrame()
                        status_hist_df = load_status_historical_dataframe()
                        monthly_status_df = build_monthly_member_status_table(
                            display_df, att_df_m, ministries_df, status_hist_df
                        )
                        if monthly_status_df is not None and not monthly_status_df.empty:
                            _cg_individual_attendance_fragment(monthly_status_df, daily_colors, mc_ministry_filter)
                        else:
                            st.info(
                                "No individual attendance breakdown yet. Check that Attendance row 1 "
                                "from column D has parseable dates (e.g. DD/MM/YYYY or MM/DD/YYYY)."
                            )
                    else:
                        st.info("Could not load data for the individual attendance table.")
                else:
                    st.info("No member data to show individual attendance.")

            with st.expander("📊 CELL BREAKDOWN & ATTENDANCE", expanded=False):
                _nwst_cell_breakdown_fragment(display_df, daily_colors, mc_ministry_filter)
                st.markdown("---")
                st.markdown("")
                if display_df is None or display_df.empty:
                    st.info("No member data to show attendance charts.")
                else:
                    # For ministry health, aggregate all members into one trend line per ministry
                    # instead of splitting by cell group (which produces irrelevant cell-level lines).
                    _mc_agg_label = mc_ministry_filter if mc_ministry_filter != "All" else "All Ministries"
                    render_nwst_service_attendance_rate_charts(
                        display_df,
                        daily_colors,
                        tab_each_cell_when_all=False,
                        aggregate_label=_mc_agg_label,
                    )

            with st.expander("📋 DETAILED MEMBERS", expanded=False):
                _render_cg_detailed_members_section(display_df, daily_colors)

            with st.expander("👔 LEADERSHIP", expanded=False):
                _render_cg_leadership_section(display_df, mc_ministry_filter, mc_cell_columns, daily_colors)

            with st.expander("⛪ MINISTRY", expanded=False):
                _render_cg_ministry_section(display_df, daily_colors)

        else:
            st.warning("No ministries data found. Click 'Sync from Google Sheets' on the CG Health tab to load data.")

    except Exception as e:
        st.error(f"Error loading ministry data: {e}")

elif current_page == "analytics":
    nwst_analytics_colors = {
        "primary": daily_colors["primary"],
        "light": daily_colors["light"],
        "background": "#000000",
        "text": "#ffffff",
        "text_muted": "#999999",
        "card_bg": "#0a0a0a",
        "border": daily_colors["primary"],
    }
    st.markdown(
        f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {nwst_analytics_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            Attendance Analytics
        </h1>
        <p style="color: {nwst_analytics_colors['text_muted']}; font-size: 0.9rem; margin: 0;">Saturday Service Attendance Trends</p>
    </div>
    """,
        unsafe_allow_html=True,
    )
    render_nwst_analytics_page(nwst_analytics_colors)
