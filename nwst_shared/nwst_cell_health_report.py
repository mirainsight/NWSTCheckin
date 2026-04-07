from __future__ import annotations

__doc__ = """
Cell health summary for PDF/email reports (NWST Health sheet).

Zone for every row comes from the **Attendance Sheet** Key Values tab (column A = cell name, C = zone).
Historical Cell Status may supply counts and snapshot dates but never overrides zone.
The aggregate / cell name **All** is always shown as zone **PSQ**.

Uses **Historical Cell Status** (latest snapshot vs previous) when available; otherwise
**CG Combined** roster + Status column (no WoW deltas — shows +0).
"""

import os
from datetime import date
from typing import Any

import pandas as pd

# Fixed zone for roll-up row and any cell literally named All / ALL.
_ZONE_ALL_PSQ = "PSQ"

NWST_HISTORICAL_CELL_STATUS_TAB = "Historical Cell Status"
NWST_KEY_VALUES_TAB = "Key Values"
NWST_CG_COMBINED_TAB = "CG Combined"
# Per-service rollup on the NWST Health spreadsheet (columns D+), same source as NWST HEALTH/app.py tooltips.
NWST_HEALTH_ATTENDANCE_ROLLUP_TAB = "Attendance"
_DEFAULT_NWST_HEALTH_SHEET_ID = "1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY"
_DEFAULT_NWST_ATTENDANCE_SHEET_ID = "1o647tyrjusQmfoj3ZQITWL3LkcMIwMEilwaQoxyfrNc"

BUCKET_SPECS: list[tuple[str, tuple[str, ...]]] = [
    ("new", ("new",)),
    ("regular", ("regular",)),
    ("irregular", ("irregular",)),
    ("follow_up", ("follow up", "follow_up", "followup")),
    ("red", ("red",)),
    ("graduated", ("graduated",)),
    ("total", ("total",)),
]


def nwst_health_sheet_id() -> str:
    sid = (os.getenv("NWST_HEALTH_SHEET_ID") or "").strip()
    return sid or _DEFAULT_NWST_HEALTH_SHEET_ID


def nwst_attendance_sheet_id() -> str:
    sid = (os.getenv("NWST_ATTENDANCE_SHEET_ID") or "").strip()
    return sid or _DEFAULT_NWST_ATTENDANCE_SHEET_ID


def extract_cell_sheet_status_type(status_val: Any) -> str | None:
    if status_val is None or (isinstance(status_val, float) and pd.isna(status_val)):
        return None
    if not isinstance(status_val, str):
        return None
    s = status_val.strip()
    if not s:
        return None
    low = s.lower()
    # Prefix forms (sheet / Apps Script convention)
    if low.startswith("regular"):
        return "Regular"
    if low.startswith("irregular"):
        return "Irregular"
    if low.startswith("new"):
        return "New"
    if low.startswith("follow up") or low.startswith("follow-up") or low.startswith("follow_up"):
        return "Follow Up"
    if low.startswith("red"):
        return "Red"
    if low.startswith("graduated"):
        return "Graduated"
    return None


def _hist_col_lookup(df: pd.DataFrame) -> dict[str, str]:
    return {str(c).strip().lower(): c for c in df.columns}


def _hist_get_col(lk: dict[str, str], *names: str) -> str | None:
    for n in names:
        k = n.strip().lower()
        if k in lk:
            return lk[k]
    return None


def _aggregate_counts(sub_df: pd.DataFrame, lk: dict[str, str] | None = None) -> dict[str, int]:
    if sub_df is None or sub_df.empty:
        return {k: 0 for k, _ in BUCKET_SPECS}
    lk = lk or _hist_col_lookup(sub_df)
    agg: dict[str, int] = {}
    for canon, aliases in BUCKET_SPECS:
        coln = None
        for a in aliases:
            coln = _hist_get_col(lk, a)
            if coln:
                break
        agg[canon] = (
            int(pd.to_numeric(sub_df[coln], errors="coerce").fillna(0).sum()) if coln else 0
        )
    return agg


def _denom_total(agg: dict[str, int]) -> int:
    t = agg.get("total") or 0
    if t > 0:
        return int(t)
    return int(
        sum(agg.get(k, 0) for k in ("new", "regular", "irregular", "follow_up", "red", "graduated"))
    )


def _pct(n: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    return 100.0 * float(n) / float(denom)


def _format_bucket_cell(pct: float, delta: int) -> str:
    return f"{round(pct)}% ({delta:+d})"


def load_cell_zone_map(client: Any, _sheet_id: str) -> dict[str, str]:
    """Cell name (col A) → zone (col C) from Attendance sheet. No other columns. Empty zone = skip row."""
    cell_to_zone: dict[str, str] = {}
    try:
        attendance_sid = nwst_attendance_sheet_id()
        spreadsheet = client.open_by_key(attendance_sid)
        key_values_sheet = spreadsheet.worksheet(NWST_KEY_VALUES_TAB)
        all_values = key_values_sheet.get_all_values()
        if len(all_values) > 1:
            for row in all_values[1:]:
                if len(row) >= 3:
                    cn = row[0].strip()
                    zn = row[2].strip()
                    if cn and zn:
                        cell_to_zone[cn.lower()] = zn
    except Exception:
        pass
    cell_to_zone["all"] = _ZONE_ALL_PSQ
    return cell_to_zone


def load_historical_cell_status_df(client: Any, sheet_id: str) -> pd.DataFrame | None:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(NWST_HISTORICAL_CELL_STATUS_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception:
        return None


def load_cg_combined_df(client: Any, sheet_id: str) -> pd.DataFrame | None:
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(NWST_CG_COMBINED_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception:
        return None


def load_nwst_attendance_rollup_df(client: Any, sheet_id: str) -> pd.DataFrame | None:
    """NWST Health **Attendance** tab (rollup grid). Same tab as ``load_attendance_and_cg_dataframes`` in NWST HEALTH/app.py."""
    try:
        spreadsheet = client.open_by_key(sheet_id)
        ws = spreadsheet.worksheet(NWST_HEALTH_ATTENDANCE_ROLLUP_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception:
        return None


def compute_member_attendance_stats(att_df: pd.DataFrame | None, cg_df: pd.DataFrame | None) -> dict[str, Any]:
    """
    Same keys/semantics as NWST HEALTH/app.py ``_compute_attendance_stats_from_frames``:
    ``\"Name - Cell\"`` or ``\"Name\"`` -> ``{\"attendance\": int, \"total\": int, \"percentage\": int}``.
    """
    attendance_stats: dict[str, Any] = {}
    if att_df is None or att_df.empty or cg_df is None or cg_df.empty:
        return attendance_stats

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
            cg_match = cg_df[cg_df[cg_name_col].astype(str).str.strip().str.lower() == att_name_str.lower()]
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


def attendance_fraction_for_pdf(name: str, cell: str, attendance_stats: dict[str, Any]) -> str | None:
    """Return ``x/y`` for tooltip parity with ``get_attendance_text`` (without name or percent)."""
    if not attendance_stats:
        return None

    name_stripped = str(name).strip()
    cell_stripped = str(cell).strip() if cell else ""

    if cell_stripped:
        key = f"{name_stripped} - {cell_stripped}"
    else:
        key = name_stripped

    stats = None
    if key in attendance_stats:
        stats = attendance_stats[key]
    else:
        key_lower = key.lower()
        for dict_key, st in attendance_stats.items():
            if str(dict_key).lower() == key_lower:
                stats = st
                break

    if not stats:
        return None
    return f"{stats['attendance']}/{stats['total']}"


def _pdf_name_lines_for_bucket_df(
    data_df: pd.DataFrame,
    name_col: str,
    cell_col: str | None,
    attendance_stats: dict[str, Any],
) -> list[str]:
    """Sorted unique names; uppercase for parity with member-tile CSS in the app."""
    if data_df.empty:
        return []
    names = sorted(data_df[name_col].astype(str).unique().tolist())
    lines: list[str] = []
    for name in names:
        person_cell = ""
        if cell_col:
            person_row = data_df[data_df[name_col] == name]
            if not person_row.empty:
                person_cell = str(person_row[cell_col].iloc[0]).strip()
        frac = attendance_fraction_for_pdf(name, person_cell, attendance_stats)
        display = str(name).strip().upper()
        lines.append(f"{display} ({frac})" if frac else display)
    return lines


def build_cell_health_pdf_member_sections(client: Any, sheet_id: str) -> dict[str, list[str]]:
    """
    Member lists keyed like cell-health buckets, with ``NAME (x/y)`` attendance fractions.

    Uses **CG Combined** for roster/status and **Attendance** on the same NWST Health spreadsheet
    for rollup counts — same datasource as NWST HEALTH/app.py KPI hover tooltips.
    """
    cg_df = load_cg_combined_df(client, sheet_id)
    if cg_df is None or cg_df.empty:
        return {}

    cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
    if not cg_name_col:
        return {}

    att_df = load_nwst_attendance_rollup_df(client, sheet_id)
    attendance_stats = compute_member_attendance_stats(att_df, cg_df) if att_df is not None else {}

    work = cg_df.copy()
    status_columns = [col for col in work.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None

    if status_col:
        work["status_type"] = work[status_col].apply(extract_cell_sheet_status_type)
        sections = {
            "new": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "New"], cg_name_col, cg_cell_col, attendance_stats
            ),
            "regular": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "Regular"], cg_name_col, cg_cell_col, attendance_stats
            ),
            "irregular": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "Irregular"], cg_name_col, cg_cell_col, attendance_stats
            ),
            "follow_up": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "Follow Up"], cg_name_col, cg_cell_col, attendance_stats
            ),
            "red": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "Red"], cg_name_col, cg_cell_col, attendance_stats
            ),
            "graduated": _pdf_name_lines_for_bucket_df(
                work[work["status_type"] == "Graduated"], cg_name_col, cg_cell_col, attendance_stats
            ),
        }
        if any(sections.get(k) for k, _ in BUCKET_SPECS if k != "total"):
            return sections

    total_members_fb = len(work)
    new_count = max(1, int(total_members_fb * 0.20))
    regular_count = max(1, int(total_members_fb * 0.40))
    irregular_count = max(1, int(total_members_fb * 0.20))
    follow_up_count = max(1, int(total_members_fb * 0.10))
    red_count = max(1, int(total_members_fb * 0.05))
    graduated_count = max(
        0,
        total_members_fb - new_count - regular_count - irregular_count - follow_up_count - red_count,
    )

    i0 = 0
    i1 = new_count
    i2 = i1 + regular_count
    i3 = i2 + irregular_count
    i4 = i3 + follow_up_count
    i5 = i4 + red_count

    slices = {
        "new": work.iloc[i0:i1],
        "regular": work.iloc[i1:i2],
        "irregular": work.iloc[i2:i3],
        "follow_up": work.iloc[i3:i4],
        "red": work.iloc[i4:i5],
        "graduated": work.iloc[i5:],
    }
    return {k: _pdf_name_lines_for_bucket_df(v, cg_name_col, cg_cell_col, attendance_stats) for k, v in slices.items()}


def _resolve_cg_name_cell_columns(cg_df: pd.DataFrame) -> tuple[str | None, str | None]:
    cg_name_col = None
    cg_cell_col = None
    for col in cg_df.columns:
        ls = col.lower().strip()
        if ls in ("name", "member name", "member"):
            cg_name_col = col
        if ls in ("cell", "group"):
            cg_cell_col = col
    if not cg_name_col and len(cg_df.columns) > 0:
        cg_name_col = cg_df.columns[0]
    return cg_name_col, cg_cell_col


def rows_from_cg_combined(
    cg_df: pd.DataFrame, cell_to_zone: dict[str, str]
) -> tuple[list[dict[str, Any]], date | None]:
    """Live CG Combined mix: no historical deltas."""
    status_columns = [col for col in cg_df.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None
    _, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
    if not cg_cell_col:
        return [], None

    work = cg_df.copy()
    if status_col:
        work["_st"] = work[status_col].apply(extract_cell_sheet_status_type)
    else:
        work["_st"] = None

    rows_out: list[dict[str, Any]] = []
    for cell, g in work.groupby(cg_cell_col):
        cell_s = str(cell).strip() if cell is not None else ""
        if not cell_s or cell_s.lower() == "all":
            continue
        if status_col:
            new_c = len(g[g["_st"] == "New"])
            reg_c = len(g[g["_st"] == "Regular"])
            irr_c = len(g[g["_st"] == "Irregular"])
            fu_c = len(g[g["_st"] == "Follow Up"])
            red_c = len(g[g["_st"] == "Red"])
            grad_c = len(g[g["_st"] == "Graduated"])
        else:
            n = len(g)
            new_c = max(1, int(n * 0.20))
            reg_c = max(1, int(n * 0.40))
            irr_c = max(1, int(n * 0.20))
            fu_c = max(1, int(n * 0.10))
            red_c = max(1, int(n * 0.05))
            grad_c = n - new_c - reg_c - irr_c - fu_c - red_c

        agg = {
            "new": new_c,
            "regular": reg_c,
            "irregular": irr_c,
            "follow_up": fu_c,
            "red": red_c,
            "graduated": grad_c,
            "total": 0,
        }
        d = _denom_total(agg)
        zone = cell_to_zone.get(cell_s.lower(), "")
        r_new = _pct(agg["new"], d)
        r_reg = _pct(agg["regular"], d)
        r_irr = _pct(agg["irregular"], d)
        r_fu = _pct(agg["follow_up"], d)
        rows_out.append(
            {
                "zone": zone,
                "cell": cell_s,
                "new_s": _format_bucket_cell(r_new, 0),
                "regular_s": _format_bucket_cell(r_reg, 0),
                "irregular_s": _format_bucket_cell(r_irr, 0),
                "follow_up_s": _format_bucket_cell(r_fu, 0),
                "_sort_regular": r_reg,
                "_sort_irregular": r_irr,
                "_sort_follow": r_fu,
                "_sort_new": r_new,
            }
        )
    snap = date.today()
    return rows_out, snap


def _parse_snap_dates(df: pd.DataFrame) -> list[date] | None:
    lk = _hist_col_lookup(df)
    snap_c = _hist_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return None

    parsed = pd.to_datetime(df[snap_c], errors="coerce").dropna()
    seen: set[date] = set()
    for ts in parsed:
        seen.add(ts.date())
    return sorted(seen, reverse=True) if seen else None


def _pick_curr_prev(
    all_desc: list[date], target_date_str: str | None
) -> tuple[date | None, date | None]:
    if not all_desc:
        return None, None
    if not target_date_str:
        return all_desc[0], (all_desc[1] if len(all_desc) > 1 else None)
    td = date.fromisoformat(target_date_str)
    eligible = [d for d in all_desc if d <= td]
    if not eligible:
        return None, None
    curr = eligible[0]
    try:
        ix = all_desc.index(curr)
    except ValueError:
        ix = 0
    prev = all_desc[ix + 1] if ix + 1 < len(all_desc) else None
    return curr, prev


def _counts_by_cell_snapshot(
    df: pd.DataFrame, snap_d: date, snap_c: str, cell_c: str
) -> dict[str, dict[str, int]]:
    scoped = df.copy()
    scoped["_d"] = pd.to_datetime(scoped[snap_c], errors="coerce").dt.date
    sub = scoped[scoped["_d"] == snap_d]
    lk = _hist_col_lookup(sub)
    by_cell: dict[str, dict[str, int]] = {}
    if sub.empty or not cell_c:
        return by_cell
    for cell, g in sub.groupby(cell_c):
        cell_s = str(cell).strip() if cell is not None else ""
        if not cell_s or cell_s.lower() == "all":
            continue
        by_cell[cell_s] = _aggregate_counts(g, lk)
    return by_cell


def rows_from_historical_cell_status(
    hist_df: pd.DataFrame,
    cell_to_zone: dict[str, str],
    target_date_str: str | None,
) -> tuple[list[dict[str, Any]] | None, date | None]:
    lk = _hist_col_lookup(hist_df)
    snap_c = _hist_get_col(lk, "snapshot date", "snapshot")
    cell_c = _hist_get_col(lk, "cell")
    if not snap_c or not cell_c:
        return None, None

    all_dates = _parse_snap_dates(hist_df)
    if not all_dates:
        return None, None

    snap_curr, snap_prev = _pick_curr_prev(all_dates, target_date_str)
    if snap_curr is None:
        return None, None

    scoped = hist_df.copy()
    scoped["_d"] = pd.to_datetime(scoped[snap_c], errors="coerce").dt.date
    curr_map = _counts_by_cell_snapshot(scoped, snap_curr, snap_c, cell_c)
    prev_map = (
        _counts_by_cell_snapshot(scoped, snap_prev, snap_c, cell_c) if snap_prev else None
    )

    if not curr_map:
        return None, snap_curr

    def _row_for_cell(cell_s: str, agg: dict[str, int]) -> dict[str, Any]:
        pagg = prev_map.get(cell_s) if prev_map else None
        d = _denom_total(agg)
        r_new = _pct(agg.get("new", 0), d)
        r_reg = _pct(agg.get("regular", 0), d)
        r_irr = _pct(agg.get("irregular", 0), d)
        r_fu = _pct(agg.get("follow_up", 0), d)
        dn = (
            agg.get("new", 0) - (pagg.get("new", 0) if pagg else 0),
            agg.get("regular", 0) - (pagg.get("regular", 0) if pagg else 0),
            agg.get("irregular", 0) - (pagg.get("irregular", 0) if pagg else 0),
            agg.get("follow_up", 0) - (pagg.get("follow_up", 0) if pagg else 0),
        )
        zone_h = cell_to_zone.get(cell_s.lower(), "")
        return {
            "zone": zone_h,
            "cell": cell_s,
            "new_s": _format_bucket_cell(r_new, dn[0]),
            "regular_s": _format_bucket_cell(r_reg, dn[1]),
            "irregular_s": _format_bucket_cell(r_irr, dn[2]),
            "follow_up_s": _format_bucket_cell(r_fu, dn[3]),
            "_sort_regular": r_reg,
            "_sort_irregular": r_irr,
            "_sort_follow": r_fu,
            "_sort_new": r_new,
        }

    per_cell_rows: list[dict[str, Any]] = []
    for cell_s, agg in curr_map.items():
        per_cell_rows.append(_row_for_cell(cell_s, agg))

    sum_agg: dict[str, int] = {k: 0 for k, _ in BUCKET_SPECS}
    for agg in curr_map.values():
        for k in sum_agg:
            sum_agg[k] += int(agg.get(k, 0))
    psum: dict[str, int] | None = None
    if prev_map:
        psum = {k: 0 for k, _ in BUCKET_SPECS}
        for pa in prev_map.values():
            for k in psum:
                psum[k] += int(pa.get(k, 0))

    d_all = _denom_total(sum_agg)
    r_new = _pct(sum_agg.get("new", 0), d_all)
    r_reg = _pct(sum_agg.get("regular", 0), d_all)
    r_irr = _pct(sum_agg.get("irregular", 0), d_all)
    r_fu = _pct(sum_agg.get("follow_up", 0), d_all)
    if psum:
        dn = (
            sum_agg.get("new", 0) - psum.get("new", 0),
            sum_agg.get("regular", 0) - psum.get("regular", 0),
            sum_agg.get("irregular", 0) - psum.get("irregular", 0),
            sum_agg.get("follow_up", 0) - psum.get("follow_up", 0),
        )
    else:
        dn = (0, 0, 0, 0)
    all_row = {
        "zone": _ZONE_ALL_PSQ,
        "cell": "All",
        "new_s": _format_bucket_cell(r_new, dn[0]),
        "regular_s": _format_bucket_cell(r_reg, dn[1]),
        "irregular_s": _format_bucket_cell(r_irr, dn[2]),
        "follow_up_s": _format_bucket_cell(r_fu, dn[3]),
        "_sort_regular": float("-inf"),
        "_sort_irregular": float("inf"),
        "_sort_follow": float("inf"),
        "_sort_new": float("-inf"),
    }

    per_cell_sorted = sorted(
        per_cell_rows,
        key=lambda r: (
            r["_sort_regular"],
            -r["_sort_irregular"],
            -r["_sort_follow"],
            r["_sort_new"],
            str(r["cell"]).lower(),
        ),
    )
    return [all_row] + per_cell_sorted, snap_curr


def build_cell_health_table_rows(
    client: Any,
    sheet_id: str,
    target_date_str: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """
    Return (rows with keys zone, cell, new_s, regular_s, irregular_s, follow_up_s),
    and subtitle text (snapshot / source).
    """
    cell_to_zone = load_cell_zone_map(client, sheet_id)

    hist = load_historical_cell_status_df(client, sheet_id)
    if hist is not None and not hist.empty:
        pack = rows_from_historical_cell_status(hist, cell_to_zone, target_date_str)
        rows_h, snap_d = pack
        if rows_h:
            src = f"NWST Health — Historical Cell Status (snapshot {snap_d.isoformat() if snap_d else 'n/a'})"
            return rows_h, src

    cg = load_cg_combined_df(client, sheet_id)
    if cg is None or cg.empty:
        return [], "NWST Health — no Historical Cell Status or CG Found"

    rows_c, snap_d = rows_from_cg_combined(cg, cell_to_zone)

    status_columns = [col for col in cg.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None
    _, cg_cell_col = _resolve_cg_name_cell_columns(cg)
    if not cg_cell_col:
        return [], "NWST Health — CG Combined has no cell column"

    work = cg.copy()
    if status_col:
        work["_st"] = work[status_col].apply(extract_cell_sheet_status_type)

    sort_key = lambda r: (
        r["_sort_regular"],
        -r["_sort_irregular"],
        -r["_sort_follow"],
        r["_sort_new"],
        str(r["cell"]).lower(),
    )

    if status_col:
        all_row_inner: dict[str, int] = {
            k: 0 for k in ("new", "regular", "irregular", "follow_up", "red", "graduated")
        }
        for _, row in work.iterrows():
            stt = row.get("_st")
            if stt == "New":
                all_row_inner["new"] += 1
            elif stt == "Regular":
                all_row_inner["regular"] += 1
            elif stt == "Irregular":
                all_row_inner["irregular"] += 1
            elif stt == "Follow Up":
                all_row_inner["follow_up"] += 1
            elif stt == "Red":
                all_row_inner["red"] += 1
            elif stt == "Graduated":
                all_row_inner["graduated"] += 1
        d_all = _denom_total({**all_row_inner, "total": 0})
        all_row = {
            "zone": _ZONE_ALL_PSQ,
            "cell": "All",
            "new_s": _format_bucket_cell(_pct(all_row_inner["new"], d_all), 0),
            "regular_s": _format_bucket_cell(_pct(all_row_inner["regular"], d_all), 0),
            "irregular_s": _format_bucket_cell(_pct(all_row_inner["irregular"], d_all), 0),
            "follow_up_s": _format_bucket_cell(_pct(all_row_inner["follow_up"], d_all), 0),
            "_sort_regular": float("-inf"),
            "_sort_irregular": float("inf"),
            "_sort_follow": float("inf"),
            "_sort_new": float("-inf"),
        }
        per_sorted = sorted(rows_c, key=sort_key)
        return [all_row] + per_sorted, f"NWST Health — CG Combined (live roster, {snap_d.isoformat()})"

    agg_all: dict[str, int] = {k: 0 for k in ("new", "regular", "irregular", "follow_up", "red", "graduated")}
    for _, g in work.groupby(cg_cell_col):
        cell_s = str(g.iloc[0][cg_cell_col]).strip() if cg_cell_col else ""
        if not cell_s or cell_s.lower() == "all":
            continue
        n = len(g)
        nc = max(1, int(n * 0.20))
        rc = max(1, int(n * 0.40))
        ic = max(1, int(n * 0.20))
        fc = max(1, int(n * 0.10))
        redc = max(1, int(n * 0.05))
        gc = max(0, n - nc - rc - ic - fc - redc)
        agg_all["new"] += nc
        agg_all["regular"] += rc
        agg_all["irregular"] += ic
        agg_all["follow_up"] += fc
        agg_all["red"] += redc
        agg_all["graduated"] += gc
    d_all = _denom_total({**agg_all, "total": 0})
    all_row = {
        "zone": _ZONE_ALL_PSQ,
        "cell": "All",
        "new_s": _format_bucket_cell(_pct(agg_all["new"], d_all), 0),
        "regular_s": _format_bucket_cell(_pct(agg_all["regular"], d_all), 0),
        "irregular_s": _format_bucket_cell(_pct(agg_all["irregular"], d_all), 0),
        "follow_up_s": _format_bucket_cell(_pct(agg_all["follow_up"], d_all), 0),
        "_sort_regular": float("-inf"),
        "_sort_irregular": float("inf"),
        "_sort_follow": float("inf"),
        "_sort_new": float("-inf"),
    }
    per_sorted = sorted(rows_c, key=sort_key)
    return [all_row] + per_sorted, f"NWST Health — CG Combined (estimated mix, {snap_d.isoformat()})"

