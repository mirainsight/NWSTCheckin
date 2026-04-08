"""Upstash cache for Cell Health data (calculated in NWST HEALTH app.py).

Single source of truth for cell health metrics used by:
1. NWST HEALTH KPI cards (app.py)
2. PDF email reports (nwst_cell_health_report.py)

Data is refreshed when "Sync from Google Sheets" is triggered in app.py.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

REDIS_CELL_HEALTH_KEY = "nwst_cell_health_data_v1"
# Long TTL as safety net; sync refreshes the value.
CELL_HEALTH_REDIS_TTL_SEC = 86400 * 7  # 7 days


def get_cell_health_from_redis(redis_client: Any) -> dict[str, Any] | None:
    """
    Retrieve cell health data from Redis.

    Returns dict with keys:
    - "snapshot_date": ISO date string of when data was calculated
    - "all_row": dict with zone, cell, counts, percentages, deltas for "All" aggregate
    - "cell_rows": list of dicts, each with zone, cell, counts, percentages, deltas
    - "source": description string (e.g., "CG Combined + Historical Cell Status")

    Returns None if not found or error.
    """
    if not redis_client:
        return None
    try:
        raw = redis_client.get(REDIS_CELL_HEALTH_KEY)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None
        return data
    except (json.JSONDecodeError, TypeError, OSError, ValueError):
        return None


def store_cell_health_in_redis(redis_client: Any, cell_health_data: dict[str, Any]) -> bool:
    """
    Store cell health data in Redis.

    cell_health_data should have keys:
    - "snapshot_date": ISO date string
    - "all_row": dict for "All" aggregate row
    - "cell_rows": list of dicts for individual cells
    - "source": description string

    Returns True if successful, False otherwise.
    """
    if not redis_client:
        return False
    try:
        redis_client.set(
            REDIS_CELL_HEALTH_KEY,
            json.dumps(cell_health_data),
            ex=CELL_HEALTH_REDIS_TTL_SEC,
        )
        return True
    except Exception:
        return False


def build_cell_health_row(
    cell_name: str,
    zone: str,
    new_count: int,
    regular_count: int,
    irregular_count: int,
    follow_up_count: int,
    red_count: int,
    graduated_count: int,
    delta_new: int = 0,
    delta_regular: int = 0,
    delta_irregular: int = 0,
    delta_follow_up: int = 0,
) -> dict[str, Any]:
    """
    Build a cell health row dict with counts, percentages, and deltas.

    Percentages are calculated using all 6 buckets as denominator (matching app.py "All" view).
    """
    total = new_count + regular_count + irregular_count + follow_up_count + red_count + graduated_count

    def pct(n: int) -> float:
        return round(100.0 * n / total, 1) if total > 0 else 0.0

    return {
        "zone": zone,
        "cell": cell_name,
        "new_count": new_count,
        "regular_count": regular_count,
        "irregular_count": irregular_count,
        "follow_up_count": follow_up_count,
        "red_count": red_count,
        "graduated_count": graduated_count,
        "total_count": total,
        "new_pct": pct(new_count),
        "regular_pct": pct(regular_count),
        "irregular_pct": pct(irregular_count),
        "follow_up_pct": pct(follow_up_count),
        "red_pct": pct(red_count),
        "graduated_pct": pct(graduated_count),
        "delta_new": delta_new,
        "delta_regular": delta_regular,
        "delta_irregular": delta_irregular,
        "delta_follow_up": delta_follow_up,
    }


def format_bucket_cell_from_cache(pct: float, count: int, delta: int) -> str:
    """Format bucket cell as: **pct%** . count (+delta) with bold percentage for PDF."""
    return f"<b>{round(pct)}%</b> . {count} ({delta:+d})"


def build_table_rows_from_cache(cell_health_data: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """
    Convert cached cell health data to the table row format expected by PDF reports.

    Returns (rows, source_string) where rows have keys:
    - zone, cell, new_s, regular_s, irregular_s, follow_up_s
    - _sort_regular, _sort_irregular, _sort_follow, _sort_new (for sorting)
    """
    if not cell_health_data:
        return [], "No cached cell health data"

    all_row_data = cell_health_data.get("all_row")
    cell_rows_data = cell_health_data.get("cell_rows", [])
    snapshot_date = cell_health_data.get("snapshot_date", "unknown")
    source = cell_health_data.get("source", "Upstash cache")

    def _to_table_row(row_data: dict[str, Any]) -> dict[str, Any]:
        return {
            "zone": row_data.get("zone", ""),
            "cell": row_data.get("cell", ""),
            "new_s": format_bucket_cell_from_cache(
                row_data.get("new_pct", 0),
                row_data.get("new_count", 0),
                row_data.get("delta_new", 0),
            ),
            "regular_s": format_bucket_cell_from_cache(
                row_data.get("regular_pct", 0),
                row_data.get("regular_count", 0),
                row_data.get("delta_regular", 0),
            ),
            "irregular_s": format_bucket_cell_from_cache(
                row_data.get("irregular_pct", 0),
                row_data.get("irregular_count", 0),
                row_data.get("delta_irregular", 0),
            ),
            "follow_up_s": format_bucket_cell_from_cache(
                row_data.get("follow_up_pct", 0),
                row_data.get("follow_up_count", 0),
                row_data.get("delta_follow_up", 0),
            ),
            "_sort_regular": row_data.get("regular_pct", 0),
            "_sort_irregular": row_data.get("irregular_pct", 0),
            "_sort_follow": row_data.get("follow_up_pct", 0),
            "_sort_new": row_data.get("new_pct", 0),
        }

    rows: list[dict[str, Any]] = []

    # Add "All" row first with special sort values
    if all_row_data:
        all_table_row = _to_table_row(all_row_data)
        all_table_row["_sort_regular"] = float("-inf")
        all_table_row["_sort_irregular"] = float("inf")
        all_table_row["_sort_follow"] = float("inf")
        all_table_row["_sort_new"] = float("-inf")
        rows.append(all_table_row)

    # Add individual cell rows, sorted by regular% asc, irregular% desc, follow_up% desc, new% asc
    cell_table_rows = [_to_table_row(r) for r in cell_rows_data]
    cell_table_rows_sorted = sorted(
        cell_table_rows,
        key=lambda r: (
            r["_sort_regular"],
            -r["_sort_irregular"],
            -r["_sort_follow"],
            r["_sort_new"],
            str(r["cell"]).lower(),
        ),
    )
    rows.extend(cell_table_rows_sorted)

    src_str = f"NWST Health — {source} (snapshot {snapshot_date})"
    return rows, src_str
