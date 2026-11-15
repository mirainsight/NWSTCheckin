#!/usr/bin/env python3
"""
Sync Google Sheets data to Redis cache.
Run this script periodically (e.g., every 5 minutes via GitHub Actions)
to keep Redis in sync with Google Sheets.

This ensures:
1. Any manual edits to Google Sheets are reflected in the app
2. Users always see fresh data without hitting Sheets API
"""

import os
import json
import base64
from datetime import datetime, timedelta, timezone

# Try to load from .env file (for local testing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# Redis
from upstash_redis import Redis

# Configuration
SHEET_ID = os.environ.get("ATTENDANCE_SHEET_ID", "")
OPTIONS_TAB_NAME = "Options"
ATTENDANCE_TAB_NAME = "Attendance"
LEADERS_ATTENDANCE_TAB_NAME = "Leaders Attendance"
KEY_VALUES_TAB_NAME = "Key Values"

# Redis keys
REDIS_CACHE_TTL = 86400  # 24 hours
REDIS_OPTIONS_KEY = "attendance:options"
REDIS_ATTENDANCE_KEY_PREFIX = "attendance:data:"
REDIS_ZONE_MAPPING_KEY = "attendance:zone_mapping"
# Check-ins the app has queued but the flush job has not written to the sheet yet
# (same key format as attendance_app.save_attendance_to_sheet / flush_pending.py)
REDIS_PENDING_ATTENDANCE_PREFIX = "attendance:pending_rows:"


def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


def get_gsheet_client():
    """Connect to Google Sheets using service account credentials"""
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    # Try environment variable with JSON string (for GitHub Actions)
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    print(f"DEBUG: GOOGLE_CREDENTIALS_JSON length: {len(creds_json)}")
    print(f"DEBUG: First 50 chars: {creds_json[:50] if creds_json else 'EMPTY'}")

    if creds_json:
        import json
        # Support both base64-encoded and plain JSON
        try:
            # Try base64 decode first
            print("DEBUG: Attempting base64 decode...")
            decoded = base64.b64decode(creds_json).decode('utf-8')
            print(f"DEBUG: Decoded length: {len(decoded)}")
            creds_dict = json.loads(decoded)
            print("DEBUG: Successfully parsed as base64-encoded JSON")
        except Exception as e:
            print(f"DEBUG: Base64 decode failed: {e}")
            # Fall back to plain JSON
            creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)

    # Try local credentials file
    if os.path.exists("credentials.json"):
        creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
        return gspread.authorize(creds)

    raise Exception("No Google credentials found")


def get_redis_client():
    """Connect to Upstash Redis"""
    url = os.environ.get("UPSTASH_REDIS_REST_URL", "")
    token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

    if not url or not token:
        raise Exception("Redis credentials not found")

    return Redis(url=url, token=token)


def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)"""
    if not name_cell_group_str:
        return None, None

    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    else:
        return parts[0].strip(), "Unknown"


def _is_email_format(value):
    """Return True if value looks like an email address (treat role as blank)."""
    if not value or not isinstance(value, str):
        return False
    v = value.strip()
    return "@" in v and "." in v


def sync_options(gsheet_client, redis_client):
    """Sync Options tab (Column C + Column D role) to Redis"""
    print("Syncing Options...")

    spreadsheet = gsheet_client.open_by_key(SHEET_ID)

    try:
        options_sheet = spreadsheet.worksheet(OPTIONS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Warning: {OPTIONS_TAB_NAME} tab not found")
        return

    column_c_values = options_sheet.col_values(3)
    column_d_values = options_sheet.col_values(4)

    if not column_c_values:
        print("  Warning: Column C is empty")
        return

    header = column_c_values[0].strip() if column_c_values[0] else "Name"
    option_values = []
    name_to_role = {}
    for i, v in enumerate(column_c_values[1:]):
        v = v.strip()
        if v:
            option_values.append(v)
            name, _ = parse_name_cell_group(v)
            if name:
                role_raw = column_d_values[i + 1].strip() if i + 1 < len(column_d_values) else ""
                role = "" if _is_email_format(role_raw) else (role_raw or "")
                if role:
                    name_to_role[name] = role

    options = {header: option_values}

    redis_client.set(
        REDIS_OPTIONS_KEY,
        json.dumps({"options": options, "name_to_role": name_to_role}),
        ex=REDIS_CACHE_TTL
    )

    print(f"  Synced {len(option_values)} options, {len(name_to_role)} roles")


def sync_zone_mapping(gsheet_client, redis_client):
    """Sync Key Values tab (cell-to-zone mapping) to Redis"""
    print("Syncing Zone Mapping...")

    spreadsheet = gsheet_client.open_by_key(SHEET_ID)

    try:
        key_values_sheet = spreadsheet.worksheet(KEY_VALUES_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Warning: {KEY_VALUES_TAB_NAME} tab not found")
        return

    all_values = key_values_sheet.get_all_values()

    if len(all_values) <= 1:
        print("  Warning: Key Values sheet is empty")
        return

    cell_to_zone = {}
    for row in all_values[1:]:
        if len(row) >= 3:
            cell_name = row[0].strip()
            zone = row[2].strip()
            if cell_name and zone:
                cell_to_zone[cell_name.lower()] = zone

    redis_client.set(
        REDIS_ZONE_MAPPING_KEY,
        json.dumps({"mapping": cell_to_zone}),
        ex=REDIS_CACHE_TTL
    )

    print(f"  Synced {len(cell_to_zone)} zone mappings")


def read_pending_checkins(redis_client, today_myt, tab_name):
    """Return today's queued-but-not-yet-flushed check-ins as (timestamp, "Name - Cell") pairs."""
    pending_key = f"{REDIS_PENDING_ATTENDANCE_PREFIX}{today_myt}:{tab_name}"
    items = []
    for raw in redis_client.lrange(pending_key, 0, -1) or []:
        try:
            s = raw.decode() if isinstance(raw, bytes) else raw
            d = json.loads(s)
            items.append((str(d["ts"]).strip(), str(d["opt"]).strip()))
        except Exception:
            continue
    return items


def sync_attendance(gsheet_client, redis_client, tab_name):
    """Sync attendance data for a specific tab to Redis.

    Today's snapshot = rows already in the sheet + check-ins still waiting in the pending
    queue. Without the pending part, a sync that runs before the flush job would wipe
    those check-ins from the app's "Checked in today" count.
    """
    print(f"Syncing {tab_name}...")

    spreadsheet = gsheet_client.open_by_key(SHEET_ID)
    today_myt = get_today_myt_date()

    try:
        attendance_sheet = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Warning: {tab_name} tab not found")
        return

    # Read the queue BEFORE the sheet. The flush appends to the sheet and only then clears
    # the queue, so a flush running in between shows up in at least one of the two reads.
    pending = read_pending_checkins(redis_client, today_myt, tab_name)

    all_rows = attendance_sheet.get_all_values()

    cell_group_data = {}
    checked_in_set = set()
    checked_in_list = []
    recent_checkins = []
    seen_recent = set()

    def add_checkin(timestamp_str, name_cell_group):
        if not timestamp_str or not name_cell_group:
            return
        if len(timestamp_str) < 10 or timestamp_str[:10] != today_myt:
            return
        # Same row can be in both the sheet and the queue if a flush ran mid-sync
        if (timestamp_str, name_cell_group) in seen_recent:
            return
        seen_recent.add((timestamp_str, name_cell_group))
        recent_checkins.append((timestamp_str, name_cell_group))

        if name_cell_group not in checked_in_set:
            checked_in_set.add(name_cell_group)
            checked_in_list.append(name_cell_group)

            name, cell_group = parse_name_cell_group(name_cell_group)

            if cell_group not in cell_group_data:
                cell_group_data[cell_group] = []
            cell_group_data[cell_group].append(name)

    for row in all_rows[1:]:
        if len(row) < 2:
            continue
        try:
            add_checkin(row[0].strip() if row[0] else "", row[1].strip() if row[1] else "")
        except Exception:
            continue

    for timestamp_str, name_cell_group in pending:
        try:
            add_checkin(timestamp_str, name_cell_group)
        except Exception:
            continue

    recent_checkins.sort(key=lambda x: x[0], reverse=True)

    redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
    cache_data = {
        "cell_group_data": cell_group_data,
        "checked_in_list": checked_in_list,
        "recent_checkins": recent_checkins
    }

    redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)

    print(f"  Synced {len(checked_in_list)} check-ins for today ({len(pending)} still pending flush)")


def main():
    """Main sync function"""
    print(f"Starting sync at {datetime.now().isoformat()}")
    print(f"Today (MYT): {get_today_myt_date()}")
    print("-" * 40)

    try:
        gsheet_client = get_gsheet_client()
        print("Connected to Google Sheets")
    except Exception as e:
        print(f"ERROR: Failed to connect to Google Sheets: {e}")
        return 1

    try:
        redis_client = get_redis_client()
        print("Connected to Redis")
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        return 1

    print("-" * 40)

    errors = []

    try:
        sync_options(gsheet_client, redis_client)
    except Exception as e:
        print(f"  ERROR syncing options: {e}")
        errors.append(f"Options: {e}")

    try:
        sync_zone_mapping(gsheet_client, redis_client)
    except Exception as e:
        print(f"  ERROR syncing zone mapping: {e}")
        errors.append(f"Zone mapping: {e}")

    try:
        sync_attendance(gsheet_client, redis_client, ATTENDANCE_TAB_NAME)
    except Exception as e:
        print(f"  ERROR syncing {ATTENDANCE_TAB_NAME}: {e}")
        errors.append(f"{ATTENDANCE_TAB_NAME}: {e}")

    try:
        sync_attendance(gsheet_client, redis_client, LEADERS_ATTENDANCE_TAB_NAME)
    except Exception as e:
        print(f"  ERROR syncing {LEADERS_ATTENDANCE_TAB_NAME}: {e}")
        errors.append(f"{LEADERS_ATTENDANCE_TAB_NAME}: {e}")

    print("-" * 40)

    if errors:
        print(f"Sync completed with {len(errors)} error(s)")
        for err in errors:
            print(f"  - {err}")
        return 1
    else:
        print("Sync completed successfully!")
        return 0


if __name__ == "__main__":
    exit(main())
