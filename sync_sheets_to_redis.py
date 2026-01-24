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
    if creds_json:
        import json
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


def sync_options(gsheet_client, redis_client):
    """Sync Options tab (Column C) to Redis"""
    print("Syncing Options...")

    spreadsheet = gsheet_client.open_by_key(SHEET_ID)

    try:
        options_sheet = spreadsheet.worksheet(OPTIONS_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Warning: {OPTIONS_TAB_NAME} tab not found")
        return

    column_c_values = options_sheet.col_values(3)

    if not column_c_values:
        print("  Warning: Column C is empty")
        return

    header = column_c_values[0].strip() if column_c_values[0] else "Name"
    option_values = [v.strip() for v in column_c_values[1:] if v.strip()]

    options = {header: option_values}

    redis_client.set(
        REDIS_OPTIONS_KEY,
        json.dumps({"options": options}),
        ex=REDIS_CACHE_TTL
    )

    print(f"  Synced {len(option_values)} options")


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


def sync_attendance(gsheet_client, redis_client, tab_name):
    """Sync attendance data for a specific tab to Redis"""
    print(f"Syncing {tab_name}...")

    spreadsheet = gsheet_client.open_by_key(SHEET_ID)
    today_myt = get_today_myt_date()

    try:
        attendance_sheet = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Warning: {tab_name} tab not found")
        return

    all_rows = attendance_sheet.get_all_values()

    if len(all_rows) <= 1:
        print(f"  No attendance data for today")
        return

    cell_group_data = {}
    checked_in_set = set()
    checked_in_list = []
    recent_checkins = []

    for row in all_rows[1:]:
        if len(row) < 2:
            continue

        timestamp_str = row[0].strip() if row[0] else ""
        name_cell_group = row[1].strip() if len(row) > 1 and row[1] else ""

        if not timestamp_str or not name_cell_group:
            continue

        try:
            if len(timestamp_str) >= 10:
                date_part = timestamp_str[:10]

                if date_part == today_myt:
                    recent_checkins.append((timestamp_str, name_cell_group))

                    if name_cell_group not in checked_in_set:
                        checked_in_set.add(name_cell_group)
                        checked_in_list.append(name_cell_group)

                        name, cell_group = parse_name_cell_group(name_cell_group)

                        if cell_group not in cell_group_data:
                            cell_group_data[cell_group] = []
                        cell_group_data[cell_group].append(name)
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

    print(f"  Synced {len(checked_in_list)} check-ins for today")


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
