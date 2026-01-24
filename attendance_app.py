import os
import json
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import pandas as pd
import plotly.express as px
import hashlib
import colorsys
import qrcode
from io import BytesIO

# Upstash Redis for caching (reduces Google Sheets API calls)
try:
    from upstash_redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Load environment variables
load_dotenv()

# Configuration - check .env first, then Streamlit secrets
SHEET_ID = os.getenv("ATTENDANCE_SHEET_ID", "")
if not SHEET_ID:
    try:
        if hasattr(st, 'secrets') and 'ATTENDANCE_SHEET_ID' in st.secrets:
            SHEET_ID = st.secrets["ATTENDANCE_SHEET_ID"]
    except FileNotFoundError:
        pass  # No secrets.toml file, continue with empty SHEET_ID
OPTIONS_TAB_NAME = "Options"  # Tab name where options are stored
ATTENDANCE_TAB_NAME = "Attendance"  # Tab name where attendance is recorded
LEADERS_ATTENDANCE_TAB_NAME = "Leaders Attendance"  # Tab name for leaders discipleship check-in
KEY_VALUES_TAB_NAME = "Key Values"  # Tab name for cell-to-zone mapping

# Redis cache configuration
REDIS_CACHE_TTL = 86400  # 24 hours in seconds (cache resets daily via key)
REDIS_OPTIONS_KEY = "attendance:options"
REDIS_ATTENDANCE_KEY_PREFIX = "attendance:data:"  # Will be suffixed with date and tab name
REDIS_ZONE_MAPPING_KEY = "attendance:zone_mapping"

def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")

def get_now_myt():
    """Get current datetime in MYT timezone"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt)

@st.cache_resource
def get_redis_client():
    """Initialize Upstash Redis client - cached as resource to reuse connection"""
    if not REDIS_AVAILABLE:
        return None

    # Try environment variables first, then Streamlit secrets
    redis_url = os.getenv("UPSTASH_REDIS_REST_URL", "")
    redis_token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

    if not redis_url or not redis_token:
        try:
            if hasattr(st, 'secrets'):
                redis_url = st.secrets.get("UPSTASH_REDIS_REST_URL", "")
                redis_token = st.secrets.get("UPSTASH_REDIS_REST_TOKEN", "")
        except:
            pass

    if redis_url and redis_token:
        try:
            return Redis(url=redis_url, token=redis_token)
        except Exception as e:
            st.warning(f"Redis connection failed: {e}. Falling back to Google Sheets.")
            return None
    return None

def clear_redis_cache_for_today(tab_name=None):
    """Clear Redis cache for today's attendance data. Used for manual refresh."""
    redis_client = get_redis_client()
    if not redis_client:
        return

    today_myt = get_today_myt_date()
    try:
        if tab_name:
            # Clear specific tab
            redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
            redis_client.delete(redis_key)
        else:
            # Clear all attendance tabs for today
            for tn in [ATTENDANCE_TAB_NAME, LEADERS_ATTENDANCE_TAB_NAME]:
                redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tn}"
                redis_client.delete(redis_key)
        # Also clear options and zone mapping
        redis_client.delete(REDIS_OPTIONS_KEY)
        redis_client.delete(REDIS_ZONE_MAPPING_KEY)
    except Exception:
        pass

def get_gsheet_client():
    """Connect to Google Sheets - works with both local files and Streamlit secrets"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = None
        
        # Try credentials.json file first (for local development)
        # Check multiple possible locations
        possible_paths = [
            'credentials.json',  # Same folder as app
            'CHECK IN/credentials.json',  # In CHECK IN folder
            '../credentials.json',  # Parent folder
            os.path.join(os.path.dirname(__file__), 'credentials.json'),  # Same dir as script
        ]
        
        credentials_path = None
        for path in possible_paths:
            if os.path.exists(path):
                credentials_path = path
                break
        
        if credentials_path:
            try:
                creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
            except Exception as e:
                st.error(f"❌ Error reading credentials.json from {credentials_path}: {str(e)}")
                return None
        # Try Streamlit secrets (for cloud deployment)
        elif hasattr(st, 'secrets'):
            try:
                if 'gcp_service_account' in st.secrets:
                    creds_dict = dict(st.secrets['gcp_service_account'])
                    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            except Exception as e:
                # If secrets exist but can't be read, fall through to error
                pass
        
        if creds is None:
            st.error("❌ No Google Sheets credentials found.")
            checked_locations = [
                'credentials.json',
                'CHECK IN/credentials.json',
                '../credentials.json',
            ]
            location_status = '\n'.join([f'- {path} {"✅" if os.path.exists(path) else "❌"}' for path in checked_locations])
            st.info(f"""
            **For local development, you need:**
            1. Download `credentials.json` from Google Cloud Console
            2. Place it in one of these locations:
               - `/Users/miracle.wong/Desktop/Cursor/disc-app/DISC/CHECK IN/credentials.json` ✅ (preferred)
               - `/Users/miracle.wong/Desktop/Cursor/disc-app/DISC/credentials.json`
            
            **Checked locations:**
            {location_status}
            
            **See the setup guide for detailed instructions.**
            """)
            return None
            
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Could not connect to Google Sheets: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Local cache for 5 minutes as fallback
def get_options_from_sheet(_client, sheet_id):
    """Read options from Column C of the Options tab in Google Sheets.
    Uses Redis cache to minimize API calls."""

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_OPTIONS_KEY)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("options"), None
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the Options worksheet
        try:
            options_sheet = spreadsheet.worksheet(OPTIONS_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return None, f"❌ Tab '{OPTIONS_TAB_NAME}' not found. Please create it in your Google Sheet."

        # Read only column C (index 2: A=0, B=1, C=2)
        # Get all values from column C
        column_c_values = options_sheet.col_values(3)  # Column C is index 3 (1-indexed)

        if not column_c_values:
            return {}, "⚠️ Column C in Options sheet is empty. Please add options to column C."

        # Get the header from first row (C1)
        header = column_c_values[0].strip() if column_c_values[0] else "Name"

        # Get all options from row 2 onwards (skip header row)
        option_values = []
        for value in column_c_values[1:]:  # Skip first row (header)
            value = value.strip()
            if value:  # Only add non-empty values
                option_values.append(value)

        if not option_values:
            return {}, "⚠️ No options found in column C (starting from row 2)."

        # Return single option type with all column C values
        options = {header: option_values}

        # Store in Redis cache
        if redis_client:
            try:
                redis_client.set(REDIS_OPTIONS_KEY, json.dumps({"options": options}), ex=REDIS_CACHE_TTL)
            except Exception:
                pass  # Redis write failed, continue anyway

        return options, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return None, "⚠️ API quota exceeded. Please wait a moment and refresh the page."
        return None, f"❌ Error reading options from column C: {str(e)}"
    except Exception as e:
        return None, f"❌ Error reading options from column C: {str(e)}"

@st.cache_data(ttl=300)  # Local cache for 5 minutes as fallback
def get_cell_to_zone_mapping(_client, sheet_id):
    """Read cell-to-zone mapping from Key Values tab in Google Sheets.
    Column A = Cell Names, Column C = Zones. Uses Redis cache."""

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(REDIS_ZONE_MAPPING_KEY)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return data.get("mapping", {}), None
        except Exception:
            pass  # Redis failed, fall back to Sheets

    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the Key Values worksheet
        try:
            key_values_sheet = spreadsheet.worksheet(KEY_VALUES_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return {}, f"Tab '{KEY_VALUES_TAB_NAME}' not found."

        # Get all values from the sheet
        all_values = key_values_sheet.get_all_values()

        if len(all_values) <= 1:  # Only header row or empty
            return {}, "Key Values sheet is empty or has only headers."

        # Build mapping from Cell Name to Zone (skip header row)
        # Column A (index 0) = Cell Names, Column C (index 2) = Zones
        cell_to_zone = {}
        for row in all_values[1:]:  # Skip header
            if len(row) >= 3:
                cell_name = row[0].strip()  # Column A: "Anchor Street"
                zone = row[2].strip()        # Column C: "Syd"
                if cell_name and zone:
                    cell_to_zone[cell_name.lower()] = zone

        # Store in Redis cache
        if redis_client:
            try:
                redis_client.set(REDIS_ZONE_MAPPING_KEY, json.dumps({"mapping": cell_to_zone}), ex=REDIS_CACHE_TTL)
            except Exception:
                pass

        return cell_to_zone, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return {}, "API quota exceeded."
        return {}, f"Error reading Key Values: {str(e)}"
    except Exception as e:
        return {}, f"Error reading Key Values: {str(e)}"

def generate_daily_colors():
    """Generate random colors based on today's date (consistent throughout the day)"""
    today = get_today_myt_date()
    # Use date as seed for consistent colors throughout the day
    seed = int(hashlib.md5(today.encode()).hexdigest(), 16)
    
    # Generate vibrant colors using the seed
    import random
    random.seed(seed)
    
    # Generate a primary accent color (bright, vibrant)
    hue = random.random()  # 0.0 to 1.0
    saturation = random.uniform(0.7, 1.0)
    lightness = random.uniform(0.45, 0.65)
    
    # Convert HSL to RGB then to hex
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0]*255), 
        int(rgb[1]*255), 
        int(rgb[2]*255)
    )
    
    # Generate a lighter variant for accents
    rgb_light = colorsys.hls_to_rgb(hue, min(lightness + 0.2, 0.9), saturation)
    light_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb_light[0]*255), 
        int(rgb_light[1]*255), 
        int(rgb_light[2]*255)
    )
    
    return {
        'primary': primary_color,
        'light': light_color,
        'background': '#000000',  # Keep black background for edgy style
        'accent': primary_color
    }

def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)"""
    if not name_cell_group_str:
        return None, None
    
    # Split by " - " to separate name and cell group
    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    else:
        # If no " - " found, treat entire string as name, cell group as "Unknown"
        return parts[0].strip(), "Unknown"

@st.cache_data(ttl=300)  # Local cache for 5 minutes as fallback
def get_today_attendance_data(_client, sheet_id, refresh_key=0, tab_name=ATTENDANCE_TAB_NAME):
    """Get today's attendance data with names and cell groups grouped.
    Uses Redis cache to minimize API calls - cache key includes date so it resets daily."""

    today_myt = get_today_myt_date()
    redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"

    # Try Redis cache first
    redis_client = get_redis_client()
    if redis_client:
        try:
            cached = redis_client.get(redis_key)
            if cached:
                data = json.loads(cached) if isinstance(cached, str) else cached
                return (
                    data.get("cell_group_data", {}),
                    data.get("checked_in_list", []),
                    data.get("recent_checkins", [])
                )
        except Exception:
            pass  # Redis failed, fall back to Sheets

    # Read from Google Sheets
    try:
        spreadsheet = _client.open_by_key(sheet_id)

        # Try to get the specified worksheet
        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            return {}, [], []

        # Get all rows from the Attendance sheet
        all_rows = attendance_sheet.get_all_values()

        if len(all_rows) <= 1:  # Only header row or empty
            return {}, [], []

        # Dictionary to store cell group -> list of names
        cell_group_data = {}
        # List to store all checked-in entries (for set deduplication)
        checked_in_set = set()
        checked_in_list = []  # Keep order for first occurrence
        # List to store recent check-ins with timestamps (timestamp, name_cell_group)
        recent_checkins = []

        # Skip header row (index 0), check from row 1 onwards
        for row in all_rows[1:]:
            if len(row) < 2:  # Skip incomplete rows
                continue

            timestamp_str = row[0].strip() if row[0] else ""
            name_cell_group = row[1].strip() if len(row) > 1 and row[1] else ""

            if not timestamp_str or not name_cell_group:
                continue

            # Parse timestamp - it should be in format "YYYY-MM-DD HH:MM:SS"
            # Extract just the date part (first 10 characters)
            try:
                # Handle different timestamp formats
                if len(timestamp_str) >= 10:
                    date_part = timestamp_str[:10]  # Get "YYYY-MM-DD" part

                    # Check if this timestamp is from today (MYT)
                    if date_part == today_myt:
                        # Add to recent checkins (include duplicates for the table)
                        recent_checkins.append((timestamp_str, name_cell_group))

                        # Only add if not already in set (avoid duplicates for counts)
                        if name_cell_group not in checked_in_set:
                            checked_in_set.add(name_cell_group)
                            checked_in_list.append(name_cell_group)

                            # Parse name and cell group
                            name, cell_group = parse_name_cell_group(name_cell_group)

                            # Add to cell group data
                            if cell_group not in cell_group_data:
                                cell_group_data[cell_group] = []
                            cell_group_data[cell_group].append(name)
            except Exception as e:
                # If parsing fails, skip this row
                continue

        # Sort recent_checkins by timestamp descending (most recent first)
        recent_checkins.sort(key=lambda x: x[0], reverse=True)

        # Store in Redis cache
        if redis_client:
            try:
                cache_data = {
                    "cell_group_data": cell_group_data,
                    "checked_in_list": checked_in_list,
                    "recent_checkins": recent_checkins
                }
                redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)
            except Exception:
                pass

        return cell_group_data, checked_in_list, recent_checkins

    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            # Return empty data but don't show error (will use cached data if available)
            return {}, [], []
        return {}, [], []
    except Exception as e:
        return {}, [], []

def get_checked_in_today(client, sheet_id, tab_name=ATTENDANCE_TAB_NAME):
    """Get a set of people who have already checked in today (MYT date)"""
    try:
        refresh_key = st.session_state.get('refresh_counter', 0)
        _, checked_in_list, _ = get_today_attendance_data(client, sheet_id, refresh_key, tab_name)
        return set(checked_in_list)
    except Exception as e:
        # If there's an error reading attendance, return empty set (show all options)
        return set()

def save_attendance_to_sheet(client, attendance_data, tab_name=ATTENDANCE_TAB_NAME):
    """Save attendance data to the specified tab - supports batch check-ins.
    Also updates Redis cache to ensure immediate visibility for other users."""
    try:
        spreadsheet = client.open_by_key(SHEET_ID)

        # Try to get the Attendance worksheet, create if it doesn't exist
        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
            # Check if headers exist, if not add them
            existing_headers = attendance_sheet.row_values(1)
            if not existing_headers:
                headers = ["Timestamp", attendance_data.get("option_type", "Option")]
                attendance_sheet.append_row(headers)
        except gspread.exceptions.WorksheetNotFound:
            # Create the worksheet
            attendance_sheet = spreadsheet.add_worksheet(
                title=tab_name,
                rows=1000,
                cols=20
            )
            # Add headers
            headers = ["Timestamp", attendance_data.get("option_type", "Option")]
            attendance_sheet.append_row(headers)

        # Get current time in Malaysia Time (MYT, UTC+8)
        myt = timezone(timedelta(hours=8))
        timestamp = datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")
        today_myt = get_today_myt_date()

        # Check if this is a batch check-in (list of options) or single
        selected_options = attendance_data.get("selected_options", [])
        if not selected_options:
            # Single check-in (backwards compatibility)
            selected_option = attendance_data.get("selected_option", "")
            if selected_option:
                selected_options = [selected_option]

        if not selected_options:
            return False, "No options selected"

        # Prepare all rows for batch insert
        rows = [[timestamp, option] for option in selected_options]

        # Batch append all rows at once (single API call)
        attendance_sheet.append_rows(rows)

        # Update Redis cache with new check-ins (so other users see immediately)
        redis_client = get_redis_client()
        if redis_client:
            try:
                redis_key = f"{REDIS_ATTENDANCE_KEY_PREFIX}{today_myt}:{tab_name}"
                cached = redis_client.get(redis_key)

                if cached:
                    # Update existing cache
                    data = json.loads(cached) if isinstance(cached, str) else cached
                    cell_group_data = data.get("cell_group_data", {})
                    checked_in_list = data.get("checked_in_list", [])
                    recent_checkins = data.get("recent_checkins", [])
                else:
                    # Initialize new cache
                    cell_group_data = {}
                    checked_in_list = []
                    recent_checkins = []

                # Add new check-ins to cache
                checked_in_set = set(checked_in_list)
                for option in selected_options:
                    # Add to recent checkins
                    recent_checkins.insert(0, (timestamp, option))

                    # Only add to list if not already checked in
                    if option not in checked_in_set:
                        checked_in_set.add(option)
                        checked_in_list.append(option)

                        # Parse and add to cell group data
                        name, cell_group = parse_name_cell_group(option)
                        if cell_group not in cell_group_data:
                            cell_group_data[cell_group] = []
                        cell_group_data[cell_group].append(name)

                # Save updated cache
                cache_data = {
                    "cell_group_data": cell_group_data,
                    "checked_in_list": checked_in_list,
                    "recent_checkins": recent_checkins
                }
                redis_client.set(redis_key, json.dumps(cache_data), ex=REDIS_CACHE_TTL)
            except Exception:
                pass  # Redis update failed, but Sheets save succeeded

        count = len(selected_options)
        return True, f"Checked in {count} {'person' if count == 1 else 'people'} successfully!"

    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return False, "⚠️ API quota exceeded. Please wait a moment and try again."
        return False, f"Failed to save attendance: {str(e)}"
    except Exception as e:
        return False, f"Failed to save attendance: {str(e)}"

# ---------- Streamlit App ----------
st.set_page_config(
    page_title="Church Check-In",
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize session state for cache invalidation
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0

# Track last refresh time for cache countdown (MYT timezone)
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = get_now_myt()
# Ensure existing session state is timezone-aware (fix for mixed tz issues)
elif st.session_state.last_refresh_time.tzinfo is None:
    st.session_state.last_refresh_time = get_now_myt()

CACHE_TTL_SECONDS = 60  # Local Streamlit cache duration (Redis handles main caching)

# Generate daily colors
daily_colors = generate_daily_colors()

# Determine current page early for color scheme
_early_query_params = st.query_params
_early_page = _early_query_params.get("page", "nwst")
is_leaders_page = _early_page == "leaders"

# Create color schemes for each page type
if is_leaders_page:
    # Leaders page: Light theme (white background, dark text)
    page_colors = {
        'primary': daily_colors['primary'],
        'light': daily_colors['light'],
        'background': '#ffffff',
        'text': '#000000',
        'text_muted': '#666666',
        'card_bg': '#f5f5f5',
        'border': daily_colors['primary']
    }
else:
    # NWST page: Dark theme (black background, light text)
    page_colors = {
        'primary': daily_colors['primary'],
        'light': daily_colors['light'],
        'background': '#000000',
        'text': '#ffffff',
        'text_muted': '#999999',
        'card_bg': '#0a0a0a',
        'border': daily_colors['primary']
    }

# Add CSS to reduce Streamlit default spacing and style buttons with daily color
st.markdown(f"""
<style>
    /* Force instruction text to be white */
    .instruction-text {{
        color: #ffffff !important;
    }}

    /* Base theme colors */
    .stApp {{
        background-color: {page_colors['background']} !important;
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

    /* Style all buttons with daily color theme */
    .stButton > button {{
        background-color: transparent !important;
        color: {page_colors['primary']} !important;
        border: 2px solid {page_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.2s ease !important;
    }}
    .stButton > button:hover {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        transform: scale(1.02) !important;
    }}

    /* Primary buttons (Check In, Close) */
    .stButton > button[kind="primary"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        border: 2px solid {page_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {page_colors['light']} !important;
        border-color: {page_colors['light']} !important;
    }}

    /* Form submit button */
    .stFormSubmitButton > button {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
        border: 2px solid {page_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
    }}
    .stFormSubmitButton > button:hover {{
        background-color: {page_colors['light']} !important;
        border-color: {page_colors['light']} !important;
        transform: scale(1.02) !important;
    }}

    /* Multiselect styling */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {page_colors['primary']} !important;
        color: {page_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {page_colors['primary']} !important;
    }}

    /* Style checked-in options (starting with ✓) in multiselect dropdown */
    .stMultiSelect [data-baseweb="menu"] li[aria-disabled="false"]:has(div[title^="✓"]),
    .stMultiSelect [role="listbox"] li:has(div[title^="✓"]) {{
        opacity: 0.5 !important;
        color: #888 !important;
    }}
    /* Alternative selector for dropdown items containing tick */
    [data-baseweb="menu"] [role="option"] {{
        transition: opacity 0.2s ease;
    }}

    /* Text colors for leaders page */
    {"" if not is_leaders_page else '''
    .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown div {
        color: #000000 !important;
    }
    /* Keep instruction text white on dark overlay */
    .instruction-text {
        color: #ffffff !important;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #000000 !important;
    }
    .stRadio label {
        color: #000000 !important;
    }
    [data-testid="stSidebar"] {
        background-color: #f0f0f0 !important;
    }
    [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {
        color: #000000 !important;
    }
    /* Form labels */
    .stMultiSelect label, .stSelectbox label, .stTextInput label {
        color: #000000 !important;
    }
    '''}
</style>
""", unsafe_allow_html=True)

# GIF Background Section
# You can set a GIF URL in your .env file as BANNER_GIF_URL, or upload a file
gif_url = os.getenv("BANNER_GIF_URL", "")
gif_path = os.path.join(os.path.dirname(__file__), "banner.gif")

# Prepare background GIF
background_gif = ""
gif_src = ""
if gif_url:
    background_gif = f"url('{gif_url}')"
    gif_src = gif_url
elif os.path.exists(gif_path):
    import base64
    with open(gif_path, "rb") as f:
        gif_data = base64.b64encode(f.read()).decode()
    background_gif = f"url('data:image/gif;base64,{gif_data}')"
    gif_src = f"data:image/gif;base64,{gif_data}"

# Initialize Google Sheets client
client = get_gsheet_client()

if not client:
    st.stop()

# Check if sheet ID is configured
if not SHEET_ID:
    st.error("⚠️ Please configure ATTENDANCE_SHEET_ID in your .env file or Streamlit secrets.")
    st.info("""
    **Setup Instructions:**
    1. Create a Google Sheet
    2. Add a tab named "Options" with your form options
    3. Format: Column C only
    4. Example:
       - Row 1, Column C: Header name (e.g., "Name" or "Attendee")
       - Row 2+, Column C: Options listed vertically
       - Example: "Miracle Wong - Narrowstreet Core Team"
    5. Set ATTENDANCE_SHEET_ID in your environment variables
    """)
    st.stop()

# Get options from Google Sheets
with st.spinner("Loading options..."):
    options, error_msg = get_options_from_sheet(client, SHEET_ID)

if options is None:
    if error_msg:
        st.error(error_msg)
        st.info("Tip: If you're seeing quota errors, wait a moment and refresh the page. Data is cached to reduce API calls.")
    st.stop()

if not options:
    if error_msg:
        st.warning(error_msg)
    else:
        st.warning("""
        No options found. Please add options to your Google Sheet.

        **Format in the Options tab:**
        - Column C, Row 1: Header name (e.g., "Name" or "Attendee")
        - Column C, Row 2+: Options listed vertically
        - Example:
          |   |   | Name - Cell Group        |
          |---|---|--------------------------|
          |   |   | Miracle Wong - Narrowstreet Core Team |
          |   |   | Shaun Quek - Narrowstreet Core Team |
        """)
    st.stop()

# Get the single option type and values
option_type = list(options.keys())[0]
all_option_values = list(options.values())[0]


def render_check_in_form(tab_name, form_key, page_label="Check In"):
    """Render the check-in form for a specific tab"""
    # Get list of people who have already checked in today (MYT) for this specific tab
    with st.spinner("Checking today's attendance..."):
        checked_in_today = get_checked_in_today(client, SHEET_ID, tab_name)

    # Keep all options but track which are already checked in
    available_options = [opt for opt in all_option_values if opt not in checked_in_today]
    checked_in_options = [opt for opt in all_option_values if opt in checked_in_today]

    # Wrap form section with GIF background
    if background_gif and gif_src:
        st.markdown(f"""
        <div style="
            position: relative;
            padding: 2rem;
            margin: 0;
            border-radius: 8px;
            border: 2px solid {page_colors['primary']};
            min-height: 250px;
            overflow: hidden;
        ">
            <img src="{gif_src}"
                 style="
                     position: absolute;
                     top: 0;
                     left: 0;
                     width: 100%;
                     height: 100%;
                     object-fit: cover;
                     z-index: 0;
                     opacity: 0.8;
                 " />
            <!-- Page label badge -->
            <div style="
                position: absolute;
                top: 10px;
                left: 10px;
                background: {page_colors['primary']};
                color: {page_colors['background']};
                padding: 0.4rem 1rem;
                font-family: 'Inter', sans-serif;
                font-weight: 800;
                font-size: 0.85rem;
                letter-spacing: 1px;
                text-transform: uppercase;
                z-index: 2;
            ">{page_label}</div>
            <div style="position: relative; z-index: 1;">
        """, unsafe_allow_html=True)

    # Display form in centered column
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Show instruction text
        if background_gif:
            components.html("""
            <div style="
                background: rgba(0, 0, 0, 0.6);
                padding: 0.75rem 1rem;
                border-radius: 6px;
                margin-bottom: 1rem;
            ">
                <p style="
                    font-size: 1rem;
                    margin: 0;
                    color: #ffffff;
                    text-shadow: 1px 1px 2px rgba(0,0,0,0.8);
                    text-align: center;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                ">Select your name from the dropdown below to check in.</p>
            </div>
            """, height=60)
        else:
            st.markdown('<p style="font-size: 1rem; margin-bottom: 1rem; text-align: center;">Select your name from the dropdown below to check in.</p>', unsafe_allow_html=True)

        # Show simple refresh message
        if checked_in_today:
            st.success("Refreshed!")

        # Display form
        with st.form(form_key, clear_on_submit=True):
            # Check if there are any available options
            if not available_options:
                st.warning("All attendees have already checked in for today!")
                st.form_submit_button("Check In", type="primary", use_container_width=True, disabled=True)
            else:
                # Create formatted options: available ones normal, checked-in ones with tick prefix
                # Format: "✓ Name - Cell" for checked in, "Name - Cell" for available
                # Sort by Cell Group first, then by Name within each group
                def get_sort_key(opt):
                    parts = opt.split(" - ", 1)
                    if len(parts) == 2:
                        name, cell = parts[0].strip(), parts[1].strip()
                        return (cell.lower(), name.lower())
                    return (opt.lower(), "")

                sorted_options = sorted(all_option_values, key=get_sort_key)

                formatted_options = []
                option_mapping = {}  # Maps display name back to original name

                for opt in sorted_options:
                    if opt in checked_in_today:
                        display_name = f"✓ {opt}"
                        formatted_options.append(display_name)
                        option_mapping[display_name] = opt
                    else:
                        formatted_options.append(opt)
                        option_mapping[opt] = opt

                # Sort formatted options again to ensure correct order
                # (ignoring ✓ prefix for sorting)
                def get_display_sort_key(display_opt):
                    opt = display_opt.lstrip("✓ ")
                    parts = opt.split(" - ", 1)
                    if len(parts) == 2:
                        name, cell = parts[0].strip(), parts[1].strip()
                        return (cell.lower(), name.lower())
                    return (opt.lower(), "")

                formatted_options = sorted(formatted_options, key=get_display_sort_key)

                # Multi-select for batch check-ins (reduces API calls)
                selected_display_options = st.multiselect(
                    f"Select {option_type}(s) *",
                    options=formatted_options,
                    help="Select up to 5 people to check in at once. Names with ✓ are already checked in today.",
                    default=[],
                    max_selections=5,
                    format_func=lambda x: x  # Use as-is since we already formatted
                )

                # Add JavaScript to gray out checked-in options in the dropdown
                components.html(f"""
                <script>
                    // Function to style checked-in options (those starting with ✓)
                    function styleCheckedInOptions() {{
                        // Find all option items in the multiselect dropdown
                        const options = document.querySelectorAll('[data-baseweb="menu"] li, [role="listbox"] li, [data-baseweb="select"] [role="option"]');
                        options.forEach(opt => {{
                            const text = opt.textContent || opt.innerText;
                            if (text && text.trim().startsWith('✓')) {{
                                opt.style.opacity = '0.5';
                                opt.style.color = '#888';
                                opt.style.fontStyle = 'italic';
                            }}
                        }});
                    }}

                    // Run on page load and observe for dropdown changes
                    const observer = new MutationObserver(styleCheckedInOptions);
                    observer.observe(document.body, {{ childList: true, subtree: true }});
                    styleCheckedInOptions();
                </script>
                """, height=0)

                # Filter out any already checked-in options that user might have selected
                # (convert back to original names and filter)
                selected_options = []
                already_checked_in_selected = []
                for display_opt in selected_display_options:
                    original_name = option_mapping.get(display_opt, display_opt)
                    if original_name in checked_in_today:
                        already_checked_in_selected.append(original_name)
                    else:
                        selected_options.append(original_name)

                # Submit button
                submitted = st.form_submit_button("Check In", type="primary", use_container_width=True)

                if submitted:
                    # Warn if user selected already checked-in people
                    if already_checked_in_selected:
                        st.warning(f"Note: {', '.join(already_checked_in_selected)} already checked in today and were skipped.")

                    # Validation
                    if not selected_options:
                        if already_checked_in_selected:
                            st.error("All selected people have already checked in today.")
                        else:
                            st.error("Please select at least one person.")
                    else:
                        # Prepare attendance data for batch check-in
                        attendance_data = {
                            "selected_options": selected_options,
                            "option_type": option_type
                        }

                        # Save to Google Sheets (single API call for all) - use tab_name
                        success, message = save_attendance_to_sheet(client, attendance_data, tab_name)

                        if success:
                            # Increment refresh counter to invalidate local Streamlit cache
                            st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                            st.session_state.last_refresh_time = get_now_myt()
                            # Note: Redis cache is updated in save_attendance_to_sheet()
                            # No need to clear caches - Redis has the updated data

                            st.success(f"{message}")
                            st.balloons()
                            # Refresh the page to update the dropdown
                            st.rerun()
                        else:
                            st.error(f"{message}")

    # Close background GIF container if it was opened
    if background_gif:
        st.markdown("</div></div>", unsafe_allow_html=True)
    else:
        # Show placeholder if no GIF
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 1rem; padding: 1rem; background: {page_colors['card_bg']}; border: 2px dashed {page_colors['primary']}; border-radius: 8px;">
            <p style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 600; margin: 0;">
                Add your banner GIF by setting BANNER_GIF_URL in .env or placing banner.gif in the CHECK IN folder
            </p>
        </div>
        """, unsafe_allow_html=True)

    return checked_in_today


def render_qr_section():
    """Render the I'm New QR code section with newcomer form workflow"""
    st.markdown("<br><br>", unsafe_allow_html=True)
    col_qr1, col_qr2, col_qr3 = st.columns([3, 1, 3])
    with col_qr2:
        if st.button("I'm New!", type="secondary", use_container_width=True, key="new_btn"):
            # Toggle the modal on/off
            st.session_state.show_qr_modal = not st.session_state.get('show_qr_modal', False)
            st.rerun()

    # Show QR code in modal/spotlight mode
    if st.session_state.get('show_qr_modal', False):
        # Generate QR code
        feedback_url = "https://forms.gle/yEX1kh24LPV6PVm77"
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(feedback_url)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")

        # Convert to base64 for embedding in HTML
        buffer = BytesIO()
        qr_img.save(buffer, format="PNG")
        import base64
        qr_base64 = base64.b64encode(buffer.getvalue()).decode()

        # Modal overlay with QR code
        components.html(f"""
        <style>
            .modal-overlay {{
                position: fixed;
                top: 0;
                left: 0;
                width: 100vw;
                height: 100vh;
                background: rgba(0, 0, 0, 0.85);
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 9999;
                backdrop-filter: blur(5px);
            }}
            .modal-content {{
                background: #1a1a1a;
                padding: 2rem;
                border-radius: 16px;
                text-align: center;
                max-width: 350px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.5);
            }}
            .qr-image {{
                width: 250px;
                height: 250px;
                border-radius: 8px;
            }}
            .modal-title {{
                color: #fff;
                font-size: 1.3rem;
                margin-bottom: 1rem;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }}
            .modal-subtitle {{
                color: #888;
                font-size: 0.9rem;
                margin-top: 1rem;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }}
            .link-btn {{
                color: #4da6ff;
                text-decoration: none;
                font-size: 0.95rem;
            }}
            .link-btn:hover {{
                text-decoration: underline;
            }}
        </style>
        <div class="modal-overlay" id="qrModal">
            <div class="modal-content">
                <div class="modal-title">Welcome! Scan to fill out the form</div>
                <img src="data:image/png;base64,{qr_base64}" class="qr-image" alt="QR Code"/>
                <div class="modal-subtitle">
                    <a href="{feedback_url}" target="_blank" class="link-btn">Or click here</a>
                </div>
            </div>
        </div>
        """, height=500)

        # Buttons: Newcomer Form Filled and Close
        col_filled, col_close = st.columns(2)
        with col_filled:
            if st.button("Newcomer Form Filled", type="secondary", use_container_width=True, key="newcomer_filled"):
                # Hard refresh - clear all caches and reload from Google Sheets
                st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                st.session_state.last_refresh_time = get_now_myt()
                # Clear local Streamlit caches
                get_today_attendance_data.clear()
                get_options_from_sheet.clear()
                # Clear Redis cache for options
                redis_client = get_redis_client()
                if redis_client:
                    try:
                        redis_client.delete(REDIS_OPTIONS_KEY)
                    except Exception:
                        pass
                st.session_state.show_qr_modal = False
                st.rerun()
        with col_close:
            if st.button("Close", type="primary", use_container_width=True, key="close_modal"):
                st.session_state.show_qr_modal = False
                st.rerun()


def render_recent_checkins_table(tab_name):
    """Render a scrollable table showing recent check-ins ordered by latest first"""
    # Get today's attendance data including recent check-ins
    refresh_key = st.session_state.get('refresh_counter', 0)
    _, _, recent_checkins = get_today_attendance_data(client, SHEET_ID, refresh_key, tab_name)

    if not recent_checkins:
        return

    st.markdown("---")
    st.markdown(f"""
    <div style="margin-bottom: 0.5rem;">
        <span style="font-family: 'Inter', sans-serif; font-weight: 700; font-size: 1rem;
                     color: {page_colors['primary']}; text-transform: uppercase; letter-spacing: 1px;">
            Recent Check-Ins
        </span>
    </div>
    """, unsafe_allow_html=True)

    # Create dataframe for the table
    table_data = []
    for timestamp_str, name_cell_group in recent_checkins:
        # Extract just the time part (HH:MM:SS) from timestamp
        time_part = timestamp_str[11:19] if len(timestamp_str) >= 19 else timestamp_str
        table_data.append({
            "Time": time_part,
            "Name - Cell Group": name_cell_group
        })

    df = pd.DataFrame(table_data)

    # Calculate height for 5 rows (approximately 35px per row + header)
    row_height = 35
    header_height = 38
    max_visible_rows = 5
    table_height = header_height + (row_height * min(len(table_data), max_visible_rows))

    # Display as a scrollable dataframe
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=table_height if len(table_data) > max_visible_rows else None
    )


def render_dashboard(tab_name, group_by_zone=False):
    """Render the dashboard section for a specific tab.
    If group_by_zone=True, groups by Zone instead of Cell Group."""
    st.markdown("---")

    # Simple refresh button - reads from Redis (no Sheets API call)
    col_refresh1, col_refresh2, col_refresh3 = st.columns([3, 1, 3])
    with col_refresh2:
        if st.button("Refresh", type="secondary", use_container_width=True, key=f"refresh_{tab_name}"):
            # Increment refresh counter to invalidate local Streamlit cache
            st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
            st.session_state.last_refresh_time = get_now_myt()
            # Clear local Streamlit cache only - Redis stays intact
            # This just re-reads from Redis, not from Google Sheets
            get_today_attendance_data.clear()
            get_options_from_sheet.clear()
            get_cell_to_zone_mapping.clear()
            st.rerun()

    # Show last refresh time
    last_refresh_str = st.session_state.last_refresh_time.strftime("%H:%M:%S")
    st.markdown(f"""
    <div style="text-align: center; padding: 0.3rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <span style="color: #888; font-size: 0.8rem;">
            Last refreshed at <b>{last_refresh_str}</b>
        </span>
    </div>
    """, unsafe_allow_html=True)

    # Get today's attendance data for the specific tab
    with st.spinner("Loading dashboard data..."):
        refresh_key = st.session_state.get('refresh_counter', 0)
        cell_group_data, checked_in_list, _ = get_today_attendance_data(client, SHEET_ID, refresh_key, tab_name)

    total_checked_in = len(checked_in_list)

    # If grouping by zone, convert cell_group_data to zone_data
    if group_by_zone:
        # Get zone mapping from Key Values tab (cell name -> zone)
        cell_to_zone, zone_error = get_cell_to_zone_mapping(client, SHEET_ID)

        # Convert cell groups to zones
        zone_data = {}
        for cell_group, names in cell_group_data.items():
            # Look up zone from Key Values (case-insensitive)
            zone = cell_to_zone.get(cell_group.lower(), cell_group)
            if zone not in zone_data:
                zone_data[zone] = []
            zone_data[zone].extend(names)
        display_data = zone_data
        group_label = "Zone"
    else:
        display_data = cell_group_data
        group_label = "Cell Group"

    # Convert hex color to RGB for rgba shadows
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    primary_rgb = hex_to_rgb(daily_colors['primary'])

    # Modern Edgy Dashboard Styling with Dynamic Colors
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@700;900&display=swap');

        .kpi-card {{
            background: {page_colors['card_bg']};
            padding: 2rem 2.5rem;
            border-radius: 0px;
            border-left: 6px solid {page_colors['primary']};
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.15);
            transition: all 0.3s ease;
        }}
        .kpi-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.25);
            border-left-width: 8px;
        }}
        .kpi-label {{
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
            font-weight: 700;
            color: {page_colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .kpi-number {{
            font-family: 'Inter', sans-serif;
            font-size: 5.5rem;
            font-weight: 900;
            color: {page_colors['primary']};
            line-height: 1;
            margin: 0.5rem 0;
            text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
        }}
        .kpi-subtitle {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            color: {page_colors['text_muted']};
            margin-top: 0.5rem;
            font-weight: 500;
        }}
        .dashboard-section {{
            background: {page_colors['card_bg']};
            padding: 2rem;
            border-radius: 0px;
            border: 2px solid {page_colors['primary']};
            margin: 2rem 0;
        }}
        .section-title {{
            font-family: 'Inter', sans-serif;
            font-size: 1.8rem;
            font-weight: 900;
            color: {page_colors['primary']};
            text-transform: uppercase;
            letter-spacing: 3px;
            margin-bottom: 1.5rem;
            border-bottom: 3px solid {page_colors['primary']};
            padding-bottom: 0.5rem;
            display: inline-block;
        }}
        .name-badge {{
            background: {page_colors['background']};
            border: 1px solid {page_colors['primary']};
            color: {page_colors['primary']};
            padding: 0.6rem 1.2rem;
            margin: 0.4rem 0.4rem 0.4rem 0;
            border-radius: 0px;
            display: inline-block;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            font-size: 0.9rem;
            letter-spacing: 0.5px;
            transition: all 0.2s ease;
        }}
        .name-badge:hover {{
            background: {page_colors['primary']};
            color: {page_colors['background']};
            transform: scale(1.05);
        }}
        .empty-state {{
            text-align: center;
            padding: 4rem 2rem;
            background: {page_colors['card_bg']};
            border: 2px dashed {page_colors['text_muted']};
            border-radius: 0px;
        }}
        .empty-state-text {{
            font-family: 'Inter', sans-serif;
            font-size: 1.5rem;
            color: {page_colors['text_muted']};
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 2px;
        }}
    </style>
    """, unsafe_allow_html=True)

    # KPI Card - Total Checked In
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Total Checked In Today</div>
        <div class="kpi-number">{total_checked_in}</div>
        <div class="kpi-subtitle">People checked in as of now</div>
    </div>
    """, unsafe_allow_html=True)

    # Zone tiles (only for zone grouping)
    if group_by_zone and total_checked_in > 0 and cell_group_data:
        # Get zone mapping from Key Values tab
        cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)

        # Aggregate by zone for tiles
        zone_counts = {}
        for cell_group, names in cell_group_data.items():
            # Look up zone from Key Values (case-insensitive)
            zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
            if zone not in zone_counts:
                zone_counts[zone] = 0
            zone_counts[zone] += len(names)

        sorted_zones_for_tiles = sorted(zone_counts.items(), key=lambda x: x[0].lower())

        # Create tiles in a grid
        st.markdown(f"""
        <style>
            .zone-tiles-container {{
                display: flex;
                flex-wrap: wrap;
                gap: 1rem;
                margin-bottom: 2rem;
            }}
            .zone-tile {{
                background: {page_colors['card_bg']};
                border: 2px solid {page_colors['primary']};
                padding: 1.2rem 1.5rem;
                min-width: 140px;
                flex: 1;
                text-align: center;
                transition: all 0.2s ease;
            }}
            .zone-tile:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
            }}
            .zone-name {{
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 700;
                color: {page_colors['text_muted']};
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 0.3rem;
            }}
            .zone-count {{
                font-family: 'Inter', sans-serif;
                font-size: 2.5rem;
                font-weight: 900;
                color: {page_colors['primary']};
                line-height: 1;
            }}
        </style>
        <div class="zone-tiles-container">
            {''.join([f'<div class="zone-tile"><div class="zone-name">{zone}</div><div class="zone-count">{count}</div></div>' for zone, count in sorted_zones_for_tiles])}
        </div>
        """, unsafe_allow_html=True)

    if total_checked_in > 0:
        # Bar Chart Section
        chart_title = "Attendance by Zone" if group_by_zone else "Check-Ins by Cell Group"
        st.markdown(f'<div class="section-title">{chart_title}</div>', unsafe_allow_html=True)

        # Prepare data for bar chart - sort by count descending
        sorted_groups = sorted(display_data.items(), key=lambda x: len(x[1]), reverse=True)

        chart_data = {
            group_label: [group for group, _ in sorted_groups],
            'Count': [len(names) for _, names in sorted_groups]
        }
        df_chart = pd.DataFrame(chart_data)

        # Create bar chart with modern edgy style
        fig = px.bar(
            df_chart,
            x=group_label,
            y='Count',
            color='Count',
            color_continuous_scale=[page_colors['background'], page_colors['primary']],
            text='Count',
            labels={'Count': 'Number of People', group_label: group_label},
            height=400
        )

        # Update layout for modern edgy style
        fig.update_layout(
            plot_bgcolor=page_colors['background'],
            paper_bgcolor=page_colors['card_bg'],
            font=dict(family='Inter, sans-serif', size=12, color=page_colors['primary']),
            xaxis=dict(
                title=dict(font=dict(size=14, color=page_colors['primary'], family='Inter')),
                tickfont=dict(color=page_colors['text_muted'], family='Inter'),
                gridcolor=page_colors['text_muted'],
                linecolor=page_colors['primary'],
                linewidth=2,
                categoryorder='total descending'
            ),
            yaxis=dict(
                title=dict(font=dict(size=14, color=page_colors['primary'], family='Inter')),
                tickfont=dict(color=page_colors['text_muted'], family='Inter'),
                gridcolor=page_colors['text_muted'],
                linecolor=page_colors['primary'],
                linewidth=2
            ),
            coloraxis_showscale=False,
            showlegend=False,
            margin=dict(l=50, r=50, t=60, b=50)
        )

        # Update bar style
        fig.update_traces(
            textfont=dict(size=14, color=page_colors['background'], family='Inter', weight='bold'),
            textposition='inside',
            insidetextanchor='middle',
            marker=dict(line=dict(color=page_colors['primary'], width=2)),
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>',
            hoverlabel=dict(bgcolor=page_colors['background'], font=dict(color=page_colors['primary'], family='Inter'))
        )

        st.plotly_chart(fig, use_container_width=True)

        # Names Breakdown Section
        names_title = "Attendees by Zone" if group_by_zone else "Attendees by Cell Group"
        st.markdown(f'<div class="section-title">{names_title}</div>', unsafe_allow_html=True)

        # Display names for each group
        if group_by_zone:
            # For zone grouping, show Zone -> Cell -> Names hierarchy
            # Build zone -> cell -> names structure
            zone_cell_names = {}
            cell_to_zone_map, _ = get_cell_to_zone_mapping(client, SHEET_ID)
            for cell_group, names in cell_group_data.items():
                zone = cell_to_zone_map.get(cell_group.lower(), cell_group)
                if zone not in zone_cell_names:
                    zone_cell_names[zone] = {}
                if cell_group not in zone_cell_names[zone]:
                    zone_cell_names[zone][cell_group] = []
                zone_cell_names[zone][cell_group].extend(names)

            # Display with hierarchy
            for zone in sorted(zone_cell_names.keys(), key=str.lower):
                cells = zone_cell_names[zone]
                total_in_zone = sum(len(names) for names in cells.values())
                st.markdown(f"""
                <div style="margin-bottom: 2rem;">
                    <h3 style="font-family: 'Inter', sans-serif; font-size: 1.3rem; font-weight: 900; color: {page_colors['primary']};
                               text-transform: uppercase; letter-spacing: 2px; margin-bottom: 1rem;">
                        {zone} <span style="color: {page_colors['text_muted']}; font-size: 0.9rem;">({total_in_zone})</span>
                    </h3>
                </div>
                """, unsafe_allow_html=True)

                # Show each cell within the zone
                for cell_group in sorted(cells.keys(), key=str.lower):
                    names = cells[cell_group]
                    st.markdown(f"""
                    <div style="margin-left: 1.5rem; margin-bottom: 1.5rem;">
                        <h4 style="font-family: 'Inter', sans-serif; font-size: 1rem; font-weight: 700; color: {page_colors['text_muted']};
                                   letter-spacing: 1px; margin-bottom: 0.5rem;">
                            {cell_group} <span style="color: {page_colors['text_muted']}; font-size: 0.85rem;">({len(names)})</span>
                        </h4>
                        <div>
                            {''.join([f'<span class="name-badge">{name}</span>' for name in sorted(names)])}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            # Regular cell group display - sorted alphabetically
            sorted_groups_alpha = sorted(display_data.items(), key=lambda x: x[0].lower())
            for group_name, names in sorted_groups_alpha:
                st.markdown(f"""
                <div style="margin-bottom: 2rem;">
                    <h3 style="font-family: 'Inter', sans-serif; font-size: 1.3rem; font-weight: 900; color: {page_colors['primary']};
                               text-transform: uppercase; letter-spacing: 2px; margin-bottom: 1rem;">
                        {group_name} <span style="color: {page_colors['text_muted']}; font-size: 0.9rem;">({len(names)})</span>
                    </h3>
                    <div>
                        {''.join([f'<span class="name-badge">{name}</span>' for name in sorted(names)])}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="empty-state">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📋</div>
            <div class="empty-state-text">No check-ins yet today</div>
            <div style="font-size: 1rem; color: {page_colors['text_muted']}; margin-top: 1rem; font-weight: 500;">
                Be the first to check in!
            </div>
        </div>
        """, unsafe_allow_html=True)


# ========== SIDEBAR NAVIGATION ==========
# Use query params for persistent page selection across refreshes
query_params = st.query_params
default_page = query_params.get("page", "nwst")

# Map query param to page name
page_map = {
    "nwst": "NWST Check In",
    "leaders": "Leaders Discipleship Check In"
}
reverse_page_map = {v: k for k, v in page_map.items()}

# Get current page from query params (source of truth)
current_page = page_map.get(default_page, "NWST Check In")
page = current_page  # Set page variable for use later

with st.sidebar:
    # Email Report Button
    st.markdown(f"""
    <h3 style="color: {page_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 700; letter-spacing: 1px; font-size: 0.9rem;">
        ADMIN ACTIONS
    </h3>
    """, unsafe_allow_html=True)

    if st.button("📧 Email Report Now", type="secondary", use_container_width=True, key="send_email_btn"):
        st.session_state.show_email_confirm = True

    # Email confirmation dialog
    if st.session_state.get('show_email_confirm', False):
        st.warning("Send weekly report email now?")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("Yes, Send", type="primary", key="confirm_send"):
                st.session_state.show_email_confirm = False
                st.session_state.sending_email = True
                st.rerun()
        with col_no:
            if st.button("Cancel", key="cancel_send"):
                st.session_state.show_email_confirm = False
                st.rerun()

    # Handle email sending
    if st.session_state.get('sending_email', False):
        st.session_state.sending_email = False
        with st.spinner("Sending email report..."):
            try:
                from weekly_email_report import main as send_weekly_report
                # Redirect stdout to capture output
                import io
                import sys
                old_stdout = sys.stdout
                sys.stdout = io.StringIO()

                send_weekly_report()

                output = sys.stdout.getvalue()
                sys.stdout = old_stdout

                if "SUCCESS" in output:
                    st.success("Email report sent successfully!")
                else:
                    st.error("Failed to send email. Check configuration.")
                    if output:
                        st.text(output)
            except ImportError:
                st.error("Email module not found. Please ensure weekly_email_report.py exists.")
            except Exception as e:
                st.error(f"Error sending email: {str(e)}")

# ========== RENDER SELECTED PAGE ==========
# Add toggle tabs in main content area for switching between check-in types
st.markdown(f"""
<style>
    .checkin-tabs {{
        display: flex;
        justify-content: center;
        gap: 0;
        margin-bottom: 1rem;
    }}
    .checkin-tab {{
        padding: 0.8rem 1.5rem;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 0.9rem;
        letter-spacing: 1px;
        text-transform: uppercase;
        cursor: pointer;
        transition: all 0.2s ease;
        border: 2px solid {page_colors['primary']};
        text-decoration: none;
    }}
    .checkin-tab-active {{
        background: {page_colors['primary']};
        color: {page_colors['background']};
    }}
    .checkin-tab-inactive {{
        background: transparent;
        color: {page_colors['primary']};
    }}
    .checkin-tab-inactive:hover {{
        background: rgba({int(page_colors['primary'][1:3], 16)}, {int(page_colors['primary'][3:5], 16)}, {int(page_colors['primary'][5:7], 16)}, 0.2);
    }}
    .checkin-tab:first-child {{
        border-right: none;
    }}
</style>
""", unsafe_allow_html=True)

# Create tabs using columns and buttons
tab_col1, tab_col2 = st.columns(2)
with tab_col1:
    nwst_active = page == "NWST Check In"
    if st.button(
        "NWST Check In",
        type="primary" if nwst_active else "secondary",
        use_container_width=True,
        key="tab_nwst",
        disabled=nwst_active
    ):
        st.query_params["page"] = "nwst"
        st.rerun()

with tab_col2:
    leaders_active = page == "Leaders Discipleship Check In"
    if st.button(
        "Leaders Check In",
        type="primary" if leaders_active else "secondary",
        use_container_width=True,
        key="tab_leaders",
        disabled=leaders_active
    ):
        st.query_params["page"] = "leaders"
        st.rerun()

# Display clear page header
if page == "NWST Check In":
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {page_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            NWST Check In
        </h1>
        <p style="color: {page_colors['text_muted']}; font-size: 0.9rem; margin: 0;">NWST Service Attendance</p>
    </div>
    """, unsafe_allow_html=True)
    render_check_in_form(ATTENDANCE_TAB_NAME, "attendance_form", "NWST Check In")
    render_qr_section()
    render_recent_checkins_table(ATTENDANCE_TAB_NAME)
    render_dashboard(ATTENDANCE_TAB_NAME)
else:
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {page_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            Leaders Discipleship
        </h1>
        <p style="color: {page_colors['text_muted']}; font-size: 0.9rem; margin: 0;">Leaders Check-In (Grouped by Zone)</p>
    </div>
    """, unsafe_allow_html=True)
    render_check_in_form(LEADERS_ATTENDANCE_TAB_NAME, "leaders_attendance_form", "Leaders Check In")
    render_recent_checkins_table(LEADERS_ATTENDANCE_TAB_NAME)
    render_dashboard(LEADERS_ATTENDANCE_TAB_NAME, group_by_zone=True)

# Footer
st.markdown("---")
st.markdown(
    f"<div style='text-align: center; color: {page_colors['text_muted']}; font-size: 0.9em;'>"
    "Church Check-In System | Powered by Streamlit"
    "</div>",
    unsafe_allow_html=True
)
