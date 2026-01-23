import os
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

@st.cache_data(ttl=60)  # Cache for 60 seconds to reduce API calls
def get_options_from_sheet(_client, sheet_id):
    """Read options from Column C of the Options tab in Google Sheets"""
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
        
        return options, None
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            return None, "⚠️ API quota exceeded. Please wait a moment and refresh the page."
        return None, f"❌ Error reading options from column C: {str(e)}"
    except Exception as e:
        return None, f"❌ Error reading options from column C: {str(e)}"

def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")

def get_now_myt():
    """Get current datetime in MYT timezone"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt)

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

@st.cache_data(ttl=60)  # Cache for 60 seconds to reduce API calls
def get_today_attendance_data(_client, sheet_id, refresh_key=0):
    """Get today's attendance data with names and cell groups grouped"""
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        
        # Try to get the Attendance worksheet
        try:
            attendance_sheet = spreadsheet.worksheet(ATTENDANCE_TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return {}, []
        
        # Get all rows from the Attendance sheet
        all_rows = attendance_sheet.get_all_values()
        
        if len(all_rows) <= 1:  # Only header row or empty
            return {}, []
        
        # Get today's date in MYT
        today_myt = get_today_myt_date()
        
        # Dictionary to store cell group -> list of names
        cell_group_data = {}
        # List to store all checked-in entries (for set deduplication)
        checked_in_set = set()
        checked_in_list = []  # Keep order for first occurrence
        
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
                        # Only add if not already in set (avoid duplicates)
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
        
        return cell_group_data, checked_in_list
        
    except gspread.exceptions.APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            # Return empty data but don't show error (will use cached data if available)
            return {}, []
        return {}, []
    except Exception as e:
        return {}, []

def get_checked_in_today(client, sheet_id):
    """Get a set of people who have already checked in today (MYT date)"""
    try:
        refresh_key = st.session_state.get('refresh_counter', 0)
        _, checked_in_list = get_today_attendance_data(client, sheet_id, refresh_key)
        return set(checked_in_list)
    except Exception as e:
        # If there's an error reading attendance, return empty set (show all options)
        return set()

def save_attendance_to_sheet(client, attendance_data):
    """Save attendance data to the Attendance tab - supports batch check-ins"""
    try:
        spreadsheet = client.open_by_key(SHEET_ID)

        # Try to get the Attendance worksheet, create if it doesn't exist
        try:
            attendance_sheet = spreadsheet.worksheet(ATTENDANCE_TAB_NAME)
            # Check if headers exist, if not add them
            existing_headers = attendance_sheet.row_values(1)
            if not existing_headers:
                headers = ["Timestamp", attendance_data.get("option_type", "Option")]
                attendance_sheet.append_row(headers)
        except gspread.exceptions.WorksheetNotFound:
            # Create the Attendance worksheet
            attendance_sheet = spreadsheet.add_worksheet(
                title=ATTENDANCE_TAB_NAME,
                rows=1000,
                cols=20
            )
            # Add headers
            headers = ["Timestamp", attendance_data.get("option_type", "Option")]
            attendance_sheet.append_row(headers)

        # Get current time in Malaysia Time (MYT, UTC+8)
        myt = timezone(timedelta(hours=8))
        timestamp = datetime.now(myt).strftime("%Y-%m-%d %H:%M:%S")

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

CACHE_TTL_SECONDS = 60  # 1 minute cache duration

# Generate daily colors
daily_colors = generate_daily_colors()

# Add CSS to reduce Streamlit default spacing and style buttons with daily color
st.markdown(f"""
<style>
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
        color: #000 !important;
        transform: scale(1.02) !important;
    }}

    /* Primary buttons (Check In, Close) */
    .stButton > button[kind="primary"] {{
        background-color: {daily_colors['primary']} !important;
        color: #000 !important;
        border: 2px solid {daily_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
    }}

    /* Form submit button */
    .stFormSubmitButton > button {{
        background-color: {daily_colors['primary']} !important;
        color: #000 !important;
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
        color: #000 !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {daily_colors['primary']} !important;
    }}
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
        st.info("💡 **Tip:** If you're seeing quota errors, wait a moment and refresh the page. Data is cached to reduce API calls.")
    st.stop()

if not options:
    if error_msg:
        st.warning(error_msg)
    else:
        st.warning("""
        ⚠️ No options found. Please add options to your Google Sheet.
        
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

# Get list of people who have already checked in today (MYT)
with st.spinner("Checking today's attendance..."):
    checked_in_today = get_checked_in_today(client, SHEET_ID)

# Get the single option type and values
option_type = list(options.keys())[0]
all_option_values = list(options.values())[0]

# Filter out options that have already checked in today
available_options = [opt for opt in all_option_values if opt not in checked_in_today]

# Wrap form section with GIF background
if background_gif and gif_src:
    st.markdown(f"""
    <div style="
        position: relative;
        padding: 2rem;
        margin: 0;
        border-radius: 8px;
        border: 2px solid {daily_colors['primary']};
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
        <div style="position: relative; z-index: 1;">
    """, unsafe_allow_html=True)

# Display form in centered column
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    # Show instruction text
    if background_gif:
        st.markdown(f"""
        <div style="
            background: rgba(0, 0, 0, 0.6);
            padding: 0.75rem 1rem;
            border-radius: 6px;
            margin-bottom: 1rem;
        ">
            <p style="
                font-size: 1rem; 
                margin: 0; 
                color: white;
                text-shadow: 1px 1px 2px rgba(0,0,0,0.8);
                text-align: center;
            ">Select your name from the dropdown below to check in.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown('<p style="font-size: 1rem; margin-bottom: 1rem; text-align: center;">Select your name from the dropdown below to check in.</p>', unsafe_allow_html=True)
    
    # Show simple refresh message
    if checked_in_today:
        st.success("Refreshed!")
    
    # Display form
    with st.form("attendance_form", clear_on_submit=True):
        # Check if there are any available options
        if not available_options:
            st.warning("✅ All attendees have already checked in for today!")
            st.stop()

        # Multi-select for batch check-ins (reduces API calls)
        selected_options = st.multiselect(
            f"Select {option_type}(s) *",
            options=available_options,
            help="Select one or more people to check in at once. This reduces API calls.",
            default=[]
        )

        # Submit button
        submitted = st.form_submit_button("Check In", type="primary", use_container_width=True)

        if submitted:
            # Validation
            if not selected_options:
                st.error("❌ Please select at least one person.")
            else:
                # Prepare attendance data for batch check-in
                attendance_data = {
                    "selected_options": selected_options,
                    "option_type": option_type
                }

                # Save to Google Sheets (single API call for all)
                success, message = save_attendance_to_sheet(client, attendance_data)

                if success:
                    # Increment refresh counter to invalidate cache
                    st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
                    st.session_state.last_refresh_time = get_now_myt()
                    # Also clear cache for immediate effect
                    get_today_attendance_data.clear()
                    get_options_from_sheet.clear()

                    st.success(f"✅ {message}")
                    st.balloons()
                    # Refresh the page to update the dropdown
                    st.rerun()
                else:
                    st.error(f"❌ {message}")

# Close background GIF container if it was opened
if background_gif:
    st.markdown("</div></div>", unsafe_allow_html=True)
else:
    # Show placeholder if no GIF
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 1rem; padding: 1rem; background: #0a0a0a; border: 2px dashed {daily_colors['primary']}; border-radius: 8px;">
        <p style="color: {daily_colors['primary']}; font-family: 'Inter', sans-serif; font-weight: 600; margin: 0;">
            Add your banner GIF by setting BANNER_GIF_URL in .env or placing banner.gif in the CHECK IN folder
        </p>
    </div>
    """, unsafe_allow_html=True)

# ========== FEEDBACK FORM QR CODE SECTION ==========
st.markdown("<br><br>", unsafe_allow_html=True)
col_qr1, col_qr2, col_qr3 = st.columns([3, 1, 3])
with col_qr2:
    if st.button("👋 I'm New!", type="secondary", use_container_width=True):
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
            <div class="modal-title">👋 Welcome! Scan to fill out the form</div>
            <img src="data:image/png;base64,{qr_base64}" class="qr-image" alt="QR Code"/>
            <div class="modal-subtitle">
                <a href="{feedback_url}" target="_blank" class="link-btn">Or click here</a>
            </div>
        </div>
    </div>
    """, height=500)

    # Close button using Streamlit
    col_close1, col_close2, col_close3 = st.columns([2, 1, 2])
    with col_close2:
        if st.button("✕ Close", type="primary", use_container_width=True):
            st.session_state.show_qr_modal = False
            st.rerun()

# ========== DASHBOARD SECTION ==========
st.markdown("---")

# Calculate cache status
time_since_refresh = (get_now_myt() - st.session_state.last_refresh_time).total_seconds()
cache_remaining = max(0, CACHE_TTL_SECONDS - int(time_since_refresh))
cache_expired = cache_remaining == 0
qr_modal_open = st.session_state.get('show_qr_modal', False)

# Add refresh button with cache indicator (disabled until cache expires, OR enabled if QR modal is open)
col_refresh1, col_refresh2, col_refresh3 = st.columns([3, 1, 3])
with col_refresh2:
    if cache_expired or qr_modal_open:
        if st.button("🔄 Refresh Dashboard", type="secondary", use_container_width=True):
            # Increment refresh counter to invalidate cache
            st.session_state.refresh_counter = st.session_state.get('refresh_counter', 0) + 1
            st.session_state.last_refresh_time = get_now_myt()
            # Also clear cache for immediate effect
            get_today_attendance_data.clear()
            get_options_from_sheet.clear()
            st.rerun()
    else:
        # Show disabled button with live countdown using HTML/JS
        refresh_timestamp = st.session_state.last_refresh_time.timestamp() * 1000
        components.html(f"""
        <style>
            .disabled-btn {{
                background-color: transparent;
                color: {daily_colors['primary']};
                border: 2px solid {daily_colors['primary']};
                border-radius: 0px;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                font-weight: 600;
                font-size: 14px;
                letter-spacing: 0.5px;
                padding: 0.5rem 1rem;
                width: 100%;
                opacity: 0.5;
                cursor: not-allowed;
            }}
        </style>
        <button class="disabled-btn" disabled>
            🔄 Refresh in <span id="btn-countdown">{cache_remaining}s</span>
        </button>
        <script>
            const refreshTime = {refresh_timestamp};
            const cacheDuration = {CACHE_TTL_SECONDS} * 1000;
            const countdownEl = document.getElementById('btn-countdown');

            function updateBtnCountdown() {{
                const now = Date.now();
                const elapsed = now - refreshTime;
                const remaining = Math.max(0, cacheDuration - elapsed);

                if (remaining > 0) {{
                    const mins = Math.floor(remaining / 60000);
                    const secs = Math.floor((remaining % 60000) / 1000);
                    countdownEl.textContent = mins > 0 ? mins + 'm ' + secs + 's' : secs + 's';
                }} else {{
                    countdownEl.textContent = 'now!';
                }}
            }}

            updateBtnCountdown();
            setInterval(updateBtnCountdown, 1000);
        </script>
        """, height=45)

# Show cache status with live countdown timer
last_refresh_str = st.session_state.last_refresh_time.strftime("%H:%M:%S")
refresh_timestamp = st.session_state.last_refresh_time.timestamp() * 1000  # Convert to JS milliseconds

components.html(f"""
<div style="text-align: center; padding: 0.5rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <span style="color: #888; font-size: 0.85rem;">
        📦 Last refreshed at <b>{last_refresh_str}</b> •
        New data drops in <b><span id="countdown">--</span></b>
    </span>
</div>
<script>
    const refreshTime = {refresh_timestamp};
    const cacheDuration = {CACHE_TTL_SECONDS} * 1000;
    const countdownEl = document.getElementById('countdown');

    function updateCountdown() {{
        const now = Date.now();
        const elapsed = now - refreshTime;
        const remaining = Math.max(0, cacheDuration - elapsed);

        if (remaining > 0) {{
            const mins = Math.floor(remaining / 60000);
            const secs = Math.floor((remaining % 60000) / 1000);
            countdownEl.textContent = mins > 0 ? mins + 'm ' + secs + 's' : secs + 's';
        }} else {{
            countdownEl.textContent = 'now! hit refresh 🔄';
            countdownEl.style.color = '#4CAF50';
        }}
    }}

    updateCountdown();
    setInterval(updateCountdown, 1000);
</script>
""", height=50)

# Get today's attendance data
with st.spinner("Loading dashboard data..."):
    refresh_key = st.session_state.get('refresh_counter', 0)
    cell_group_data, checked_in_list = get_today_attendance_data(client, SHEET_ID, refresh_key)

total_checked_in = len(checked_in_list)

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
        background: #000000;
        padding: 2rem 2.5rem;
        border-radius: 0px;
        border-left: 6px solid {daily_colors['primary']};
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
        color: #999;
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
        color: #666;
        margin-top: 0.5rem;
        font-weight: 500;
    }}
    .dashboard-section {{
        background: #0a0a0a;
        padding: 2rem;
        border-radius: 0px;
        border: 2px solid {daily_colors['primary']};
        margin: 2rem 0;
    }}
    .section-title {{
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem;
        font-weight: 900;
        color: {daily_colors['primary']};
        text-transform: uppercase;
        letter-spacing: 3px;
        margin-bottom: 1.5rem;
        border-bottom: 3px solid {daily_colors['primary']};
        padding-bottom: 0.5rem;
        display: inline-block;
    }}
    .name-badge {{
        background: #1a1a1a;
        border: 1px solid {daily_colors['primary']};
        color: {daily_colors['primary']};
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
        background: {daily_colors['primary']};
        color: #000;
        transform: scale(1.05);
    }}
    .empty-state {{
        text-align: center;
        padding: 4rem 2rem;
        background: #0a0a0a;
        border: 2px dashed #333;
        border-radius: 0px;
    }}
    .empty-state-text {{
        font-family: 'Inter', sans-serif;
        font-size: 1.5rem;
        color: #666;
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

if total_checked_in > 0:
    # Bar Chart Section
    st.markdown('<div class="section-title">📊 Check-Ins by Cell Group</div>', unsafe_allow_html=True)
    
    # Prepare data for bar chart - sort alphabetically by cell group name
    sorted_cell_groups = sorted(cell_group_data.items(), key=lambda x: x[0].lower())
    
    chart_data = {
        'Cell Group': [group for group, _ in sorted_cell_groups],
        'Count': [len(names) for _, names in sorted_cell_groups]
    }
    df_chart = pd.DataFrame(chart_data)
    
    # Create bar chart with modern edgy style
    fig = px.bar(
        df_chart,
        x='Cell Group',
        y='Count',
        color='Count',
        color_continuous_scale=['#000000', daily_colors['primary']],
        text='Count',
        labels={'Count': 'Number of People', 'Cell Group': 'Cell Group'},
        height=400
    )
    
    # Update layout for modern edgy style
    fig.update_layout(
        plot_bgcolor='#000000',
        paper_bgcolor='#0a0a0a',
        font=dict(family='Inter, sans-serif', size=12, color=daily_colors['primary']),
        xaxis=dict(
            title=dict(font=dict(size=14, color=daily_colors['primary'], family='Inter')),
            tickfont=dict(color='#999', family='Inter'),
            gridcolor='#333',
            linecolor=daily_colors['primary'],
            linewidth=2
        ),
        yaxis=dict(
            title=dict(font=dict(size=14, color=daily_colors['primary'], family='Inter')),
            tickfont=dict(color='#999', family='Inter'),
            gridcolor='#333',
            linecolor=daily_colors['primary'],
            linewidth=2
        ),
        coloraxis_showscale=False,
        showlegend=False,
        margin=dict(l=50, r=50, t=30, b=50)
    )
    
    # Update bar style
    fig.update_traces(
        textfont=dict(size=14, color=daily_colors['primary'], family='Inter', weight='bold'),
        textposition='outside',
        marker=dict(line=dict(color=daily_colors['primary'], width=2)),
        hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>',
        hoverlabel=dict(bgcolor='#000', font=dict(color=daily_colors['primary'], family='Inter'))
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Names Breakdown Section
    st.markdown('<div class="section-title">👥 Attendees by Cell Group</div>', unsafe_allow_html=True)
    
    # Display names for each cell group
    for cell_group, names in sorted_cell_groups:
        st.markdown(f"""
        <div style="margin-bottom: 2rem;">
            <h3 style="font-family: 'Inter', sans-serif; font-size: 1.3rem; font-weight: 900; color: {daily_colors['primary']}; 
                       text-transform: uppercase; letter-spacing: 2px; margin-bottom: 1rem;">
                {cell_group} <span style="color: #666; font-size: 0.9rem;">({len(names)})</span>
            </h3>
            <div>
                {''.join([f'<span class="name-badge">{name}</span>' for name in sorted(names)])}
            </div>
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="empty-state">
        <div style="font-size: 4rem; margin-bottom: 1rem;">📋</div>
        <div class="empty-state-text">No check-ins yet today</div>
        <div style="font-size: 1rem; color: #999; margin-top: 1rem; font-weight: 500;">
            Be the first to check in! 🎯
        </div>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666; font-size: 0.9em;'>"
    "Church Check-In System | Powered by Streamlit"
    "</div>",
    unsafe_allow_html=True
)
