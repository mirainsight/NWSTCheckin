#!/usr/bin/env python3
"""
Weekly Email Report Generator for Church Check-In Dashboard

This script generates and sends weekly email reports containing:
- NWST Check In dashboard summary
- Leaders Discipleship Check In dashboard summary
- Attendance graphs (as embedded images)
- Cell group breakdown

Scheduled to run every Saturday at 8:30 PM MYT.
"""

import os
import smtplib
import ssl
import hashlib
import colorsys
import random
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import datetime, timedelta, timezone
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import pandas as pd
import altair as alt

# Load environment variables (check current dir and parent dir)
load_dotenv()  # Current directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))  # Parent directory (DISC/.env)

# Configuration
SHEET_ID = os.getenv("ATTENDANCE_SHEET_ID", "")
OPTIONS_TAB_NAME = "Options"
ATTENDANCE_TAB_NAME = "Attendance"
LEADERS_ATTENDANCE_TAB_NAME = "Leaders Attendance"
KEY_VALUES_TAB_NAME = "Key Values"

# Email configuration - uses same credentials as DISC app
# Tries Streamlit secrets first, then environment variables
def get_email_credentials():
    """Get email credentials - works with both .env and Streamlit secrets"""
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            if 'SENDER_EMAIL' in st.secrets and 'SENDER_PASSWORD' in st.secrets:
                print("Using Streamlit secrets for email credentials")
                return st.secrets['SENDER_EMAIL'], st.secrets['SENDER_PASSWORD']
    except ImportError:
        pass
    except Exception as e:
        print(f"Warning: Could not load email credentials from Streamlit secrets: {str(e)}")

    return os.getenv("SENDER_EMAIL", ""), os.getenv("SENDER_PASSWORD", "")


def get_sheet_id():
    """Get Sheet ID from .env or Streamlit secrets"""
    sheet_id = os.getenv("ATTENDANCE_SHEET_ID", "")
    if not sheet_id:
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'ATTENDANCE_SHEET_ID' in st.secrets:
                sheet_id = st.secrets['ATTENDANCE_SHEET_ID']
                print("Using Streamlit secrets for ATTENDANCE_SHEET_ID")
        except ImportError:
            pass
        except Exception as e:
            print(f"Warning: Could not load ATTENDANCE_SHEET_ID from Streamlit secrets: {str(e)}")
    return sheet_id

EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "shaun.quek@sibkl.org.my")
EMAIL_CC = os.getenv("EMAIL_CC", "narrowstreet.sibkl@gmail.com")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# MYT Timezone
MYT = timezone(timedelta(hours=8))


def get_now_myt():
    """Get current datetime in MYT timezone"""
    return datetime.now(MYT)


def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    return get_now_myt().strftime("%Y-%m-%d")


def generate_daily_colors():
    """Generate random colors based on today's date (consistent throughout the day)"""
    today = get_today_myt_date()
    # Use date as seed for consistent colors throughout the day
    seed = int(hashlib.md5(today.encode()).hexdigest(), 16)

    # Generate vibrant colors using the seed
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
        'background': '#000000',
        'accent': primary_color
    }


def get_gsheet_client():
    """Connect to Google Sheets - works with both local files and Streamlit secrets"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = None

        # Try credentials.json file first (for local development)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        possible_paths = [
            os.path.join(script_dir, 'credentials.json'),
            'credentials.json',
            'CHECK IN/credentials.json',
        ]

        credentials_path = None
        for path in possible_paths:
            if os.path.exists(path):
                credentials_path = path
                break

        if credentials_path:
            creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        else:
            # Try Streamlit secrets (for cloud deployment)
            try:
                import streamlit as st
                if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                    creds_dict = dict(st.secrets['gcp_service_account'])
                    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
                    print("Using Streamlit secrets for Google Sheets credentials")
            except ImportError:
                pass
            except Exception as e:
                print(f"Warning: Could not load Streamlit secrets: {str(e)}")

        if creds is None:
            print("ERROR: No Google Sheets credentials found")
            print("For local: Place credentials.json in script directory")
            print("For Streamlit Cloud: Configure gcp_service_account in secrets")
            return None

        client = gspread.authorize(creds)
        return client

    except Exception as e:
        print(f"ERROR: Could not connect to Google Sheets: {str(e)}")
        return None


def get_cell_to_zone_mapping(client, sheet_id):
    """Read cell-to-zone mapping from Key Values tab in Google Sheets."""
    try:
        spreadsheet = client.open_by_key(sheet_id)
        key_values_sheet = spreadsheet.worksheet(KEY_VALUES_TAB_NAME)
        all_values = key_values_sheet.get_all_values()

        if len(all_values) <= 1:
            return {}

        cell_to_zone = {}
        for row in all_values[1:]:
            if len(row) >= 3:
                cell_name = row[0].strip()
                zone = row[2].strip()
                if cell_name and zone:
                    cell_to_zone[cell_name.lower()] = zone

        return cell_to_zone
    except Exception as e:
        print(f"Warning: Could not read Key Values: {str(e)}")
        return {}


def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)"""
    if not name_cell_group_str:
        return None, None

    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    else:
        return parts[0].strip(), "Unknown"


def get_today_attendance_data(client, sheet_id, tab_name):
    """Get today's attendance data with names and cell groups grouped"""
    try:
        spreadsheet = client.open_by_key(sheet_id)

        try:
            attendance_sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            return {}, []

        all_rows = attendance_sheet.get_all_values()

        if len(all_rows) <= 1:
            return {}, []

        today_myt = get_today_myt_date()
        cell_group_data = {}
        checked_in_set = set()
        checked_in_list = []

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
                        if name_cell_group not in checked_in_set:
                            checked_in_set.add(name_cell_group)
                            checked_in_list.append(name_cell_group)

                            name, cell_group = parse_name_cell_group(name_cell_group)

                            if cell_group not in cell_group_data:
                                cell_group_data[cell_group] = []
                            cell_group_data[cell_group].append(name)
            except Exception:
                continue

        return cell_group_data, checked_in_list

    except Exception as e:
        print(f"ERROR getting attendance data: {str(e)}")
        return {}, []


def generate_bar_chart(display_data, group_label, title, primary_color="#00aa66"):
    """Generate a bar chart image for email embedding using Altair"""
    if not display_data:
        return None

    try:
        sorted_groups = sorted(display_data.items(), key=lambda x: len(x[1]), reverse=True)

        # Prepare data for Altair
        df = pd.DataFrame({
            group_label: [group for group, _ in sorted_groups],
            'Count': [len(names) for _, names in sorted_groups]
        })

        # Create Altair chart
        chart = alt.Chart(df).mark_bar(
            color=primary_color,
            cornerRadiusTopLeft=4,
            cornerRadiusTopRight=4
        ).encode(
            x=alt.X(f'{group_label}:N',
                   sort='-y',
                   axis=alt.Axis(
                       labelAngle=-45,
                       labelFontSize=11,
                       titleFontSize=13,
                       titleFontWeight='bold'
                   )),
            y=alt.Y('Count:Q',
                   axis=alt.Axis(
                       labelFontSize=11,
                       titleFontSize=13,
                       titleFontWeight='bold'
                   ))
        ).properties(
            width=500,
            height=300,
            title=alt.TitleParams(
                text=title,
                fontSize=16,
                fontWeight='bold',
                anchor='middle'
            )
        )

        # Add text labels on bars
        text = chart.mark_text(
            align='center',
            baseline='bottom',
            dy=-5,
            fontSize=12,
            fontWeight='bold'
        ).encode(
            text='Count:Q'
        )

        final_chart = (chart + text).configure_view(
            strokeWidth=0
        ).configure_axis(
            grid=False
        )

        # Save to PNG bytes using vl-convert
        img_bytes = final_chart.to_image(format='png', scale_factor=2)
        return img_bytes

    except Exception as e:
        print(f"Warning: Could not generate chart - {str(e)}")
        return None


def generate_report_html(nwst_data, leaders_data, cell_to_zone, daily_colors, has_nwst_chart=True, has_leaders_chart=True):
    """Generate HTML email content with dashboard summary"""
    today = get_now_myt().strftime("%A, %B %d, %Y")
    current_time = get_now_myt().strftime("%I:%M %p MYT")
    primary_color = daily_colors['primary']
    light_color = daily_colors['light']

    # Process NWST data
    nwst_cell_data, nwst_list = nwst_data
    nwst_total = len(nwst_list)

    # Process Leaders data with zone grouping
    leaders_cell_data, leaders_list = leaders_data
    leaders_total = len(leaders_list)

    # Convert leaders cell data to zone data
    zone_data = {}
    zone_cell_names = {}
    for cell_group, names in leaders_cell_data.items():
        zone = cell_to_zone.get(cell_group.lower(), cell_group)
        if zone not in zone_data:
            zone_data[zone] = []
            zone_cell_names[zone] = {}
        zone_data[zone].extend(names)
        if cell_group not in zone_cell_names[zone]:
            zone_cell_names[zone][cell_group] = []
        zone_cell_names[zone][cell_group].extend(names)

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            line-height: 1.6;
            color: #333333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: #ffffff;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            border-bottom: 3px solid {primary_color};
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: {primary_color};
            margin: 0;
            font-size: 28px;
        }}
        .header p {{
            color: #666666;
            margin: 10px 0 0 0;
        }}
        .section {{
            margin-bottom: 40px;
        }}
        .section-title {{
            background: linear-gradient(135deg, {primary_color}, {light_color});
            color: white;
            padding: 15px 20px;
            border-radius: 6px;
            font-size: 20px;
            font-weight: bold;
            margin-bottom: 20px;
        }}
        .kpi-box {{
            background: #f8f9fa;
            border-left: 5px solid {primary_color};
            padding: 20px;
            margin-bottom: 20px;
        }}
        .kpi-number {{
            font-size: 48px;
            font-weight: bold;
            color: {primary_color};
        }}
        .kpi-label {{
            color: #666666;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .chart-container {{
            text-align: center;
            margin: 20px 0;
        }}
        .chart-container img {{
            max-width: 100%;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .breakdown {{
            margin-top: 20px;
        }}
        .zone-header {{
            background: {light_color}22;
            padding: 12px 15px;
            border-radius: 4px;
            font-weight: bold;
            color: {primary_color};
            margin-top: 15px;
            border-left: 4px solid {primary_color};
        }}
        .cell-header {{
            color: #555;
            font-weight: 600;
            padding: 10px 0 5px 15px;
            border-bottom: 1px solid #eee;
        }}
        .names-list {{
            padding: 10px 15px 10px 30px;
            color: #666;
        }}
        .name-badge {{
            display: inline-block;
            background: #e3f2fd;
            color: #1565c0;
            padding: 4px 10px;
            margin: 3px;
            border-radius: 15px;
            font-size: 13px;
        }}
        .footer {{
            text-align: center;
            color: #999999;
            font-size: 12px;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #eeeeee;
        }}
        .no-data {{
            text-align: center;
            color: #999;
            padding: 40px;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Weekly Check-In Report</h1>
            <p>{today} | Generated at {current_time}</p>
        </div>

        <!-- NWST Check In Section -->
        <div class="section">
            <div class="section-title">NWST Check In</div>
            <div class="kpi-box">
                <div class="kpi-number">{nwst_total}</div>
                <div class="kpi-label">Total Checked In Today</div>
            </div>
"""

    # Add NWST chart placeholder
    if nwst_total > 0:
        html += """
            <div class="chart-container">
                <img src="cid:nwst_chart" alt="NWST Attendance Chart">
            </div>
            <div class="breakdown">
                <h3>Cell Group Breakdown</h3>
"""
        # Add cell group breakdown for NWST
        for cell_group in sorted(nwst_cell_data.keys(), key=str.lower):
            names = nwst_cell_data[cell_group]
            html += f"""
                <div class="cell-header">{cell_group} ({len(names)})</div>
                <div class="names-list">
                    {''.join([f'<span class="name-badge">{name}</span>' for name in sorted(names)])}
                </div>
"""
        html += "</div>"
    else:
        html += '<div class="no-data">No check-ins recorded today</div>'

    html += """
        </div>

        <!-- Leaders Discipleship Check In Section -->
        <div class="section">
            <div class="section-title">Leaders Discipleship Check In</div>
"""
    html += f"""
            <div class="kpi-box">
                <div class="kpi-number">{leaders_total}</div>
                <div class="kpi-label">Total Checked In Today</div>
            </div>
"""

    # Add Leaders chart and breakdown
    if leaders_total > 0:
        html += """
            <div class="chart-container">
                <img src="cid:leaders_chart" alt="Leaders Attendance Chart">
            </div>
            <div class="breakdown">
                <h3>Zone & Cell Breakdown</h3>
"""
        # Add zone -> cell -> names breakdown for Leaders
        for zone in sorted(zone_cell_names.keys(), key=str.lower):
            cells = zone_cell_names[zone]
            total_in_zone = sum(len(names) for names in cells.values())
            html += f'<div class="zone-header">{zone} ({total_in_zone})</div>'

            for cell_group in sorted(cells.keys(), key=str.lower):
                names = cells[cell_group]
                html += f"""
                <div class="cell-header">{cell_group} ({len(names)})</div>
                <div class="names-list">
                    {''.join([f'<span class="name-badge">{name}</span>' for name in sorted(names)])}
                </div>
"""
        html += "</div>"
    else:
        html += '<div class="no-data">No check-ins recorded today</div>'

    html += f"""
        </div>

        <div class="footer">
            <p>This is an automated weekly report from the Church Check-In System.</p>
            <p>Generated on {today} at {current_time}</p>
        </div>
    </div>
</body>
</html>
"""

    return html


def send_email_report(html_content, nwst_chart_bytes, leaders_chart_bytes):
    """Send the email report with embedded charts"""

    # Get email credentials
    sender_email, sender_password = get_email_credentials()

    if not sender_email or not sender_password:
        print("ERROR: Email credentials not configured.")
        print("Please set SENDER_EMAIL and SENDER_PASSWORD in .env file")
        return False

    try:
        # Create message
        msg = MIMEMultipart('related')
        msg['Subject'] = f"Weekly Check-In Report - {get_now_myt().strftime('%B %d, %Y')}"
        msg['From'] = sender_email
        msg['To'] = EMAIL_RECIPIENT
        msg['Cc'] = EMAIL_CC

        # Build recipient list for sending (includes CC)
        all_recipients = [EMAIL_RECIPIENT, EMAIL_CC]

        # Create HTML part
        msg_alternative = MIMEMultipart('alternative')
        msg.attach(msg_alternative)

        # Plain text fallback
        text_content = f"""
Weekly Check-In Report - {get_now_myt().strftime('%B %d, %Y')}

This email contains the weekly attendance dashboard report.
Please view this email in an HTML-compatible email client to see the full report with charts.

This is an automated report from the Church Check-In System.
        """

        msg_alternative.attach(MIMEText(text_content, 'plain'))
        msg_alternative.attach(MIMEText(html_content, 'html'))

        # Attach NWST chart if available
        if nwst_chart_bytes:
            img = MIMEImage(nwst_chart_bytes)
            img.add_header('Content-ID', '<nwst_chart>')
            img.add_header('Content-Disposition', 'inline', filename='nwst_chart.png')
            msg.attach(img)

        # Attach Leaders chart if available
        if leaders_chart_bytes:
            img = MIMEImage(leaders_chart_bytes)
            img.add_header('Content-ID', '<leaders_chart>')
            img.add_header('Content-Disposition', 'inline', filename='leaders_chart.png')
            msg.attach(img)

        # Send email
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, all_recipients, msg.as_string())

        print(f"SUCCESS: Email sent to {EMAIL_RECIPIENT} (CC: {EMAIL_CC})")
        return True

    except Exception as e:
        print(f"ERROR sending email: {str(e)}")
        return False


def main():
    """Main function to generate and send the weekly report"""
    print(f"{'='*60}")
    print(f"Weekly Email Report Generator")
    print(f"Running at: {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"{'='*60}")

    # Get Sheet ID (from .env or Streamlit secrets)
    sheet_id = get_sheet_id()
    if not sheet_id:
        print("ERROR: ATTENDANCE_SHEET_ID not configured")
        print("Set it in .env file or Streamlit secrets")
        return

    # Connect to Google Sheets
    print("\n[1/5] Connecting to Google Sheets...")
    client = get_gsheet_client()
    if not client:
        return
    print("Connected successfully!")

    # Get cell-to-zone mapping
    print("\n[2/5] Loading zone mappings...")
    cell_to_zone = get_cell_to_zone_mapping(client, sheet_id)
    print(f"Loaded {len(cell_to_zone)} zone mappings")

    # Get NWST attendance data
    print("\n[3/5] Fetching NWST Check In data...")
    nwst_cell_data, nwst_list = get_today_attendance_data(client, sheet_id, ATTENDANCE_TAB_NAME)
    print(f"Found {len(nwst_list)} NWST check-ins")

    # Get Leaders attendance data
    print("\n[4/5] Fetching Leaders Discipleship Check In data...")
    leaders_cell_data, leaders_list = get_today_attendance_data(client, sheet_id, LEADERS_ATTENDANCE_TAB_NAME)
    print(f"Found {len(leaders_list)} Leaders check-ins")

    # Generate charts
    print("\n[5/5] Generating charts and email...")

    # Get daily color theme
    daily_colors = generate_daily_colors()
    print(f"Using daily color theme: {daily_colors['primary']}")

    # NWST chart (by cell group)
    nwst_chart = None
    if nwst_cell_data:
        nwst_chart = generate_bar_chart(
            nwst_cell_data,
            "Cell Group",
            "NWST Check-Ins by Cell Group",
            daily_colors['primary']
        )

    # Leaders chart (by zone)
    leaders_chart = None
    if leaders_cell_data:
        # Convert to zone data for chart
        zone_data = {}
        for cell_group, names in leaders_cell_data.items():
            zone = cell_to_zone.get(cell_group.lower(), cell_group)
            if zone not in zone_data:
                zone_data[zone] = []
            zone_data[zone].extend(names)

        leaders_chart = generate_bar_chart(
            zone_data,
            "Zone",
            "Leaders Check-Ins by Zone",
            daily_colors['primary']
        )

    # Generate HTML report
    html_content = generate_report_html(
        (nwst_cell_data, nwst_list),
        (leaders_cell_data, leaders_list),
        cell_to_zone,
        daily_colors
    )

    # Send email
    print("\nSending email report...")
    success = send_email_report(html_content, nwst_chart, leaders_chart)

    if success:
        print(f"\n{'='*60}")
        print("Report sent successfully!")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("Failed to send report. Please check configuration.")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
