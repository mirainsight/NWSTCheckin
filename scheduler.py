#!/usr/bin/env python3
"""
Scheduler for Weekly Email Reports

Runs the following scheduled reports:
  - NWST Core Team Report: Every Saturday at 4:45 PM MYT
  - Weekly Report: Every Saturday at 8:30 PM MYT

Usage:
  python scheduler.py                    # Run scheduler in foreground
  python scheduler.py --now              # Send weekly report immediately (for testing)
  python scheduler.py --now-core-team    # Send NWST Core Team report immediately
  python scheduler.py --test             # Test email configuration without sending
"""

import argparse
import schedule
import time
from datetime import datetime, timedelta, timezone
from weekly_email_report import main as send_report, send_to_nwst_core_team, get_gsheet_client, get_email_credentials, get_sheet_id

# MYT Timezone (UTC+8)
MYT = timezone(timedelta(hours=8))


def get_now_myt():
    """Get current datetime in MYT timezone"""
    return datetime.now(MYT)


def job_weekly_report():
    """Job to run the weekly email report"""
    print(f"\n{'='*60}")
    print(f"Weekly Report triggered at {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"{'='*60}")
    send_report()


def job_nwst_core_team():
    """Job to run the NWST Core Team report"""
    print(f"\n{'='*60}")
    print(f"NWST Core Team Report triggered at {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"{'='*60}")
    send_to_nwst_core_team()


def test_configuration():
    """Test all configuration without sending email"""
    print(f"\n{'='*60}")
    print("Testing Configuration")
    print(f"{'='*60}")

    errors = []

    # Test Google Sheets connection
    print("\n[1] Testing Google Sheets connection...")
    sheet_id = get_sheet_id()
    if not sheet_id:
        errors.append("ATTENDANCE_SHEET_ID not configured in .env file")
        print("   FAILED: ATTENDANCE_SHEET_ID not configured")
    else:
        client = get_gsheet_client()
        if client:
            print("   OK: Google Sheets connection successful")
        else:
            errors.append("Could not connect to Google Sheets")
            print("   FAILED: Could not connect to Google Sheets")

    # Test email configuration
    print("\n[2] Testing email configuration...")
    sender_email, sender_password = get_email_credentials()

    if not sender_email:
        errors.append("SENDER_EMAIL not configured in .env file")
        print("   FAILED: SENDER_EMAIL not configured")
    else:
        print(f"   OK: SENDER_EMAIL = {sender_email}")

    if not sender_password:
        errors.append("SENDER_PASSWORD not configured in .env file")
        print("   FAILED: SENDER_PASSWORD not configured")
    else:
        print("   OK: SENDER_PASSWORD is set")

    # Summary
    print(f"\n{'='*60}")
    if errors:
        print(f"Configuration test FAILED with {len(errors)} error(s):")
        for err in errors:
            print(f"  - {err}")
    else:
        print("Configuration test PASSED!")
        print("\nYou can now run:")
        print("  python scheduler.py --now              # Send weekly report immediately")
        print("  python scheduler.py --now-core-team    # Send NWST Core Team report immediately")
        print("  python scheduler.py                    # Start scheduler")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='Weekly Email Report Scheduler')
    parser.add_argument('--now', action='store_true', help='Send weekly report immediately')
    parser.add_argument('--now-core-team', action='store_true', help='Send NWST Core Team report immediately')
    parser.add_argument('--test', action='store_true', help='Test configuration')
    args = parser.parse_args()

    if args.test:
        test_configuration()
        return

    if args.now:
        print("Sending weekly report immediately...")
        send_report()
        return

    if args.now_core_team:
        print("Sending NWST Core Team report immediately...")
        send_to_nwst_core_team()
        return

    # Schedule the jobs
    print(f"\n{'='*60}")
    print("Weekly Email Report Scheduler")
    print(f"{'='*60}")
    print(f"Current time: {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"\nScheduled reports:")
    print(f"  - NWST Core Team: Every Saturday at 4:45 PM MYT")
    print(f"  - Weekly Report:  Every Saturday at 8:30 PM MYT")
    print(f"{'='*60}\n")

    # Schedule NWST Core Team report - Saturday 4:45 PM
    schedule.every().saturday.at("16:45").do(job_nwst_core_team)

    # Schedule Weekly report - Saturday 8:30 PM
    schedule.every().saturday.at("20:30").do(job_weekly_report)

    print("Scheduler is running. Press Ctrl+C to stop.\n")
    print(f"Next scheduled runs:")
    for job in schedule.get_jobs():
        print(f"  - {job.next_run}")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()
