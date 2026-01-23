#!/usr/bin/env python3
"""
Scheduler for Weekly Email Report

Runs the weekly email report every Saturday at 8:30 PM MYT (Malaysia Time).

Usage:
  python scheduler.py           # Run scheduler in foreground
  python scheduler.py --now     # Send report immediately (for testing)
  python scheduler.py --test    # Test email configuration without sending
"""

import argparse
import schedule
import time
from datetime import datetime, timedelta, timezone
from weekly_email_report import main as send_report, get_gsheet_client, get_email_credentials, SHEET_ID

# MYT Timezone (UTC+8)
MYT = timezone(timedelta(hours=8))


def get_now_myt():
    """Get current datetime in MYT timezone"""
    return datetime.now(MYT)


def job():
    """Job to run the weekly email report"""
    print(f"\n{'='*60}")
    print(f"Scheduled job triggered at {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"{'='*60}")
    send_report()


def test_configuration():
    """Test all configuration without sending email"""
    print(f"\n{'='*60}")
    print("Testing Configuration")
    print(f"{'='*60}")

    errors = []

    # Test Google Sheets connection
    print("\n[1] Testing Google Sheets connection...")
    if not SHEET_ID:
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
        print("  python scheduler.py --now    # Send report immediately")
        print("  python scheduler.py          # Start scheduler")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='Weekly Email Report Scheduler')
    parser.add_argument('--now', action='store_true', help='Send report immediately')
    parser.add_argument('--test', action='store_true', help='Test configuration')
    args = parser.parse_args()

    if args.test:
        test_configuration()
        return

    if args.now:
        print("Sending report immediately...")
        send_report()
        return

    # Schedule the job for every Saturday at 8:30 PM MYT
    # Note: schedule library uses system time, so we need to calculate the correct time
    print(f"\n{'='*60}")
    print("Weekly Email Report Scheduler")
    print(f"{'='*60}")
    print(f"Current time: {get_now_myt().strftime('%Y-%m-%d %H:%M:%S MYT')}")
    print(f"Scheduled: Every Saturday at 8:30 PM MYT")
    print(f"Recipients: shaun.quek@sibkl.org.my (CC: narrowstreet.sibkl@gmail.com)")
    print(f"{'='*60}\n")

    # Schedule for Saturday at 20:30 (8:30 PM)
    # The schedule library uses local time, so adjust if needed
    schedule.every().saturday.at("20:30").do(job)

    print("Scheduler is running. Press Ctrl+C to stop.\n")
    print(f"Next run: {schedule.next_run()}")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()
