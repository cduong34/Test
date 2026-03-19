#!/usr/bin/env python3
"""
Compass Job Seeker Upload — scheduled hourly (e.g. 11:00, 12:00, ...).

Pulls enterprise_vmscompass shifts from Redshift from today (or --start) onwards,
writes Job Seeker CSV, uploads to Google Drive, runs a Fieldglass capacity check,
then submits to Fieldglass and saves the dedup log.

The WAI (Worker Activity Item) upload runs separately in compass_wai_upload.py
at :30 past each hour, giving Fieldglass time to create work orders first.

Usage:
    python3 compass_js_upload.py
    python3 compass_js_upload.py --start 2026-03-20 --fg-upload
    python3 compass_js_upload.py --fg-upload --fg-env prod

    --start      Start date YYYY-MM-DD (default: today); query is always open-ended
    --creator    Fieldglass creator username (default: cduong_compass)
    --timezone   Time Zone field in Job Seeker Upload (default: US/Pacific)
    --out        Output folder (default: compass_MMDDYYYY_onwards/ beside this script)
    --no-upload  Skip Google Drive upload
    --fg-upload  Upload Job Seeker CSV directly to Fieldglass API
    --fg-env     Fieldglass environment: test (default) or prod
    --test-jp    Override Job Posting ID for every row (testing only)
    --no-email   Print capacity alert drafts only, do not send emails
"""

import argparse
from datetime import datetime
from pathlib import Path

from generate_compass_upload import (
    FG_TEST,
    FG_PROD,
    check_jp_capacity,
    fetch_redshift_data,
    fg_get_token,
    fg_upload_job_seeker_csv,
    load_cost_center_lookup,
    load_upload_log,
    save_upload_log,
    send_capacity_alert,
    send_failure_alert,
    upload_to_drive,
    write_input_csv,
    write_job_seeker_csv,
)

_SCRIPT = "compass_js_upload.py"


def main() -> None:
    today = datetime.today().strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Compass Job Seeker Upload — runs every hour on the hour."
    )
    parser.add_argument(
        "--start", "-s", default=today,
        help="Start date YYYY-MM-DD (default: today); query is always open-ended",
    )
    parser.add_argument(
        "--creator", default="cduong_compass",
        help="Fieldglass Creator Username (default: cduong_compass)",
    )
    parser.add_argument(
        "--timezone", default="US/Pacific",
        help="Time Zone field value in Job Seeker Upload (default: US/Pacific)",
    )
    parser.add_argument(
        "--out", "-o",
        help="Output folder (default: compass_MMDDYYYY_onwards/ beside this script)",
    )
    parser.add_argument(
        "--no-upload", action="store_true",
        help="Skip Google Drive upload",
    )
    parser.add_argument(
        "--fg-upload", action="store_true",
        help="Upload Job Seeker CSV directly to Fieldglass via API",
    )
    parser.add_argument(
        "--fg-env", choices=["test", "prod"], default="test",
        help="Fieldglass environment: test (default) or prod",
    )
    parser.add_argument(
        "--test-jp",
        help="Override Job Posting ID for every row (for testing only)",
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Print capacity alert drafts to console only — do not send emails",
    )
    args = parser.parse_args()

    start_date  = args.start
    start_label = datetime.strptime(start_date, "%Y-%m-%d").strftime("%m%d%Y")
    date_label  = f"{start_label}_onwards"
    folder_name = f"compass_{date_label}"

    out_dir = Path(args.out) if args.out else Path(__file__).parent / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[JS Upload] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Start date:    {start_date} onwards")
    print(f"Creator:       {args.creator}")
    print(f"Time Zone:     {args.timezone}")
    print(f"Output folder: {out_dir}")
    if args.fg_upload:
        print(f"FG upload:     {args.fg_env} environment")
    if args.test_jp:
        print(f"Test JP override: {args.test_jp}")
    print()

    # 1. Load cost-center lookup from Google Sheets
    try:
        by_business, by_company = load_cost_center_lookup()
    except Exception as exc:
        print(f"WARNING: Could not load cost-center sheet: {exc}")
        print("         Cost Center Location Code will be blank for all rows.")
        by_business, by_company = {}, {}

    # 2. Fetch shift data from Redshift
    print()
    try:
        data = fetch_redshift_data(start_date)
    except Exception as exc:
        send_failure_alert(
            _SCRIPT,
            "Redshift data fetch",
            str(exc),
        )
        return

    if not data:
        send_failure_alert(
            _SCRIPT,
            "Redshift data fetch",
            f"No rows returned for start date {start_date} onwards.",
            "This may indicate a Redshift connectivity issue or no active Compass shifts.",
        )
        return

    data.sort(key=lambda r: (
        r.get("shift_date", ""),
        r.get("business_name", "").lower(),
        r.get("last_name", "").lower(),
    ))

    # 3. Write output files
    input_path = out_dir / f"{date_label}_input.csv"
    js_path    = out_dir / f"{date_label}_job_seeker_upload.csv"

    print()
    write_input_csv(data, input_path)
    fg_env = FG_TEST if args.fg_env == "test" else FG_PROD
    supplier_code = fg_env["supplier_code"] if args.fg_upload else "INSTA"

    # Cross-run dedup: skip workers already uploaded for the same JP + date
    already_uploaded = load_upload_log()
    test_jp_val = args.test_jp or ""
    fresh_data = [
        r for r in data
        if f"{str(r.get('worker_id', '')).strip()}|{test_jp_val or str(r.get('job_posting_id', '')).strip()}"
        not in already_uploaded
    ]
    cross_run_skipped = len(data) - len(fresh_data)
    if cross_run_skipped:
        print(f"  Cross-run dedup: skipped {cross_run_skipped} worker/JP pair(s) already uploaded.")

    js_written_keys = write_job_seeker_csv(
        fresh_data, js_path, by_business, by_company, args.creator, args.timezone,
        test_jp=test_jp_val, supplier_code=supplier_code,
    )

    written_files = [input_path, js_path]
    print(f"\nDone. {len(fresh_data)} rows in Job Seeker CSV "
          f"({cross_run_skipped} cross-run dupes skipped) → {out_dir}")

    # 4. Upload to Google Drive
    if not args.no_upload:
        print(f"\nUploading to Google Drive folder '{folder_name}'...")
        try:
            upload_to_drive(folder_name, written_files)
            print("Upload complete.")
        except Exception as exc:
            print(f"Drive upload failed: {exc}")

    # 4b. Capacity pre-check — warn for any full job postings before uploading.
    # Uses fresh_data (already-assigned workers excluded) so the count reflects
    # genuinely new submissions, not workers already in Fieldglass.
    if args.fg_upload:
        jp_worker_counts: dict[str, int] = {}
        for row in fresh_data:
            jp = str(row.get("job_posting_id", "")).strip()
            if jp:
                jp_worker_counts[jp] = jp_worker_counts.get(jp, 0) + 1

        if jp_worker_counts:
            print("\nChecking job posting capacity in Fieldglass...")
            try:
                cap_token  = fg_get_token(fg_env)
                jp_capacity = check_jp_capacity(fg_env, cap_token)
                full_jps: set[str] = set()
                for jp_id, count in jp_worker_counts.items():
                    info = jp_capacity.get(jp_id)
                    if info is not None and not info["can_submit"]:
                        send_capacity_alert(jp_id, count, send=not args.no_email)
                        full_jps.add(jp_id)
                    elif info is None:
                        print(f"  WARNING: JP {jp_id} not found in Job Posting Supplier Download — skipping capacity check.")
                if not full_jps:
                    print("  All job postings have open capacity.")
            except Exception as exc:
                print(f"  Capacity check failed (will still attempt upload): {exc}")

    # 5. Upload Job Seeker CSV directly to Fieldglass API
    if args.fg_upload:
        if not fg_env["app_key"] or not fg_env["iu_user"] or not fg_env["iu_password"]:
            print(f"\nERROR: Fieldglass {args.fg_env} credentials are not configured.")
        else:
            print(f"\nUploading Job Seeker CSV to Fieldglass ({args.fg_env})...")
            try:
                rc106_errors = fg_upload_job_seeker_csv(js_path, fg_env)
                if rc106_errors:
                    detail_lines = "\n".join(f"  {e}" for e in rc106_errors[:20])
                    if len(rc106_errors) > 20:
                        detail_lines += f"\n  ... and {len(rc106_errors) - 20} more (see Integration Audit Trail in Fieldglass)"
                    send_failure_alert(
                        _SCRIPT,
                        f"Fieldglass Job Seeker Upload PARTIAL — {len(rc106_errors)} record(s) rejected",
                        f"{len(rc106_errors)} record(s) were rejected by Fieldglass:",
                        detail_lines,
                    )
                if js_written_keys:
                    save_upload_log(js_written_keys)
            except Exception as exc:
                send_failure_alert(
                    _SCRIPT,
                    "Fieldglass Job Seeker Upload FAILED",
                    str(exc),
                )


if __name__ == "__main__":
    main()
