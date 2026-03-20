#!/usr/bin/env python3
"""
Compass WAI Upload — scheduled at :30 past each hour (e.g. 11:30, 12:30, ...).

Runs 30 minutes after compass_js_upload.py to give Fieldglass time to create
work orders from the Job Seeker submissions. Downloads the Worker Activity Item
reference, matches workers to their work orders, and uploads the Activity Item
Completion CSV (onboarding attestation + ServSafe cert dates).

Shift data is sourced from Mode Analytics (CURRENT_DATE onwards).
The Job Seeker upload runs separately in compass_js_upload.py on the hour.

Usage:
    python3 compass_wai_upload.py
    python3 compass_wai_upload.py --fg-upload --fg-env prod

    --out        Output folder (default: compass_MMDDYYYY_onwards/ beside this script)
    --fg-upload  Upload WAI CSV directly to Fieldglass API
    --fg-env     Fieldglass environment: test (default) or prod
    --test-jp    Override Job Posting ID for every row (testing only)
"""

import argparse
from datetime import datetime
from pathlib import Path

from generate_compass_upload import (
    FG_TEST,
    FG_PROD,
    build_wai_rows,
    download_wai_reference,
    fetch_redshift_data,
    fg_get_token,
    fg_upload_wai_csv,
    send_failure_alert,
    write_wai_csv,
)

_SCRIPT = "compass_wai_upload.py"


def main() -> None:
    today = datetime.today().strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Compass WAI Upload — runs every hour at :30 past the hour."
    )
    parser.add_argument(
        "--out", "-o",
        help="Output folder (default: compass_MMDDYYYY_onwards/ beside this script)",
    )
    parser.add_argument(
        "--fg-upload", action="store_true",
        help="Upload WAI CSV directly to Fieldglass API",
    )
    parser.add_argument(
        "--fg-env", choices=["test", "prod"], default="test",
        help="Fieldglass environment: test (default) or prod",
    )
    parser.add_argument(
        "--test-jp",
        help="Override Job Posting ID for every row (for testing only)",
    )
    args = parser.parse_args()

    today_label = datetime.today().strftime("%m%d%Y")
    date_label  = f"{today_label}_onwards"
    folder_name = f"compass_{date_label}"

    out_dir = Path(args.out) if args.out else Path(__file__).parent / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[WAI Upload] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode report:   CURRENT_DATE")
    print(f"Output folder: {out_dir}")
    if args.fg_upload:
        print(f"FG upload:     {args.fg_env} environment")
    if args.test_jp:
        print(f"Test JP override: {args.test_jp}")
    print()

    # 1. Fetch shift data from Mode (needed for completion date fields:
    #    onboarding date, ServSafe/alcohol cert dates, BGC pass date)
    try:
        data = fetch_redshift_data(today)
    except Exception as exc:
        send_failure_alert(
            _SCRIPT,
            "Mode data fetch",
            str(exc),
        )
        return

    if not data:
        print("No rows returned from Mode report. Exiting.")
        return

    # If a test JP override is set, patch each row so the WAI lookup key matches
    test_jp_val = args.test_jp or ""
    if test_jp_val:
        for row in data:
            row["job_posting_id"] = test_jp_val

    # 2. Download WAI reference from Fieldglass
    fg_env = FG_TEST if args.fg_env == "test" else FG_PROD

    if not fg_env["app_key"] or not fg_env["iu_user"] or not fg_env["iu_password"]:
        print(f"ERROR: Fieldglass {args.fg_env} credentials are not configured.")
        return

    print("Downloading WAI reference from Fieldglass...")
    try:
        wai_token = fg_get_token(fg_env)
        wo_lookup = download_wai_reference(fg_env, wai_token)
    except Exception as exc:
        send_failure_alert(
            _SCRIPT,
            "WAI Reference Download FAILED",
            str(exc),
        )
        return

    if not wo_lookup:
        print("WAI reference returned no work orders.")
        print("NOTE: Work orders are only created after the CGHP buyer processes")
        print("      Job Seeker submissions in Fieldglass. If uploads are recent,")
        print("      wait a few minutes and re-run.")
        return

    # 3. Build WAI rows
    wai_rows, unmatched = build_wai_rows(data, wo_lookup)

    if unmatched:
        print(f"\nWAI: {len(unmatched)} worker(s) not found in WAI reference: "
              f"{', '.join(unmatched[:10])}"
              + (" ..." if len(unmatched) > 10 else ""))

    if not wai_rows:
        print("WAI: No rows to upload. Skipping.")
        return

    # 4. Write WAI CSV
    wai_path = out_dir / f"{date_label}_wai_upload.csv"
    write_wai_csv(wai_rows, wai_path)

    # 5. Upload WAI CSV to Fieldglass
    if args.fg_upload:
        print(f"\nUploading WAI CSV to Fieldglass ({args.fg_env})...")
        try:
            rc106_errors = fg_upload_wai_csv(wai_path, fg_env)
            if rc106_errors:
                detail_lines = "\n".join(f"  {e}" for e in rc106_errors[:20])
                if len(rc106_errors) > 20:
                    detail_lines += f"\n  ... and {len(rc106_errors) - 20} more (see Integration Audit Trail in Fieldglass)"
                send_failure_alert(
                    _SCRIPT,
                    f"Fieldglass WAI Upload PARTIAL — {len(rc106_errors)} record(s) rejected",
                    f"{len(rc106_errors)} record(s) were rejected by Fieldglass:",
                    detail_lines,
                )
        except Exception as exc:
            send_failure_alert(
                _SCRIPT,
                "Fieldglass WAI Upload FAILED",
                str(exc),
            )
    else:
        print(f"\nWAI CSV written but not uploaded (pass --fg-upload to submit).")


if __name__ == "__main__":
    main()
