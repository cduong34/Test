#!/usr/bin/env python3
"""
Generate VMS Compass Fieldglass Job Seeker Upload CSVs from Mode Analytics.

Triggers a Mode report (report token c34f7095968e) that queries all
enterprise_vmscompass companies from today onwards, then produces two output files:
  1. {label}_input.csv             — raw data (mirrors the Input report format)
  2. {label}_job_seeker_upload.csv — Fieldglass Job Seeker Upload format

Cost Center Location codes are fetched from the configured Google Sheet via
the Sheets API using the existing service account credentials.json.

Usage:
    python generate_compass_upload.py
    python generate_compass_upload.py --fg-upload
    python generate_compass_upload.py --fg-upload --fg-env prod

    --creator   Fieldglass creator username (default: cduong_compass)
    --timezone  Time Zone field in Job Seeker Upload (default: US/Pacific)
    --out       Output folder (default: compass_MMDDYYYY_onwards/ beside this script)
    --no-upload Skip Google Drive upload
    --fg-upload  Also upload the Job Seeker CSV directly to Fieldglass API
    --fg-env     Fieldglass environment: test (default) or prod
    --wai-upload After Job Seeker upload, download WAI reference and upload WAI CSV
    --wai-delay  Seconds to wait before WAI step (default: 300 = 5 min)
    --no-email   Print capacity alert drafts only, do not send emails

Required environment variables (or set defaults below):
    MODE_API_KEY_ID       Mode API token ID
    MODE_API_SECRET       Mode API token secret
    MODE_REPORT_TOKEN     Mode report token (default: c34f7095968e)
    GOOGLE_APPLICATION_CREDENTIALS  path to credentials.json
    SHEETS_ID             Google Sheet ID for cost-center lookup
"""

import csv
import io
import os
import time
import argparse
import smtplib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── Mode Analytics config ──────────────────────────────────────────────────────
MODE_API_KEY_ID     = os.getenv("MODE_API_KEY_ID",     "913ae899b38a")
MODE_API_SECRET     = os.getenv("MODE_API_SECRET",     "5267e2734ba930c46c4fd651")
MODE_WORKSPACE      = os.getenv("MODE_WORKSPACE",      "instawork")
MODE_REPORT_TOKEN   = os.getenv("MODE_REPORT_TOKEN",   "c34f7095968e")
MODE_BASE_URL       = "https://app.mode.com/api"

# ── Google config ──────────────────────────────────────────────────────────────
GOOGLE_CREDENTIALS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str(Path(__file__).parent / "credentials.json"),
)
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",
]

# Cost-center lookup sheet: https://docs.google.com/spreadsheets/d/1TNd4Fj7Bnu5o3lZ6ESZzlRPpV5RKRR1CbMEtUl-tfpc/edit?gid=1374512336
SHEETS_ID    = os.getenv("SHEETS_ID", "1TNd4Fj7Bnu5o3lZ6ESZzlRPpV5RKRR1CbMEtUl-tfpc")
# gid=1374512336 → convert to sheet name at runtime via Sheets API metadata
SHEETS_GID   = os.getenv("SHEETS_GID", "1374512336")

# Google Drive folder to upload output into
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID", "0ALWkbq6tvGD7Uk9PVA")

# ── Capacity alert email config ────────────────────────────────────────────────
# Set ALERT_FROM_EMAIL + ALERT_APP_PASSWORD to enable actual sending.
# Generate an App Password at: myaccount.google.com → Security → App Passwords
ALERT_FROM_EMAIL   = os.getenv("ALERT_FROM_EMAIL",   "cduong@instawork.com")  # Gmail address to send from
ALERT_APP_PASSWORD = os.getenv("ALERT_APP_PASSWORD", "")  # 16-char Google App Password
ALERT_TO_EMAIL     = os.getenv("ALERT_TO_EMAIL",     "cduong@instawork.com")

# ── Fieldglass API config ─────────────────────────────────────────────────────
# Test environment
FG_TEST = {
    "base_url":              "https://supplier.imp1.us.fieldglasstest.cloud.sap",
    "app_key":               os.getenv("FG_TEST_APP_KEY",     "7MpMW966RHee2XCbdTFCD2EZ8AF"),
    "iu_user":               os.getenv("FG_TEST_IU_USER",     "enkhjin"),
    "iu_password":           os.getenv("FG_TEST_IU_PASSWORD", "IWORK_C5YdbVEnR8TDLe7ggm8FUHaHWQV"),
    "connector":             "Job Seeker Upload",
    "supplier_code":         "IWORK",
    "wai_download_connector": "Worker Activity Item Download",
    "wai_upload_connector":   "Activity Item Completion Upload",
}
# Production environment (fill in prod credentials when ready)
FG_PROD = {
    "base_url":              "https://www.us.fieldglass.cloud.sap",
    "app_key":               os.getenv("FG_PROD_APP_KEY",     "dlgEbqGNj4Eg45JA5ll9AMWbhSa"),
    "iu_user":               os.getenv("FG_PROD_IU_USER",     "cduong_compass"),
    "iu_password":           os.getenv("FG_PROD_IU_PASSWORD", "INSTA_jBMTeGaKPZarS6fLkBJgnbcDCU2"),
    "connector":             "Job Seeker Upload",
    "supplier_code":         "INSTA",
    "wai_download_connector": "Worker Activity Item Download",
    "wai_upload_connector":   "Activity Item Completion Upload",
}

# ── Job Seeker Upload fixed header block ──────────────────────────────────────
JS_HEADER_LINES = [
    ["Type=Job Seeker Upload"],
    ["Send Notification?=True"],
    ["Language=English (United States)"],
    ["Number Format=#,##9.99 (Example: 1,234,567.99)"],
    ["Date Format=MM/DD/YYYY"],
    ["Buyer=CGHP"],
    ["Comments="],
    [],
    [],
]

JS_COLUMNS = [
    "Job Seeker Code",
    "External Job Seeker ID",
    "Creator Username",
    "Job Posting ID",
    "Supplier Code",
    "First Name",
    "Last Name",
    "Date Available",
    "Time Zone",
    "Submitted to other Job Postings",
    "Comments",
    "Remit-to Address Code",
    "Username",
    "Email",
    "Display candidate's Workforce record to the Buyer?",
    "Cost Center Location Code",
    "Location Code",
    "Security ID",
    "Number of Tiers",
    "Calculate Rate Using",
    "Rate Change Matrix Start Date",
    "Supplier Email",
    "Register On Behalf Of Worker?",
    "Cost Center Location Tax",
    "[c] Customer Account Number (DCN)",
    "[c] If taxable, select the state where the tax is applied.",
    "[c] Specify type of visa.",
    "[c] Is worker on a visa to perform work in the United States?",
    "[c] Worker Classification",
    "[c] Provide the tax % to be applied to the worker.",
    "",
    "",
    "Errors Found In Upload",
]

# ── Mode Analytics data fetch ──────────────────────────────────────────────────

def fetch_redshift_data(start_date: str, end_date: str | None = None) -> list[dict]:
    """Trigger a Mode report run and return all rows as a list of dicts.

    start_date and end_date are accepted for API compatibility but ignored —
    the Mode report (c34f7095968e) uses CURRENT_DATE in its SQL.
    """
    auth = HTTPBasicAuth(MODE_API_KEY_ID, MODE_API_SECRET)
    base = f"{MODE_BASE_URL}/{MODE_WORKSPACE}"

    print(f"Triggering Mode report {MODE_REPORT_TOKEN}...")
    run_resp = requests.post(
        f"{base}/reports/{MODE_REPORT_TOKEN}/runs",
        auth=auth,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"parameters": {}},
        timeout=30,
    )
    if run_resp.status_code != 202:
        raise RuntimeError(
            f"Mode report run failed (HTTP {run_resp.status_code}): {run_resp.text[:300]}"
        )

    run_token = run_resp.json()["token"]
    print(f"  Run token: {run_token} — waiting for completion...")

    for attempt in range(60):
        time.sleep(5)
        status_resp = requests.get(
            f"{base}/reports/{MODE_REPORT_TOKEN}/runs/{run_token}",
            auth=auth,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        status_resp.raise_for_status()
        state = status_resp.json().get("state", "")
        print(f"  [{attempt + 1}] state: {state}")
        if state == "succeeded":
            break
        if state in ("failed", "cancelled"):
            raise RuntimeError(f"Mode report run {state}: {status_resp.text[:300]}")
    else:
        raise RuntimeError("Timed out waiting for Mode report to complete.")

    print("  Downloading CSV results...")
    csv_resp = requests.get(
        f"{base}/reports/{MODE_REPORT_TOKEN}/runs/{run_token}/results/content.csv",
        auth=auth,
        timeout=120,
    )
    if csv_resp.status_code != 200:
        raise RuntimeError(
            f"Mode CSV download failed (HTTP {csv_resp.status_code}): {csv_resp.text[:300]}"
        )

    rows = list(csv.DictReader(io.StringIO(csv_resp.text)))
    print(f"Fetched {len(rows)} rows from Mode.")
    return rows


# ── Google Sheets cost-center lookup ──────────────────────────────────────────

def _sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=GOOGLE_SCOPES
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _gid_to_sheet_name(service, spreadsheet_id: str, gid: str) -> str:
    """Resolve a numeric gid to the actual sheet tab name."""
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if str(props.get("sheetId", "")) == gid:
            return props["title"]
    raise ValueError(f"Sheet with gid={gid} not found in spreadsheet {spreadsheet_id}")


def load_cost_center_lookup() -> dict[str, str]:
    """
    Read the cost-center Google Sheet and return a lookup dict keyed by business_id.

    Sheet columns (0-indexed):
        0 = cost_center_code
        1 = cost_center_name
        2 = business_id
    """
    COL_COST_CENTER = 0
    COL_BUSINESS_ID = 2

    svc = _sheets_service()
    sheet_name = _gid_to_sheet_name(svc, SHEETS_ID, SHEETS_GID)
    range_name = f"'{sheet_name}'!A:Z"

    print(f"Reading cost-center sheet '{sheet_name}' from Google Sheets...")
    result = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEETS_ID, range=range_name)
        .execute()
    )
    values = result.get("values", [])
    if not values:
        print("WARNING: Cost-center sheet returned no data.")
        return {}

    by_business: dict[str, str] = {}

    for i, row in enumerate(values):
        if i == 0:
            continue  # skip header row
        if len(row) <= COL_COST_CENTER:
            continue
        code = str(row[COL_COST_CENTER]).strip()
        if not code:
            continue

        if len(row) > COL_BUSINESS_ID:
            biz_id = str(row[COL_BUSINESS_ID]).strip()
            if biz_id:
                by_business[biz_id] = code

    print(f"Loaded {len(by_business)} business-level cost-center mappings.")
    return by_business


# ── CSV writers ────────────────────────────────────────────────────────────────

INPUT_COLUMNS = [
    "company_id", "company_name", "business_id", "business_name",
    "shift_id", "shift_name", "position",
    "shift_date", "start_time", "end_time",
    "pro_rate", "business_rate",
    "worker_id", "first_name", "last_name", "name_source",
    "worker_email", "worker_dob", "security_id",
    "tam_alcohol_cert_expires_at", "ca_rbs_cert_expires_at", "az_title4_cert_expires_at",
    "background_check_status", "bgc_request_status", "bg_check_pass_date",
    "job_posting_id",
]


def write_input_csv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written input CSV: {path}  ({len(rows)} rows)")


def build_js_row(
    row: dict,
    by_business: dict[str, str],
    creator: str,
    timezone: str,
    test_jp: str = "",
    supplier_code: str = "INSTA",
) -> list:
    """Map one input row to a Job Seeker Upload data row (33 columns)."""
    business_id = str(row.get("business_id", "")).strip()

    cost_center = by_business.get(business_id, "")
    if not cost_center:
        print(f"  WARNING: No cost-center code for business_id={business_id} "
              f"({row.get('business_name', '')})")

    job_posting_id = test_jp if test_jp else row.get("job_posting_id", "")

    return [
        "",                                      # Job Seeker Code
        row.get("worker_id", ""),                # External Job Seeker ID
        creator,                                 # Creator Username
        job_posting_id,                          # Job Posting ID
        supplier_code,                           # Supplier Code
        row.get("first_name", ""),               # First Name
        row.get("last_name", ""),                # Last Name
        row.get("shift_date", ""),               # Date Available
        timezone,                                # Time Zone
        "No",                                    # Submitted to other Job Postings
        "",                                      # Comments
        "",                                      # Remit-to Address Code
        "",                                      # Username
        row.get("worker_email", ""),             # Email
        "",                                      # Display candidate's Workforce record?
        cost_center,                             # Cost Center Location Code
        cost_center,                             # Location Code
        row.get("security_id", ""),              # Security ID
        "",                                      # Number of Tiers
        "",                                      # Calculate Rate Using
        "",                                      # Rate Change Matrix Start Date
        "",                                      # Supplier Email
        "",                                      # Register On Behalf Of Worker?
        "",                                      # Cost Center Location Tax
        row.get("company_id", ""),               # [c] Customer Account Number (DCN)
        "N/A",                                   # [c] If taxable, state
        "",                                      # [c] Specify type of visa
        "No",                                    # [c] Is worker on a visa?
        "W2",                                    # [c] Worker Classification
        "0",                                     # [c] Tax %
        "",                                      # (blank)
        "",                                      # (blank)
        "",                                      # Errors Found In Upload
    ]


def write_job_seeker_csv(
    rows: list[dict],
    path: Path,
    by_business: dict[str, str],
    creator: str,
    timezone: str,
    test_jp: str = "",
    supplier_code: str = "INSTA",
) -> list[str]:
    """
    Write the Job Seeker Upload CSV.

    Deduplicates within this run by (worker_id, job_posting_id): for workers on
    multi-day shifts (same JP, multiple dates), only the first/earliest row is
    written so Date Available is always the first day of the engagement.

    Returns a list of "worker_id|job_posting_id" keys that were written,
    for use in the cross-run upload log.
    """
    written_keys: list[str] = []
    seen_within_run: set[tuple[str, str]] = set()
    skipped = 0

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for line in JS_HEADER_LINES:
            writer.writerow(line)
        writer.writerow(JS_COLUMNS)

        for row in rows:
            jp         = test_jp or str(row.get("job_posting_id", "")).strip()
            worker_id  = str(row.get("worker_id", "")).strip()
            shift_date = str(row.get("shift_date", "")).strip()
            key = (worker_id, jp)

            if key in seen_within_run:
                print(f"  SKIP within-run duplicate: worker {worker_id} / JP {jp} / {shift_date}")
                skipped += 1
                continue

            seen_within_run.add(key)
            writer.writerow(build_js_row(row, by_business, creator, timezone,
                                         test_jp, supplier_code))
            written_keys.append(f"{worker_id}|{jp}")

    written = len(written_keys)
    suffix = f"  [test-jp={test_jp}]" if test_jp else ""
    skip_note = f"  ({skipped} within-run duplicates skipped)" if skipped else ""
    print(f"Written Job Seeker Upload CSV: {path}  ({written} rows){suffix}{skip_note}")
    return written_keys


# ── Google Drive upload ────────────────────────────────────────────────────────

def upload_to_drive(folder_name: str, file_paths: list[Path]) -> None:
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH, scopes=GOOGLE_SCOPES
    )
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)

    folder_meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [DRIVE_PARENT_FOLDER_ID],
    }
    folder = svc.files().create(
        body=folder_meta, fields="id", supportsAllDrives=True
    ).execute()
    folder_id = folder["id"]
    print(f"Created Drive folder '{folder_name}' (id={folder_id})")

    for path in file_paths:
        media = MediaFileUpload(str(path), mimetype="text/csv", resumable=False)
        file_meta = {"name": path.name, "parents": [folder_id]}
        svc.files().create(
            body=file_meta, media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        print(f"  Uploaded: {path.name}")


# ── Fieldglass API helpers ────────────────────────────────────────────────────

def fg_get_token(env: dict) -> str:
    """Obtain a Bearer token from the Fieldglass OAuth2 endpoint."""
    url = f"{env['base_url']}/api/oauth2/v2.0/token"
    resp = requests.post(
        url,
        params={"grant_type": "client_credentials", "response_type": "token"},
        headers={"x-api-key": env["app_key"], "Content-Type": "application/json"},
        auth=(env["iu_user"], env["iu_password"]),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in Fieldglass response: {resp.text}")
    print(f"Fieldglass token acquired (expires in {resp.json().get('expires_in', '?')}s).")
    return token


def fg_upload_job_seeker_csv(csv_path: Path, env: dict) -> list[str]:
    """
    Upload a Job Seeker CSV to Fieldglass via the Integration Connectivity connector.

    Returns a list of RC=106 error strings if any records were rejected, or an
    empty list on full success. Raises RuntimeError on hard failures (non-200, non-106).
    """
    token = fg_get_token(env)
    url = f"{env['base_url']}/api/vc/connector/{requests.utils.quote(env['connector'])}"
    csv_bytes = csv_path.read_bytes()

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": env["app_key"],
            "Content-Type": "text/csv;charset=UTF-8",
        },
        data=csv_bytes,
        timeout=120,
    )

    # Fieldglass returns XML; parse ReturnCode / Message
    try:
        root = ET.fromstring(resp.text)
        rc  = root.findtext("ReturnCode", "?")
        msg = root.findtext("Message", resp.text).strip()
        txn = root.findtext("TransactionID", "?")
    except ET.ParseError:
        rc, msg, txn = "?", resp.text.strip(), "?"

    if resp.status_code == 200 or rc == "0":
        print(f"  Fieldglass upload SUCCESS  TransactionID={txn}  Message={msg}")
        return []
    elif rc == "106":
        # RC 106 = row-level validation errors — file was processed, some records failed.
        # Split on "Record " boundaries so ALL error lines per record are captured
        # (Fieldglass can return multiple error lines per record on consecutive lines).
        lines   = [ln.strip() for ln in msg.splitlines()]
        summary = next((ln for ln in reversed(lines) if "of" in ln and "record" in ln.lower()), "")
        record_blocks = [blk.strip() for blk in msg.split("Record ") if blk.strip()]
        errors = [
            f"Record {blk}"
            for blk in record_blocks
            if not ("of" in blk and "record" in blk.lower())
        ]

        # Build record-number → Job Posting ID map from the uploaded CSV.
        # JS CSV layout: 9 JS_HEADER_LINES rows + 1 JS_COLUMNS header row = 10 rows
        # to skip; data rows follow 1-indexed from Fieldglass's perspective.
        # JS_COLUMNS index 3 = "Job Posting ID"
        JP_COL_IDX  = 3
        SKIP_ROWS   = len(JS_HEADER_LINES) + 1  # 9 fixed header rows + column header
        jp_by_record: dict[int, str] = {}
        try:
            with open(csv_path, newline="", encoding="utf-8") as _f:
                reader = csv.reader(_f)
                for _i, _row in enumerate(reader):
                    if _i < SKIP_ROWS:
                        continue
                    record_num = _i - SKIP_ROWS + 1  # 1-based
                    jp_id = _row[JP_COL_IDX].strip() if len(_row) > JP_COL_IDX else ""
                    if jp_id:
                        jp_by_record[record_num] = jp_id
        except Exception:
            pass  # annotation is best-effort; don't block error reporting

        # Annotate each error string with the JP ID if available
        annotated_errors = []
        import re as _re
        for err in errors:
            m = _re.match(r"Record\s+(\d+)", err)
            if m:
                rec_num = int(m.group(1))
                jp_id   = jp_by_record.get(rec_num, "")
                if jp_id:
                    err = err.replace(f"Record {rec_num}", f"Record {rec_num} [JP={jp_id}]", 1)
            annotated_errors.append(err)

        print(f"  Fieldglass upload PARTIAL  TransactionID={txn}")
        print(f"  Summary: {summary or 'see errors below'}")
        if annotated_errors:
            print(f"  Row errors ({len(annotated_errors)}):")
            for err_line in annotated_errors[:20]:
                print(f"    {err_line.strip()}")
            if len(annotated_errors) > 20:
                print(f"    ... and {len(annotated_errors) - 20} more.")
        return annotated_errors
    else:
        raise RuntimeError(
            f"Fieldglass upload FAILED (HTTP {resp.status_code}, RC={rc}): {msg}"
        )


# ── Job Posting capacity check ────────────────────────────────────────────────

def check_jp_capacity(env: dict, token: str) -> dict[str, dict]:
    """
    GET the Job Posting Supplier Download connector and return capacity info
    for every job posting currently visible to the supplier.

    Returns:
        {jp_id: {"quantity": int, "can_submit": bool}}
    """
    connector = "Job Posting Supplier Download"
    url = f"{env['base_url']}/api/vc/connector/{requests.utils.quote(connector)}"
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": env["app_key"],
        },
        timeout=60,
    )

    if resp.status_code != 200:
        try:
            msg = ET.fromstring(resp.text).findtext("Message", resp.text).strip()
        except ET.ParseError:
            msg = resp.text[:200]
        raise RuntimeError(f"Job Posting Supplier Download failed (HTTP {resp.status_code}): {msg}")

    if "no data to download" in resp.text.lower():
        print("  Job Posting Supplier Download: no active postings returned.")
        return {}

    capacity: dict[str, dict] = {}
    try:
        # The response may contain multiple <StaffingOrder> elements; parse all.
        # Strip any <?xml ...?> declaration before wrapping, as it can't appear
        # mid-document after we add a synthetic <root> element.
        body = resp.text
        if body.lstrip().startswith("<?xml"):
            body = body[body.index("?>") + 2:]
        wrapped = f"<root>{body}</root>"
        root = ET.fromstring(wrapped)
        ns = {"jp": "jobPosting"}
        for order in root.iter("{jobPosting}StaffingOrder"):
            jp_id = ""
            for order_id in order.iter("{jobPosting}OrderId"):
                for id_val in order_id.iter("{jobPosting}IdValue"):
                    jp_id = (id_val.text or "").strip()
                    break
                break

            qty_el = order.find("{jobPosting}PositionQuantity")
            quantity = int(qty_el.text.strip()) if qty_el is not None and qty_el.text else 0

            can_el = order.find("{jobPosting}CanSubmitJobSeeker")
            can_submit = (can_el.text or "").strip().lower() == "yes" if can_el is not None else True

            if jp_id:
                capacity[jp_id] = {"quantity": quantity, "can_submit": can_submit}
    except ET.ParseError as exc:
        raise RuntimeError(f"Failed to parse Job Posting Supplier Download XML: {exc}") from exc

    return capacity


def send_capacity_alert(jp_id: str, positions_needed: int, send: bool = True) -> None:
    """
    Print a formatted capacity-alert email draft to stdout, and optionally
    send it via Gmail SMTP.

    Actual sending requires ALERT_FROM_EMAIL and ALERT_APP_PASSWORD to be set.
    Pass send=False (via --no-email) to print only, even if credentials exist.
    """
    subject = f"Action needed \u2013 please add positions for JP {jp_id}"
    body = (
        f"Hi,\n\n"
        f"Can you please add {positions_needed} more position(s) for JP with ID: {jp_id}.\n"
        f"Previously submitted job seekers canceled, and other workers picked up the shift.\n\n"
        f"Thank you so much,"
    )

    # Always print to console
    border = "=" * 52
    print(f"\n{border}")
    print(f"  CAPACITY ALERT \u2014 ACTION REQUIRED")
    print(f"  Job Posting : {jp_id}")
    print(f"  Workers blocked (no open slots): {positions_needed}")
    print(f"{'-' * 52}")
    print(f"  {'EMAIL SENT' if (send and ALERT_FROM_EMAIL and ALERT_APP_PASSWORD) else 'DRAFT EMAIL (not sent)'}")
    print(f"{'-' * 52}")
    print(f"  To:      {ALERT_TO_EMAIL}")
    print(f"  Subject: {subject}")
    print()
    for line in body.splitlines():
        print(f"  {line}")
    print(f"{border}\n")

    # Send via Gmail SMTP if credentials are configured and sending is enabled
    if not send:
        return
    if not ALERT_FROM_EMAIL or not ALERT_APP_PASSWORD:
        print("  (Email not sent: set ALERT_FROM_EMAIL and ALERT_APP_PASSWORD env vars to enable.)")
        return

    try:
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"]    = ALERT_FROM_EMAIL
        msg["To"]      = ALERT_TO_EMAIL

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(ALERT_FROM_EMAIL, ALERT_APP_PASSWORD)
            smtp.sendmail(ALERT_FROM_EMAIL, [ALERT_TO_EMAIL], msg.as_string())

        print(f"  Email sent to {ALERT_TO_EMAIL}")
    except Exception as exc:
        print(f"  WARNING: Failed to send email: {exc}")


def send_failure_alert(script: str, step: str, error: str, detail: str = "") -> None:
    """
    Send a failure notification email when a Compass upload step fails.

    Uses the same Gmail SMTP credentials as send_capacity_alert().
    Always prints to console; sends email if ALERT_APP_PASSWORD is set.

    Args:
        script: the script filename, e.g. "compass_js_upload.py"
        step:   short description of the failed step, e.g. "Fieldglass Job Seeker Upload"
        error:  the exception message or failure summary
        detail: optional extra lines (e.g. list of RC=106 rejected records)
    """
    now     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[ALERT] {script} — {step} ({now})"
    rerun   = f"python3 /Users/christineduong/projects/{script} --fg-upload --fg-env prod"
    body = (
        f"Script:  {script}\n"
        f"Time:    {now}\n"
        f"Step:    {step}\n\n"
        f"Error:\n  {error}\n"
        + (f"\n{detail}\n" if detail else "")
        + f"\nRe-run manually:\n  {rerun}\n"
    )

    border = "=" * 56
    print(f"\n{border}")
    print(f"  UPLOAD ALERT — {script}")
    print(f"  Step : {step}")
    print(f"{'-' * 56}")
    print(f"  {'EMAIL SENT' if (ALERT_FROM_EMAIL and ALERT_APP_PASSWORD) else 'DRAFT (not sent — set ALERT_APP_PASSWORD)'}")
    print(f"{'-' * 56}")
    print(f"  To:      {ALERT_TO_EMAIL}")
    print(f"  Subject: {subject}")
    print()
    for line in body.splitlines():
        print(f"  {line}")
    print(f"{border}\n")

    if not ALERT_FROM_EMAIL or not ALERT_APP_PASSWORD:
        return

    try:
        msg            = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"]    = ALERT_FROM_EMAIL
        msg["To"]      = ALERT_TO_EMAIL

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(ALERT_FROM_EMAIL, ALERT_APP_PASSWORD)
            smtp.sendmail(ALERT_FROM_EMAIL, [ALERT_TO_EMAIL], msg.as_string())

        print(f"  Failure alert sent to {ALERT_TO_EMAIL}")
    except Exception as exc:
        print(f"  WARNING: Failed to send failure alert email: {exc}")


# ── WAI helpers ───────────────────────────────────────────────────────────────

def _wai_day_before(date_str: str) -> str:
    """Return MM/DD/YYYY for the day before the given MM/DD/YYYY date string."""
    if not date_str:
        return ""
    try:
        d = datetime.strptime(date_str.strip(), "%m/%d/%Y") - timedelta(days=1)
        return d.strftime("%m/%d/%Y")
    except ValueError:
        return ""


def download_wai_reference(env: dict, token: str) -> dict[str, list[dict]]:
    """
    GET the Worker Activity Item download connector from Fieldglass.

    Returns a dict keyed by  "job_posting_id|worker_id"  →  list of
        {"work_order_id": str, "activity_code": str}
    entries (one per activity item on that work order).

    The connector returns CSV with at least these columns:
        Job Posting ID, External Reference ID, Work Order ID,
        Work Order Activity Item Code
    """
    connector = env["wai_download_connector"]
    url = f"{env['base_url']}/api/vc/connector/{requests.utils.quote(connector)}"
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": env["app_key"],
        },
        timeout=120,
    )

    # If the server returns a Fieldglass status XML (error), surface it clearly
    content_type = resp.headers.get("Content-Type", "")
    if resp.status_code != 200 or "xml" in content_type.lower():
        try:
            root = ET.fromstring(resp.text)
            rc  = root.findtext("ReturnCode", "?")
            msg = root.findtext("Message", resp.text).strip()
        except ET.ParseError:
            rc, msg = "?", resp.text[:200]
        raise RuntimeError(f"WAI reference download failed (RC={rc}): {msg}")

    # Parse CSV response
    reader = csv.DictReader(io.StringIO(resp.text))
    lookup: dict[str, list[dict]] = {}
    for row in reader:
        jp_id        = row.get("Job Posting ID", "").strip()
        ext_ref_id   = row.get("External Reference ID", "").strip()
        work_order_id = row.get("Work Order ID", "").strip()
        activity_code = row.get("Work Order Activity Item Code", "").strip()

        if not activity_code:
            continue

        key = f"{jp_id}|{ext_ref_id}"
        lookup.setdefault(key, []).append({
            "work_order_id": work_order_id,
            "activity_code": activity_code,
        })

    print(f"WAI reference: {len(lookup)} job-posting/worker combinations, "
          f"{sum(len(v) for v in lookup.values())} activity items.")
    return lookup


def build_wai_rows(data: list[dict], wo_lookup: dict[str, list[dict]]) -> tuple[list[list], list[str]]:
    """
    Build WAI upload data rows from input data + WO reference lookup.

    Returns (wai_rows, unmatched_worker_ids).
    Each wai_row is a 15-element list matching WAI_COLUMNS order.
    """
    seen_workers: set[str] = set()
    wai_rows: list[list] = []
    unmatched: list[str] = []

    for row in data:
        worker_id     = str(row.get("worker_id", "")).strip()
        job_posting_id = str(row.get("job_posting_id", "")).strip()

        if not worker_id:
            continue
        if worker_id in seen_workers:
            continue
        seen_workers.add(worker_id)

        # Dates
        shift_date         = str(row.get("shift_date", "")).strip()
        onboarding_date    = _wai_day_before(shift_date)

        # ServSafe date: alcohol cert → CA RBS cert → BGC pass date
        servsafe_date = (
            str(row.get("tam_alcohol_cert_expires_at", "")).strip()
            or str(row.get("ca_rbs_cert_expires_at", "")).strip()
            or str(row.get("bg_check_pass_date", "")).strip()
        )

        key = f"{job_posting_id}|{worker_id}"
        entries = wo_lookup.get(key)
        if not entries:
            unmatched.append(worker_id)
            continue

        for entry in entries:
            if entry["activity_code"] == "Onboarding Requirements Attestation":
                completion_date = onboarding_date
            elif entry["activity_code"] == "ServSafe Alcohol Certification":
                completion_date = servsafe_date
            else:
                completion_date = ""

            wai_rows.append([
                entry["work_order_id"],  # Object ID
                "",                       # Activity Item ZID
                "Activity",               # Type
                entry["activity_code"],   # Code
                "0",                      # Revision Number
                "",                       # Due Date
                completion_date,          # Completion Date
                "",                       # Comments
                "",                       # Completed By Username
                "",                       # Document Expiration Date
                "",                       # Action
                "",                       # Reject Reason
                "",                       # (blank)
                "",                       # (blank)
                "",                       # (blank)
            ])

    return wai_rows, unmatched


WAI_HEADER_LINES = [
    ["Type=Activity Item Completion Upload"],
    ["Transaction=True"],
    ["Language=English (United States)"],
    ["Number Format=#,##9.99 (Example: 1,234,567.99)"],
    ["Date Format=MM/DD/YYYY"],
    ["Buyer=CGHP"],
    ["Comments="],
    [],
]

WAI_COLUMNS = [
    "Object ID", "Activity Item ZID", "Type", "Code", "Revision Number",
    "Due Date", "Completion Date", "Comments", "Completed By Username",
    "Document Expiration Date", "Action", "Reject Reason", "", "", "",
]


def write_wai_csv(wai_rows: list[list], path: Path) -> None:
    """Write the Activity Item Completion Upload CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for line in WAI_HEADER_LINES:
            writer.writerow(line)
        writer.writerow(WAI_COLUMNS)
        for row in wai_rows:
            writer.writerow(row)
    print(f"Written WAI CSV: {path}  ({len(wai_rows)} rows)")


def fg_upload_wai_csv(csv_path: Path, env: dict) -> list[str]:
    """
    Upload the Activity Item Completion Upload CSV to Fieldglass.

    Returns a list of RC=106 error strings if any records were rejected, or an
    empty list on full success. Raises RuntimeError on hard failures.
    """
    token = fg_get_token(env)
    connector = env["wai_upload_connector"]
    url = f"{env['base_url']}/api/vc/connector/{requests.utils.quote(connector)}"
    csv_bytes = csv_path.read_bytes()

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": env["app_key"],
            "Content-Type": "text/csv;charset=UTF-8",
        },
        data=csv_bytes,
        timeout=120,
    )

    try:
        root = ET.fromstring(resp.text)
        rc  = root.findtext("ReturnCode", "?")
        msg = root.findtext("Message", resp.text).strip()
        txn = root.findtext("TransactionID", "?")
    except ET.ParseError:
        rc, msg, txn = "?", resp.text.strip(), "?"

    if resp.status_code == 200 or rc == "0":
        print(f"  WAI upload SUCCESS  TransactionID={txn}  Message={msg}")
        return []
    elif rc == "106":
        # Split on "Record " boundaries so ALL error lines per record are captured.
        lines   = [ln.strip() for ln in msg.splitlines()]
        summary = next((ln for ln in reversed(lines) if "of" in ln and "record" in ln.lower()), "")
        record_blocks = [blk.strip() for blk in msg.split("Record ") if blk.strip()]
        errors = [
            f"Record {blk}"
            for blk in record_blocks
            if not ("of" in blk and "record" in blk.lower())
        ]
        print(f"  WAI upload PARTIAL  TransactionID={txn}")
        print(f"  Summary: {summary or 'see errors below'}")
        if errors:
            print(f"  Row errors ({len(errors)}):")
            for err_line in errors[:20]:
                print(f"    {err_line}")
            if len(errors) > 20:
                print(f"    ... and {len(errors) - 20} more.")
        return errors
    else:
        raise RuntimeError(
            f"WAI upload FAILED (HTTP {resp.status_code}, RC={rc}): {msg}"
        )


# ── Upload dedup log ──────────────────────────────────────────────────────────

UPLOAD_LOG_PATH = Path(__file__).parent / "uploaded_log.json"


def load_upload_log(path: Path = UPLOAD_LOG_PATH) -> set[str]:
    """
    Load the set of already-uploaded "worker_id|job_posting_id" keys.
    Returns an empty set if the log file doesn't exist yet.
    """
    if not path.exists():
        return set()
    try:
        import json
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data.keys())
    except Exception as exc:
        print(f"  WARNING: Could not read upload log ({path}): {exc} — treating as empty.")
        return set()


def save_upload_log(new_keys: list[str], path: Path = UPLOAD_LOG_PATH) -> None:
    """
    Append new "worker_id|job_posting_id" keys (with ISO timestamp) to the log.
    Creates the file if it doesn't exist.
    """
    import json
    existing: dict[str, str] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass

    now = datetime.utcnow().isoformat()
    for key in new_keys:
        existing[key] = now

    path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
    print(f"  Upload log updated: {len(new_keys)} new key(s) → {path}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    today = datetime.today().strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Generate VMS Compass Fieldglass Job Seeker Upload CSVs."
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
        help="Fieldglass environment to upload to: test (default) or prod",
    )
    parser.add_argument(
        "--test-jp",
        help="Override Job Posting ID for every row (for testing only, e.g. CGHPJP00000863)",
    )
    parser.add_argument(
        "--wai-upload", action="store_true",
        help="After the Job Seeker upload, download WAI reference and upload Activity Item Completion CSV",
    )
    parser.add_argument(
        "--wai-delay", type=int, default=300, metavar="SECONDS",
        help="Seconds to wait after Job Seeker upload before starting WAI step (default: 300 = 5 min)",
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Print capacity alert drafts to console only — do not send emails even if credentials are set",
    )
    args = parser.parse_args()

    today_label = datetime.today().strftime("%m%d%Y")
    date_label  = f"{today_label}_onwards"
    folder_name = f"compass_{date_label}"

    out_dir = Path(args.out) if args.out else Path(__file__).parent / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Mode report:   {MODE_REPORT_TOKEN} (CURRENT_DATE)")
    print(f"Creator:       {args.creator}")
    print(f"Time Zone:     {args.timezone}")
    print(f"Output folder: {out_dir}")
    if args.fg_upload:
        print(f"FG upload:     {args.fg_env} environment")
    if args.wai_upload:
        print(f"WAI upload:    enabled (delay={args.wai_delay}s)")
    if args.test_jp:
        print(f"Test JP override: {args.test_jp}")
    print()

    # 1. Load cost-center lookup from Google Sheets
    try:
        by_business = load_cost_center_lookup()
    except Exception as exc:
        print(f"WARNING: Could not load cost-center sheet: {exc}")
        print("         Cost Center Location Code will be blank for all rows.")
        by_business = {}

    # 2. Fetch shift data from Mode
    print()
    data = fetch_redshift_data(today)

    if not data:
        print("No rows returned from Mode report. Exiting.")
        return

    # Sort by shift date, business name, last name (already ordered by SQL, but ensure)
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

    # Cross-run dedup: skip workers already uploaded for the same JP
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
        fresh_data, js_path, by_business, args.creator, args.timezone,
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
                fg_upload_job_seeker_csv(js_path, fg_env)
                # Save successfully submitted worker/JP pairs to the dedup log
                if js_written_keys:
                    save_upload_log(js_written_keys)
            except Exception as exc:
                print(f"Fieldglass Job Seeker upload failed: {exc}")

    # 6. WAI upload step (only when both --fg-upload and --wai-upload are set)
    if args.fg_upload and args.wai_upload:
        if not fg_env["app_key"] or not fg_env["iu_user"] or not fg_env["iu_password"]:
            print(f"\nERROR: Fieldglass {args.fg_env} credentials are not configured for WAI.")
        else:
            # Wait N seconds with countdown
            if args.wai_delay > 0:
                print(f"\nWaiting {args.wai_delay}s before WAI step", end="", flush=True)
                for remaining in range(args.wai_delay, 0, -10):
                    time.sleep(min(10, remaining))
                    print(f" ...{remaining - min(10, remaining)}s", end="", flush=True)
                print(" done.")

            print(f"\nDownloading WAI reference from Fieldglass ({args.fg_env})...")
            try:
                wai_token = fg_get_token(fg_env)
                wo_lookup = download_wai_reference(fg_env, wai_token)
            except Exception as exc:
                print(f"WAI reference download failed: {exc}")
                return

            wai_rows, unmatched = build_wai_rows(data, wo_lookup)

            if unmatched:
                print(f"\nWAI: {len(unmatched)} worker(s) not found in WAI reference: "
                      f"{', '.join(unmatched[:10])}"
                      + (" ..." if len(unmatched) > 10 else ""))

            if not wai_rows:
                print("WAI: No rows to upload. Skipping WAI CSV upload.")
            else:
                wai_path = out_dir / f"{date_label}_wai_upload.csv"
                write_wai_csv(wai_rows, wai_path)

                print(f"\nUploading WAI CSV to Fieldglass ({args.fg_env})...")
                try:
                    fg_upload_wai_csv(wai_path, fg_env)
                except Exception as exc:
                    print(f"Fieldglass WAI upload failed: {exc}")


if __name__ == "__main__":
    main()
