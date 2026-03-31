#!/usr/bin/env python3
"""
Generate CGHP TITO timesheet CSVs organised by Fieldglass job posting.

The TSheet CSV is the single source of truth for scope — no date range flags
needed. Job Posting IDs and their start/end dates are read from the TSheet,
and the Redshift query is scoped to exactly those postings and dates.

Matches workers using backend_shift.external_id → TSheet Job Posting ID (with
a secondary per-worker match on External Reference ID → worker_id for the
correct Work Order ID), then writes two TITO-format CSVs per job posting:

  cghp{company_id}_{jp_id}_Draft.csv   Submit=False  (review copy)
  cghp{company_id}_{jp_id}_Final.csv   Submit=True   (upload copy)

Rows within each file are sorted by Work Order ID, then Entry Date.

Additional output files (written only when rows are present):
  _unmatched_{label}.csv
  _potential_fraudsters_{label}.csv
  _flagged_anomalies_{label}.csv

Usage:
    python generate_cghp_tito_by_job_posting.py --company-id 109288

    # Skip Google Drive upload
    ... --no-upload

    # Upload Final CSVs to Fieldglass test env
    ... --fg-upload

    # Explicit TSheet path
    ... --tsheet ~/Downloads/MyTSheet.csv

    # Explicit timezone (skips Redshift lookup)
    ... --timezone America/Phoenix
"""

import argparse
import csv
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── Redshift config ────────────────────────────────────────────────────────────
CLUSTER_ID = "instawork-dw"
DATABASE   = "instawork"
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")

# ── Google Drive config ────────────────────────────────────────────────────────
DRIVE_CREDENTIALS_PATH = str(Path(__file__).parent / "credentials.json")
DRIVE_PARENT_FOLDER_ID = "0AG5xYs7dGEG6Uk9PVA"
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

# ── Fieldglass test-env config (test only — no prod credentials here) ──────────
FG_TEST = {
    "base_url":    "https://supplier.imp1.us.fieldglasstest.cloud.sap",
    "app_key":     os.getenv("FG_TEST_APP_KEY",     "7MpMW966RHee2XCbdTFCD2EZ8AF"),
    "iu_user":     os.getenv("FG_TEST_IU_USER",     "enkhjin"),
    "iu_password": os.getenv("FG_TEST_IU_PASSWORD", "IWORK_C5YdbVEnR8TDLe7ggm8FUHaHWQV"),
}
FG_TITO_CONNECTOR = "TITO Details for Rate Schedule Time Sheet Upload"

# ── TSheet CSV search paths ────────────────────────────────────────────────────
_DEFAULT_TSHEET_PATHS = [
    Path.home() / "Downloads" / "TSheet_Report_Correct.csv",
    Path.home() / "Downloads" / "TSheet_Report (8).csv",
    Path.home() / "Downloads" / "TSheet_Report (7).csv",
    Path.home() / "Downloads" / "TSheet_Report (6).csv",
    Path.home() / "Downloads" / "TSheet_Report (5).csv",
    Path.home() / "Downloads" / "TSheet_Report (4).csv",
    Path.home() / "Downloads" / "TSheet_Report (3).csv",
    Path.home() / "Downloads" / "TSheet_Report (2).csv",
    Path.home() / "Downloads" / "TSheet_Report (1).csv",
    Path.home() / "Downloads" / "TSheet_Report.csv",
]

# ── TITO column headers ────────────────────────────────────────────────────────
COLUMN_HEADERS = [
    "Worker_Id", "Job Seeker ID", "Work Order ID", "First Name", "Last Name",
    "TIMESHEET ID", "Date", "Entry Date", "Cost Center Code", "Task Code",
    "GL Account Code", "Segmented Object Detail", "Shift Code",
    "Time In", "Meal Break 1 Out", "Meal Break 1 In",
    "Meal Break 2 Out", "Meal Break 2 In",
    "Meal Break 3 Out", "Meal Break 3 In", "Time Out",
]

_DAY_NAME_TO_WEEKDAY = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}

# ── Anomaly thresholds ─────────────────────────────────────────────────────────
_SHORT_SHIFT_MINS  = 10
_LONG_SHIFT_HOURS  = 16

# ── Final-upload hold ──────────────────────────────────────────────────────────
_FINAL_HOLD_DAYS = 3


# ══════════════════════════════════════════════════════════════════════════════
# Redshift helpers
# ══════════════════════════════════════════════════════════════════════════════

def _redshift_client():
    return boto3.client("redshift-data", region_name=AWS_REGION)


def _run_redshift_sql(sql: str, timeout_iters: int = 120, poll_secs: int = 5) -> list[dict]:
    """Execute SQL via Redshift Data API and return a list of row dicts."""
    client  = _redshift_client()
    resp    = client.execute_statement(ClusterIdentifier=CLUSTER_ID, Database=DATABASE, Sql=sql)
    stmt_id = resp["Id"]
    print(f"  Redshift statement {stmt_id} — waiting...")

    for _ in range(timeout_iters):
        time.sleep(poll_secs)
        desc   = client.describe_statement(Id=stmt_id)
        status = desc["Status"]
        print(f"    status: {status}")
        if status == "FINISHED":
            break
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift query failed: {desc.get('Error', 'unknown')}")
    else:
        raise TimeoutError("Redshift query timed out.")

    rows     = []
    columns  = None
    for page in client.get_paginator("get_statement_result").paginate(Id=stmt_id):
        if columns is None:
            columns = [c["name"] for c in page["ColumnMetadata"]]
        for record in page["Records"]:
            row = {}
            for col, field in zip(columns, record):
                key      = list(field.keys())[0]
                row[col] = None if key == "isNull" else list(field.values())[0]
            rows.append(row)
    return rows


def _run_lookup(sql: str, fallback):
    """Run a small single-value query; return fallback on any error."""
    try:
        rows = _run_redshift_sql(sql, timeout_iters=30, poll_secs=2)
        if rows:
            return list(rows[0].values())[0]
    except Exception:
        pass
    return fallback


def fetch_company_name(company_id: int) -> str:
    sql = (
        "SELECT b.name "
        "FROM iw_backend_db.backend_gigtemplate gt "
        "JOIN iw_backend_db.business b ON b.id = gt.business_id "
        f"WHERE gt.company_id = {company_id} LIMIT 1"
    )
    raw = _run_lookup(sql, fallback=None)
    if not raw:
        return f"cghp{company_id}"
    safe = re.sub(r"[^\w\s-]", "", str(raw)).strip()
    return re.sub(r"\s+", "_", safe)


def fetch_company_timezone(company_id: int) -> str:
    sql = (
        "SELECT DISTINCT p.timezone "
        "FROM iw_backend_db.backend_gigtemplate gt "
        "JOIN iw_backend_db.business b ON b.id = gt.business_id "
        "JOIN iw_backend_db.places_place p ON p.id = b.place_id "
        f"WHERE gt.company_id = {company_id} LIMIT 1"
    )
    return _run_lookup(sql, fallback="America/Chicago") or "America/Chicago"


def build_shift_query(
    company_id: int,
    jp_ids: list[str],
    start_date: str,
    end_date: str,
    timezone: str,
) -> str:
    """
    jp_ids      — list of Fieldglass Job Posting IDs from the TSheet
    start_date  — earliest Job Posting Start Date across all TSheet entries (YYYY-MM-DD)
    end_date    — latest  Job Posting End Date   across all TSheet entries (YYYY-MM-DD)
    """
    jp_list = ", ".join(f"'{jp}'" for jp in jp_ids)
    return f"""
WITH shift_data AS (
    SELECT
        bc.id                                                                                                        AS company_id,
        bs.external_id                                                                                               AS external_id,
        b.name                                                                                                       AS business_name,
        bs.id                                                                                                        AS shift_id,
        bs.worker_id,
        bs.break_length                                                                                              AS break_length_mins,
        convert_timezone('{timezone}', sg.starts_at)                                                                AS shift_starts_at,
        convert_timezone('{timezone}', sg.ends_at)                                                                  AS shift_ends_at,
        TO_CHAR(convert_timezone('{timezone}', COALESCE(bs.resolved_starts_at, bs.actual_starts_at)), 'HH24:MI')    AS actual_clock_in,
        TO_CHAR(convert_timezone('{timezone}', COALESCE(bs.resolved_ends_at,   bs.actual_ends_at)),   'HH24:MI')    AS actual_clock_out
    FROM iw_backend_db.backend_shift bs
    JOIN iw_backend_db.backend_shiftgroup sg ON sg.id = bs.shift_group_id
    JOIN iw_backend_db.backend_gigtemplate gt ON gt.id = sg.gig_id
    JOIN iw_backend_db.business b ON b.id = gt.business_id
    JOIN iw_backend_db.places_place p ON p.id = b.place_id
    JOIN iw_backend_db.backend_company bc ON bc.id = b.company_id
    WHERE bc.id = {company_id}
      AND bs.external_id IN ({jp_list})
      AND DATE(DATE_TRUNC('day', convert_timezone('{timezone}', sg.starts_at)))
          BETWEEN '{start_date}' AND '{end_date}'
      AND bs.worker_id IS NOT NULL
      AND bs.is_cancelled = 0
      AND bs.actual_starts_at IS NOT NULL
      AND bs.actual_ends_at   IS NOT NULL
),
persona_names AS (
    SELECT
        dvl.user_id,
        dvl.first_name                                                                 AS persona_first_name,
        dvl.last_name                                                                  AS persona_last_name,
        ROW_NUMBER() OVER (PARTITION BY dvl.user_id ORDER BY dvl.created_at DESC)     AS rn
    FROM iw_backend_db.identity_verification_documentverificationlog dvl
    WHERE dvl.is_valid_name = true
      AND dvl.user_id IN (SELECT DISTINCT worker_id FROM shift_data)
),
worker_resolved AS (
    SELECT
        sd.*,
        INITCAP(COALESCE(pn.persona_first_name, up.given_name,
            CASE WHEN up.name IS NOT NULL AND REGEXP_COUNT(up.name, ' ') >= 1
                 THEN SPLIT_PART(up.name, ' ', 1) END
        )) AS first_name,
        INITCAP(COALESCE(pn.persona_last_name, up.family_name,
            CASE WHEN up.name IS NOT NULL AND REGEXP_COUNT(up.name, ' ') >= 1
                 THEN SPLIT_PART(up.name, ' ', 2) END
        )) AS last_name
    FROM shift_data sd
    JOIN iw_backend_db.backend_userprofile up ON up.id = CAST(sd.worker_id AS BIGINT)
    LEFT JOIN persona_names pn ON pn.user_id = sd.worker_id AND pn.rn = 1
)
SELECT
    w.worker_id,
    w.first_name,
    w.last_name,
    w.external_id,
    w.shift_id,
    w.business_name,
    TO_CHAR(w.shift_starts_at, 'MM/DD/YYYY')                                          AS shift_date,
    COALESCE(w.actual_clock_in,  TO_CHAR(w.shift_starts_at, 'HH24:MI'))               AS start_time,
    COALESCE(w.actual_clock_out, TO_CHAR(w.shift_ends_at,   'HH24:MI'))               AS end_time,
    w.shift_starts_at,
    w.break_length_mins
FROM worker_resolved w
ORDER BY w.shift_starts_at, w.last_name
"""


# ══════════════════════════════════════════════════════════════════════════════
# TSheet loader — keyed by Job Posting ID
# ══════════════════════════════════════════════════════════════════════════════

def load_tsheet(paths: list[Path]) -> dict[str, list[dict]]:
    """
    Load TSheet CSV and return a lookup keyed by Job Posting ID.

    Each value is a list of per-worker entries:
        {ext_ref_id, work_order_id, cost_center_code, week_start_weekday,
         start_date, end_date, job_posting_title, tsheet_worker_name}
    """
    all_rows: list[dict] = []
    used_path = None
    for path in paths:
        if path.exists():
            with open(path, newline="", encoding="utf-8-sig") as f:
                all_rows.extend(list(csv.DictReader(f)))
            used_path = path
            break

    if not all_rows:
        raise FileNotFoundError(
            "TSheet CSV not found. Tried:\n  " + "\n  ".join(str(p) for p in paths)
        )
    print(f"Loaded TSheet CSV: {used_path}  ({len(all_rows)} rows)")

    mapping: dict[str, list[dict]] = {}
    for row in all_rows:
        jp_id       = row.get("Job Posting ID",        "").strip().strip('"')
        ext_ref     = row.get("External Reference ID", "").strip().strip('"')
        cost_center = (
            row.get("Primary Cost Center Code") or row.get("Location Code", "")
        ).strip().strip('"')
        work_order  = row.get("Work Order ID",          "").strip().strip('"')
        jp_title    = row.get("Job Posting Title",      "").strip().strip('"')
        start_str   = row.get("Job Posting Start Date", "").strip().strip('"')
        end_str     = row.get("Job Posting End Date",   "").strip().strip('"')
        start_day   = row.get("Start Day of Week",      "").strip().strip('"').lower()
        worker_name = row.get("Job Seeker / Worker",    "").strip().strip('"')

        if not jp_id or not cost_center:
            continue

        try:
            start_dt = datetime.strptime(start_str, "%m/%d/%Y")
            end_dt   = datetime.strptime(end_str,   "%m/%d/%Y")
        except ValueError:
            start_dt = end_dt = None

        mapping.setdefault(jp_id, []).append({
            "ext_ref_id":         ext_ref,
            "work_order_id":      work_order,
            "cost_center_code":   cost_center,
            "week_start_weekday": _DAY_NAME_TO_WEEKDAY.get(start_day, 4),
            "start_date":         start_dt,
            "end_date":           end_dt,
            "job_posting_title":  jp_title,
            "tsheet_worker_name": worker_name,
        })

    print(f"TSheet lookup: {len(mapping)} unique Job Posting IDs, "
          f"{sum(len(v) for v in mapping.values())} worker entries.")
    return mapping


def tsheet_date_scope(jp_lookup: dict) -> tuple[list[str], str, str]:
    """
    Derive the Redshift query scope entirely from the loaded TSheet.

    Returns:
        jp_ids      — all Job Posting IDs present in the TSheet
        start_date  — earliest Job Posting Start Date (YYYY-MM-DD)
        end_date    — latest  Job Posting End Date   (YYYY-MM-DD)
    """
    jp_ids     = list(jp_lookup.keys())
    all_starts = [e["start_date"] for entries in jp_lookup.values() for e in entries if e["start_date"]]
    all_ends   = [e["end_date"]   for entries in jp_lookup.values() for e in entries if e["end_date"]]

    if not all_starts or not all_ends:
        raise ValueError("TSheet entries have no parseable Job Posting Start/End dates.")

    start_date = min(all_starts).strftime("%Y-%m-%d")
    end_date   = max(all_ends).strftime("%Y-%m-%d")
    return jp_ids, start_date, end_date


def _best_entry_for_worker(entries: list[dict], worker_id: str, shift_dt: datetime) -> dict | None:
    """
    Two-step match within a job posting's entries:
    1. Prefer entries where ext_ref_id == worker_id AND date range covers shift_dt.
    2. Fall back to any entry whose date range covers shift_dt (narrowest span wins).
    """
    worker_id_str = str(worker_id)

    exact = [
        e for e in entries
        if e["ext_ref_id"] == worker_id_str
        and e["start_date"] and e["end_date"]
        and e["start_date"] <= shift_dt <= e["end_date"]
    ]
    if exact:
        return min(exact, key=lambda e: (e["end_date"] - e["start_date"]).days)

    in_range = [
        e for e in entries
        if e["start_date"] and e["end_date"]
        and e["start_date"] <= shift_dt <= e["end_date"]
    ]
    if in_range:
        return min(in_range, key=lambda e: (e["end_date"] - e["start_date"]).days)

    return None


# ══════════════════════════════════════════════════════════════════════════════
# Time / date helpers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_time(value: str) -> datetime | None:
    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I:%M:%S %p"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except (ValueError, AttributeError):
            pass
    return None


def format_time(value: str) -> str:
    dt = _parse_time(value)
    return dt.strftime("%-H:%M") if dt else value.strip()


def week_start_from_date(shift_date_str: str, week_start_weekday: int) -> str:
    """Return MM/DD/YYYY of the most recent week_start_weekday on or before shift_date."""
    try:
        dt        = datetime.strptime(shift_date_str, "%m/%d/%Y")
        days_back = (dt.weekday() - week_start_weekday) % 7
        return (dt - timedelta(days=days_back)).strftime("%m/%d/%Y")
    except ValueError:
        return shift_date_str


def compute_break_times(clock_in_str: str, clock_out_str: str, break_mins) -> tuple[str, str]:
    """Return (break_out, break_in) as 'H:MM'. Empty strings when no break."""
    try:
        mins = float(break_mins) if break_mins is not None else 0
    except (TypeError, ValueError):
        mins = 0
    if mins <= 0:
        return "", ""

    clock_in  = _parse_time(clock_in_str)
    clock_out = _parse_time(clock_out_str)
    if not clock_in:
        return "", ""

    break_out_dt = clock_in + timedelta(hours=4)
    break_in_dt  = break_out_dt + timedelta(minutes=mins)

    if clock_out and break_out_dt >= clock_out:
        break_in_dt  = clock_out
        break_out_dt = clock_out - timedelta(minutes=mins)

    return break_out_dt.strftime("%-H:%M"), break_in_dt.strftime("%-H:%M")


def _shift_duration_mins(time_in: str, time_out: str) -> float | None:
    """Shift length in minutes, handling overnight. None if either time is empty."""
    t_in  = _parse_time(time_in)
    t_out = _parse_time(time_out)
    if not t_in or not t_out:
        return None
    delta = (t_out - t_in).total_seconds() / 60
    if delta < -60:   # overnight shift
        delta += 1440
    return delta


# ══════════════════════════════════════════════════════════════════════════════
# Fraudster / name-match helper
# ══════════════════════════════════════════════════════════════════════════════

def _names_match(iw_first: str, iw_last: str, tsheet_raw: str) -> bool:
    """True if Instawork and TSheet names refer to the same person.

    Handles:
    - TSheet "Last, First Middle" format
    - Nickname prefix variants  (Jeff/Jeffrey, Ben/Benjamin)
    - Apostrophes / hyphens     (D'Arby → darby)
    - Flipped first/last order
    """
    def _norm(s: str) -> str:
        return re.sub(r"['\-]", "", s.lower()).strip()

    def _tok_match(a: str, b: str) -> bool:
        return bool(a and b and (
            a == b
            or (len(a) >= 3 and b.startswith(a))
            or (len(b) >= 3 and a.startswith(b))
        ))

    ts = tsheet_raw.strip()
    if "," in ts:
        parts        = ts.split(",", 1)
        ts_last_raw  = parts[0].strip()
        ts_first_raw = parts[1].strip().split()[0] if parts[1].strip() else ""
    else:
        tokens       = ts.split()
        ts_first_raw = tokens[0] if tokens else ""
        ts_last_raw  = tokens[1] if len(tokens) > 1 else ""

    iw_f = _norm(iw_first.split()[0]) if iw_first.split() else ""
    iw_l = _norm(iw_last.split()[0])  if iw_last.split()  else ""
    ts_f = _norm(ts_first_raw)
    ts_l = _norm(ts_last_raw)

    return (_tok_match(iw_f, ts_f) and _tok_match(iw_l, ts_l)) or \
           (_tok_match(iw_f, ts_l) and _tok_match(iw_l, ts_f))


# ══════════════════════════════════════════════════════════════════════════════
# Anomaly checker
# ══════════════════════════════════════════════════════════════════════════════

def check_anomalies(
    worker_id: str, first_name: str, last_name: str,
    shift_date: str, work_order_id: str, jp_id: str,
    time_in: str, time_out: str, break_mins,
    jp_end_date: datetime | None = None,
) -> list[dict]:
    """Return a list of anomaly dicts (empty list if all clear)."""
    flags    = []
    duration = _shift_duration_mins(time_in, time_out)

    def _flag(code: str, notes: str):
        flags.append({
            "worker_id":      worker_id,
            "first_name":     first_name,
            "last_name":      last_name,
            "entry_date":     shift_date,
            "work_order_id":  work_order_id,
            "job_posting_id": jp_id,
            "time_in":        time_in,
            "time_out":       time_out,
            "duration_mins":  f"{duration:.1f}" if duration is not None else "",
            "flag":           code,
            "notes":          notes,
        })

    no_clock_out = not time_out or (time_in and time_in == time_out)
    if no_clock_out:
        _flag("ZERO_DURATION", "Clock-out equals clock-in or is missing.")
    elif duration is not None:
        if duration < 0:
            _flag("NEGATIVE_DURATION",
                  f"Clock-out ({time_out}) is before clock-in ({time_in}).")
        elif duration < _SHORT_SHIFT_MINS:
            _flag("SHORT_SHIFT",
                  f"Shift is only {duration:.1f} min (< {_SHORT_SHIFT_MINS} min threshold).")
        elif duration > _LONG_SHIFT_HOURS * 60:
            _flag("LONG_SHIFT",
                  f"Shift is {duration / 60:.1f} hrs (> {_LONG_SHIFT_HOURS} hr threshold).")

    try:
        bmins = float(break_mins) if break_mins is not None else 0
    except (TypeError, ValueError):
        bmins = 0
    if bmins > 0 and duration is not None and not no_clock_out and bmins >= duration:
        _flag("BREAK_EXCEEDS_SHIFT",
              f"Break ({bmins:.0f} min) ≥ shift duration ({duration:.1f} min).")

    if jp_end_date:
        try:
            sdt = datetime.strptime(shift_date, "%m/%d/%Y").date()
            if sdt > jp_end_date.date():
                _flag("LATE_SUBMISSION",
                      f"Shift date {shift_date} is after job posting end "
                      f"({jp_end_date.strftime('%m/%d/%Y')}).")
        except ValueError:
            pass

    return flags


# ══════════════════════════════════════════════════════════════════════════════
# Row transformer
# ══════════════════════════════════════════════════════════════════════════════

def transform_row(row: dict, jp_lookup: dict) -> dict:
    """
    Match a Redshift shift row to the TSheet job-posting lookup.

    Primary key:   external_id (Redshift) → Job Posting ID (TSheet)
    Secondary key: worker_id   (Redshift) → ext_ref_id     (TSheet entry)

    Returns a result dict:
        matched, csv_row, sort_key, jp_id, jp_title, name_info, anomalies
    """
    worker_id   = str(row.get("worker_id",   "") or "").strip()
    external_id = str(row.get("external_id", "") or "").strip()
    shift_date  = str(row.get("shift_date",  "") or "").strip()
    start_time  = str(row.get("start_time",  "") or "").strip()
    end_time    = str(row.get("end_time",    "") or "").strip()
    first_name  = str(row.get("first_name",  "") or "").strip()
    last_name   = str(row.get("last_name",   "") or "").strip()
    break_mins  = row.get("break_length_mins")

    work_order_id      = ""
    cost_center_code   = ""
    week_start_weekday = 4          # Friday default (overridden by TSheet entry)
    jp_id              = external_id
    jp_title           = ""
    jp_end_date: datetime | None = None
    matched            = False
    name_info: dict    = {}

    candidates = jp_lookup.get(external_id) if external_id else None
    if candidates:
        try:
            shift_dt = datetime.strptime(shift_date, "%m/%d/%Y")
        except ValueError:
            shift_dt = None

        if shift_dt:
            best = _best_entry_for_worker(candidates, worker_id, shift_dt)
            if best:
                work_order_id      = best["work_order_id"]
                cost_center_code   = best["cost_center_code"]
                week_start_weekday = best["week_start_weekday"]
                jp_title           = best["job_posting_title"]
                jp_end_date        = best.get("end_date")
                matched            = True

                tsheet_name = best.get("tsheet_worker_name", "")
                iw_name     = f"{first_name} {last_name}".strip()
                name_check  = ""
                if tsheet_name and iw_name:
                    name_check = "MATCH" if _names_match(first_name, last_name, tsheet_name) else "MISMATCH"
                name_info = {
                    "worker_id":      worker_id,
                    "work_order_id":  work_order_id,
                    "job_posting_id": jp_id,
                    "entry_date":     shift_date,
                    "instawork_name": iw_name,
                    "tsheet_name":    tsheet_name,
                    "name_check":     name_check,
                }
            else:
                print(f"  WARNING: No date-range match for {first_name} {last_name} "
                      f"(worker_id={worker_id}, external_id={external_id}) on {shift_date}")
    elif external_id:
        print(f"  WARNING: external_id={external_id} ({first_name} {last_name}) "
              f"not found in TSheet (worker_id={worker_id})")
    else:
        print(f"  WARNING: worker_id={worker_id} ({first_name} {last_name}) "
              f"has no external_id — cannot match to TSheet")

    week_start          = week_start_from_date(shift_date, week_start_weekday)
    break_out, break_in = compute_break_times(start_time, end_time, break_mins)
    fmt_in              = format_time(start_time)
    fmt_out             = format_time(end_time)

    csv_row = [
        "",               # Worker_Id
        "",               # Job Seeker ID
        work_order_id,    # Work Order ID
        first_name,       # First Name
        last_name,        # Last Name
        "",               # TIMESHEET ID
        week_start,       # Date (week-start)
        shift_date,       # Entry Date
        cost_center_code, # Cost Center Code
        "Hours Worked",   # Task Code
        "Default",        # GL Account Code
        "",               # Segmented Object Detail
        "Standard",       # Shift Code
        fmt_in,           # Time In
        break_out,        # Meal Break 1 Out
        break_in,         # Meal Break 1 In
        "",               # Meal Break 2 Out
        "",               # Meal Break 2 In
        "",               # Meal Break 3 Out
        "",               # Meal Break 3 In
        fmt_out,          # Time Out
    ]

    try:
        date_iso = datetime.strptime(shift_date, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        date_iso = shift_date

    anomalies = check_anomalies(
        worker_id, first_name, last_name, shift_date,
        work_order_id, jp_id, fmt_in, fmt_out, break_mins,
        jp_end_date=jp_end_date,
    )

    return {
        "matched":   matched,
        "csv_row":   csv_row,
        "sort_key":  (work_order_id, date_iso),
        "jp_id":     jp_id,
        "jp_title":  jp_title,
        "name_info": name_info,
        "anomalies": anomalies,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Hold-date helper
# ══════════════════════════════════════════════════════════════════════════════

def _final_ready(hold_date: date, today: date) -> bool:
    """Return True when today is on or after hold_date."""
    return today >= hold_date


# ══════════════════════════════════════════════════════════════════════════════
# CSV writers
# ══════════════════════════════════════════════════════════════════════════════

def _build_tito_header(submit: bool) -> list[str]:
    return [
        "Type=TITO Details for Rate Schedule Time Sheet Upload",
        "Language=English (United States)",
        "Date Format=MM/DD/YYYY",
        "Approval Required=True",
        f"Submit={'True' if submit else 'False'}",
        "Buyer=CGHP",
        "Comments=",
    ]


def write_tito_csv(rows: list[list], out_path: Path, submit: bool) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for line in _build_tito_header(submit):
            writer.writerow([line])
        writer.writerow([])
        writer.writerow(COLUMN_HEADERS)
        for row in rows:
            writer.writerow(row)
    tag = "Final (Submit=True)" if submit else "Draft (Submit=False)"
    print(f"    [{tag}] {out_path.name}  ({len(rows)} rows)")


def write_unmatched_csv(rows: list[list], out_path: Path) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMN_HEADERS)
        for row in rows:
            writer.writerow(row)
    print(f"  Written [unmatched]: {out_path.name}  ({len(rows)} rows)")


def write_fraudster_report(rows: list[dict], out_path: Path) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["worker_id", "entry_date", "work_order_id",
                         "job_posting_id", "instawork_name", "tsheet_name"])
        for r in rows:
            writer.writerow([r["worker_id"], r["entry_date"], r["work_order_id"],
                             r["job_posting_id"], r["instawork_name"], r["tsheet_name"]])
    print(f"  Written [fraudsters]: {out_path.name}  ({len(rows)} row(s))")


def write_anomaly_report(rows: list[dict], out_path: Path) -> None:
    cols = ["worker_id", "first_name", "last_name", "entry_date", "work_order_id",
            "job_posting_id", "time_in", "time_out", "duration_mins", "flag", "notes"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for r in rows:
            writer.writerow([r.get(c, "") for c in cols])
    print(f"  Written [anomalies]: {out_path.name}  ({len(rows)} row(s))")


# ══════════════════════════════════════════════════════════════════════════════
# Google Drive upload
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_drive(folder_name: str, file_paths: list[Path]) -> None:
    creds   = service_account.Credentials.from_service_account_file(
        DRIVE_CREDENTIALS_PATH, scopes=DRIVE_SCOPES
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    folder = service.files().create(
        body={
            "name":     folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents":  [DRIVE_PARENT_FOLDER_ID],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()
    folder_id = folder["id"]
    print(f"Created Drive folder '{folder_name}' (id={folder_id})")

    for path in file_paths:
        service.files().create(
            body={"name": path.name, "parents": [folder_id]},
            media_body=MediaFileUpload(str(path), mimetype="text/csv", resumable=False),
            fields="id",
            supportsAllDrives=True,
        ).execute()
        print(f"  Uploaded to Drive: {path.name}")


# ══════════════════════════════════════════════════════════════════════════════
# Fieldglass upload — test env only
# ══════════════════════════════════════════════════════════════════════════════

def _fg_get_token() -> str:
    url  = f"{FG_TEST['base_url']}/api/oauth2/v2.0/token"
    resp = requests.post(
        url,
        params={"grant_type": "client_credentials", "response_type": "token"},
        headers={"x-api-key": FG_TEST["app_key"], "Content-Type": "application/json"},
        auth=(FG_TEST["iu_user"], FG_TEST["iu_password"]),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in Fieldglass response: {resp.text}")
    print(f"  FG token acquired (expires in {resp.json().get('expires_in', '?')}s).")
    return token


def _parse_fg_response(resp) -> tuple[str, str, str]:
    try:
        root = ET.fromstring(resp.text)
        rc   = root.findtext("ReturnCode", "?")
        msg  = root.findtext("Message", resp.text).strip()
        txn  = root.findtext("TransactionID", "?")
    except ET.ParseError:
        rc, msg, txn = "?", resp.text.strip()[:300], "?"
    return rc, msg, txn


def upload_tito_to_fg(csv_path: Path) -> bool:
    """Upload one Final TITO CSV to Fieldglass test env. Returns True on success."""
    print(f"  Uploading to Fieldglass TEST: {csv_path.name} ...")
    token = _fg_get_token()
    url   = (
        f"{FG_TEST['base_url']}/api/vc/connector/"
        f"{requests.utils.quote(FG_TITO_CONNECTOR)}"
    )
    resp     = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key":     FG_TEST["app_key"],
            "Content-Type":  "text/csv;charset=UTF-8",
        },
        data=csv_path.read_bytes(),
        timeout=120,
    )
    rc, msg, txn = _parse_fg_response(resp)

    if resp.status_code == 200 or rc == "0":
        print(f"  FG upload SUCCESS  TransactionID={txn}")
        return True

    if rc == "106":
        lines   = [ln.strip() for ln in msg.splitlines()]
        summary = next(
            (ln for ln in reversed(lines) if "of" in ln and "record" in ln.lower()), ""
        )
        errors, i = [], 0
        while i < len(lines):
            if lines[i].startswith("Record "):
                err_lines, i = [], i + 1
                while i < len(lines) \
                        and not lines[i].startswith("Record ") \
                        and not ("of" in lines[i] and "record" in lines[i].lower()):
                    if lines[i]:
                        err_lines.append(lines[i])
                    i += 1
                errors.append(" | ".join(err_lines))
            else:
                i += 1
        print(f"  FG upload PARTIAL  TransactionID={txn}  "
              f"Summary: {summary or 'see errors below'}")
        for err in errors[:10]:
            print(f"    {err}")
        return False

    print(f"  FG upload FAILED  HTTP={resp.status_code}  RC={rc}  {msg[:200]}")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Generate CGHP TITO CSVs organised by Fieldglass job posting."
    )
    parser.add_argument("--company-id",  type=int, required=True,
                        help="Instawork company ID (e.g. 109288)")
    parser.add_argument("--timezone",    default=None,
                        help="Venue timezone — auto-fetched from Redshift if omitted")
    parser.add_argument("--tsheet",      default=None,
                        help="Explicit path to TSheet CSV")
    parser.add_argument("--no-upload",   action="store_true",
                        help="Skip Google Drive upload")
    parser.add_argument("--fg-upload",   action="store_true",
                        help="Upload Final CSVs to Fieldglass test env after writing")
    args = parser.parse_args()

    company_id = args.company_id

    # ── 1. Resolve timezone ────────────────────────────────────────────────────
    if args.timezone:
        timezone = args.timezone
        print(f"Timezone: {timezone}  (--timezone flag)")
    else:
        print("Fetching company timezone from Redshift...")
        timezone = fetch_company_timezone(company_id)
        print(f"Timezone: {timezone}")

    # ── 2. Load TSheet — derive all scope from it ──────────────────────────────
    tsheet_paths         = [Path(args.tsheet)] if args.tsheet else _DEFAULT_TSHEET_PATHS
    jp_lookup            = load_tsheet(tsheet_paths)
    jp_ids, start_date, end_date = tsheet_date_scope(jp_lookup)
    start_label          = start_date.replace("-", "")
    end_label            = end_date.replace("-", "")
    label                = f"{start_label}_{end_label}"
    print(f"TSheet scope: {len(jp_ids)} job posting(s), {start_date} → {end_date}")

    # ── 3. Resolve company name & create output folder ─────────────────────────
    print("Fetching company name from Redshift...")
    company_name = fetch_company_name(company_id)
    folder_name  = f"{company_name}_{company_id}_{start_label}_{end_label}"
    out_dir      = Path(__file__).parent / folder_name
    out_dir.mkdir(exist_ok=True)
    print(f"Output folder: {out_dir}")

    # ── 4. Query Redshift ──────────────────────────────────────────────────────
    print(f"\nQuerying Redshift — company {company_id}, "
          f"{len(jp_ids)} JP(s), {start_date} → {end_date} ...")
    rows = _run_redshift_sql(
        build_shift_query(company_id, jp_ids, start_date, end_date, timezone)
    )
    print(f"Fetched {len(rows)} shift rows.")

    # ── 5. Transform rows ──────────────────────────────────────────────────────
    matched_by_jp: dict[str, list[tuple]] = defaultdict(list)
    jp_titles:     dict[str, str]         = {}
    unmatched_rows: list[list]            = []
    fraudster_rows: list[dict]            = []
    anomaly_rows:   list[dict]            = []

    for row in rows:
        # Treat same-time clock-in/out as missing clock-out
        start = str(row.get("start_time", "") or "").strip()
        end   = str(row.get("end_time",   "") or "").strip()
        if start and end and start == end:
            row = dict(row)
            row["end_time"] = ""

        result = transform_row(row, jp_lookup)

        ni = result["name_info"]
        if ni.get("name_check") == "MISMATCH":
            print(
                f"  *** POTENTIAL FRAUDSTER: worker_id={ni['worker_id']}"
                f"  Instawork: \"{ni['instawork_name']}\""
                f"  TSheet: \"{ni['tsheet_name']}\""
                f"  (JP: {ni['job_posting_id']}, WO: {ni['work_order_id']},"
                f" Date: {ni['entry_date']}) — flag to TNS"
            )
            fraudster_rows.append(ni)

        for anom in result["anomalies"]:
            print(
                f"  WARNING [{anom['flag']}]: {anom['first_name']} {anom['last_name']}"
                f" worker_id={anom['worker_id']} on {anom['entry_date']}"
                f" — {anom['notes']}"
            )
            anomaly_rows.append(anom)

        if result["matched"]:
            jp_id = result["jp_id"]
            matched_by_jp[jp_id].append((result["sort_key"], result["csv_row"]))
            jp_titles.setdefault(jp_id, result["jp_title"])
        else:
            unmatched_rows.append(result["csv_row"])

    total_matched = sum(len(v) for v in matched_by_jp.values())
    print(
        f"\nTransformed: {total_matched} matched across "
        f"{len(matched_by_jp)} job posting(s), {len(unmatched_rows)} unmatched"
        + (f", {len(fraudster_rows)} potential fraudster(s)" if fraudster_rows else "")
        + (f", {len(anomaly_rows)} anomaly flag(s)" if anomaly_rows else "")
        + "."
    )

    # ── 6. Write per-job-posting Draft + Final CSVs ────────────────────────────
    all_output_files: list[Path] = []
    final_csv_paths:  list[Path] = []

    today = datetime.now().date()
    print(f"\nWriting CSV files to {out_dir} ...")
    for jp_id in sorted(matched_by_jp.keys()):
        entries = matched_by_jp[jp_id]
        title   = jp_titles.get(jp_id, "")
        print(f"\n  Job Posting: {jp_id}"
              + (f"  ({title})" if title else "")
              + f"  — {len(entries)} rows")

        # Determine posting duration from TSheet entries
        jp_tsheet_entries = jp_lookup.get(jp_id, [])
        jp_starts = [e["start_date"] for e in jp_tsheet_entries if e["start_date"]]
        jp_ends   = [e["end_date"]   for e in jp_tsheet_entries if e["end_date"]]
        jp_min_start = min(jp_starts) if jp_starts else None
        jp_max_end   = max(jp_ends)   if jp_ends   else None

        jp_duration_days = (
            (jp_max_end - jp_min_start).days
            if jp_min_start and jp_max_end else 0
        )

        if jp_duration_days <= 7:
            # ── Short posting: one Draft + Final, held until jp_end + 3 days ──
            hold_date = (jp_max_end + timedelta(days=_FINAL_HOLD_DAYS)).date() if jp_max_end else today
            if not _final_ready(hold_date, today):
                print(f"    [HELD] posting ends "
                      f"{jp_max_end.strftime('%m/%d/%Y')}, "
                      f"available {hold_date.strftime('%m/%d/%Y')} — skipping.")
                continue

            sorted_rows = [r for _, r in sorted(entries, key=lambda x: x[0])]
            draft_path  = out_dir / f"cghp{company_id}_{jp_id}_Draft.csv"
            final_path  = out_dir / f"cghp{company_id}_{jp_id}_Final.csv"
            write_tito_csv(sorted_rows, draft_path, submit=False)
            write_tito_csv(sorted_rows, final_path, submit=True)
            all_output_files.extend([draft_path, final_path])
            final_csv_paths.append(final_path)

        else:
            # ── Long posting: one Draft + Final per work-week ──
            # Group by csv_row[6] which is the week-start date (MM/DD/YYYY).
            weeks: dict[str, list] = {}
            for sort_key, csv_row in entries:
                week_label = csv_row[6]   # Date column = week-start
                weeks.setdefault(week_label, []).append((sort_key, csv_row))

            for week_label in sorted(weeks.keys(),
                                     key=lambda d: datetime.strptime(d, "%m/%d/%Y")):
                try:
                    week_start_dt = datetime.strptime(week_label, "%m/%d/%Y").date()
                except ValueError:
                    print(f"    WARNING: cannot parse week-start '{week_label}' — skipping.")
                    continue

                week_end_dt = week_start_dt + timedelta(days=6)
                hold_date   = week_end_dt + timedelta(days=_FINAL_HOLD_DAYS)
                week_rows   = weeks[week_label]

                if not _final_ready(hold_date, today):
                    print(f"    [HELD] week {week_label}–"
                          f"{week_end_dt.strftime('%m/%d/%Y')}, "
                          f"available {hold_date.strftime('%m/%d/%Y')} — skipping.")
                    continue

                sorted_rows  = [r for _, r in sorted(week_rows, key=lambda x: x[0])]
                week_tag     = week_start_dt.strftime("%m%d%Y")
                draft_path   = out_dir / f"cghp{company_id}_{jp_id}_{week_tag}_Draft.csv"
                final_path   = out_dir / f"cghp{company_id}_{jp_id}_{week_tag}_Final.csv"
                print(f"    Week {week_label} — {len(sorted_rows)} rows")
                write_tito_csv(sorted_rows, draft_path, submit=False)
                write_tito_csv(sorted_rows, final_path, submit=True)
                all_output_files.extend([draft_path, final_path])
                final_csv_paths.append(final_path)

    # ── 7. Write unmatched ─────────────────────────────────────────────────────
    if unmatched_rows:
        p = out_dir / f"_unmatched_{label}.csv"
        write_unmatched_csv(unmatched_rows, p)
        all_output_files.append(p)

    # ── 8. Write fraudster report ──────────────────────────────────────────────
    if fraudster_rows:
        p = out_dir / f"_potential_fraudsters_{label}.csv"
        write_fraudster_report(fraudster_rows, p)
        all_output_files.append(p)

    # ── 9. Write anomaly report ────────────────────────────────────────────────
    if anomaly_rows:
        p = out_dir / f"_flagged_anomalies_{label}.csv"
        write_anomaly_report(anomaly_rows, p)
        all_output_files.append(p)

    # ── 10. Fieldglass test-env upload ─────────────────────────────────────────
    if args.fg_upload:
        print(f"\n{'='*60}")
        print(f"Uploading {len(final_csv_paths)} Final CSV(s) to Fieldglass TEST env ...")
        print(f"{'='*60}")
        for path in final_csv_paths:
            upload_tito_to_fg(path)

    # ── 11. Google Drive upload ────────────────────────────────────────────────
    if not args.no_upload:
        print(f"\nUploading {len(all_output_files)} file(s) to Google Drive "
              f"folder '{folder_name}' ...")
        upload_to_drive(folder_name, all_output_files)

    print("\nDone.")


if __name__ == "__main__":
    main()
