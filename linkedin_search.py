"""
LinkedIn Google search — first-name only, first result writer.

Reads the `google_linkedin_search` column from Sheet1 of the spreadsheet.
Each cell contains a full Google search URL (URL-encoded). This script:
  1. Decodes the URL to extract the original query.
  2. Strips it down to: site:linkedin.com/in "Firstname"  (first name only, no location).
  3. Runs that simplified query on Google.
  4. Writes the first result URL into the `linkedin_google_search` column of the same row.

Skips rows that already have a value in linkedin_google_search.
Handles rate limits with exponential back-off; never crashes the run.

Usage:
    python linkedin_search.py

Env vars (optional):
    LINKEDIN_SEARCH_DELAY_SEC    base pause between queries (default 4)
    LINKEDIN_SEARCH_MAX_RETRIES  retries on transient errors (default 3)
"""

import os
import re
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote_plus, quote_plus

import gspread
from google.oauth2.service_account import Credentials
from googlesearch import search as _gsearch
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID       = "1OVr2EigkJ5ZHceilXGn-Zl8xpGTGPVxp8jKIEJnOgmo"
SERVICE_ACCOUNT_FILE = "google_service_account.json"
SHEET_TAB_NAME       = "Sheet1"   # tab that holds google_linkedin_search

SOURCE_COL  = "google_linkedin_search"   # read from here
DEST_COL    = "linkedin_google_search"   # write first result URL here

BASE_DELAY  = float(os.getenv("LINKEDIN_SEARCH_DELAY_SEC",  "4"))
MAX_RETRIES = int(os.getenv("LINKEDIN_SEARCH_MAX_RETRIES",  "3"))


# ── Google Sheets helpers ─────────────────────────────────────────────────────
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)


def get_sheet():
    sh = _client().open_by_key(SPREADSHEET_ID)
    for ws in sh.worksheets():
        if ws.title == SHEET_TAB_NAME:
            return ws
    raise ValueError(f"Tab '{SHEET_TAB_NAME}' not found.")


# ── Query helpers ─────────────────────────────────────────────────────────────
def decode_google_url(cell_value: str) -> str:
    """
    Turn a Google search URL into the plain query string.
    e.g. https://www.google.com/search?q=site%3Alinkedin...  →  site:linkedin.com/in "Ryan Winter" "Wellington"
    If the cell is already a plain string (not a URL), return it as-is.
    """
    cell_value = (cell_value or "").strip()
    if not cell_value:
        return ""
    if cell_value.startswith("http"):
        try:
            parsed = urlparse(cell_value)
            q = parse_qs(parsed.query).get("q", [""])[0]
            return unquote_plus(q)
        except Exception:
            return cell_value
    return cell_value


def extract_first_name(query: str) -> str:
    """
    Extract the first name from the first quoted term in the query.
    e.g. 'site:linkedin.com/in "Ryan Winter" "Wellington"'  →  'Ryan'
    """
    # Find all double-quoted groups
    quoted = re.findall(r'"([^"]+)"', query)
    if not quoted:
        return ""
    # First quoted group is the person's name
    full_name = quoted[0].strip()
    # First word = first name
    first_name = full_name.split()[0] if full_name else ""
    return first_name


def build_linkedin_query(first_name: str) -> str:
    """Build simplified LinkedIn Google search: site:linkedin.com/in "Firstname"."""
    return f'site:linkedin.com/in "{first_name}"'


# ── Google search (rate-limit aware) ─────────────────────────────────────────
def fetch_first_result(query: str) -> tuple[str, str]:
    """
    Returns (first_url, status).
    status: OK | NOT_FOUND | RATE_LIMITED | ERROR
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            results = list(_gsearch(query, num_results=1, sleep_interval=0))
            if not results:
                return "", "NOT_FOUND"
            return results[0], "OK"
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "too many" in msg or "captcha" in msg:
                wait = BASE_DELAY * (3 ** attempt) + random.uniform(0, 2)
                print(f"  ⚠ Rate-limited (attempt {attempt}/{MAX_RETRIES}). Back-off {wait:.1f}s …")
                time.sleep(wait)
                if attempt == MAX_RETRIES:
                    return "", "RATE_LIMITED"
            else:
                if attempt == MAX_RETRIES:
                    return "", f"ERROR:{str(exc)[:80]}"
                time.sleep(BASE_DELAY)
    return "", "ERROR"


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Reading sheet …")
    ws     = get_sheet()
    header = ws.row_values(1)

    if SOURCE_COL not in header:
        print(f"Column '{SOURCE_COL}' not found in header. Aborting.")
        return
    if DEST_COL not in header:
        print(f"Column '{DEST_COL}' not found in header. Aborting.")
        return

    src_idx  = header.index(SOURCE_COL)   # 0-based for list indexing
    dest_idx = header.index(DEST_COL)     # 0-based
    dest_col_letter = chr(ord('A') + dest_idx)  # works for cols A-Z

    all_rows    = ws.get_all_values()      # list of lists (row 0 = header)
    data_rows   = all_rows[1:]             # skip header

    to_process  = []   # list of (sheet_row_number_1based, query_string)
    skipped     = 0

    for i, row in enumerate(data_rows, start=2):   # row 2 = first data row
        src_val  = row[src_idx]  if src_idx  < len(row) else ""
        dest_val = row[dest_idx] if dest_idx < len(row) else ""

        if not src_val.strip():
            continue  # nothing to search
        if dest_val.strip():
            skipped += 1
            continue  # already has a result; skip

        raw_query  = decode_google_url(src_val)
        first_name = extract_first_name(raw_query)
        if not first_name:
            print(f"  Row {i}: could not extract first name from {raw_query!r:.60} — skip")
            continue

        query = build_linkedin_query(first_name)
        to_process.append((i, query, first_name))

    print(f"Rows to process: {len(to_process)}  |  Already filled (skipped): {skipped}")
    if not to_process:
        print("Nothing to do.")
        return

    # Stats
    ok_count   = 0
    fail_count = 0

    for idx, (sheet_row, query, first_name) in enumerate(to_process, start=1):
        t0 = time.time()
        url, status = fetch_first_result(query)
        elapsed = round(time.time() - t0, 2)

        icon = "✓" if status == "OK" else ("⚠" if "RATE" in status else "✗")
        print(f"[{idx:03d}/{len(to_process)}] {icon} row {sheet_row}  {first_name!r:<15}  "
              f"{status:<12}  {elapsed:4.1f}s  {url[:65] if url else status}")

        # Write the result URL (or status marker) back to the sheet
        cell = f"{dest_col_letter}{sheet_row}"
        ws.update(values=[[url]], range_name=cell, value_input_option="RAW")

        if status == "OK":
            ok_count += 1
        else:
            fail_count += 1

        # Polite delay
        if idx < len(to_process):
            sleep = BASE_DELAY + random.uniform(0.5, 2.5)
            time.sleep(sleep)

    print("\n" + "─" * 70)
    print(f"Done.  OK: {ok_count}  |  Failed/rate-limited: {fail_count}")
    if fail_count:
        print("Tip: increase LINKEDIN_SEARCH_DELAY_SEC to reduce rate-limit hits.")


if __name__ == "__main__":
    main()
