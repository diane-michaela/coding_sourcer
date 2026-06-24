"""
Google first-result fetcher — reads queries from any CSV file.

For each row in the input CSV, builds a search query from the columns you choose,
fetches the first Google result URL, and saves everything to an output CSV.

Usage:
    python google_search.py --csv path/to/file.csv --columns "name" "company"
    python google_search.py --csv path/to/file.csv   # will prompt you to pick columns

Config env vars (optional):
    GOOGLE_SEARCH_DELAY_SEC   base delay between queries  (default 3)
    GOOGLE_SEARCH_MAX_RETRIES retries on transient errors (default 3)
    GOOGLE_SEARCH_TO_SHEETS   "true" to also write to Google Sheets
"""

import os
import csv
import time
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path

from googlesearch import search as _gsearch

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT_SOURCE        = "google_search"
BASE_DELAY_SEC       = float(os.getenv("GOOGLE_SEARCH_DELAY_SEC",   "3"))
MAX_RETRIES          = int(os.getenv("GOOGLE_SEARCH_MAX_RETRIES",   "3"))
WRITE_TO_SHEETS      = os.getenv("GOOGLE_SEARCH_TO_SHEETS", "false").lower() in ("1", "true", "yes")

SPREADSHEET_ID       = "1OVr2EigkJ5ZHceilXGn-Zl8xpGTGPVxp8jKIEJnOgmo"
SERVICE_ACCOUNT_FILE = "google_service_account.json"
SHEET_TAB_NAME       = "google_search"

HEADER = [
    "source", "execution_timestamp", "row_index",
    "query", "first_result_url",
    "status",        # OK | NOT_FOUND | RATE_LIMITED | ERROR
    "error_detail",
    "elapsed_sec",
]


# ── CSV helpers ───────────────────────────────────────────────────────────────
def load_csv(filepath: str) -> tuple[list[str], list[dict]]:
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames or [], rows


def pick_columns(available: list[str], chosen: list[str] | None) -> list[str]:
    if chosen:
        invalid = [c for c in chosen if c not in available]
        if invalid:
            raise ValueError(f"Columns not found in CSV: {invalid}\nAvailable: {available}")
        return chosen

    print("\nAvailable columns in your CSV:")
    for i, col in enumerate(available, 1):
        print(f"  {i}. {col}")
    raw = input("\nWhich columns to use for the search query? (comma-separated names or numbers): ")
    selected = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            selected.append(available[int(part) - 1])
        else:
            if part not in available:
                raise ValueError(f"Column '{part}' not found in CSV")
            selected.append(part)
    return selected


def build_query(row: dict, columns: list[str]) -> str:
    return " ".join(str(row[c]) for c in columns if row.get(c))


# ── Google Sheets helpers ─────────────────────────────────────────────────────
def _gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_tab(tab_name: str, cols: int = len(HEADER)):
    client = _gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    for ws in sh.worksheets():
        if ws.title == tab_name:
            return ws
    return sh.add_worksheet(title=tab_name, rows=200, cols=cols)


def ensure_header(ws) -> None:
    existing = ws.row_values(1)
    if not existing:
        ws.append_row(HEADER)
    else:
        missing = [c for c in HEADER if c not in set(existing)]
        if missing:
            ws.update(values=[existing + missing], range_name="1:1")


def append_to_sheet(ws, rows: list[dict]) -> None:
    if not rows:
        return
    next_row = len(ws.col_values(1)) + 1
    needed   = next_row + len(rows) - 1
    if needed > ws.row_count:
        ws.add_rows(max(len(rows) + 200, 500))
    matrix = [[r.get(col, "") for col in HEADER] for r in rows]
    ws.update(values=matrix, range_name=f"A{next_row}", value_input_option="RAW")


# ── Core fetch ───────────────────────────────────────────────────────────────
def fetch_first_result(query: str) -> tuple[str, str, str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            results = list(_gsearch(query, num_results=1, sleep_interval=0))
            if not results:
                return "", "NOT_FOUND", ""
            return results[0], "OK", ""
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "too many" in msg or "captcha" in msg:
                wait = BASE_DELAY_SEC * (3 ** attempt) + random.uniform(0, 2)
                print(f"  ⚠ Rate-limited (attempt {attempt}/{MAX_RETRIES}). Backing off {wait:.1f}s …")
                time.sleep(wait)
                if attempt == MAX_RETRIES:
                    return "", "RATE_LIMITED", str(exc)
            else:
                if attempt == MAX_RETRIES:
                    return "", "ERROR", str(exc)[:200]
                time.sleep(BASE_DELAY_SEC)
    return "", "ERROR", "max retries"


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Google search from any CSV file")
    parser.add_argument("--csv",     required=True, help="Path to input CSV file")
    parser.add_argument("--columns", nargs="+",     help="Column name(s) to build the search query from")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    fieldnames, rows = load_csv(str(csv_path))
    columns = pick_columns(fieldnames, args.columns)

    output_csv = csv_path.with_name(csv_path.stem + "_google_results.csv")
    run_ts = datetime.now(timezone.utc).strftime("%d/%m/%y")
    results: list[dict] = []

    ok_count = fail_count = rl_count = 0
    total_time = 0.0

    print(f"\nSearching {len(rows)} rows from '{csv_path.name}' using columns: {columns}")
    print(f"Base delay: {BASE_DELAY_SEC}s | Max retries: {MAX_RETRIES} | Write to Sheets: {WRITE_TO_SHEETS}")
    print("─" * 70)

    for i, row in enumerate(rows, start=1):
        query = build_query(row, columns)
        if not query.strip():
            print(f"[{i:04d}] Skipped — empty query")
            continue

        t0 = time.time()
        url, status, err = fetch_first_result(query)
        elapsed = round(time.time() - t0, 2)
        total_time += elapsed

        result = {
            "source":              SCRIPT_SOURCE,
            "execution_timestamp": run_ts,
            "row_index":           i,
            "query":               query,
            "first_result_url":    url,
            "status":              status,
            "error_detail":        err,
            "elapsed_sec":         elapsed,
        }
        results.append(result)

        icon = "✓" if status == "OK" else ("⚠" if status == "RATE_LIMITED" else "✗")
        print(f"[{i:04d}] {icon} {status:<12} {elapsed:5.1f}s  {url[:60] if url else err[:60]}")

        if status == "OK":
            ok_count += 1
        elif status == "RATE_LIMITED":
            rl_count += 1
            fail_count += 1
        else:
            fail_count += 1

        if i < len(rows):
            time.sleep(BASE_DELAY_SEC + random.uniform(0.5, 2.0))

    # ── Write CSV ─────────────────────────────────────────────────────────────
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV saved: {output_csv.resolve()}")

    # ── Write to Sheets (optional) ────────────────────────────────────────────
    if WRITE_TO_SHEETS:
        try:
            ws = get_or_create_tab(SHEET_TAB_NAME)
            ensure_header(ws)
            append_to_sheet(ws, results)
            print(f"Appended {len(results)} rows to Google Sheets tab '{SHEET_TAB_NAME}'")
        except Exception as e:
            print(f"Sheets write failed: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print(f"Summary for {len(rows)} rows:")
    print(f"  OK            : {ok_count}")
    print(f"  Rate-limited  : {rl_count}")
    print(f"  Other failures: {fail_count - rl_count}")
    print(f"  Total time    : {total_time:.1f}s  (avg {total_time/max(len(results),1):.1f}s/query)")
    if rl_count:
        print(f"\n⚠ Rate limits hit {rl_count} times. Increase GOOGLE_SEARCH_DELAY_SEC to reduce.")


if __name__ == "__main__":
    main()
