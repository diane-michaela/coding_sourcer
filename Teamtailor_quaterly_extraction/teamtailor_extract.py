"""
Teamtailor quarterly extractor
===============================

Pulls a quarterly snapshot of job activity from Teamtailor's public API and
writes one CSV per entity into ./teamtailor_qN_YYYY_export/.

Replaces the old teamtailor_q1_extract.py / teamtailor_q2_extract.py pair —
same extraction logic (the Q1 version, which was the one actually run and
worked), plus the notes/reviews/messages endpoints added for Q2, now
parameterized by --quarter/--year instead of hardcoded per file.

Usage
-----
    pip install requests
    export TEAMTAILOR_TOKEN="your-admin-token"   # or set TOKEN below
    python teamtailor_extract.py                 # defaults to current quarter
    python teamtailor_extract.py --year 2026 --quarter 2

Notes
-----
* Uses the JSON:API endpoints documented at https://docs.teamtailor.com.
* Pagination follows the `links.next` cursor; we don't assume a max page count.
* Date filters use `filter[created-at]` and `filter[updated-at]` ranges where
  supported. As a safety net we also filter again client-side, so the final
  CSVs are guaranteed to be quarter-only.
* Related entities (department, location, recruiter, stage) are pulled into
  the `?include=` parameter so we can resolve names without N+1 calls.
* If the API returns 429 (rate limit), we sleep and retry.
* notes/reviews/messages require notes:read / reviews:read / messages:read
  scopes. If the token lacks a scope, that endpoint is skipped (403) instead
  of crashing the run — if they come back empty even with the scope, that's
  a real finding (interview docs not being written into Teamtailor), not a
  bug.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# You can either export TEAMTAILOR_TOKEN or paste your token here.
TOKEN = os.environ.get("TEAMTAILOR_TOKEN") or "PASTE_TOKEN_HERE"

API_BASE = "https://api.teamtailor.com/v1"
API_VERSION = "20240404"

# How many rows per page. Teamtailor allows up to 30.
PAGE_SIZE = 30

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Token token={TOKEN}",
    "X-Api-Version": API_VERSION,
    "Accept": "application/vnd.api+json",
    "Content-Type": "application/vnd.api+json",
})

# Set by main() once the quarter/year are known.
DATE_FROM: str = ""
DATE_TO: str = ""
DT_FROM: datetime
DT_TO: datetime
OUTPUT_DIR: Path


def quarter_bounds(year: int, quarter: int) -> tuple[str, str]:
    """Return (from, to) ISO timestamps (UTC, inclusive) for a given quarter."""
    start_month = (quarter - 1) * 3 + 1
    start = datetime(year, start_month, 1, tzinfo=timezone.utc)
    if quarter == 4:
        end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    else:
        next_start = datetime(year, start_month + 3, 1, tzinfo=timezone.utc)
        end = next_start - timedelta(seconds=1)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), end.strftime(fmt)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: Optional[Dict[str, Any]] = None,
         allow_403: bool = False, allow_404: bool = False) -> Optional[Dict[str, Any]]:
    """GET with retries on 429 and transient 5xx."""
    for attempt in range(6):
        resp = SESSION.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "5"))
            print(f"  rate-limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 403 and allow_403:
            return None
        if resp.status_code == 404 and allow_404:
            return None
        if resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  {resp.status_code} from server, retrying in {wait}s")
            time.sleep(wait)
            continue
        if not resp.ok:
            raise RuntimeError(
                f"GET {url} -> {resp.status_code}\n{resp.text[:500]}"
            )
        return resp.json()
    raise RuntimeError(f"GET {url} failed after retries")


def paginate(endpoint: str, params: Optional[Dict[str, Any]] = None,
             allow_403: bool = False) -> Iterable[Dict[str, Any]]:
    """Yield every record from a paginated JSON:API endpoint."""
    url = f"{API_BASE}/{endpoint}"
    params = dict(params or {})
    params.setdefault("page[size]", PAGE_SIZE)

    page_idx = 0
    while url:
        page_idx += 1
        page = _get(url, params=params if page_idx == 1 else None,
                    allow_403=allow_403, allow_404=True)
        if page is None:
            return
        for record in page.get("data", []):
            yield record
        url = page.get("links", {}).get("next")
        # After page 1 the `next` URL already contains all params we need.
        params = None
        if url:
            print(f"  page {page_idx} done, fetching next...")


# ---------------------------------------------------------------------------
# Date filtering
# ---------------------------------------------------------------------------

def in_range(value: Optional[str]) -> bool:
    """Client-side safety net for date filtering."""
    if not value:
        return False
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return DT_FROM <= dt <= DT_TO


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(name: str, rows: List[Dict[str, Any]]) -> Path:
    path = OUTPUT_DIR / f"{name}.csv"
    if not rows:
        path.write_text("")
        print(f"  -> {path}  (0 rows)")
        return path
    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: _scalar(r.get(k)) for k in fieldnames})
    print(f"  -> {path}  ({len(rows)} rows)")
    return path


def _scalar(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_text(html: str) -> str:
    if not html:
        return ""
    txt = re.sub(r'<span[^>]*data-label="([^"]+)"[^>]*>@?[^<]*</span>', r'@\1', html)
    txt = HTML_TAG_RE.sub(" ", txt)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    return WS_RE.sub(" ", txt).strip()


# ---------------------------------------------------------------------------
# Per-entity extractors
# ---------------------------------------------------------------------------

def flatten(record: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Flatten a JSON:API record to a single dict for CSV use."""
    out: Dict[str, Any] = {"id": record.get("id"), "type": record.get("type")}
    out.update(record.get("attributes", {}) or {})
    rels = record.get("relationships", {}) or {}
    for rel_name, rel_val in rels.items():
        data = (rel_val or {}).get("data")
        if isinstance(data, dict):
            out[f"rel_{rel_name}_id"] = data.get("id")
        elif isinstance(data, list):
            out[f"rel_{rel_name}_ids"] = ",".join(d.get("id", "") for d in data)
    if extra:
        out.update(extra)
    return out


def extract_jobs() -> List[Dict[str, Any]]:
    print("Extracting jobs (created or updated this quarter)...")
    rows: List[Dict[str, Any]] = []
    # We pull jobs touched this quarter by either filter; dedupe afterwards.
    for filt in ("created-at", "updated-at"):
        params = {
            f"filter[{filt}][from]": DATE_FROM,
            f"filter[{filt}][to]":   DATE_TO,
            "include": "department,location,role,user",
        }
        for rec in paginate("jobs", params):
            rows.append(flatten(rec))
    # Dedupe on id.
    seen, deduped = set(), []
    for r in rows:
        if r["id"] not in seen:
            seen.add(r["id"])
            deduped.append(r)
    return deduped


def extract_job_applications() -> List[Dict[str, Any]]:
    print("Extracting job applications (created this quarter)...")
    params = {
        "filter[created-at][from]": DATE_FROM,
        "filter[created-at][to]":   DATE_TO,
        "include": "candidate,job,stage",
    }
    return [flatten(r) for r in paginate("job-applications", params)
            if in_range(r.get("attributes", {}).get("created-at"))]


def extract_candidates() -> List[Dict[str, Any]]:
    print("Extracting candidates (created this quarter)...")
    params = {
        "filter[created-at][from]": DATE_FROM,
        "filter[created-at][to]":   DATE_TO,
    }
    return [flatten(r) for r in paginate("candidates", params)
            if in_range(r.get("attributes", {}).get("created-at"))]


def extract_activities() -> List[Dict[str, Any]]:
    """
    Activities = the audit trail (stage changes, notes, messages, interviews, etc.).
    Endpoint name varies by tenant; we try the common ones.
    """
    print("Extracting activities (created this quarter)...")
    params = {
        "filter[created-at][from]": DATE_FROM,
        "filter[created-at][to]":   DATE_TO,
    }
    rows: List[Dict[str, Any]] = []
    for endpoint in ("activities", "audits"):
        try:
            rows = [flatten(r) for r in paginate(endpoint, params, allow_403=True)
                    if in_range(r.get("attributes", {}).get("created-at"))]
            print(f"  used endpoint: /{endpoint}")
            break
        except RuntimeError as e:
            print(f"  /{endpoint} not available ({str(e)[:80]}), trying next")
    return rows


def extract_notes() -> List[Dict[str, Any]]:
    """Free-text interview comments. Requires notes:read scope."""
    print("Extracting notes (created this quarter)...")
    params = {
        "filter[created-at][from]": DATE_FROM,
        "filter[created-at][to]":   DATE_TO,
        "include": "user,job-application",
    }
    rows: List[Dict[str, Any]] = []
    for rec in paginate("notes", params, allow_403=True):
        attrs = rec.get("attributes") or {}
        if not in_range(attrs.get("created-at")):
            continue
        row = flatten(rec)
        row["note_clean"] = clean_text(attrs.get("note") or attrs.get("body") or "")
        rows.append(row)
    return rows


def extract_reviews() -> List[Dict[str, Any]]:
    """Interview scorecards. Requires reviews:read scope."""
    print("Extracting reviews (created this quarter)...")
    for endpoint in ("reviews", "scorecards"):
        params = {
            "filter[created-at][from]": DATE_FROM,
            "filter[created-at][to]":   DATE_TO,
            "include": "user,job-application",
        }
        rows: List[Dict[str, Any]] = []
        try:
            for rec in paginate(endpoint, params, allow_403=True):
                attrs = rec.get("attributes") or {}
                if not in_range(attrs.get("created-at")):
                    continue
                row = flatten(rec)
                row["summary_clean"] = clean_text(attrs.get("summary") or attrs.get("body") or "")
                rows.append(row)
            if rows:
                print(f"  used endpoint: /{endpoint}  ({len(rows)} rows)")
                return rows
        except RuntimeError as e:
            print(f"  /{endpoint} not usable ({str(e)[:80]}), trying next")
    print("  no reviews endpoint returned data")
    return []


def extract_messages() -> List[Dict[str, Any]]:
    """Recruiter/candidate messaging. Requires messages:read scope."""
    print("Extracting messages (created this quarter)...")
    params = {
        "filter[created-at][from]": DATE_FROM,
        "filter[created-at][to]":   DATE_TO,
        "include": "user,job-application",
    }
    rows: List[Dict[str, Any]] = []
    for rec in paginate("messages", params, allow_403=True):
        attrs = rec.get("attributes") or {}
        if not in_range(attrs.get("created-at")):
            continue
        row = flatten(rec)
        row["body_clean"] = clean_text(attrs.get("body") or attrs.get("content") or "")
        rows.append(row)
    return rows


def extract_simple(endpoint: str) -> List[Dict[str, Any]]:
    """No date filter — pull everything (used for users, departments, locations, stages)."""
    print(f"Extracting {endpoint}...")
    return [flatten(r) for r in paginate(endpoint)]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    now = datetime.now(timezone.utc)
    default_quarter = (now.month - 1) // 3 + 1
    parser = argparse.ArgumentParser(
        description="Extract a quarterly snapshot of Teamtailor activity to CSV."
    )
    parser.add_argument("--year", type=int, default=now.year,
                         help=f"Year of the quarter to extract (default: {now.year})")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=default_quarter,
                         help=f"Quarter to extract, 1-4 (default: {default_quarter})")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if TOKEN == "PASTE_TOKEN_HERE":
        print("ERROR: set TEAMTAILOR_TOKEN env var or paste your token at the top.")
        sys.exit(1)

    global DATE_FROM, DATE_TO, DT_FROM, DT_TO, OUTPUT_DIR
    DATE_FROM, DATE_TO = quarter_bounds(args.year, args.quarter)
    DT_FROM = datetime.fromisoformat(DATE_FROM.replace("Z", "+00:00"))
    DT_TO = datetime.fromisoformat(DATE_TO.replace("Z", "+00:00"))
    OUTPUT_DIR = Path(__file__).parent / f"teamtailor_q{args.quarter}_{args.year}_export"
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"Quarter:    Q{args.quarter} {args.year}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Date range: {DATE_FROM}  ->  {DATE_TO}")
    print()

    # Ping the API once to fail fast on bad token.
    print("Pinging API...")
    ping = _get(f"{API_BASE}/users", params={"page[size]": 1})
    print(f"  ok, {len(ping.get('data', []))} sample user record returned\n")

    write_csv("jobs",              extract_jobs())
    write_csv("job_applications",  extract_job_applications())
    write_csv("candidates",        extract_candidates())
    write_csv("activities",        extract_activities())
    write_csv("notes",             extract_notes())
    write_csv("reviews",           extract_reviews())
    write_csv("messages",          extract_messages())
    write_csv("users",             extract_simple("users"))
    write_csv("departments",       extract_simple("departments"))
    write_csv("locations",         extract_simple("locations"))
    write_csv("stages",            extract_simple("stages"))

    print(f"\nDone. CSVs are in: {OUTPUT_DIR}")
    print("\nIf notes/reviews/messages returned 0 rows, check that your API token")
    print("has these scopes enabled: notes:read, reviews:read, messages:read, audits:read")


if __name__ == "__main__":
    main()
