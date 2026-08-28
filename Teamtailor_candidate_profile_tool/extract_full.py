"""
Full-base Teamtailor extractor
===============================

Pulls the ENTIRE candidate base (not a quarter slice) plus job-applications,
jobs, and stages, so a candidate-profile tool can be built on top of it.

Reuses the pagination/retry logic from
../Teamtailor_quaterly_extraction/teamtailor_extract.py, just without the
created-at date filter.

Usage
-----
    export TEAMTAILOR_TOKEN="your-admin-token"
    python extract_full.py
"""

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "Teamtailor_quaterly_extraction" / ".env")

TOKEN = os.environ.get("TEAMTAILOR_TOKEN") or "PASTE_TOKEN_HERE"
API_BASE = "https://api.teamtailor.com/v1"
API_VERSION = "20240404"
PAGE_SIZE = 30

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Token token={TOKEN}",
    "X-Api-Version": API_VERSION,
    "Accept": "application/vnd.api+json",
    "Content-Type": "application/vnd.api+json",
})

OUTPUT_DIR = Path(__file__).parent / "export"


def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    for attempt in range(6):
        resp = SESSION.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "5"))
            print(f"  rate-limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  {resp.status_code} from server, retrying in {wait}s")
            time.sleep(wait)
            continue
        if not resp.ok:
            raise RuntimeError(f"GET {url} -> {resp.status_code}\n{resp.text[:500]}")
        return resp.json()
    raise RuntimeError(f"GET {url} failed after retries")


def paginate(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
    url = f"{API_BASE}/{endpoint}"
    params = dict(params or {})
    params.setdefault("page[size]", PAGE_SIZE)

    page_idx = 0
    while url:
        page_idx += 1
        page = _get(url, params=params if page_idx == 1 else None)
        for record in page.get("data", []):
            yield record
        url = page.get("links", {}).get("next")
        params = None
        if url and page_idx % 20 == 0:
            print(f"  page {page_idx} done, fetching next...")


def flatten(record: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"id": record.get("id"), "type": record.get("type")}
    out.update(record.get("attributes", {}) or {})
    rels = record.get("relationships", {}) or {}
    for rel_name, rel_val in rels.items():
        data = (rel_val or {}).get("data")
        if isinstance(data, dict):
            out[f"rel_{rel_name}_id"] = data.get("id")
        elif isinstance(data, list):
            out[f"rel_{rel_name}_ids"] = ",".join(d.get("id", "") for d in data)
    return out


def _scalar(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


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


def main() -> None:
    if TOKEN == "PASTE_TOKEN_HERE":
        raise SystemExit("ERROR: set TEAMTAILOR_TOKEN env var.")

    OUTPUT_DIR.mkdir(exist_ok=True)

    print("Pinging API...")
    ping = _get(f"{API_BASE}/users", params={"page[size]": 1})
    print(f"  ok, {len(ping.get('data', []))} sample user record returned\n")

    print("Extracting ALL candidates (no date filter)...")
    write_csv("candidates", [flatten(r) for r in paginate("candidates")])

    print("Extracting ALL job applications...")
    write_csv("job_applications", [flatten(r) for r in paginate(
        "job-applications", {"include": "candidate,job,stage"})])

    print("Extracting ALL jobs...")
    write_csv("jobs", [flatten(r) for r in paginate(
        "jobs", {"include": "department,location,role", "filter[status]": "all"})])

    print("Extracting ALL stages...")
    write_csv("stages", [flatten(r) for r in paginate("stages")])

    print(f"\nDone. CSVs are in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
