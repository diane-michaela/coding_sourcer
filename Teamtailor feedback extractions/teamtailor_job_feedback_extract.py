"""
Teamtailor — interview notes + reviews extractor for ONE job
============================================================

Pulls all `/notes` and `/reviews` attached to applications/candidates for a
specific job, excluding entries authored by a specific user. Writes the text
to CSV so it can be synthesized.

Usage
-----
    pip install requests
    export TEAMTAILOR_TOKEN="your-admin-token"
    python teamtailor_job_feedback_extract.py

Notes
-----
* JOB_ID and EXCLUDE_AUTHOR_EMAIL are configured at the top.
* Pulls applications for the job (paginated), then for each application:
    - GET /job-applications/{id}/notes
    - GET /job-applications/{id}/reviews   (if endpoint enabled on your tenant)
* Falls back to /candidates/{id}/notes if the application-scoped path 404s.
* If your token lacks `notes:read` or `reviews:read` you'll see 403s — that's
  the signal to widen the scopes in TeamTailor admin.
* The exclude is matched on the note's `user.email` field, not display name.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

# --- config ------------------------------------------------------------------

TOKEN = os.environ.get("TEAMTAILOR_TOKEN") or "PASTE_TOKEN_HERE"
JOB_ID = "7238913"                                # Senior Front End Engineer
EXCLUDE_AUTHOR_EMAIL = "mateja.jokovic.ext@thephantomcompany.com"

API_BASE = "https://api.teamtailor.com/v1"
API_VERSION = "20240404"
PAGE_SIZE = 30

OUTPUT_DIR = Path(__file__).parent / f"job_{JOB_ID}_feedback"
OUTPUT_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Token token={TOKEN}",
    "X-Api-Version": API_VERSION,
    "Accept": "application/vnd.api+json",
    "Content-Type": "application/vnd.api+json",
})

# --- http --------------------------------------------------------------------

def _get(url: str, params: Optional[Dict[str, Any]] = None,
         allow_404: bool = False) -> Optional[Dict[str, Any]]:
    for attempt in range(6):
        r = SESSION.get(url, params=params, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            print(f"  rate-limited, sleeping {wait}s")
            time.sleep(wait); continue
        if r.status_code == 404 and allow_404:
            return None
        if r.status_code == 403:
            print(f"  403 on {url} — token likely missing scope")
            return None
        if r.status_code >= 500:
            time.sleep(2 ** attempt); continue
        if not r.ok:
            raise RuntimeError(f"GET {url} -> {r.status_code}\n{r.text[:400]}")
        return r.json()
    raise RuntimeError(f"GET {url} failed after retries")


def paginate(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
    url = f"{API_BASE}/{endpoint}"
    p = dict(params or {})
    p.setdefault("page[size]", PAGE_SIZE)
    page_idx = 0
    while url:
        page_idx += 1
        page = _get(url, params=p if page_idx == 1 else None)
        if not page:
            break
        for rec in page.get("data", []):
            yield rec
        url = page.get("links", {}).get("next")
        p = None

# --- main --------------------------------------------------------------------

def author_email(rec: Dict[str, Any], included_index: Dict[str, Dict[str, Any]]) -> str:
    """Resolve the author email from a note/review record using included data."""
    rels = (rec.get("relationships") or {})
    user_rel = (rels.get("user") or rels.get("author") or {}).get("data")
    if not user_rel:
        return ""
    key = f"{user_rel.get('type')}:{user_rel.get('id')}"
    user = included_index.get(key)
    if not user:
        return ""
    return (user.get("attributes") or {}).get("email", "")


def fetch_with_user(endpoint: str) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """GET an endpoint with ?include=user and return data + included index."""
    params = {"include": "user", "page[size]": PAGE_SIZE}
    url = f"{API_BASE}/{endpoint}"
    out: List[Dict[str, Any]] = []
    included: Dict[str, Dict[str, Any]] = {}
    page_idx = 0
    while url:
        page_idx += 1
        page = _get(url, params=params if page_idx == 1 else None, allow_404=True)
        if not page:
            break
        for rec in page.get("data", []):
            out.append(rec)
        for inc in page.get("included", []):
            included[f"{inc.get('type')}:{inc.get('id')}"] = inc
        url = page.get("links", {}).get("next")
        params = None
    return out, included


def main() -> None:
    if TOKEN == "PASTE_TOKEN_HERE":
        print("ERROR: set TEAMTAILOR_TOKEN env var or paste your token at the top.")
        sys.exit(1)

    print(f"Job: {JOB_ID}  | Excluding author: {EXCLUDE_AUTHOR_EMAIL}")
    print(f"Output dir: {OUTPUT_DIR}\n")

    # 1. Pull all applications for this job
    print("Fetching applications for the job...")
    apps = list(paginate("job-applications", {"filter[job][id]": JOB_ID}))
    print(f"  -> {len(apps)} applications\n")

    notes_rows: List[Dict[str, Any]] = []
    reviews_rows: List[Dict[str, Any]] = []

    # 2. For each application, pull notes + reviews
    for i, app in enumerate(apps, 1):
        app_id = app["id"]
        cand_rel = ((app.get("relationships") or {}).get("candidate") or {}).get("data") or {}
        candidate_id = cand_rel.get("id", "")
        stage_rel = ((app.get("relationships") or {}).get("stage") or {}).get("data") or {}
        stage_id = stage_rel.get("id", "")
        rejected_at = (app.get("attributes") or {}).get("rejected-at")

        print(f"[{i}/{len(apps)}] app {app_id}  cand {candidate_id}")

        # --- notes ---
        for endpoint in (f"job-applications/{app_id}/notes",
                         f"candidates/{candidate_id}/notes" if candidate_id else None):
            if not endpoint:
                continue
            recs, included = fetch_with_user(endpoint)
            if recs is None:
                continue
            for rec in recs:
                email = author_email(rec, included)
                if email.lower() == EXCLUDE_AUTHOR_EMAIL.lower():
                    continue
                attrs = rec.get("attributes") or {}
                notes_rows.append({
                    "application_id": app_id,
                    "candidate_id":   candidate_id,
                    "stage_id":       stage_id,
                    "rejected_at":    rejected_at,
                    "note_id":        rec.get("id"),
                    "author_email":   email,
                    "created_at":     attrs.get("created-at"),
                    "note":           attrs.get("note") or attrs.get("body") or "",
                })
            if recs:
                break  # got notes from one of the two endpoints, no need to retry

        # --- reviews / scorecards ---
        for endpoint in (f"job-applications/{app_id}/reviews",
                         f"job-applications/{app_id}/scorecards"):
            recs, included = fetch_with_user(endpoint)
            if not recs:
                continue
            for rec in recs:
                email = author_email(rec, included)
                if email.lower() == EXCLUDE_AUTHOR_EMAIL.lower():
                    continue
                attrs = rec.get("attributes") or {}
                reviews_rows.append({
                    "application_id": app_id,
                    "candidate_id":   candidate_id,
                    "stage_id":       stage_id,
                    "rejected_at":    rejected_at,
                    "review_id":      rec.get("id"),
                    "author_email":   email,
                    "created_at":     attrs.get("created-at"),
                    "score":          attrs.get("score") or attrs.get("rating"),
                    "summary":        attrs.get("summary") or "",
                    "answers":        json.dumps(attrs.get("answers") or attrs.get("responses") or {}, ensure_ascii=False),
                })

    # 3. Write CSVs
    def write(name: str, rows: List[Dict[str, Any]]) -> None:
        path = OUTPUT_DIR / f"{name}.csv"
        if not rows:
            path.write_text("")
            print(f"  -> {path}  (0 rows)")
            return
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"  -> {path}  ({len(rows)} rows)")

    print()
    write("notes",   notes_rows)
    write("reviews", reviews_rows)
    print(f"\nDone. Files in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
