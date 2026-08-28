"""
sync_to_airtable.py — Diff dashboard_data.json against the last known Airtable
state, producing small, safe-to-import CSV deltas instead of a full reload.

Run this AFTER refreshing the pipeline (extract_full.py -> tag_candidates.py
-> build_dashboard.py), so dashboard_data.json reflects Teamtailor's current
state — including candidates removed since the last run (GDPR erasure,
account merge, or otherwise). extract_full.py always pulls the full
candidate list fresh with no date filter, so a removed candidate simply
won't be in the next dashboard_data.json — this script is what turns that
absence into something Airtable can act on.

This script never writes to Airtable directly — CSV import stays manual,
per this workspace's convention (see root CLAUDE.md: batches of <=1000 rows,
generated as CSV rather than pushed via API).

First run (no prior state file): produces the full backfill, chunked into
batches of SAFE_ROW_LIMIT rows.

Every run after: produces only the delta since the last run — added,
changed (role/stage/tags), and removed candidates. A week's delta is
normally tens to low hundreds of rows, well under the batch limit; the
chunking logic below is a guardrail for the unusual case, not something
that should trigger in steady state.

Usage:
    python3 sync_to_airtable.py
"""

import csv
import json
from pathlib import Path
from datetime import datetime, timezone

DASHBOARD_DATA = Path(__file__).parent / "dashboard_data.json"
STATE_FILE = Path(__file__).parent / "airtable_sync_state.json"
DELTA_DIR = Path(__file__).parent / "airtable_deltas"

SAFE_ROW_LIMIT = 900  # stay comfortably under Airtable's ~1,000-row import ceiling

FIELDS = [
    "id", "name", "linkedin_url", "profile_url",
    "latest_role_applied", "latest_stage",
    "suggested_title", "keywords", "seniority", "location",
]


def load_dashboard():
    with DASHBOARD_DATA.open() as f:
        return json.load(f)


def to_row(candidate):
    row = {k: candidate.get(k, "") for k in FIELDS}
    if isinstance(row["keywords"], list):
        row["keywords"] = ", ".join(row["keywords"])
    return row


def load_state():
    if not STATE_FILE.exists():
        return {}
    with STATE_FILE.open() as f:
        return json.load(f)


def save_state(current_rows):
    STATE_FILE.write_text(json.dumps(current_rows, indent=2, ensure_ascii=False))


def write_batched_csv(rows, label, stamp):
    if not rows:
        print(f"  {label}: nothing to write")
        return
    DELTA_DIR.mkdir(exist_ok=True)
    batches = [rows[i:i + SAFE_ROW_LIMIT] for i in range(0, len(rows), SAFE_ROW_LIMIT)]
    for i, batch in enumerate(batches, start=1):
        suffix = f"_{i}of{len(batches)}" if len(batches) > 1 else ""
        path = DELTA_DIR / f"{stamp}_{label}{suffix}.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            for r in batch:
                w.writerow(r)
        print(f"  -> {path}  ({len(batch)} rows)")
    if len(batches) > 1:
        print(f"  NOTE: {label} split into {len(batches)} batches — import each "
              f"<={SAFE_ROW_LIMIT}-row file separately, never combine them into one import.")


def write_removed(ids, stamp):
    if not ids:
        print("  removed: nothing to write")
        return
    DELTA_DIR.mkdir(exist_ok=True)
    path = DELTA_DIR / f"{stamp}_removed_ids.txt"
    path.write_text("\n".join(sorted(ids)))
    print(f"  -> {path}  ({len(ids)} candidate ids no longer in Teamtailor — "
          f"delete these records from Airtable, likely GDPR erasure or account merge)")


def main():
    print("Loading current dashboard data...")
    candidates = load_dashboard()
    current = {c["id"]: to_row(c) for c in candidates}

    previous = load_state()
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    added = [current[cid] for cid in current if cid not in previous]
    removed = [cid for cid in previous if cid not in current]
    changed = [
        current[cid] for cid in current
        if cid in previous and current[cid] != previous[cid]
    ]

    print("\nDelta since last sync:")
    print(f"  added:   {len(added)}")
    print(f"  changed: {len(changed)}")
    print(f"  removed: {len(removed)}")

    if not previous:
        print("\nNo prior state found — this is the initial backfill.")

    write_batched_csv(added, "add", stamp)
    write_batched_csv(changed, "update", stamp)
    write_removed(removed, stamp)

    save_state(current)
    print(f"\nState saved to {STATE_FILE.name} — import the CSVs above into the "
          f"teamtailor-candidates Airtable table, then you're in sync until the next run.")


if __name__ == "__main__":
    main()
