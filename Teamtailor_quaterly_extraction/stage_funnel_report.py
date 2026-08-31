"""
Per-role interview funnel + outcome + days-open report.

Runs on top of a quarter's export from teamtailor_extract.py
(job_applications.csv, jobs.csv). For each role, computes:

  - Total applications
  - Total reached: TAS/Screening, HM/Job-fit interview, Live coding/Test,
    Culture fit -- computed against each job's OWN real pipeline order,
    pulled live from Teamtailor's /jobs/{id}/stages. NOT one order assumed
    across every role: some roles combine two categories into one stage
    (e.g. "Job-fit interview + test") or skip a category entirely, and using
    a single global order would fabricate "reached" credit for stages a
    role's pipeline never had.
  - Outcome: "Hired" if the role has an active (non-rejected) application
    currently at the Offered stage or beyond, else N/A.
  - Days open: role creation date -> the date an application entered the
    Hired stage (if any), else the job's archived date, else today if still
    open. Color-coded: green 1-45d, orange 46-89d, red 90+d.

Role identity / merges are QUARTER-SPECIFIC -- edit JOB_TITLES and
EXCLUDED_JOB_IDS by hand each quarter (e.g. combining two postings that are
really the same req reposted, or dropping test jobs). Nothing else in this
script assumes a merge will look the same next time.

Usage
-----
    export TEAMTAILOR_TOKEN=...   # same token as teamtailor_extract.py,
                                   # or reuse the .env in this folder
    python stage_funnel_report.py --year 2026 --quarter 2
"""
from __future__ import annotations

import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

FOLDER = Path(__file__).parent

# ---------------------------------------------------------------------------
# QUARTER-SPECIFIC CONFIG -- edit these by hand every quarter.
# ---------------------------------------------------------------------------

# Job IDs to drop entirely from the report (test/dummy postings).
EXCLUDED_JOB_IDS: set[str] = {"7621740"}  # "Test job" (Q2 2026)

# Job title -> display label. Map every job ID referenced by this quarter's
# job_applications.csv here. Give two IDs the SAME label to merge them into
# one row (e.g. a role that was reposted under a new job ID) -- this is the
# manual step; nothing below infers merges automatically.
JOB_TITLES: dict[str, str] = {
    "7075130": "Analytics Engineer",
    "7223186": "Growth Marketer",
    "7463157": "Product Expert - Customer Care Agent",
    "7497394": "SEO & AI Search Specialist",
    "7804520": "Senior Backend & Platform Engineer",
    "7238913": "Senior Full-Stack Engineer (React-focused)",  # fused with 7652367
    "7652367": "Senior Full-Stack Engineer (React-focused)",  # fused with 7238913
    "7804451": "Senior Full-Stack Engineer",  # fused with 7743636
    "7743636": "Senior Full-Stack Engineer",  # fused with 7804451
    "6295196": "Staff ML Engineer",  # fused with 7696182
    "7696182": "Staff ML Engineer",  # fused with 6295196
    "7816285": "Senior ML Engineer - AI Platform & Agents (#7816285)",
}

# ---------------------------------------------------------------------------
# Stable logic -- shouldn't need changes quarter to quarter.
# ---------------------------------------------------------------------------

BUCKETS = ["screening", "hm_interview", "live_coding_test", "culture_fit"]
BUCKET_LABELS = {
    "screening": "Total TAS Screening",
    "hm_interview": "Total HM Interview",
    "live_coding_test": "Total Live coding/Test",
    "culture_fit": "Total Culture fit",
}

SCREENING_RE = re.compile(r"screening", re.I)
HM_RE = re.compile(r"job[- ]?fit", re.I)
LIVE_CODING_RE = re.compile(r"use[- ]?case|home assignment|aptitude test|technical interview", re.I)
CULTURE_RE = re.compile(r"cultur", re.I)
OFFER_RE = re.compile(r"^offer", re.I)
HIRED_RE = re.compile(r"^hired", re.I)

DAYS_OPEN_THRESHOLDS = (45, 89)  # <=45 green, <=89 orange, else red


def matched_buckets(name: str) -> set[str]:
    """A stage can legitimately match >1 bucket -- combined stages like
    'Job-fit interview + test' cover both HM interview and live coding."""
    out = set()
    if SCREENING_RE.search(name):
        out.add("screening")
    if HM_RE.search(name):
        out.add("hm_interview")
    if LIVE_CODING_RE.search(name):
        out.add("live_coding_test")
    if CULTURE_RE.search(name):
        out.add("culture_fit")
    return out


def load_token() -> str:
    token = os.environ.get("TEAMTAILOR_TOKEN")
    if token:
        return token
    env_path = FOLDER / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("TEAMTAILOR_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("ERROR: set TEAMTAILOR_TOKEN env var or add it to .env in this folder.")


def api_get(session: requests.Session, path: str) -> dict:
    r = session.get(f"https://api.teamtailor.com/v1/{path}", timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_job_pipeline(session: requests.Session, job_id: str) -> dict[str, tuple[str, int]]:
    """stage_id -> (name, row_order) for one job's own pipeline."""
    data = api_get(session, f"jobs/{job_id}/stages")
    return {
        s["id"]: (s["attributes"]["name"], s["attributes"].get("row-order", 0))
        for s in data.get("data", [])
    }


def fetch_job_meta(session: requests.Session, job_id: str) -> dict:
    data = api_get(session, f"jobs/{job_id}")
    attrs = data["data"]["attributes"]
    return {
        "created_at": attrs.get("created-at"),
        "updated_at": attrs.get("updated-at"),
        "status": attrs.get("status"),
    }


def days_open_indicator(days: int) -> str:
    green_max, orange_max = DAYS_OPEN_THRESHOLDS
    if days <= green_max:
        return "\U0001F7E2"  # green circle
    if days <= orange_max:
        return "\U0001F7E0"  # orange circle
    return "\U0001F534"  # red circle


def main() -> None:
    now = datetime.now(timezone.utc)
    default_quarter = (now.month - 1) // 3 + 1
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=now.year)
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=default_quarter)
    args = parser.parse_args()

    export_dir = FOLDER / f"teamtailor_q{args.quarter}_{args.year}_export"
    if not (export_dir / "job_applications.csv").exists():
        raise SystemExit(
            f"ERROR: {export_dir}/job_applications.csv not found. "
            f"Run teamtailor_extract.py --year {args.year} --quarter {args.quarter} first."
        )

    token = load_token()
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Token token={token}",
        "X-Api-Version": "20240404",
        "Accept": "application/vnd.api+json",
    })

    job_ids = sorted(set(JOB_TITLES) - EXCLUDED_JOB_IDS)
    print(f"Fetching pipeline + metadata for {len(job_ids)} jobs from the Teamtailor API...")
    pipelines = {jid: fetch_job_pipeline(session, jid) for jid in job_ids}
    job_meta = {jid: fetch_job_meta(session, jid) for jid in job_ids}

    # Per-job entry row-order for each bucket (None = pipeline has no such stage)
    entry_order: dict[str, dict[str, int | None]] = {}
    offer_order: dict[str, int | None] = {}
    for jid, stages in pipelines.items():
        entry_order[jid] = {}
        for b in BUCKETS:
            matches = [row_order for (name, row_order) in stages.values() if b in matched_buckets(name)]
            entry_order[jid][b] = min(matches) if matches else None
        offer_matches = [row_order for (name, row_order) in stages.values() if OFFER_RE.search(name)]
        offer_order[jid] = min(offer_matches) if offer_matches else None

    per_role_counts = defaultdict(lambda: {b: 0 for b in BUCKETS})
    per_role_na = defaultdict(lambda: {b: False for b in BUCKETS})
    per_role_total = defaultdict(int)
    per_role_hired_outcome = defaultdict(bool)
    per_role_hired_at: dict[str, str] = {}  # earliest changed-stage-at into Hired, per role
    unresolved_stage = defaultdict(int)

    with open(export_dir / "job_applications.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            job_id = row["rel_job_id"]
            if job_id in EXCLUDED_JOB_IDS or job_id not in JOB_TITLES:
                continue
            stage_id = row["rel_stage_id"]
            role = JOB_TITLES[job_id]
            per_role_total[role] += 1

            stage_info = pipelines.get(job_id, {}).get(stage_id)
            if stage_info is None:
                unresolved_stage[role] += 1
                continue
            current_name, current_order = stage_info
            is_active = not row["rejected-at"]

            if is_active and offer_order[job_id] is not None and current_order >= offer_order[job_id]:
                per_role_hired_outcome[role] = True

            if is_active and HIRED_RE.search(current_name) and row["changed-stage-at"]:
                prev = per_role_hired_at.get(role)
                if prev is None or row["changed-stage-at"] < prev:
                    per_role_hired_at[role] = row["changed-stage-at"]

            for b in BUCKETS:
                if entry_order[job_id][b] is None:
                    per_role_na[role][b] = True
                    continue
                if current_order >= entry_order[job_id][b]:
                    per_role_counts[role][b] += 1

    # Days open: pick, per role, the LATEST-created job ID among the ones
    # mapped to it (the manual-merge rule), then use hire date if we found
    # one, else that job's archived date, else today.
    role_latest_job: dict[str, str] = {}
    for jid, role in JOB_TITLES.items():
        if jid in EXCLUDED_JOB_IDS:
            continue
        created = job_meta[jid]["created_at"]
        if role not in role_latest_job or created > job_meta[role_latest_job[role]]["created_at"]:
            role_latest_job[role] = jid

    per_role_days_open: dict[str, int] = {}
    for role, jid in role_latest_job.items():
        created = datetime.fromisoformat(job_meta[jid]["created_at"])
        hired_at = per_role_hired_at.get(role)
        if hired_at:
            end = datetime.fromisoformat(hired_at)
        elif job_meta[jid]["status"] == "open":
            end = now
        else:
            end = datetime.fromisoformat(job_meta[jid]["updated_at"])
        per_role_days_open[role] = (end - created).days

    # --- write CSV ---
    out_path = export_dir / "stage_funnel_report.csv"
    all_roles = sorted(per_role_total)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["role", "total_applications"] + [BUCKET_LABELS[b] for b in BUCKETS]
            + ["outcome", "days_open", "unresolved_stage"]
        )
        totals = {b: 0 for b in BUCKETS}
        total_apps = 0
        for role in all_roles:
            row_vals = []
            for b in BUCKETS:
                v = "N/A" if per_role_na[role][b] and per_role_counts[role][b] == 0 else per_role_counts[role][b]
                row_vals.append(v)
                if isinstance(v, int):
                    totals[b] += v
            outcome = "Hired" if per_role_hired_outcome[role] else "N/A"
            days = per_role_days_open.get(role)
            days_str = f"{days} {days_open_indicator(days)}" if days is not None else "N/A"
            total_apps += per_role_total[role]
            w.writerow([role, per_role_total[role]] + row_vals + [outcome, days_str, unresolved_stage[role]])
        w.writerow(["TOTAL (all roles)", total_apps] + [totals[b] for b in BUCKETS] + ["-", "-", sum(unresolved_stage.values())])

    print(f"\nWrote {out_path}\n")
    header = f"{'Role':50} {'Total':>6} " + " ".join(f"{BUCKET_LABELS[b][:16]:>16}" for b in BUCKETS) + f" {'Outcome':>10} {'Days open':>10}"
    print(header)
    for role in all_roles:
        row_vals = []
        for b in BUCKETS:
            v = "N/A" if per_role_na[role][b] and per_role_counts[role][b] == 0 else per_role_counts[role][b]
            row_vals.append(str(v))
        outcome = "Hired" if per_role_hired_outcome[role] else "N/A"
        days = per_role_days_open.get(role)
        days_str = f"{days} {days_open_indicator(days)}" if days is not None else "N/A"
        print(f"{role[:50]:50} {per_role_total[role]:>6} " + " ".join(f"{v:>16}" for v in row_vals) + f" {outcome:>10} {days_str:>10}")


if __name__ == "__main__":
    main()
