"""
Make module ("Build final summary") — code:ExecuteCode, Python
Scenario: TeamTailor-report — Final Summary V2 (diane) — new scenario, People folder in Make.

Runs once per role, triggered when its Data Store 1 (Roles Registry) record's
`status` flips to "filled". By the time this module runs, the scenario has
already:
  1. DS1: Watch Records → fired with the role's record (job_id, job_title,
     channel, date_added, date_filled)
  2. Filter → status == "filled"
  3. DS2: Search Records, filter job_id = <trigger job_id> → every weekly
     snapshot saved for this role by Scenario 1 (see
     02_generate_weekly_report_v2.py's `result` dict — that's the shape of
     each row)

This module takes those two inputs and builds the final Slack message: a
thank-you to the hiring team with a celebration emoji, who got hired, how
many weeks the process ran, time to fill, and a simple final headcount
overview (confirmed as "simple" — no per-stage funnel breakdown, since DS2
doesn't store per-stage counts, only the totals below).

Local standalone version: role fields come from env vars, snapshot rows from
a local JSON file (SNAPSHOTS_JSON) — in Make these are the DS1 record fields
and the DS2 "Search Records" module's {{<step>.array}} output instead.
"""
import json
import os
from datetime import date

from dotenv import load_dotenv

load_dotenv()

# In Make: DS1 Watch Records trigger fields, e.g. {{1.job_id}}, {{1.job_title}}, ...
job_id = os.environ.get("JOB_ID", "7463157")
job_title = os.environ.get("JOB_TITLE", "Product Expert - Customer Care Agent")
channel = os.environ.get("SLACK_CHANNEL", "#product-expert-recruitment-2026")
date_added = os.environ.get("DATE_ADDED", "2026-06-01")
date_filled = os.environ.get("DATE_FILLED", date.today().isoformat())

# In Make: {{<DS2 Search Records step>.array}} — every weekly snapshot for this job_id
snapshots_path = os.environ.get("SNAPSHOTS_JSON", "sample_snapshots.json")
with open(snapshots_path, encoding="utf-8") as f:
    snapshots = json.load(f)

if not snapshots:
    raise ValueError(f"No Weekly Snapshots found in DS2 for job_id={job_id} — nothing to summarize.")

# Snapshots aren't guaranteed to arrive in order from a Data Store search.
snapshots.sort(key=lambda row: row["week_of"])
latest = snapshots[-1]

weeks_tracked = len(snapshots)
days_to_fill = (date.fromisoformat(date_filled) - date.fromisoformat(date_added)).days

hired_names = latest.get("hired_candidates", "").strip()
hired_line = hired_names if hired_names else "see TeamTailor for details"

total_all = latest["total_all"]
total_active = latest["total_active"]
total_rejected = latest["total_rejected"]
total_sourced = latest["total_sourced"]
total_inbound = latest["total_inbound"]


def conv(num, denom):
    return round((num / denom) * 100) if denom > 0 else 0


slack_message = f"""🎉 *Role Filled — {job_title}!*

Huge thanks to the whole hiring team — hiring managers, interviewers, and everyone who gave their time along the way. Great work! 🙌

Below is the cumulative result of that effort throughout the process.

*🏆 Hired:* {hired_line}
📅 Tracked over *{weeks_tracked}* week{"s" if weeks_tracked != 1 else ""}  |  ⏱️ Time to fill: *{days_to_fill} days*

*🔢 Final Overview*
- 👥 Total candidates: *{total_all}* | ✅ Active: *{total_active}* ({conv(total_active, total_all)}%) | ❌ Rejected: *{total_rejected}* ({conv(total_rejected, total_all)}%)
- 🎯 Sourced: *{total_sourced}* ({conv(total_sourced, total_all)}%) vs 📥 Inbound: *{total_inbound}* ({conv(total_inbound, total_all)}%)"""

result = {
    "slack_message": slack_message,
    "job_id": job_id,
    "channel": channel,
    "weeks_tracked": weeks_tracked,
    "days_to_fill": days_to_fill,
    "hired_candidates": hired_names,
}

if __name__ == "__main__":
    print(json.dumps(result, indent=2, ensure_ascii=False))
