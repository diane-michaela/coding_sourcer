"""
Make module 5 ("Generate report") — code:ExecuteCode, Python
Scenario: TeamTailor-report (diane) — https://eu1.make.com/67084/scenarios/5316616/edit

This is a local copy of the code running inside the Make scenario, adapted to
run standalone for testing/versioning:
  - job_id / channel come from sys.argv / hardcoded defaults instead of the
    Make iterator ({{40.job_id}}, {{40.channel}}).
  - API_TOKEN is read from .env (TEAMTAILOR_TOKEN) via python-dotenv instead
    of being hardcoded in the code, as it is in the live Make module.

To push a change back into Make: re-hardcode job_id/channel as {{40.job_id}}
/ {{40.channel}} pills and keep API_TOKEN as-is if you haven't yet moved the
token out of the code module (see README.md suggestion #1).
"""
import json
import os
import urllib.request
from datetime import date, datetime

from dotenv import load_dotenv

load_dotenv()

# In Make these are injected by the iterator: {{40.job_id}} / {{40.channel}}
job_id = os.environ.get("JOB_ID", "7463157")
channel = os.environ.get("SLACK_CHANNEL", "#product-expert-recruitment-2026")

API_TOKEN = os.environ.get("TEAMTAILOR_TOKEN") or "PASTE_TOKEN_HERE"
BASE_URL = f"https://api.teamtailor.com/v1/job-applications?include=candidate,stage&filter[job]={job_id}&page[size]=30"

STAGE_ORDER = [
    "Inbox",
    "Reviewing",
    "TA Screening",
    "Job-fit interview",
    "Use-case Interview",
    "Cultural-fit interview",
    "Offer",
    "Hired",
]
INTERVIEW_STAGES = ["Job-fit interview", "Use-case Interview", "Cultural-fit interview"]

HEADERS = {
    "Authorization": f"Token token={API_TOKEN}",
    "X-Api-Version": "20240904",
    "Accept": "application/vnd.api+json",
}


def make_request(url):
    req = urllib.request.Request(url)
    for key, value in HEADERS.items():
        req.add_header(key, value)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode())


# Fetch job title and creation date (used for accurate days-open count)
job_data = make_request(f"https://api.teamtailor.com/v1/jobs/{job_id}")
JOB_TITLE = job_data["data"]["attributes"]["title"]
job_created_at = job_data["data"]["attributes"].get("created-at", "")

# Fetch all paginated applications + deduplicate included items
all_applications, all_included, seen = [], [], set()
current_url = BASE_URL
while current_url:
    page = make_request(current_url)
    all_applications.extend(page["data"])
    for item in page.get("included", []):
        key = (item["type"], item["id"])
        if key not in seen:
            all_included.append(item)
            seen.add(key)
    current_url = page.get("links", {}).get("next")

# Build lookup maps
stage_map, candidate_map = {}, {}
for item in all_included:
    if item["type"] == "stages":
        stage_map[item["id"]] = item["attributes"]["name"]
    elif item["type"] == "candidates":
        first = item["attributes"].get("first-name") or ""
        last = item["attributes"].get("last-name") or ""
        candidate_map[item["id"]] = {
            "name": f"{first} {last}".strip(),
            "sourced": item["attributes"].get("sourced", False),
        }

# Counters
active = {s: 0 for s in STAGE_ORDER}
rejected = {s: 0 for s in STAGE_ORDER}
interview_candidates = {s: [] for s in INTERVIEW_STAGES}
total_sourced = 0

for app in all_applications:
    stage_id = (app["relationships"]["stage"]["data"] or {}).get("id")
    candidate_id = (app["relationships"]["candidate"]["data"] or {}).get("id")
    stage_name = stage_map.get(stage_id, "Unknown")
    is_rejected = bool(app["attributes"].get("rejected-at"))
    is_sourced = candidate_map.get(candidate_id, {}).get("sourced", False)

    if is_sourced:
        total_sourced += 1

    if stage_name in STAGE_ORDER:
        if is_rejected:
            rejected[stage_name] += 1
        else:
            active[stage_name] += 1

    if not is_rejected and stage_name in INTERVIEW_STAGES:
        name = candidate_map.get(candidate_id, {}).get("name", "")
        if name:
            interview_candidates[stage_name].append(name)

# Totals
total_active = sum(active.values())
total_rejected = sum(rejected.values())
total_all = total_active + total_rejected
total_inbound = total_all - total_sourced
in_review_active = active["Inbox"] + active["Reviewing"]
in_review_total = (
    active["Inbox"] + rejected["Inbox"]
    + active["Reviewing"] + rejected["Reviewing"]
)

# Days open
today = date.today()
if job_created_at:
    open_date = datetime.fromisoformat(job_created_at[:10]).date()
    days_open = (today - open_date).days
else:
    days_open = None

days_indicator = (
    "⚪" if days_open is None
    else "🟢" if days_open < 60
    else "🟠" if days_open < 90
    else "🔴"
)


def rs(s):
    idx = STAGE_ORDER.index(s)
    return sum(active[STAGE_ORDER[i]] + rejected[STAGE_ORDER[i]] for i in range(idx, len(STAGE_ORDER)))


def conv(num, denom):
    return round((num / denom) * 100) if denom > 0 else 0


def conv_decided(passed_to_next, rejected_at_stage):
    total = passed_to_next + rejected_at_stage
    return round((passed_to_next / total) * 100) if total > 0 else 0


def bar(pct):
    if not pct:
        return ""
    filled = round(pct / 10)
    return "█" * filled + "░" * (10 - filled)


pct_sourced = conv(total_sourced, total_all)
pct_inbound = conv(total_inbound, total_all)
pct_screening_to_jobfit = conv_decided(rs("Job-fit interview"), rejected["TA Screening"])
pct_jobfit_to_usecase = conv_decided(rs("Use-case Interview"), rejected["Job-fit interview"])
pct_usecase_to_culturafit = conv_decided(rs("Cultural-fit interview"), rejected["Use-case Interview"])
pct_culturafit_to_offer = conv_decided(rs("Offer"), rejected["Cultural-fit interview"])
pct_offer_to_hired = conv_decided(rs("Hired"), rejected["Offer"])
usecase_fail_rate = conv_decided(rejected["Use-case Interview"], rs("Cultural-fit interview"))

# Alerts
alerts = []
if days_open is not None and days_open >= 90:
    alerts.append(f"🚨 Role open for {days_open} days — urgent review needed")
elif days_open is not None and days_open >= 60:
    alerts.append(f"⚠️ Role open for {days_open} days — approaching critical delay")
if in_review_active > 20:
    alerts.append(
        f'⚠️ {in_review_active} candidates pending review '
        f'(Inbox: {active["Inbox"]}, Reviewing: {active["Reviewing"]})'
    )
if pct_screening_to_jobfit < 20 and rs("TA Screening") > 0:
    alerts.append(f"⚠️ Low pipeline quality: only {pct_screening_to_jobfit}% pass TA Screening")
if usecase_fail_rate > 60 and rs("Use-case Interview") > 0:
    alerts.append(
        f'⚠️ High Use-case fail rate: {usecase_fail_rate}% '
        f'({rejected["Use-case Interview"]} failed)'
    )
if rs("Offer") > 0 and pct_offer_to_hired < 80:
    alerts.append(f"⚠️ Offer acceptance below 80%: {pct_offer_to_hired}%")


# Pipeline snapshot lines — inline names for active interview stages
def pipeline_line(label, active_count, total_count, stage_key=None):
    names = interview_candidates.get(stage_key, []) if stage_key else []
    if names and active_count > 0:
        return f"- {label}: *{active_count} active*: {', '.join(names)} ({total_count} total)"
    else:
        return f"- {label}: *{active_count} active* ({total_count} total)"


pipeline_lines = [
    pipeline_line("📬 Inbox & Review", in_review_active, in_review_total),
    pipeline_line("TA Screening", active["TA Screening"], rs("TA Screening")),
    pipeline_line("Job-fit interview", active["Job-fit interview"], rs("Job-fit interview"), "Job-fit interview"),
    pipeline_line("Use-case Interview", active["Use-case Interview"], rs("Use-case Interview"), "Use-case Interview"),
    pipeline_line("Cultural-fit interview", active["Cultural-fit interview"], rs("Cultural-fit interview"), "Cultural-fit interview"),
    pipeline_line("Offer", active["Offer"], rs("Offer")),
    f"- Hired: *{active['Hired']}* 🎉",
]
pipeline_block = "\n".join(pipeline_lines)

# Funnel section
funnel_lines = [
    f"- TA Screening → Job-fit: {pct_screening_to_jobfit}% {bar(pct_screening_to_jobfit)}",
    f"- Job-fit → Use-case: {pct_jobfit_to_usecase}% {bar(pct_jobfit_to_usecase)}",
    f"- Use-case → Cultural-fit: {pct_usecase_to_culturafit}% {bar(pct_usecase_to_culturafit)}",
]
if rs("Cultural-fit interview") > 0:
    funnel_lines.append(f"- Cultural-fit → Offer: {pct_culturafit_to_offer}% {bar(pct_culturafit_to_offer)}")
if rs("Offer") > 0:
    funnel_lines.append(f"- Offer → Hired: {pct_offer_to_hired}% {bar(pct_offer_to_hired)}")

today_str = today.strftime("%B %d, %Y")
days_display = days_open if days_open is not None else "N/A"

funnel_block = "\n".join(funnel_lines)
alerts_block = "\n".join(alerts) if alerts else "✅ No alerts"

slack_message = f"""📊 *Weekly Hiring Report — {JOB_TITLE}*
📅 {today_str}  |  {days_indicator} Open since: {days_display} days

*🔢 Overview*
- 👥 Total: *{total_all}* | ✅ Active: *{total_active}* ({conv(total_active, total_all)}%) | ❌ Rejected: *{total_rejected}* ({conv(total_rejected, total_all)}%)
- 🎯 Sourced: *{total_sourced}* ({pct_sourced}%) vs 📥 Inbound: *{total_inbound}* ({pct_inbound}%)

*📍 Pipeline Snapshot*
{pipeline_block}

*📈 Conversion Funnel (HM stages)*
{funnel_block}

*🚨 Alerts*
{alerts_block}"""

result = {
    "slack_message": slack_message,
    "job_title": JOB_TITLE,
    "channel": channel,
    "total_all": total_all,
    "total_active": total_active,
    "total_rejected": total_rejected,
    "total_sourced": total_sourced,
    "days_open": days_display,
    "alerts": " | ".join(alerts) if alerts else "No alerts",
}

if __name__ == "__main__":
    print(json.dumps(result, indent=2, ensure_ascii=False))
