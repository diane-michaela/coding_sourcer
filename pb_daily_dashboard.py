"""
pb_daily_dashboard.py — Posts a PhantomBuster pipeline status dashboard to Slack #hiring-test.

Usage:
    python3 pb_daily_dashboard.py

Requires PB_API_KEY and SLACK_WEBHOOK_URL in talent_radar/.env.
"""

import csv
import io
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "talent_radar" / ".env")

PB_KEY        = os.environ.get("PB_API_KEY", "")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")

PB_V1 = "https://api.phantombuster.com/api/v1"
PB_V2 = "https://api.phantombuster.com/api/v2"

ENRICHMENT_TOTAL = 1146   # Twitter/GitHub — technical profiles ranks 1–7

DAILY_RATES = {
    "3319486672296602": 30,   # Twitter/X URL Finder
    "1498492852256479": 20,   # GitHub User Search
}

PROFILE_SCRAPER_ID   = "5440919304796371"
WAVE2_ENRICHER_TOTAL = 9084
PROFILE_SCRAPER_RATE = 200

EMPLOYEES_EXPORT_ID = "824349506789425"
EMPLOYEES_S3_URL    = "https://phantombuster.s3.amazonaws.com/VLyWCsB92xw/4kVh4fayldhhYiUHf2MsHg/result.csv"

# Hardcoded result URLs (PhantomBuster uses custom filenames, not result.csv)
ENRICHMENT_S3_URLS = {
    "3319486672296602": "https://phantombuster.s3.amazonaws.com/VLyWCsB92xw/HvQuPPZMKdKCrKiiq8IaXw/enrichment-twitter-urls.csv",
    "1498492852256479": "https://phantombuster.s3.amazonaws.com/VLyWCsB92xw/wVbi9D6ZX0w3dfqlDVIk7Q/enrichment-github-search.csv",
}

WAVE_CSVS = {
    "wave1": Path("/Users/dianerocher/phantombuster-api/target_companies_wave1.csv"),
    "wave2": Path("/Users/dianerocher/phantombuster-api/target_companies_wave2.csv"),
    "wave3": Path("/Users/dianerocher/phantombuster-api/target_companies_wave3.csv"),
    "wave4": Path("/Users/dianerocher/phantombuster-api/target_companies_wave4.csv"),
}

# Auto-scheduled phantoms watched for gap detection (id -> label)
AUTO_PHANTOMS = {
    EMPLOYEES_EXPORT_ID: "Employees Export",
    PROFILE_SCRAPER_ID:  "Profile Scraper",
    "3319486672296602":  "Twitter/X URL Finder",
    "1498492852256479":  "GitHub User Search",
}


# ── API helpers ───────────────────────────────────────────────────────────────

def pb_headers():
    return {"X-Phantombuster-Key": PB_KEY}


def fetch_agents():
    r = requests.get(f"{PB_V1}/user", headers=pb_headers(), timeout=15)
    r.raise_for_status()
    return {str(a["id"]): a for a in r.json().get("data", {}).get("agents", [])}


def fetch_agent_v2(agent_id):
    r = requests.get(f"{PB_V2}/agents/fetch", headers=pb_headers(),
                     params={"id": agent_id}, timeout=15)
    return r.json()


def get_last_container(agent_id):
    r = requests.get(f"{PB_V1}/agent/{agent_id}/containers",
                     headers=pb_headers(), timeout=15)
    containers = r.json().get("data", [])
    return containers[0] if containers else {}


def count_csv_rows(url):
    try:
        r = requests.get(url, timeout=20)
        rows = list(csv.reader(io.StringIO(r.text)))
        return max(0, len(rows) - 1)
    except Exception:
        return 0


def s3_url_from_meta(meta):
    org = meta.get("orgS3Folder", "")
    s3  = meta.get("s3Folder", "")
    if org and s3:
        return f"https://phantombuster.s3.amazonaws.com/{org}/{s3}/result.csv"
    return None


# ── Formatting helpers ────────────────────────────────────────────────────────

def eta_date(done, total, rate):
    remaining = total - done
    if remaining <= 0:
        return None
    days = -(-remaining // rate)
    return datetime.now(timezone.utc) + timedelta(days=days)


def eta_str(done, total, rate):
    dt = eta_date(done, total, rate)
    return "done ✅" if dt is None else dt.strftime("%b %d")


def progress_bar(done, total):
    pct = int(done / total * 100) if total else 0
    return "█" * (pct // 10) + "░" * (10 - pct // 10), pct


def last_ran_label(ts_ms):
    if not ts_ms:
        return "never"
    h = int((datetime.now(timezone.utc).timestamp() * 1000 - ts_ms) / 3600000)
    return f"{h}h ago" if h < 24 else f"{h // 24}d ago"


# ── Pre-compute progress (one S3 fetch per phantom) ───────────────────────────

def compute_progress(meta_cache):
    """Returns dict aid -> {done, pct, bar, eta_dt, s3_url} for enrichment phantoms."""
    prog = {}

    for aid, rate in DAILY_RATES.items():
        s3   = ENRICHMENT_S3_URLS.get(aid) or s3_url_from_meta(meta_cache.get(aid, {}))
        done = count_csv_rows(s3) if s3 else 0
        bar, pct = progress_bar(done, ENRICHMENT_TOTAL)
        prog[aid] = {
            "done": done, "pct": pct, "bar": bar,
            "eta_dt": eta_date(done, ENRICHMENT_TOTAL, rate),
            "s3_url": s3,
        }

    s3   = s3_url_from_meta(meta_cache.get(PROFILE_SCRAPER_ID, {}))
    done = count_csv_rows(s3) if s3 else 0
    bar, pct = progress_bar(done, WAVE2_ENRICHER_TOTAL)
    prog[PROFILE_SCRAPER_ID] = {
        "done": done, "pct": pct, "bar": bar,
        "eta_dt": eta_date(done, WAVE2_ENRICHER_TOTAL, PROFILE_SCRAPER_RATE),
        "s3_url": s3,
    }

    return prog


# ── Section formatters ────────────────────────────────────────────────────────

def format_employees_export_section():
    waves   = {}
    for wave, path in WAVE_CSVS.items():
        if not path.exists():
            waves[wave] = set()
            continue
        with open(path) as f:
            waves[wave] = {
                row["companyUrl"].rstrip("/")
                for row in csv.DictReader(f)
                if row.get("companyUrl", "").strip()
            }

    try:
        r = requests.get(EMPLOYEES_S3_URL, timeout=20)
        scraped = set(row.get("query", "").rstrip("/")
                      for row in csv.DictReader(io.StringIO(r.text)) if row.get("query"))
    except Exception:
        scraped = set()

    lines = []
    for wave_key in ("wave1", "wave2", "wave3", "wave4"):
        companies = waves.get(wave_key, set())
        if not companies:
            continue
        done    = sum(1 for c in companies if c in scraped)
        missing = len(companies) - done
        if missing == 0:
            lines.append(f"  • {wave_key.capitalize()}: {done}/{len(companies)} ✅")
        else:
            lines.append(f"  • {wave_key.capitalize()}: {done}/{len(companies)} — *{missing} remaining*")

    # ETA line for the latest incomplete wave
    for wave_key in ("wave4", "wave3"):
        wn = waves.get(wave_key, set())
        wn_left = len(wn) - sum(1 for c in wn if c in scraped)
        if wn_left > 0:
            days = -(-wn_left // 10)
            finish = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%b %d")
            lines.append(f"  → {wave_key.capitalize()} ETA: *{finish}* ({wn_left} companies left, 10/day)")
            break
        elif wn:
            lines.append(f"  → {wave_key.capitalize()} complete ✅")
            break

    return lines


def format_scraper_section(prog, last_run_cache):
    p    = prog[PROFILE_SCRAPER_ID]
    lc   = last_run_cache.get(PROFILE_SCRAPER_ID, {})
    ran  = f" _(last ran {last_ran_label(lc.get('queueDate'))})_" if lc.get("queueDate") else ""
    lines = []

    if p["done"] >= WAVE2_ENRICHER_TOTAL:
        lines.append(f"  • Wave 2: ✅ done — {p['done']:,}/{WAVE2_ENRICHER_TOTAL:,}")
        lines.append(f"  • Wave 3: ⏸ waiting — run filter_and_prepare_enricher.py to start")
    else:
        finish = p["eta_dt"].strftime("%b %d") if p["eta_dt"] else "?"
        lines.append(f"  • Wave 2: `{p['bar']}` {p['done']:,}/{WAVE2_ENRICHER_TOTAL:,} ({p['pct']}%) · ETA *{finish}* _200/day_{ran}")
        lines.append(f"  • Wave 3: ⏸ waiting — starts ~{finish} after wave 2")

    return lines


def format_enrichment_section(agents, prog, last_run_cache):
    enrichment_ids = [
        ("3319486672296602", "Twitter/X URL Finder",   "daily 8:00 AM Paris"),
        ("2461522598615921", "Twitter/X Scraper",      "manual — needs URL Finder output"),
        ("1498492852256479", "GitHub User Search",     "daily 8:30 AM Paris"),
        ("3265537247176143", "GitHub Profile Scraper", "manual — needs Search output"),
    ]
    lines = []

    for aid, name, schedule in enrichment_ids:
        a    = agents.get(aid, {})
        lc   = last_run_cache.get(aid, {})
        rate = DAILY_RATES.get(aid)

        if a.get("runningContainers", 0) > 0:
            lines.append(f"  • *{name}*: ▶ running now _{schedule}_")
            continue

        if rate:
            p      = prog[aid]
            ran    = f" _(last ran {last_ran_label(lc.get('queueDate'))})_" if lc.get("queueDate") else ""
            finish = p["eta_dt"].strftime("%b %d") if p["eta_dt"] else "done ✅"
            if p["done"] == 0 and not p["s3_url"]:
                lines.append(f"  • {name}: ⚠️ not started — ETA {finish} if starts today _{schedule}_")
            elif p["done"] >= ENRICHMENT_TOTAL:
                lines.append(f"  • {name}: ✅ done — {p['done']:,}/{ENRICHMENT_TOTAL:,} _{schedule}_")
            else:
                lines.append(f"  • {name}: `{p['bar']}` {p['done']:,}/{ENRICHMENT_TOTAL:,} ({p['pct']}%) · ETA *{finish}* _{schedule}_{ran}")
        else:
            lines.append(f"  • {name}: ⏸ waiting _{schedule}_")

    return lines


def format_up_next(prog):
    now   = datetime.now(timezone.utc)
    steps = []

    tw = prog.get("3319486672296602", {})
    gh = prog.get("1498492852256479", {})
    ps = prog[PROFILE_SCRAPER_ID]

    if tw.get("eta_dt"):
        steps.append((tw["eta_dt"],
            f"Launch *Twitter/X Scraper* manually (URL Finder {tw['pct']}% — ready ~*{tw['eta_dt'].strftime('%b %d')}*)"))
    else:
        steps.append((now, "Launch *Twitter/X Scraper* manually — URL Finder done ✅ *ready now*"))

    if gh.get("eta_dt"):
        steps.append((gh["eta_dt"],
            f"Launch *GitHub Profile Scraper* manually (User Search {gh['pct']}% — ready ~*{gh['eta_dt'].strftime('%b %d')}*)"))
    else:
        steps.append((now, "Launch *GitHub Profile Scraper* manually — User Search done ✅ *ready now*"))

    if ps.get("eta_dt"):
        d = ps["eta_dt"]
        steps.append((d,                    f"Run `rank_profiles.py` on Wave 2 enricher output (ETA *{d.strftime('%b %d')}*)"))
        steps.append((d + timedelta(days=2), "Run `push_to_airtable.py --wave 2` (after ranking)"))
        steps.append((d + timedelta(days=2), "Run `filter_and_prepare_enricher.py` → upload to Sheets → Wave 3 enricher starts"))
    else:
        steps.append((now,                    "Run `rank_profiles.py` on Wave 2 output — enricher done ✅"))
        steps.append((now + timedelta(days=2), "Run `push_to_airtable.py --wave 2` (after ranking)"))
        steps.append((now + timedelta(days=2), "Run `filter_and_prepare_enricher.py` → upload to Sheets → Wave 3 enricher starts"))

    steps.sort(key=lambda x: x[0])
    return [f"  • {label}" for _, label in steps]


def format_vigilance(meta_cache, last_run_cache, prog):
    lines  = []
    now_ms = datetime.now(timezone.utc).timestamp() * 1000

    for aid, name in AUTO_PHANTOMS.items():
        lc = last_run_cache.get(aid, {})
        ts = lc.get("queueDate")
        if ts:
            h = int((now_ms - ts) / 3600000)
            if h > 48:
                lines.append(f"  ⚠️ *{name}* hasn't run in *{h // 24}d* — check schedule")
        status = lc.get("lastEndStatus")
        if status and status not in ("success", "finished"):
            lines.append(f"  ⚠️ *{name}* last run ended with status `{status}`")

    # GitHub 429 rate-limit check — warn if progress is very slow
    gh_prog = prog.get("1498492852256479", {})
    gh_lc   = last_run_cache.get("1498492852256479", {})
    if gh_lc.get("queueDate") and gh_prog.get("done", 0) < 20:
        lines.append("  🟡 *GitHub User Search* hitting 429 rate limits — progress very slow, consider reducing daily run frequency")

    for aid, name in [(EMPLOYEES_EXPORT_ID, "Employees Export"), (PROFILE_SCRAPER_ID, "Profile Scraper")]:
        meta = meta_cache.get(aid, {})
        updated_ms = meta.get("updatedAt")
        if updated_ms:
            age_days = int((now_ms - updated_ms) / 86400000)
            if age_days >= 20:
                lines.append(f"  🔴 *{name}* LinkedIn cookie *{age_days}d old* — likely expired, refresh now")
            elif age_days >= 14:
                lines.append(f"  🟡 *{name}* LinkedIn cookie *{age_days}d old* — refresh soon")

    return lines


# ── Main dashboard ────────────────────────────────────────────────────────────

def format_dashboard(agents):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [f"*🤖 PhantomBuster Pipeline Status* — `{today}`\n"]

    all_ids    = list(AUTO_PHANTOMS.keys()) + ["2461522598615921", "3265537247176143"]
    meta_cache = {aid: fetch_agent_v2(aid) for aid in all_ids}
    last_run_cache = {aid: get_last_container(aid) for aid in AUTO_PHANTOMS}
    prog       = compute_progress(meta_cache)

    # ── Running ──────────────────────────────────────────────────────
    running = [a for a in agents.values()
               if a.get("runningContainers", 0) > 0 or a.get("queuedContainers", 0) > 0]
    if running:
        lines.append("*▶ Currently running*")
        for a in running:
            lines.append(f"• {a.get('name', '?')}")
    else:
        lines.append("*▶ Currently running:* nothing")

    # ── Company Employees Export ─────────────────────────────────────
    lines.append("\n*📊 Company Employees Export*")
    lines += format_employees_export_section()

    # ── LinkedIn Profile Scraper ─────────────────────────────────────
    lines.append("\n*👤 LinkedIn Profile Scraper (enricher)*")
    lines += format_scraper_section(prog, last_run_cache)

    # ── Enrichment Phantoms ──────────────────────────────────────────
    lines.append("\n*🔍 Enrichment Phantoms* (Twitter/X · GitHub)")
    lines += format_enrichment_section(agents, prog, last_run_cache)

    # ── Up Next ──────────────────────────────────────────────────────
    lines.append("\n*⏭ Up Next*")
    lines += format_up_next(prog)

    # ── Vigilance ────────────────────────────────────────────────────
    vigilance = format_vigilance(meta_cache, last_run_cache, prog)
    if vigilance:
        lines.append("\n*⚠️ Vigilance*")
        lines += vigilance

    return "\n".join(lines)


def post_to_slack(text):
    if not SLACK_WEBHOOK:
        print(text)
        return
    r = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
    r.raise_for_status()
    print("Posted to #hiring-test ✓")


def main():
    if not PB_KEY:
        sys.exit("PB_API_KEY not set")
    agents = fetch_agents()
    text   = format_dashboard(agents)
    post_to_slack(text)


if __name__ == "__main__":
    main()
