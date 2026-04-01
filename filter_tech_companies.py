"""
filter_tech_companies.py
─────────────────────────────────────────────────────────────────────────────
Reads teamtailor_companies.csv and produces tech_companies.csv containing
only companies that are likely Tech / SaaS.

Two-stage classification
────────────────────────
Stage 1 — Subdomain keyword match  (instant, no HTTP)
    If the company subdomain or name contains any TECH_SLUG_KEYWORDS, the
    company is marked tech and kept.  If it contains any EXCLUDE_KEYWORDS
    it is dropped immediately.

Stage 2 — /people page job-title scan  (HTTP, only for unclassified rows)
    For companies that are not classified by stage 1 AND already have a
    verified /people page, fetch the page and read employee job titles.
    If ≥ MIN_TECH_TITLES titles match TECH_TITLE_KEYWORDS the company
    is classified as tech.

Usage
─────
    python filter_tech_companies.py

Env vars (optional)
───────────────────
    INPUT_CSV        path to input CSV   (default: teamtailor_companies.csv)
    OUTPUT_CSV       path to output CSV  (default: tech_companies.csv)
    MIN_TECH_TITLES  min tech titles to qualify via stage 2  (default: 2)
    SKIP_STAGE2      "1" to skip HTTP scanning (stage 1 only)
    VERIFY_TIMEOUT   per-site HTTP timeout in seconds        (default: 15)
"""

import sys
import os
import csv
import re
import time
import random
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── Config ───────────────────────────────────────────────────────────────────
INPUT_CSV       = Path(os.getenv("INPUT_CSV",  "teamtailor_companies.csv"))
OUTPUT_CSV      = Path(os.getenv("OUTPUT_CSV", "tech_companies.csv"))
MIN_TECH_TITLES = int(os.getenv("MIN_TECH_TITLES", "2"))
SKIP_STAGE2     = os.getenv("SKIP_STAGE2", "0") == "1"
VERIFY_TIMEOUT  = int(os.getenv("VERIFY_TIMEOUT", "15"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ── Stage 1: Subdomain / name keywords ───────────────────────────────────────

# Presence of ANY of these in the subdomain → likely tech
TECH_SLUG_KEYWORDS = [
    "tech", "software", "digital", "saas", "cloud", "cyber", "fintech",
    "code", "coder", "dev", "devs", "data", "ai", "ml", "bot", "lab",
    "labs", "sys", "system", "systems", "net", "network", "bit", "byte",
    "ware", "compute", "algo", "robot", "auto", "platform", "app",
    "api", "infra", "stack", "open", "source", "quantum", "crypto",
    "blockchain", "security", "analytics", "insight", "intelligence",
    "automation", "machine", "neural", "deep", "vision", "voice",
    "stream", "hosting", "server", "cdn", "devops", "sre", "ops",
    "monitor", "observ", "log", "deploy", "container", "kubernetes",
    "k8s", "microservice", "gateway", "proxy", "vpn", "firewall",
    "endpoint", "pentest", "siem", "soar", "threat", "vuln",
    "payment", "pay", "wallet", "banking", "lend", "invest", "trade",
    "quant", "risk", "comply", "regtech", "insurtech", "proptech",
    "healthtech", "medtech", "edtech", "legaltech", "hrtech", "martech",
    "adtech", "gametech", "game", "gaming", "esport", "xr", "ar", "vr",
    "iot", "sensor", "drone", "robotics", "autonomous", "lidar",
    "satellite", "space", "aerospace",
]

# Presence of ANY of these → almost certainly NOT tech (hard exclude)
EXCLUDE_SLUG_KEYWORDS = [
    "restaurant", "bakeri", "bageri", "food", "sushi", "pasta",
    "coffee", "cafe", "kafe", "ravintola", "boulangerie",
    "tandlake", "dental", "tand", "klinik", "kliniker", "clinic",
    "rehab", "omsorg", "vard", "care", "health", "hemtjanst",
    "sjukhus", "hospital", "lakar", "doctor", "apotea", "apotek",
    "pharmacy", "optik", "glasogon",
    "bil", "auto", "motor", "motorkjor", "bilhus", "bilfirma",
    "yacht", "boat", "sjo", "marin",
    "bygg", "bygging", "construction", "maleri", "plumber",
    "hamn", "port", "logistik", "logistics", "transport", "frakt",
    "retail", "shop", "store", "butik", "handel", "market",
    "supermarket", "grocery", "ica", "coop",
    "fastighets", "fastighetsbyr", "maklare", "realestate", "estate",
    "hotel", "hotell", "resort", "spa", "holiday", "vacation",
    "museum", "teater", "theater", "kultur", "concert",
    "church", "kyrka", "diakoni", "charitable", "charity",
    "school", "skola", "university", "college", "montessori",
    "staffing", "bemanning", "personal", "recruitment", "rekryt",
    "headhunt", "executive", "search",
    "law", "legal", "advokatfirma", "juridik", "notarie",
    "accountant", "revisor", "bokfor", "audit", "skatt",
    "nail", "beauty", "salon", "hair", "spa", "skönhet",
    "sport", "gym", "fitness", "yoga", "golf", "fotboll",
    "farm", "lantbruk", "agri", "forestry",
    "printing", "print", "tryck",
    "funeral", "begravning",
]

# ── Stage 2: Job title keywords ───────────────────────────────────────────────

TECH_TITLE_KEYWORDS = [
    "engineer", "engineering", "developer", "development",
    "software", "backend", "frontend", "full-stack", "fullstack",
    "devops", "sre", "platform", "infrastructure", "cloud",
    "data scientist", "data engineer", "data analyst", "ml engineer",
    "machine learning", "ai ", " ai", "artificial intelligence",
    "product manager", "product designer", "ux", "ui ",
    "cto", "vp engineering", "head of engineering", "tech lead",
    "architect", "security engineer", "cyber", "soc analyst",
    "mobile developer", "ios developer", "android developer",
    "embedded", "firmware", "hardware engineer",
    "quantitative", "quant developer", "trading systems",
    "blockchain", "smart contract", "web3",
]

OUTPUT_FIELDNAMES = [
    "company_name", "base_url", "people_url", "source",
    "is_teamtailor", "has_people_page",
    "tech_stage",    # "slug_keyword" | "title_scan" | None
    "tech_score",    # count of tech title matches (stage 2 only)
    "checked_at",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def slug_of(base_url: str) -> str:
    """Extract just the subdomain slug from a URL."""
    host = base_url.replace("https://", "").replace("http://", "").split("/")[0]
    return host.replace(".teamtailor.com", "").replace("careers.", "").lower()


def stage1_classify(row: dict) -> str | None:
    """
    Returns "tech" if slug keywords confirm tech,
            "exclude" if slug keywords rule it out,
            None if undecided.
    """
    slug = slug_of(row["base_url"])
    name = row.get("company_name", "").lower()
    combined = slug + " " + name

    for kw in EXCLUDE_SLUG_KEYWORDS:
        if kw in combined:
            return "exclude"

    for kw in TECH_SLUG_KEYWORDS:
        if kw in combined:
            return "tech"

    return None


def fetch_people_titles(people_url: str) -> list[str]:
    """Fetch a Teamtailor /people page and return all employee job titles."""
    try:
        resp = requests.get(people_url, headers=HEADERS,
                            timeout=VERIFY_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        # Teamtailor renders titles in div.text-block-text (not .text-block-link)
        titles = [
            el.get_text(strip=True)
            for el in soup.select("div.text-block-text:not(.text-block-link)")
            if el.get_text(strip=True)
        ]
        return titles
    except Exception:
        return []


def count_tech_titles(titles: list[str]) -> int:
    """Count how many titles contain a tech keyword."""
    count = 0
    for title in titles:
        t = title.lower()
        if any(kw in t for kw in TECH_TITLE_KEYWORDS):
            count += 1
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Tech / SaaS Company Filter")
    print(f"Input : {INPUT_CSV.resolve()}")
    print(f"Output: {OUTPUT_CSV.resolve()}")
    print("=" * 70)

    # Read input
    with INPUT_CSV.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"\nLoaded {len(rows)} companies from CSV")

    tech_rows    = []
    excluded     = 0
    undecided    = []
    stage1_tech  = 0

    # ── Stage 1 ──────────────────────────────────────────────────────────────
    print("\n[Stage 1] Keyword classification ...")
    for row in rows:
        result = stage1_classify(row)
        if result == "tech":
            tech_rows.append({**row, "tech_stage": "slug_keyword", "tech_score": ""})
            stage1_tech += 1
        elif result == "exclude":
            excluded += 1
        else:
            undecided.append(row)

    print(f"  Confirmed tech (keyword match) : {stage1_tech}")
    print(f"  Excluded (non-tech keywords)   : {excluded}")
    print(f"  Undecided (need stage 2)       : {len(undecided)}")

    # ── Stage 2 ──────────────────────────────────────────────────────────────
    stage2_tech = 0

    if SKIP_STAGE2:
        print("\n[Stage 2] Skipped (SKIP_STAGE2=1).")
        for row in undecided:
            tech_rows.append({**row, "tech_stage": None, "tech_score": ""})
    else:
        # Only scan rows that already have a verified /people page
        scannable = [r for r in undecided if r.get("has_people_page") == "True"]
        no_page   = [r for r in undecided if r.get("has_people_page") != "True"]

        print(f"\n[Stage 2] Job-title scan on {len(scannable)} companies "
              f"with /people page ...")
        print(f"  (skipping {len(no_page)} unverified companies)")

        for i, row in enumerate(scannable, 1):
            people_url = row.get("people_url", "")
            print(f"  [{i:03d}/{len(scannable)}] {row['company_name']:<35}", end=" ")
            titles = fetch_people_titles(people_url)
            score  = count_tech_titles(titles)
            total  = len(titles)
            is_tech = score >= MIN_TECH_TITLES

            print(f"titles={total}  tech_titles={score}  "
                  f"{'TECH' if is_tech else 'not tech'}")

            if is_tech:
                tech_rows.append({**row, "tech_stage": "title_scan", "tech_score": score})
                stage2_tech += 1

            if i < len(scannable):
                time.sleep(0.5 + random.uniform(0, 0.3))

        # Unverified undecided rows — append without classification
        for row in no_page:
            pass   # silently drop unverified undecided rows

    # ── Output ────────────────────────────────────────────────────────────────
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(tech_rows)

    print(f"\n{'─' * 70}")
    print(f"Stage 1 (keyword match) : {stage1_tech}")
    print(f"Stage 2 (title scan)    : {stage2_tech}")
    print(f"Total tech companies    : {len(tech_rows)}")
    print(f"Excluded / non-tech     : {excluded}")
    print(f"{'─' * 70}")
    print(f"Saved {len(tech_rows)} rows -> {OUTPUT_CSV.resolve()}")
    print(f"\nNext step:")
    print(f"  python scrap-career-page.py <people_url>")


if __name__ == "__main__":
    main()
