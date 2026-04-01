"""
batch_scrape_people.py
─────────────────────────────────────────────────────────────────────────────
Reads tech_companies.csv, scrapes the /people page of every tech company
that has one (or can be verified to have one), and writes all results into
a single all_tech_people.csv with a company_name column added.

Two scraping modes
──────────────────
  list-only  (default, fast):  scrapes names + titles + departments from the
             /people listing page.  No individual profile visits.

  full       (slow, ENRICH=1): also visits each person's profile page to
             collect their LinkedIn URL and bio.  ~1 req/person extra.

Usage
─────
    python batch_scrape_people.py                # list-only mode
    set ENRICH=1 && python batch_scrape_people.py   # full mode (LinkedIn+bio)

Env vars (optional)
───────────────────
    INPUT_CSV      path to tech_companies.csv  (default: tech_companies.csv)
    OUTPUT_CSV     combined output file        (default: all_tech_people.csv)
    ENRICH         "1" to fetch individual profiles for LinkedIn + bio
    SLEEP_LIST     seconds between company list pages  (default: 1.5)
    SLEEP_PROFILE  seconds between individual profile requests (default: 1.0)
    VERIFY_TIMEOUT HTTP timeout per request (default: 20)
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

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV      = Path(os.getenv("INPUT_CSV",  "tech_companies.csv"))
OUTPUT_CSV     = Path(os.getenv("OUTPUT_CSV", "all_tech_people.csv"))
ENRICH         = os.getenv("ENRICH", "0") == "1"
SLEEP_LIST     = float(os.getenv("SLEEP_LIST",    "1.5"))
SLEEP_PROFILE  = float(os.getenv("SLEEP_PROFILE", "1.0"))
VERIFY_TIMEOUT = int(os.getenv("VERIFY_TIMEOUT",  "20"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

TEAMTAILOR_FINGERPRINT = "careersite--ready"

FIELDNAMES = [
    "company_name",
    "company_url",
    "person_id",
    "name",
    "title",
    "department",
    "linkedin_url",   # only populated when ENRICH=1
    "bio",            # only populated when ENRICH=1
    "image_url",
    "profile_url",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_soup(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=VERIFY_TIMEOUT,
                            allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, "html.parser")
    except Exception:
        pass
    return None


def has_people_page(base_url: str) -> bool:
    """Quick check: does /people exist and carry the Teamtailor fingerprint?"""
    try:
        resp = requests.get(base_url.rstrip("/") + "/people",
                            headers=HEADERS, timeout=VERIFY_TIMEOUT,
                            allow_redirects=True)
        return resp.status_code == 200 and TEAMTAILOR_FINGERPRINT in resp.text
    except Exception:
        return False


def extract_person_id(url: str) -> str | None:
    m = re.search(r"/people/(\d+)-", url)
    return m.group(1) if m else None


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_list_page(people_url: str, company_name: str,
                     company_base: str) -> list[dict]:
    """
    Scrape the /people listing page.
    Returns a list of person dicts (no LinkedIn / bio yet).
    """
    soup = get_soup(people_url)
    if soup is None:
        print(f"    Could not fetch {people_url}")
        return []

    people = []
    sections = soup.select("div.flex.flex-col.gap-16 > div")

    for section in sections:
        dept_el   = section.select_one("h2 a")
        dept_name = dept_el.get_text(strip=True) if dept_el else None

        for card in section.select('a[href*="/people/"]'):
            profile_url = card.get("href", "")
            name_el     = card.select_one("div.text-block-link")
            title_el    = card.select_one(
                              "div.text-block-text:not(.text-block-link)")
            img_el      = card.select_one("img")

            name  = name_el.get_text(strip=True)  if name_el  else None
            title = title_el.get_text(strip=True) if title_el else None
            image = img_el.get("src")             if img_el   else None

            if not name and not title:
                continue

            people.append({
                "company_name": company_name,
                "company_url":  company_base,
                "person_id":    extract_person_id(profile_url),
                "name":         name,
                "title":        title,
                "department":   dept_name,
                "linkedin_url": None,
                "bio":          None,
                "image_url":    image,
                "profile_url":  profile_url,
            })

    return people


def enrich_from_profile(person: dict) -> dict:
    """Visit the individual profile page and add LinkedIn + bio."""
    soup = get_soup(person["profile_url"])
    if soup is None:
        return person

    linkedin_el = soup.select_one('a[title="LinkedIn"]')
    person["linkedin_url"] = linkedin_el.get("href") if linkedin_el else None

    bio_el = soup.select_one("div.prose.prose-block")
    person["bio"] = (bio_el.get_text(separator=" ", strip=True)
                     if bio_el else None)
    return person


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Batch People Scraper — Tech Companies")
    print(f"Mode  : {'FULL (list + profiles)' if ENRICH else 'LIST ONLY (fast)'}")
    print(f"Input : {INPUT_CSV.resolve()}")
    print(f"Output: {OUTPUT_CSV.resolve()}")
    print("=" * 70)

    # Load company list
    with INPUT_CSV.open(encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    print(f"\nLoaded {len(companies)} companies from {INPUT_CSV.name}\n")

    all_people: list[dict] = []
    scraped     = 0
    skipped     = 0
    total_found = 0

    for i, company in enumerate(companies, 1):
        name     = company.get("company_name", "Unknown")
        base_url = company.get("base_url", "").rstrip("/")
        # Use pre-verified people_url if available, else construct it
        people_url = company.get("people_url", "").strip() or (base_url + "/people")
        verified   = company.get("has_people_page", "")

        print(f"[{i:03d}/{len(companies)}] {name}")

        # If not pre-verified, check now
        if verified != "True":
            print(f"  Checking /people ...", end=" ", flush=True)
            if not has_people_page(base_url):
                print("no people page, skipping")
                skipped += 1
                continue
            print("found!")
            people_url = base_url + "/people"

        # Scrape the listing page
        people = scrape_list_page(people_url, name, base_url)
        print(f"  {len(people)} people found", end="")

        if not people:
            skipped += 1
            print()
            continue

        # Optionally enrich with LinkedIn + bio
        if ENRICH:
            print(f"  -- enriching profiles ...")
            for j, person in enumerate(people, 1):
                print(f"    [{j}/{len(people)}] {person['name']}", end=" ")
                enrich_from_profile(person)
                has_li = bool(person.get("linkedin_url"))
                print("LinkedIn" if has_li else "-")
                time.sleep(SLEEP_PROFILE + random.uniform(0, 0.3))
        else:
            print()

        all_people.extend(people)
        total_found += len(people)
        scraped += 1

        # Polite pause between companies
        if i < len(companies):
            time.sleep(SLEEP_LIST + random.uniform(0, 0.5))

    # Write combined output
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_people)

    print("\n" + "=" * 70)
    print(f"Companies scraped   : {scraped}")
    print(f"Companies skipped   : {skipped}  (no /people page or unreachable)")
    print(f"Total people found  : {total_found}")
    print(f"Saved to            : {OUTPUT_CSV.resolve()}")
    if not ENRICH:
        print("\nTip: run with ENRICH=1 to also collect LinkedIn URLs and bios.")
    print("=" * 70)


if __name__ == "__main__":
    main()
