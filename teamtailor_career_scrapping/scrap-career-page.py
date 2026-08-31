import sys
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import time
import urllib.parse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Pass a Teamtailor career base URL as a CLI argument, e.g.:
#   python scrap-career-page.py https://careers.mentimeter.com
# Defaults to PhantomBuster when run without arguments.
_DEFAULT_BASE = "https://careers.THECOMPANY.com"
_BASE_URL     = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else _DEFAULT_BASE
LIST_URL      = _BASE_URL + "/people"

HEADERS   = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC = 1.0   # pause between profile page requests

# Derive a safe company slug for the output filename
_company_slug = re.sub(r"[^a-z0-9]+", "_",
                       urllib.parse.urlparse(_BASE_URL).netloc.lower()).strip("_")

OUTPUT_DIR  = os.path.join(os.path.expanduser("~"), "Desktop", "API")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{_company_slug}_people.csv")

FIELDNAMES = [
    "person_id",
    "name",
    "title",
    "department",
    "department_url",
    "bio",
    "linkedin_url",
    "image_url",
    "profile_url",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").strip()


def extract_person_id(url):
    m = re.search(r"/people/(\d+)-", url)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Step 1 – Scrape the people list page
#   Iterates department sections so each person gets their department name.
# ---------------------------------------------------------------------------
def scrape_list_page():
    print(f"Fetching list page: {LIST_URL}")
    soup = get_soup(LIST_URL)
    people = []

    # Each department is a <div> inside the main flex container
    for section in soup.select("div.flex.flex-col.gap-16 > div"):
        dept_el   = section.select_one("h2 a")
        dept_name = dept_el.get_text(strip=True) if dept_el else None
        dept_url  = dept_el.get("href")          if dept_el else None

        for card in section.select('a[href*="/people/"]'):
            profile_url = card.get("href", "")

            name_el  = card.select_one("div.text-block-link")
            title_el = card.select_one("div.text-block-text:not(.text-block-link)")
            img_el   = card.select_one("img")

            name  = name_el.get_text(strip=True)  if name_el  else None
            title = title_el.get_text(strip=True) if title_el else None
            image = img_el.get("src")             if img_el   else None

            if not name and not title:
                continue

            people.append({
                "person_id":      extract_person_id(profile_url),
                "name":           name,
                "title":          title,
                "department":     dept_name,
                "department_url": dept_url,
                "profile_url":    profile_url,
                "image_url":      image,
                "linkedin_url":   None,
                "bio":            None,
            })

    print(f"  -> {len(people)} people found across {len(soup.select('div.flex.flex-col.gap-16 > div'))} departments")
    return people


# ---------------------------------------------------------------------------
# Step 2 – Visit each profile page to collect LinkedIn URL and bio
# ---------------------------------------------------------------------------
def enrich_from_profile(person):
    url = person["profile_url"]
    try:
        soup = get_soup(url)

        # LinkedIn
        linkedin_el = soup.select_one('a[title="LinkedIn"]')
        person["linkedin_url"] = linkedin_el.get("href") if linkedin_el else None

        # Bio — strip HTML tags (bio is stored as HTML paragraphs)
        bio_el = soup.select_one("div.prose.prose-block")
        person["bio"] = bio_el.get_text(separator=" ", strip=True) if bio_el else None

    except Exception as e:
        print(f"    ⚠ Could not fetch profile for {person['name']}: {e}")

    return person


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
people = scrape_list_page()

print("\nFetching individual profile pages...")
for i, person in enumerate(people, 1):
    print(f"  [{i}/{len(people)}] {person['name']}")
    enrich_from_profile(person)
    time.sleep(SLEEP_SEC)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(people)

print(f"\nSaved {len(people)} rows to {OUTPUT_FILE}")
