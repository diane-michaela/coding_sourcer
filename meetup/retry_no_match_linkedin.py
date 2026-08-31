"""
Retry pass for attendees find_linkedin.py marked "no match" — the original query
(site:linkedin.com/in "{name}" {city} (figma OR designer)) is narrow enough that a
real profile can exist but not surface. This widens the query in two fallback steps
before giving up, and only touches rows still marked "no match" — leaves existing
high/medium/low matches untouched, so it doesn't re-spend API calls on those.

Usage:
    python3 retry_no_match_linkedin.py
"""

import csv
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SERPER_API_KEY")
if not API_KEY:
    raise SystemExit("Missing SERPER_API_KEY — add it to Meetup/.env (see .env.example)")

SEARCH_URL = "https://google.serper.dev/search"
HEADERS = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
CSV_PATH = "meetup_linkedin_matches.csv"


def confidence(name, result):
    name_tokens = [t.lower() for t in name.split() if len(t) > 1]
    title = result.get("title", "").lower()
    matched = sum(1 for t in name_tokens if t in title)
    if not name_tokens:
        return "low"
    ratio = matched / len(name_tokens)
    if ratio == 1:
        return "high"
    if ratio >= 0.5:
        return "medium"
    return "low"


def search(query):
    r = requests.post(SEARCH_URL, headers=HEADERS, json={"q": query, "num": 5})
    r.raise_for_status()
    results = r.json().get("organic", [])
    return [res for res in results if "linkedin.com/in" in res.get("link", "")]


def try_widen(name, city):
    """Two broader fallbacks, in order: drop industry filter, then drop city too."""
    queries = []
    if city:
        queries.append(f'site:linkedin.com/in "{name}" {city}')
    queries.append(f'site:linkedin.com/in "{name}"')

    for q in queries:
        matches = search(q)
        if matches:
            return matches[0], q
        time.sleep(0.3)
    return None, None


with open(CSV_PATH, newline="") as f:
    rows = list(csv.DictReader(f))

retried = 0
recovered = 0
for row in rows:
    if row["confidence"] != "no match":
        continue
    retried += 1
    name, city = row["name"], row["city"]
    print(f"[{retried}] Retrying {name} ({city or 'no city'})...")

    try:
        best, used_query = try_widen(name, city)
    except requests.RequestException as e:
        print(f"    error: {e}")
        continue

    if best:
        row["linkedin_url"] = best.get("link", "")
        row["match_title"] = best.get("title", "")
        row["confidence"] = f"{confidence(name, best)} (widened query)"
        recovered += 1
        print(f"    found: {best.get('link')} [{row['confidence']}]")
    else:
        print("    still no match")

with open(CSV_PATH, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["name", "city", "linkedin_url", "match_title", "confidence"])
    writer.writeheader()
    writer.writerows(rows)

print(f"\nRetried {retried} 'no match' rows, recovered {recovered}. Updated {CSV_PATH}.")
print("Widened-query matches are lower-confidence than the original industry-filtered search — review before trusting.")
