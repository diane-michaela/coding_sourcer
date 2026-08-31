"""
Cross-checks Meetup attendees against Google (via Serper.dev) to find likely
LinkedIn profiles. Query: site:linkedin.com/in {name} {city} (figma OR designer)

Requires SERPER_API_KEY in Meetup/.env (free tier: https://serper.dev, 2500 queries).
"""

import csv
import json
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


def build_query(name, city):
    parts = [f'site:linkedin.com/in "{name}"']
    if city:
        parts.append(city)
    parts.append("(figma OR designer)")
    return " ".join(parts)


def search_linkedin(name, city):
    query = build_query(name, city)
    r = requests.post(SEARCH_URL, headers=HEADERS, json={"q": query, "num": 5})
    r.raise_for_status()
    results = r.json().get("organic", [])
    return [res for res in results if "linkedin.com/in" in res.get("link", "")]


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


with open("meetup_members_details.json") as f:
    members = json.load(f)

rows = []
for i, m in enumerate(members, 1):
    name = m.get("name") or ""
    city = m.get("city") or ""
    print(f"[{i}/{len(members)}] Searching {name} ({city or 'no city'})...")

    if len(name.split()) < 2:
        rows.append({
            "name": name, "city": city, "linkedin_url": "", "match_title": "",
            "confidence": "skipped (name too short/generic)",
        })
        continue

    try:
        matches = search_linkedin(name, city)
    except requests.RequestException as e:
        rows.append({
            "name": name, "city": city, "linkedin_url": "", "match_title": "",
            "confidence": f"error: {e}",
        })
        continue

    if matches:
        best = matches[0]
        rows.append({
            "name": name, "city": city,
            "linkedin_url": best.get("link", ""),
            "match_title": best.get("title", ""),
            "confidence": confidence(name, best),
        })
    else:
        rows.append({
            "name": name, "city": city, "linkedin_url": "", "match_title": "",
            "confidence": "no match",
        })

    time.sleep(0.3)

with open("meetup_linkedin_matches.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["name", "city", "linkedin_url", "match_title", "confidence"])
    writer.writeheader()
    writer.writerows(rows)

found = sum(1 for r in rows if r["linkedin_url"])
print(f"\nDone — {found}/{len(rows)} LinkedIn matches found. Saved to meetup_linkedin_matches.csv")
print("Review 'confidence' column before trusting any match — common names will produce false positives.")
