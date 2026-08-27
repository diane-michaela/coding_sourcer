"""
Filters hf_search_llm_ops_candidates.csv down to candidates who have a github_link
and/or linkedin_link, then pulls the real GitHub profile 'location' field for the
ones with a github_link — far more reliable than the bio-guess location_guess
column (which only fires on flag-emoji/city-name mentions in an HF bio).

LinkedIn's API doesn't expose location without an authenticated scrape, so
linkedin-only candidates keep whatever location_guess they already had.

Usage:
    python3 filter_with_github_location.py
Requires GITHUB_TOKEN in huggingface/.env (falls back to unauthenticated / 60 req/hr
if unset).
"""

import csv
import os
import re
import time

import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Accept": "application/vnd.github+json"}
if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"
else:
    print("No GITHUB_TOKEN found — running unauthenticated (60 req/hr limit).")

SOURCE_CSV = "hf_search_llm_ops_candidates.csv"
OUTPUT_CSV = "hf_search_llm_ops_candidates_with_location.csv"

GITHUB_USER_RE = re.compile(r"github\.com/([^/?#]+)")


def extract_github_username(url: str) -> str:
    m = GITHUB_USER_RE.search(url or "")
    return m.group(1) if m else ""


def fetch_github_location(username: str) -> str:
    r = requests.get(f"https://api.github.com/users/{username}", headers=HEADERS, timeout=15)
    if r.status_code != 200:
        return ""
    return (r.json().get("location") or "").strip()


with open(SOURCE_CSV, newline="") as f:
    rows = list(csv.DictReader(f))

filtered = [r for r in rows if r.get("github_link") or r.get("linkedin_link")]
print(f"{len(rows)} total candidates -> {len(filtered)} with a github_link and/or linkedin_link")

fieldnames = list(filtered[0].keys()) + ["github_location"] if filtered else []

for i, row in enumerate(filtered, 1):
    username = extract_github_username(row.get("github_link", ""))
    location = ""
    if username:
        try:
            location = fetch_github_location(username)
        except requests.RequestException as e:
            print(f"[{i}/{len(filtered)}] {username}: error {e}")
        else:
            print(f"[{i}/{len(filtered)}] {username}: {location or '(no location set)'}")
        time.sleep(0.05)
    row["github_location"] = location

with open(OUTPUT_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(filtered)

with_location = sum(1 for r in filtered if r.get("github_location"))
print(f"\n{with_location}/{len(filtered)} had a location set on GitHub.")
print(f"Written: {OUTPUT_CSV}")
