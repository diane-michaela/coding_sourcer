import requests
import csv
import os
import re
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY = os.environ.get("TEAMTAILOR_API_KEY", "YOUR_API_KEY_HERE")
BASE_URL = "https://api.teamtailor.com/v1"
HEADERS = {
    "Authorization": f"Token token={API_KEY}",
    "X-Api-Version": "20161108",
}

OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "API")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "phantombuster_people_api.csv")

FIELDNAMES = [
    "name", "title", "bio",
    "email", "phone",
    "linkedin_url", "twitter_url", "facebook_url", "instagram_url", "other_profile",
    "picture_url",
    "department",
    "city", "country", "address", "zip",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def strip_html(text):
    """Remove HTML tags from bio / description fields."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def paginate(url, params=None):
    """Yield every page from a paginated JSON:API endpoint."""
    while url:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        yield data
        url = data.get("links", {}).get("next")
        params = None   # params are already encoded in the next-page URL
        time.sleep(0.2) # be polite to the API


# ---------------------------------------------------------------------------
# Step 1 – Locations endpoint
# ---------------------------------------------------------------------------
def fetch_locations():
    """
    GET /v1/locations
    Returns a dict keyed by location ID with city, country, address, zip.
    """
    locations = {}
    for page in paginate(f"{BASE_URL}/locations", params={"page[size]": 30}):
        for loc in page["data"]:
            a = loc["attributes"]
            locations[loc["id"]] = {
                "city":         a.get("city"),
                "country":      a.get("country"),
                "address":      a.get("address"),
                "zip":          a.get("zip"),
            }
    return locations


# ---------------------------------------------------------------------------
# Step 2 – Users endpoint
# ---------------------------------------------------------------------------
def fetch_users(locations):
    """
    GET /v1/users?include=department,location
    Merges each user's department and location (from the included sideload
    or from the pre-fetched locations dict as a fallback).
    """
    users = []
    params = {
        "include":      "department,location",
        "page[size]":   30,
    }

    for page in paginate(f"{BASE_URL}/users", params=params):
        # Build a quick lookup for all sideloaded (included) resources
        included = {
            (item["type"], item["id"]): item
            for item in page.get("included", [])
        }

        for user in page["data"]:
            a    = user["attributes"]
            rels = user.get("relationships", {})

            # --- department ---
            dept_ref = rels.get("department", {}).get("data")
            dept_name = None
            if dept_ref:
                dept_obj = included.get(("departments", dept_ref["id"]))
                if dept_obj:
                    dept_name = dept_obj["attributes"].get("name")

            # --- location (sideloaded first, fallback to pre-fetched dict) ---
            loc_ref = rels.get("location", {}).get("data")
            loc = {}
            if loc_ref:
                loc_obj = included.get(("locations", loc_ref["id"]))
                if loc_obj:
                    la = loc_obj["attributes"]
                    loc = {
                        "city":    la.get("city"),
                        "country": la.get("country"),
                        "address": la.get("address"),
                        "zip":     la.get("zip"),
                    }
                elif loc_ref["id"] in locations:
                    loc = locations[loc_ref["id"]]

            picture = a.get("picture") or {}
            users.append({
                "name":            a.get("name"),
                "title":           a.get("title"),
                "bio":             strip_html(a.get("description")),
                "email":           a.get("email"),
                "phone":           a.get("phone"),
                "linkedin_url":    a.get("linkedin-profile"),
                "twitter_url":     a.get("twitter-profile"),
                "facebook_url":    a.get("facebook-profile"),
                "instagram_url":   a.get("instagram-profile"),
                "other_profile":   a.get("other-profile"),
                "picture_url":     picture.get("standard"),
                "department":      dept_name,
                "city":            loc.get("city"),
                "country":         loc.get("country"),
                "address":         loc.get("address"),
                "zip":             loc.get("zip"),
            })

    return users


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if API_KEY == "YOUR_API_KEY_HERE":
    raise SystemExit(
        "No API key set.\n"
        "Set the TEAMTAILOR_API_KEY environment variable or replace "
        "'YOUR_API_KEY_HERE' in the script with your key."
    )

print("Fetching locations...")
locations = fetch_locations()
print(f"  → {len(locations)} location(s) found")

print("Fetching users...")
users = fetch_users(locations)
print(f"  → {len(users)} user(s) found")

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(users)

print(f"Saved {len(users)} rows to {OUTPUT_FILE}")
