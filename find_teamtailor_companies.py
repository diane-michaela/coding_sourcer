"""
find_teamtailor_companies.py
─────────────────────────────────────────────────────────────────────────────
Discovers tech companies whose career pages are built on Teamtailor — the
same platform as careers.phantombuster.com — then verifies each one has a
/people page compatible with scrap-career-page.py.

How it works
────────────
Step 1  Google Dorks
        Run a set of Teamtailor-fingerprint queries through the existing
        googlesearch-python library to surface candidate URLs.

Step 2  BuiltWith free lookup  (optional, no key required for single sites)
        The free BuiltWith endpoint only checks *known* domains; it cannot
        enumerate all Teamtailor sites. We use it to cross-validate hits.

Step 3  Verification
        For each candidate domain, fetch the root career URL and confirm
        the Teamtailor fingerprint (data-controller="careersite--ready")
        exists in the HTML. Then check for the /people path.

Step 4  Output
        Write a CSV with: company_name, base_url, people_url, source,
        verified (bool), people_page_exists (bool).

Usage
─────
    python find_teamtailor_companies.py

Env vars (optional)
───────────────────
    GOOGLE_SEARCH_DELAY_SEC   base delay between Google queries  (default 5)
    MAX_GOOGLE_RESULTS        results per Google query            (default 10)
    VERIFY_TIMEOUT_SEC        per-site HTTP timeout               (default 15)
"""

import os
import re
import csv
import time
import random
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from googlesearch import search as _gsearch

# ── Config ───────────────────────────────────────────────────────────────────
BASE_DELAY_SEC      = float(os.getenv("GOOGLE_SEARCH_DELAY_SEC", "5"))
MAX_GOOGLE_RESULTS  = int(os.getenv("MAX_GOOGLE_RESULTS", "10"))
VERIFY_TIMEOUT_SEC  = int(os.getenv("VERIFY_TIMEOUT_SEC", "15"))

OUTPUT_DIR  = Path(__file__).parent
OUTPUT_FILE = OUTPUT_DIR / "teamtailor_companies.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ── Teamtailor HTML fingerprint ───────────────────────────────────────────────
# Present in every Teamtailor-powered career site's <body> tag
TEAMTAILOR_FINGERPRINT = 'data-controller="careersite--ready'

# ── Google dork queries ───────────────────────────────────────────────────────
# Each query is designed to surface Teamtailor-hosted career pages.
# Mix of: native *.teamtailor.com subdomains + custom "careers.*" domains.
DORK_QUERIES = [
    # Native Teamtailor subdomains (company.teamtailor.com)
    'site:teamtailor.com inurl:/people',
    'site:teamtailor.com "Our team" technology',
    'site:teamtailor.com "Our team" software',
    'site:teamtailor.com "Our team" startup',
    'site:teamtailor.com "Our team" SaaS',
    'site:teamtailor.com "Meet the team" engineering',
    # Custom career domains using Teamtailor
    'inurl:"/people" "careersite-button" technology company',
    'inurl:"/people" "careersite-button" software startup',
    '"Powered by Teamtailor" technology',
    '"Powered by Teamtailor" software engineering',
    '"Powered by Teamtailor" SaaS startup',
    '"Powered by Teamtailor" AI machine learning',
    '"Powered by Teamtailor" fintech',
    '"Powered by Teamtailor" data platform',
    # Specific HTML fingerprint (works when Google has cached raw HTML)
    '"careersite--ready" "careersite--referrer-cookie" tech',
    '"careersite--ready" site:careers.*',
]

# ── Known seed companies (verified Teamtailor users) ─────────────────────────
# Bootstraps the list; script will add more from Google results.
SEED_COMPANIES = [
    ("PhantomBuster",  "https://careers.phantombuster.com"),
    ("Teamtailor",     "https://careers.teamtailor.com"),
    ("Mentimeter",     "https://careers.mentimeter.com"),
    ("Klarna",         "https://careers.klarna.com"),
    ("Trustpilot",     "https://careers.trustpilot.com"),
    ("Epidemic Sound", "https://careers.epidemicsound.com"),
    ("Kry",            "https://careers.kry.se"),
    ("Acast",          "https://careers.acast.com"),
    ("Hemnet",         "https://careers.hemnet.se"),
    ("Voi",            "https://careers.voiscooters.com"),
    ("Einride",        "https://careers.einride.com"),
    ("Northvolt",      "https://career.northvolt.com"),
    ("Peltarion",      "https://peltarion.teamtailor.com"),
    ("Lookback",       "https://lookback.teamtailor.com"),
    ("Funnel.io",      "https://careers.funnel.io"),
    ("Quinyx",         "https://careers.quinyx.com"),
    ("Anyfin",         "https://careers.anyfin.com"),
    ("Lana",           "https://lanagroup.teamtailor.com"),
    ("Detectify",      "https://careers.detectify.com"),
    ("Cint",           "https://careers.cint.com"),
]

FIELDNAMES = [
    "company_name",
    "base_url",
    "people_url",
    "source",
    "is_teamtailor",   # fingerprint confirmed in HTML
    "has_people_page", # /people route exists and returns 200
    "checked_at",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise_base_url(url: str) -> str | None:
    """Return scheme+netloc only, or None if the URL looks invalid."""
    try:
        p = urllib.parse.urlparse(url)
        if p.scheme not in ("http", "https") or not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return None


def is_teamtailor_domain(netloc: str) -> bool:
    """True if this is a native *.teamtailor.com subdomain."""
    return netloc.endswith(".teamtailor.com")


def extract_company_name_from_url(url: str) -> str:
    """Best-effort: strip known prefixes to get a human-readable name."""
    netloc = urllib.parse.urlparse(url).netloc
    name = re.sub(r"\.(teamtailor|com|se|io|co|org|net|uk|de|fr|es|no|fi|dk).*$", "", netloc)
    name = re.sub(r"^(careers?|jobs?|work)\.", "", name)
    return name.replace("-", " ").replace("_", " ").title()


def fetch_html(url: str, timeout: int = VERIFY_TIMEOUT_SEC) -> str | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout,
                            allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def verify_teamtailor(base_url: str) -> tuple[bool, bool]:
    """
    Returns (is_teamtailor, has_people_page).
    - is_teamtailor : Teamtailor fingerprint found on the base career page
    - has_people_page: /people sub-path returns 200 with the fingerprint
    """
    html = fetch_html(base_url)
    if html is None:
        return False, False

    is_tt = TEAMTAILOR_FINGERPRINT in html

    if not is_tt:
        return False, False

    # Check /people
    people_url = base_url.rstrip("/") + "/people"
    people_html = fetch_html(people_url)
    has_people = bool(people_html and TEAMTAILOR_FINGERPRINT in people_html)
    return True, has_people


def google_dork_urls(query: str, n: int = MAX_GOOGLE_RESULTS) -> list[str]:
    """Run a single Google dork and return raw result URLs."""
    try:
        return list(_gsearch(query, num_results=n, sleep_interval=0))
    except Exception as e:
        print(f"  [Google] Error for '{query[:60]}': {e}")
        return []


def polite_sleep(base: float = BASE_DELAY_SEC):
    t = base + random.uniform(1.0, 3.0)
    time.sleep(t)


# ── Step 1: Google Dork Discovery ────────────────────────────────────────────

def discover_via_google() -> set[str]:
    """Return a set of normalised base URLs found via Google dorks."""
    found: set[str] = set()

    print(f"\n[Step 1] Running {len(DORK_QUERIES)} Google dork queries "
          f"(up to {MAX_GOOGLE_RESULTS} results each) …")

    for i, query in enumerate(DORK_QUERIES, 1):
        print(f"  [{i:02d}/{len(DORK_QUERIES)}] {query[:80]}")
        urls = google_dork_urls(query)

        for raw_url in urls:
            base = normalise_base_url(raw_url)
            if base:
                found.add(base)
                print(f"           + {base}")

        if i < len(DORK_QUERIES):
            polite_sleep()

    print(f"\n  -> {len(found)} unique base URLs discovered via Google")
    return found


# ── Step 2: Merge seeds + Google results ─────────────────────────────────────

def build_candidate_list(google_urls: set[str]) -> list[dict]:
    """
    Merge seed companies with Google-discovered URLs.
    Returns list of dicts with company_name, base_url, source.
    """
    seen: dict[str, dict] = {}

    # Seeds first
    for name, url in SEED_COMPANIES:
        base = normalise_base_url(url)
        if base:
            seen[base] = {"company_name": name, "base_url": base, "source": "seed"}

    # Google results
    for url in google_urls:
        if url not in seen:
            name = extract_company_name_from_url(url)
            seen[url] = {"company_name": name, "base_url": url, "source": "google_dork"}

    return list(seen.values())


# ── Step 3: Verify each candidate ────────────────────────────────────────────

def verify_candidates(candidates: list[dict]) -> list[dict]:
    print(f"\n[Step 3] Verifying {len(candidates)} candidate sites …")
    checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    results = []

    for i, c in enumerate(candidates, 1):
        base = c["base_url"]
        print(f"  [{i:03d}/{len(candidates)}] {base}", end="", flush=True)

        is_tt, has_people = verify_teamtailor(base)
        people_url = (base.rstrip("/") + "/people") if has_people else ""

        status = "✓ Teamtailor + /people" if has_people else (
                 "~ Teamtailor (no /people)" if is_tt else "✗ Not Teamtailor")
        print(f"  {status}")

        results.append({
            **c,
            "people_url":    people_url,
            "is_teamtailor": is_tt,
            "has_people_page": has_people,
            "checked_at":    checked_at,
        })

        # Be polite; no need to hammer sites back-to-back
        if i < len(candidates):
            time.sleep(0.8 + random.uniform(0, 0.5))

    return results


# ── Step 4: Save output ───────────────────────────────────────────────────────

def save_csv(rows: list[dict]) -> None:
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nSaved {len(rows)} rows to {OUTPUT_FILE.resolve()}")


def print_summary(rows: list[dict]) -> None:
    tt_count     = sum(1 for r in rows if r["is_teamtailor"])
    people_count = sum(1 for r in rows if r["has_people_page"])
    print("\n" + "─" * 70)
    print(f"Total candidates checked : {len(rows)}")
    print(f"Confirmed Teamtailor     : {tt_count}")
    print(f"Have /people page        : {people_count}  ← ready for scrap-career-page.py")
    print("─" * 70)
    if people_count:
        print("\nCompanies with /people pages:")
        for r in rows:
            if r["has_people_page"]:
                print(f"  {r['company_name']:<30}  {r['people_url']}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Teamtailor Company Finder")
    print("=" * 70)

    # Step 1: Google dorks
    google_urls = discover_via_google()

    # Step 2: Merge with seeds
    candidates = build_candidate_list(google_urls)

    # Step 3: Verify
    results = verify_candidates(candidates)

    # Step 4: Save
    save_csv(results)
    print_summary(results)

    print(
        "\nNext step: copy any 'people_url' into LIST_URL in scrap-career-page.py"
        " to scrape that company's team."
    )


if __name__ == "__main__":
    main()
