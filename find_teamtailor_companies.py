"""
find_teamtailor_companies.py
─────────────────────────────────────────────────────────────────────────────
Discovers tech companies whose career pages are built on Teamtailor — the
same platform as careers.phantombuster.com — then verifies each one has a
/people page compatible with scrap-career-page.py.

How it works
────────────
Step 1  Certificate Transparency  (crt.sh)
        Query crt.sh for every SSL cert ever issued to *.teamtailor.com.
        This surfaces ALL native company.teamtailor.com subdomains — free,
        no rate-limit issues, typically returns 500-2000+ entries.

Step 2  DuckDuckGo search  (secondary, finds custom domains)
        Custom-domain Teamtailor sites (careers.company.com) don't appear in
        the crt.sh wildcard query.  DuckDuckGo is used to find these via
        searches for "Powered by Teamtailor" and related fingerprints.

Step 3  Seed companies
        20 known Teamtailor users merged in as a baseline.

Step 4  Verification
        For each candidate, fetch the career root page and confirm the
        Teamtailor fingerprint (data-controller="careersite--ready") in the
        HTML, then check the /people sub-path.

Step 5  Output
        CSV: company_name, base_url, people_url, source,
             is_teamtailor, has_people_page, checked_at

Usage
─────
    python find_teamtailor_companies.py

Optional env vars
─────────────────
    VERIFY_TIMEOUT_SEC   per-site HTTP timeout    (default 15)
    MAX_VERIFY           max candidates to verify (default 300, 0 = all)
    SKIP_VERIFY          set to "1" to skip verification and just dump URLs
"""

import os
import re
import csv
import time
import json
import random
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ───────────────────────────────────────────────────────────────────
VERIFY_TIMEOUT_SEC = int(os.getenv("VERIFY_TIMEOUT_SEC", "15"))
MAX_VERIFY         = int(os.getenv("MAX_VERIFY", "300"))   # 0 = no limit
SKIP_VERIFY        = os.getenv("SKIP_VERIFY", "0") == "1"

OUTPUT_DIR  = Path(__file__).parent
OUTPUT_FILE = OUTPUT_DIR / "teamtailor_companies.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

TEAMTAILOR_FINGERPRINT = 'careersite--ready'

FIELDNAMES = [
    "company_name",
    "base_url",
    "people_url",
    "source",
    "is_teamtailor",
    "has_people_page",
    "checked_at",
]


# ── Known seed companies (verified Teamtailor users — custom domains) ─────────
# These use custom domains so they don't appear in *.teamtailor.com certs.
# Only include ones confirmed still on Teamtailor.
SEED_COMPANIES = [
    ("PhantomBuster", "https://careers.phantombuster.com"),
    ("Acast",         "https://careers.acast.com"),
    ("Quinyx",        "https://careers.quinyx.com"),
    ("Detectify",     "https://careers.detectify.com"),
    ("Funnel.io",     "https://careers.funnel.io"),
    ("Epidemic Sound","https://careers.epidemicsound.com"),
    ("Hemnet",        "https://careers.hemnet.se"),
    ("Cint",          "https://careers.cint.com"),
    ("Mentimeter",    "https://careers.mentimeter.com"),
    ("Kry",           "https://careers.kry.se"),
    ("Einride",       "https://careers.einride.com"),
    ("Anyfin",        "https://careers.anyfin.com"),
]

# ── DuckDuckGo queries (finds custom-domain Teamtailor sites) ─────────────────
# Uses the unofficial DDG HTML search — no API key, less prone to blocking.
DDG_QUERIES = [
    '"Powered by Teamtailor" technology',
    '"Powered by Teamtailor" software',
    '"Powered by Teamtailor" SaaS',
    '"Powered by Teamtailor" startup',
    '"Powered by Teamtailor" AI',
    '"Powered by Teamtailor" fintech',
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise_base_url(url: str) -> str | None:
    try:
        p = urllib.parse.urlparse(url)
        if p.scheme not in ("http", "https") or not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return None


def extract_company_name(url: str) -> str:
    netloc = urllib.parse.urlparse(url).netloc
    name = re.sub(r"\.(teamtailor|com|se|io|co|org|net|uk|de|fr|es|no|fi|dk).*$",
                  "", netloc, flags=re.I)
    name = re.sub(r"^(careers?|jobs?|work|join)\.", "", name, flags=re.I)
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
    Checks for the Teamtailor fingerprint in static HTML.
    Sites that rely on JS rendering will appear as ✗ — they can't be
    scraped with requests/BeautifulSoup anyway.
    """
    html = fetch_html(base_url)
    if html is None:
        return False, False

    is_tt = TEAMTAILOR_FINGERPRINT in html
    if not is_tt:
        return False, False

    people_html = fetch_html(base_url.rstrip("/") + "/people")
    has_people = bool(people_html and TEAMTAILOR_FINGERPRINT in people_html)
    return True, has_people


# ── Step 1: crt.sh Certificate Transparency ──────────────────────────────────

def discover_via_crtsh() -> set[str]:
    """
    Query crt.sh for all SSL certs issued to *.teamtailor.com.
    Returns normalised https://subdomain.teamtailor.com base URLs.
    """
    print("\n[Step 1] Querying crt.sh for *.teamtailor.com certificates …")
    crt_url = "https://crt.sh/?q=%.teamtailor.com&output=json"

    try:
        resp = requests.get(crt_url, headers={"User-Agent": HEADERS["User-Agent"]},
                            timeout=30)
        resp.raise_for_status()
        entries = resp.json()
    except Exception as e:
        print(f"  [crt.sh] Error: {e}")
        return set()

    found: set[str] = set()
    for entry in entries:
        raw = entry.get("name_value", "")
        # name_value can contain multiple names separated by newlines
        for name in raw.splitlines():
            name = name.strip().lstrip("*.")
            if not name.endswith(".teamtailor.com"):
                continue
            # skip wildcards and bare teamtailor.com itself
            if name == "teamtailor.com" or name.startswith("*."):
                continue
            url = f"https://{name}"
            found.add(url)

    print(f"  -> {len(found)} unique *.teamtailor.com subdomains found")
    return found


# ── Step 2: DuckDuckGo (custom-domain Teamtailor sites) ──────────────────────

def _ddg_search(query: str, max_results: int = 10) -> list[str]:
    """
    Hits the DuckDuckGo HTML endpoint and extracts result URLs.
    No API key required.  Returns a list of raw result URLs.
    """
    urls = []
    try:
        params = {"q": query, "kl": "us-en", "kp": "-1"}
        resp = requests.get("https://html.duckduckgo.com/html/",
                            params=params, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return []
        # DDG HTML embeds result URLs in <a class="result__url"> or as
        # redirect links like //duckduckgo.com/l/?uddg=<encoded_url>
        for match in re.finditer(r'uddg=([^&"]+)', resp.text):
            decoded = urllib.parse.unquote(match.group(1))
            if decoded.startswith("http"):
                urls.append(decoded)
                if len(urls) >= max_results:
                    break
    except Exception as e:
        print(f"  [DDG] Error: {e}")
    return urls


def discover_via_duckduckgo() -> set[str]:
    """
    Run DuckDuckGo queries to surface custom-domain Teamtailor sites.
    Returns normalised base URLs.
    """
    print(f"\n[Step 2] Running {len(DDG_QUERIES)} DuckDuckGo queries …")
    found: set[str] = set()

    for i, query in enumerate(DDG_QUERIES, 1):
        print(f"  [{i:02d}/{len(DDG_QUERIES)}] {query}")
        for raw_url in _ddg_search(query, max_results=10):
            # Skip native teamtailor.com subdomains (already caught by crt.sh)
            if "teamtailor.com" in raw_url:
                continue
            base = normalise_base_url(raw_url)
            if base:
                found.add(base)
                print(f"           + {base}")
        # Polite delay between queries
        time.sleep(3 + random.uniform(0, 2))

    print(f"  -> {len(found)} unique custom-domain candidates from DDG")
    return found


# ── Step 3: Merge all sources ─────────────────────────────────────────────────

def build_candidate_list(crt_urls: set[str], ddg_urls: set[str]) -> list[dict]:
    seen: dict[str, dict] = {}

    for name, url in SEED_COMPANIES:
        base = normalise_base_url(url)
        if base:
            seen[base] = {"company_name": name, "base_url": base, "source": "seed"}

    for url in crt_urls:
        if url not in seen:
            seen[url] = {
                "company_name": extract_company_name(url),
                "base_url": url,
                "source": "crt.sh",
            }

    for url in ddg_urls:
        if url not in seen:
            seen[url] = {
                "company_name": extract_company_name(url),
                "base_url": url,
                "source": "duckduckgo",
            }

    candidates = list(seen.values())
    print(f"\n[Step 3] Total unique candidates: {len(candidates)}")
    return candidates


# ── Step 4: Verify ────────────────────────────────────────────────────────────

def verify_candidates(candidates: list[dict]) -> list[dict]:
    if SKIP_VERIFY:
        print("\n[Step 4] Skipping verification (SKIP_VERIFY=1).")
        checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return [{**c, "people_url": "", "is_teamtailor": None,
                 "has_people_page": None, "checked_at": checked_at}
                for c in candidates]

    limit = MAX_VERIFY if MAX_VERIFY > 0 else len(candidates)
    batch = candidates[:limit]

    print(f"\n[Step 4] Verifying {len(batch)} of {len(candidates)} candidates "
          f"(MAX_VERIFY={MAX_VERIFY if MAX_VERIFY > 0 else 'all'}) …")
    if len(batch) < len(candidates):
        print(f"  (set MAX_VERIFY=0 to verify all {len(candidates)})")

    checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    results = []

    for i, c in enumerate(batch, 1):
        base = c["base_url"]
        print(f"  [{i:03d}/{len(batch)}] {base}", end="", flush=True)

        is_tt, has_people = verify_teamtailor(base)
        people_url = (base.rstrip("/") + "/people") if has_people else ""

        label = ("✓ Teamtailor + /people" if has_people
                 else ("~ Teamtailor (no /people)" if is_tt
                       else "✗ Not Teamtailor / JS-rendered"))
        print(f"  {label}")

        results.append({
            **c,
            "people_url":      people_url,
            "is_teamtailor":   is_tt,
            "has_people_page": has_people,
            "checked_at":      checked_at,
        })

        if i < len(batch):
            time.sleep(0.6 + random.uniform(0, 0.4))

    # Append unverified remainder
    for c in candidates[limit:]:
        results.append({**c, "people_url": "", "is_teamtailor": None,
                        "has_people_page": None, "checked_at": checked_at})

    return results


# ── Step 5: Output ────────────────────────────────────────────────────────────

def save_csv(rows: list[dict]) -> None:
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nSaved {len(rows)} rows → {OUTPUT_FILE.resolve()}")


def print_summary(rows: list[dict]) -> None:
    tt_count     = sum(1 for r in rows if r["is_teamtailor"])
    people_count = sum(1 for r in rows if r["has_people_page"])
    print("\n" + "─" * 70)
    print(f"Total candidates  : {len(rows)}")
    print(f"Confirmed Teamtailor (SSR) : {tt_count}")
    print(f"Have /people page : {people_count}  ← ready for scrap-career-page.py")
    print("─" * 70)
    if people_count:
        print("\nCompanies with /people pages:")
        for r in rows:
            if r["has_people_page"]:
                print(f"  {r['company_name']:<35}  {r['people_url']}")
    print(
        "\nTip: companies marked ✗ may use JS rendering — open them in a browser"
        "\n     to check manually; if they have /people, add them as seeds."
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Teamtailor Company Finder")
    print("Discovery: crt.sh  +  DuckDuckGo  +  seed list")
    print("=" * 70)

    crt_urls = discover_via_crtsh()
    ddg_urls = discover_via_duckduckgo()
    candidates = build_candidate_list(crt_urls, ddg_urls)
    results = verify_candidates(candidates)
    save_csv(results)
    print_summary(results)

    print(
        "\nNext step:\n"
        "  python scrap-career-page.py <people_url_from_csv>"
    )


if __name__ == "__main__":
    main()
