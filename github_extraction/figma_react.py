"""
GitHub repo sourcer for Figma + React design engineers (repo-centric) + owner enrichment
+ location geocoding/normalization.

Targets the "product design + engineering" hybrid: people who bridge Figma (design source
of truth) and React (implementation) — design-system authors, design-token/component-library
maintainers, design-to-code tooling builders. Same architecture as lisp.py, adapted to:
- BASE_QUERY = "figma react" instead of a single-language keyword
- Standalone Excel output (no Google Sheets/service-account setup required) so it's
  immediately runnable; merges into the existing .xlsx on each run instead of a live upsert

- Searches GitHub repos for BASE_QUERY + created/pushed date range (from state/window)
- For each repo: collects repo fields
- For each owner: fetches profile fields (cached) and enriches with:
  owner_name, owner_email, owner_location (raw), blog/website, X, LinkedIn, extra links
- Geocodes/normalizes the owner's location (raw text) into:
  owner_location_norm, owner_city, owner_region, owner_country, owner_country_code, owner_lat, owner_lon
  + provider + status
- For NEW repos: pulls contributors + PR authors (merged talent pool), each resolved
  with an email via a 3-tier fallback (profile -> PushEvent commit email -> bio/blog regex)
- Writes clickable Excel (or CSV fallback), merging with the existing file by repo_full_name

Providers:
- Google Geocoding API (if GOOGLE_MAPS_API_KEY set, or GEO_PROVIDER=google)
- Nominatim (OSM) fallback by default (or GEO_PROVIDER=nominatim)

Requires:
- requests, pandas, openpyxl
- optional: geopy (only needed if using Nominatim)

Terminal (VS Code):
  pip install -r requirements.txt
Env:
  setx GITHUB_TOKEN "...."
  (optional) setx GOOGLE_MAPS_API_KEY "...."

Run:
  python figma_react.py
Output:
  github_repos_figma_react_design_engineers.xlsx (in this folder)
"""

import os
import re
import json
import time
import random
import uuid
import typing as t
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
import pandas as pd

from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

from dotenv import load_dotenv
load_dotenv()

print("TOKEN present:", bool(os.getenv("GITHUB_TOKEN")))


# ---------------- Config ----------------
GITHUB_API = "https://api.github.com"

BASE_QUERY = "figma react"

STATE_FILE = Path(__file__).with_name("state_figma_react.json")
FIRST_RUN_LOOKBACK_DAYS = 62       # ~2 months
WINDOW_OVERLAP_HOURS = 12          # safety overlap

PER_PAGE = 100   # GitHub Search supports up to 100
MAX_REPOS = 200
TIMEOUT = 20

DEFAULT_XLSX = "github_repos_figma_react_design_engineers.xlsx"

# Gentle pacing to reduce abuse detection on /search endpoints
PAGE_SLEEP_RANGE = (0.2, 0.8)  # seconds (randomized)

# Contributors (only for new repos; rate-limit safe)
INCLUDE_CONTRIBUTORS = os.getenv("INCLUDE_CONTRIBUTORS", "true").strip().lower() in ("1", "true", "yes")
TOP_N_CONTRIBUTORS = int(os.getenv("TOP_N_CONTRIBUTORS", "5"))
MIN_STARS_FOR_CONTRIB = int(os.getenv("MIN_STARS_FOR_CONTRIB", "2"))
REFRESH_CONTRIBUTORS_ON_UPDATE = os.getenv("REFRESH_CONTRIBUTORS_ON_UPDATE", "false").strip().lower() in ("1", "true", "yes")
_CONTRIB_CACHE: dict[str, dict] = {}

# PR authors: merged into the same contributor pool (catches people who only opened PRs)
TOP_N_PR_AUTHORS = int(os.getenv("TOP_N_PR_AUTHORS", "5"))

# Email fallback chain (profile -> PushEvent commit-author email -> bio/blog regex),
# resolved for each contributor the same way the repo owner's email already is.
RESOLVE_CONTRIBUTOR_EMAILS = os.getenv("RESOLVE_CONTRIBUTOR_EMAILS", "true").strip().lower() in ("1", "true", "yes")
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_NOREPLY_EMAIL_PATTERNS = [
    re.compile(r".*@users\.noreply\.github\.com$"),
    re.compile(r".*noreply.*"),
]

# Geocoding
GEO_PROVIDER = (os.getenv("GEO_PROVIDER") or "").strip().lower()  # "google" or "nominatim" (optional)
GOOGLE_MAPS_API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

# Cache file for geocoding results (disk) — separate from lisp.py/NLP.py's, per this
# workspace's convention of one geocode cache file per script variant.
GEO_CACHE_FILE = Path(__file__).with_name("geocode_cache_figma_react.json")
_GEO_CACHE: dict[str, dict] = {}

# Common non-geocodable locations
_BAD_LOCATIONS = {
    "", "remote", "worldwide", "earth", "somewhere", "internet", "everywhere", "global", "online",
    "anywhere", "planet earth", "the internet", "github", "home",
}

# ---------------- Token -----------------
# Prefer env var GITHUB_TOKEN. Optionally load from token_1.py if you keep secrets locally.
try:
    from token_1 import GITHUB_TOKEN_2
except Exception:
    GITHUB_TOKEN_2 = ""

ENV_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()
FILE_TOKEN = (GITHUB_TOKEN_2 or "").strip()
TOKEN = ENV_TOKEN or FILE_TOKEN

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/vnd.github+json",
    "User-Agent": "github-sourcer-script/5.1",
})

# Use Bearer (most robust across token types)
if TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {TOKEN}"})
else:
    print("No GitHub token found (GITHUB_TOKEN env var or token_1.py:GITHUB_TOKEN_2). "
          "Running unauthenticated => very low rate limit.")

auth = SESSION.headers.get("Authorization", "")
print("TOKEN present:", bool(TOKEN))
print("Auth scheme:", auth.split(" ")[0] if auth else "NONE")
print("Auth length:", len(auth))

r = SESSION.get("https://api.github.com/rate_limit", timeout=20)
print("rate_limit status:", r.status_code)
print("rate_limit body:", r.text[:200])


# Owner enrichment (cached in-memory)
_OWNER_CACHE: dict[str, dict] = {}


# ---------------- Diagnostics ----------------
def _print_auth_diagnostics() -> None:
    source = "env:GITHUB_TOKEN" if ENV_TOKEN else ("token_1.py:GITHUB_TOKEN_2" if FILE_TOKEN else "none")
    print(f"Token source: {source}")
    print(f"Authorization header present: {'Authorization' in SESSION.headers}")


def _print_rate_limit_snapshot() -> None:
    try:
        r = SESSION.get(f"{GITHUB_API}/rate_limit", timeout=TIMEOUT)
        if r.status_code == 200:
            data = r.json() or {}
            core = (data.get("resources") or {}).get("core") or {}
            search = (data.get("resources") or {}).get("search") or {}
            print(f"RateLimit core: remaining={core.get('remaining')} reset={core.get('reset')}")
            print(f"RateLimit search: remaining={search.get('remaining')} reset={search.get('reset')}")
        else:
            print(f"RateLimit check failed: HTTP {r.status_code}")
    except Exception as e:
        print("RateLimit check failed:", e)


# ---------------- Helpers ----------------
def safe_output_path(filename: str) -> Path:
    p = Path(__file__).with_name(filename)
    try:
        p.touch(exist_ok=True)
        return p
    except PermissionError:
        return p.with_name(p.stem + "_new" + p.suffix)


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
    try:
        return url if urlparse(url).netloc else ""
    except Exception:
        return ""


def urls_from_text(text: str) -> list[str]:
    if not text:
        return []
    urls = re.findall(r"(https?://[^\s)]+)", text, flags=re.IGNORECASE)
    out: list[str] = []
    for u in urls:
        nu = normalize_url(u.rstrip(".,);]}>\"'"))
        if nu and nu not in out:
            out.append(nu)
    return out


def extract_first_linkedin(*fields: str) -> str:
    for field in fields:
        if not field:
            continue
        urls = urls_from_text(field)
        for u in urls:
            if "linkedin.com" in u.lower():
                return u
        s = field.strip()
        if "linkedin.com" in s.lower():
            idx = s.lower().find("linkedin.com")
            candidate = s[idx:].split()[0].strip().rstrip(".,);]}>\"'")
            return normalize_url(candidate)
    return ""


def write_excel_with_fallback(df: pd.DataFrame, filename: str) -> Path:
    out = safe_output_path(filename)
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        wb = load_workbook(out)
        ws = wb.active
        headers = {c.value: i for i, c in enumerate(next(ws.iter_rows(min_row=1, max_row=1)), start=1)}

        link_cols = {
            "repo_url": "Repo",
            "owner_url": "Owner",
            "owner_blog": "Website",
            "owner_x": "X",
            "owner_linkedin": "LinkedIn",
        }

        for r in range(2, ws.max_row + 1):
            for col, label in link_cols.items():
                idx = headers.get(col)
                if not idx:
                    continue
                cell = ws.cell(row=r, column=idx)
                val = (cell.value or "").strip() if isinstance(cell.value, str) else ""
                if not val:
                    continue
                url = val.split(";")[0].strip()
                if url.lower().startswith(("http://", "https://")):
                    cell.hyperlink = url
                    cell.value = label
                    cell.font = Font(color="0563C1", underline="single")

        wb.save(out)
        print(f"Excel written: {out.resolve()} ({len(df)} rows)")
        return out

    except Exception as e:
        print("Excel failed, falling back to CSV:", e)
        out_csv = out.with_suffix(".csv")
        df.to_csv(out_csv, index=False)
        print(f"CSV written: {out_csv.resolve()} ({len(df)} rows)")
        return out_csv


def get(url: str) -> requests.Response:
    """Resilient GET with retries on transient errors + rate limiting."""
    max_attempts = 6
    base_sleep = 2.0
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)

            # If auth is wrong, do NOT retry forever
            if resp.status_code == 401:
                resp.raise_for_status()

            # Rate limit handling (403 + reset header, sometimes 429 too)
            if resp.status_code in (403, 429):
                reset = resp.headers.get("X-RateLimit-Reset")
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if reset and (remaining == "0" or remaining is None):
                    wait = max(0, int(reset) - int(time.time())) + 2
                    print(f"Rate limit hit. Sleeping {wait}s...")
                    time.sleep(wait)
                    continue

            if resp.status_code in (502, 503, 504):
                sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"Transient {resp.status_code}. Retry {attempt}/{max_attempts} in {sleep:.1f}s")
                time.sleep(sleep)
                continue

            resp.raise_for_status()
            return resp

        except (ReadTimeout, ConnectionError, HTTPError) as e:
            last_exc = e
            sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 1)
            print(f"Error {e}. Retry {attempt}/{max_attempts} in {sleep:.1f}s")
            time.sleep(sleep)

    raise RuntimeError(f"GET failed after retries: {last_exc}")


def build_created_query(window_start: datetime, window_end: datetime) -> str:
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()
    return f"{BASE_QUERY.strip()} created:{start_date}..{end_date} fork:false".strip()


def build_pushed_query(window_start: datetime, window_end: datetime) -> str:
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()
    return f"{BASE_QUERY.strip()} pushed:{start_date}..{end_date} fork:false".strip()


def search_repositories(query: str) -> t.Iterable[dict]:
    page = 1
    while True:
        qp = quote_plus(query)
        url = f"{GITHUB_API}/search/repositories?q={qp}&per_page={PER_PAGE}&page={page}"
        data = get(url).json()
        items = data.get("items") or []
        if not items:
            break

        for repo in items:
            yield repo

        page += 1
        time.sleep(random.uniform(*PAGE_SLEEP_RANGE))


def fetch_owner(login: str) -> dict:
    if not login:
        return {}
    if login in _OWNER_CACHE:
        return _OWNER_CACHE[login]
    data = get(f"{GITHUB_API}/users/{login}").json()
    _OWNER_CACHE[login] = data or {}
    return _OWNER_CACHE[login]


def fetch_pr_authors(owner_login: str, repo_name: str, top_n: int) -> list[str]:
    """GET repos/{owner}/{repo}/pulls?state=all. Returns PR author logins — a talent
    pool distinct from repo contributors (catches people who only ever opened PRs)."""
    url = (f"{GITHUB_API}/repos/{owner_login}/{repo_name}/pulls"
           f"?state=all&sort=created&direction=desc&per_page={min(top_n, 100)}")
    try:
        data = get(url).json()
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    seen: set[str] = set()
    logins: list[str] = []
    for pr in data:
        login = ((pr.get("user") or {}).get("login") or "").strip()
        if login and login not in seen and not login.endswith("[bot]"):
            seen.add(login)
            logins.append(login)
    return logins[:top_n]


def is_valid_contact_email(email: str) -> bool:
    """Reject noreply/placeholder addresses; require a plausible email shape."""
    if not email or "@" not in email:
        return False
    email_lower = email.strip().lower()
    if any(p.match(email_lower) for p in _NOREPLY_EMAIL_PATTERNS):
        return False
    return bool(EMAIL_REGEX.fullmatch(email_lower))


def fetch_events_email(login: str) -> str:
    """Mine recent public PushEvents for a commit-author email. Most users never set a
    public profile email, but their commit metadata often has a real one attached."""
    if not login:
        return ""
    try:
        data = get(f"{GITHUB_API}/users/{login}/events/public?per_page=100").json()
    except Exception:
        return ""
    if not isinstance(data, list):
        return ""
    for event in data:
        if event.get("type") != "PushEvent":
            continue
        for commit in (event.get("payload") or {}).get("commits", []):
            email = ((commit.get("author") or {}).get("email") or "").strip()
            if email and is_valid_contact_email(email):
                return email
    return ""


def email_from_text(*fields: str) -> str:
    """Last-resort: regex-scan bio/blog text for a plain email address."""
    text = " ".join(f for f in fields if f)
    for candidate in EMAIL_REGEX.findall(text):
        if is_valid_contact_email(candidate):
            return candidate
    return ""


def resolve_contributor_email(login: str) -> tuple[str, str]:
    """Email fallback chain for a contributor/PR author: profile field -> PushEvent
    commit email -> bio/blog regex. Same chain already used for the repo owner."""
    ojson = fetch_owner(login)
    if not ojson:
        return "", ""
    o = owner_fields(ojson)
    profile_email = o.get("owner_email", "")
    if profile_email and is_valid_contact_email(profile_email):
        return profile_email, "profile"
    if not RESOLVE_CONTRIBUTOR_EMAILS:
        return "", ""
    event_email = fetch_events_email(login)
    if event_email:
        return event_email, "push_event"
    bio_email = email_from_text(ojson.get("bio", "") or "", o.get("owner_blog", ""))
    if bio_email:
        return bio_email, "bio_blog"
    return "", ""


def fetch_top_contributors(owner_login: str, repo_name: str, top_n: int) -> dict:
    """GET repos/{owner}/{repo}/contributors, merged with PR authors, each resolved
    with an email via the same fallback chain as the repo owner.
    Returns contributors_top, contributors_top_n, contributors_emails, contributors_with_email_n."""
    cache_key = f"{owner_login}/{repo_name}:{top_n}"
    if cache_key in _CONTRIB_CACHE:
        return _CONTRIB_CACHE[cache_key]

    logins: list[str] = []
    seen: set[str] = set()
    url = f"{GITHUB_API}/repos/{owner_login}/{repo_name}/contributors?per_page={top_n}&anon=false"
    try:
        data = get(url).json()
        if isinstance(data, list):
            for c in data:
                login = str(c.get("login", "") or "")
                if login and login not in seen:
                    seen.add(login)
                    logins.append(login)
    except Exception:
        pass

    for login in fetch_pr_authors(owner_login, repo_name, TOP_N_PR_AUTHORS):
        if login not in seen:
            seen.add(login)
            logins.append(login)

    email_pairs: list[str] = []
    with_email_n = 0
    for login in logins:
        try:
            email, source = resolve_contributor_email(login)
        except Exception:
            email, source = "", ""
        if email:
            with_email_n += 1
            email_pairs.append(f"{login}:{email} ({source})")
        else:
            email_pairs.append(f"{login}:")

    result = {
        "contributors_top": "; ".join(logins),
        "contributors_top_n": len(logins),
        "contributors_emails": "; ".join(email_pairs),
        "contributors_with_email_n": with_email_n,
    }
    _CONTRIB_CACHE[cache_key] = result
    return result


def owner_fields(owner_json: dict) -> dict:
    name = (owner_json.get("name") or "").strip()
    email = (owner_json.get("email") or "").strip()
    location = (owner_json.get("location") or "").strip()
    blog_raw = (owner_json.get("blog") or "").strip()
    blog = normalize_url(blog_raw)
    bio = (owner_json.get("bio") or "").strip()
    twitter_user = (owner_json.get("twitter_username") or "").strip()
    x_url = normalize_url(f"https://twitter.com/{twitter_user}") if twitter_user else ""
    linkedin = extract_first_linkedin(blog_raw, bio)
    extra = urls_from_text(bio)
    for known in [blog, linkedin, x_url]:
        if known and known in extra:
            extra.remove(known)
    extra_links = "; ".join(extra)

    return {
        "owner_name": name,
        "owner_email": email,
        "owner_location": location,  # raw
        "owner_blog": blog,
        "owner_x": x_url,
        "owner_linkedin": linkedin,
        "owner_extra_links": extra_links,
    }


# ---------------- Geocoding (cache + providers) ----------------
def load_geo_cache() -> None:
    global _GEO_CACHE
    if GEO_CACHE_FILE.exists():
        try:
            _GEO_CACHE = json.loads(GEO_CACHE_FILE.read_text(encoding="utf-8"))
            if not isinstance(_GEO_CACHE, dict):
                _GEO_CACHE = {}
        except Exception:
            _GEO_CACHE = {}


def save_geo_cache() -> None:
    try:
        GEO_CACHE_FILE.write_text(
            json.dumps(_GEO_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception:
        pass


def _geo_empty(provider: str = "") -> dict:
    return {
        "owner_location_norm": "",
        "owner_city": "",
        "owner_region": "",
        "owner_country": "",
        "owner_country_code": "",
        "owner_lat": "",
        "owner_lon": "",
        "owner_geocode_provider": provider,
        "owner_geocode_status": "EMPTY",
    }


def _geo_no_match(provider: str) -> dict:
    d = _geo_empty(provider)
    d["owner_geocode_status"] = "NO_MATCH"
    return d


def geocode_google(raw: str) -> dict:
    provider = "google"
    raw = (raw or "").strip()
    key = raw.lower()

    if not raw or key in _BAD_LOCATIONS:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "SKIPPED"
        return d

    if not GOOGLE_MAPS_API_KEY:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "NO_API_KEY"
        return d

    cache_key = f"{provider}:{key}"
    if cache_key in _GEO_CACHE:
        return _GEO_CACHE[cache_key]

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": raw, "key": GOOGLE_MAPS_API_KEY}

    out = _geo_no_match(provider)
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        data = r.json() if r.content else {}
        status = (data.get("status") or "").upper()

        if status != "OK":
            out["owner_geocode_status"] = status or "ERROR"
            _GEO_CACHE[cache_key] = out
            return out

        results = data.get("results") or []
        if not results:
            _GEO_CACHE[cache_key] = out
            return out

        top = results[0]
        formatted = top.get("formatted_address") or ""
        geom = (top.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lon = geom.get("lng")

        comps = top.get("address_components") or []
        comp_map = {}
        for c in comps:
            types = c.get("types") or []
            for ty in types:
                comp_map.setdefault(ty, c)

        def _long(ty: str) -> str:
            return (comp_map.get(ty) or {}).get("long_name") or ""

        def _short(ty: str) -> str:
            return (comp_map.get(ty) or {}).get("short_name") or ""

        city = _long("locality") or _long("postal_town") or _long("administrative_area_level_3")
        region = _long("administrative_area_level_1")
        country = _long("country")
        country_code = _short("country")

        out = {
            "owner_location_norm": formatted,
            "owner_city": city,
            "owner_region": region,
            "owner_country": country,
            "owner_country_code": country_code,
            "owner_lat": "" if lat is None else str(lat),
            "owner_lon": "" if lon is None else str(lon),
            "owner_geocode_provider": provider,
            "owner_geocode_status": "OK",
        }

    except Exception:
        out["owner_geocode_status"] = "ERROR"

    _GEO_CACHE[cache_key] = out
    return out


def geocode_nominatim(raw: str) -> dict:
    provider = "nominatim"
    raw = (raw or "").strip()
    key = raw.lower()

    if not raw or key in _BAD_LOCATIONS:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "SKIPPED"
        return d

    cache_key = f"{provider}:{key}"
    if cache_key in _GEO_CACHE:
        return _GEO_CACHE[cache_key]

    out = _geo_no_match(provider)

    # Lazy import (only needed if you actually use nominatim)
    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        from geopy.exc import GeocoderInsufficientPrivileges

        geolocator = Nominatim(
            user_agent="diane-rocher-github-sourcer/1.0 (contact: dianemichaela88@gmail.com)",
            timeout=10
        )

        geocode = RateLimiter(
            geolocator.geocode,
            min_delay_seconds=1.2,   # be gentle
            max_retries=2,
            error_wait_seconds=2.0
        )

        try:
            loc = geocode(raw, addressdetails=True)
        except GeocoderInsufficientPrivileges:
            out["owner_geocode_status"] = "OSM_403_BLOCKED"
            _GEO_CACHE[cache_key] = out
            return out

        if not loc:
            _GEO_CACHE[cache_key] = out
            return out

        addr = (loc.raw or {}).get("address") or {}
        city = addr.get("city") or addr.get("town") or addr.get("village") or ""
        region = addr.get("state") or addr.get("region") or ""
        country = addr.get("country") or ""
        country_code = (addr.get("country_code") or "").upper()

        out = {
            "owner_location_norm": loc.address or "",
            "owner_city": city,
            "owner_region": region,
            "owner_country": country,
            "owner_country_code": country_code,
            "owner_lat": str(loc.latitude),
            "owner_lon": str(loc.longitude),
            "owner_geocode_provider": provider,
            "owner_geocode_status": "OK",
        }

    except Exception:
        out["owner_geocode_status"] = "ERROR"

    _GEO_CACHE[cache_key] = out
    return out


def geocode_and_normalize(raw: str) -> dict:
    """
    Choose provider:
    - If GEO_PROVIDER explicitly set: use it
    - else: use google if API key present; otherwise nominatim
    """
    provider = GEO_PROVIDER
    if not provider:
        provider = "google" if GOOGLE_MAPS_API_KEY else "nominatim"

    if provider == "google":
        return geocode_google(raw)
    if provider == "nominatim":
        return geocode_nominatim(raw)

    d = _geo_empty(provider)
    d["owner_geocode_status"] = "UNKNOWN_PROVIDER"
    return d


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def compute_window() -> tuple[datetime, datetime]:
    state = load_state()
    end = datetime.now(timezone.utc).replace(microsecond=0)
    created_utc = state.get("last_successful_created_scan_utc")
    pushed_utc = state.get("last_successful_pushed_scan_utc")
    last = None
    if created_utc or pushed_utc:
        for s in (created_utc, pushed_utc):
            if s:
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    last = dt if last is None else max(last, dt)
                except Exception:
                    pass
    if last is not None:
        start = last - timedelta(hours=WINDOW_OVERLAP_HOURS)
    else:
        start = end - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)
    return start.replace(microsecond=0), end


CONTRIBUTOR_COLUMNS = ("contributors_top", "contributors_top_n", "contributors_emails", "contributors_with_email_n")

HEADER = [
    "run_id", "run_timestamp_utc", "window_start_utc", "window_end_utc", "query",
    "repo_full_name", "repo_url", "description", "language", "stars", "forks", "open_issues",
    "created_at", "updated_at", "pushed_at",
    "owner_login", "owner_url", "owner_name", "owner_location", "owner_email",
    "owner_blog", "owner_x", "owner_linkedin", "owner_extra_links",
    "contributors_top", "contributors_top_n", "contributors_emails", "contributors_with_email_n",
    "owner_location_norm", "owner_city", "owner_region", "owner_country", "owner_country_code",
    "owner_lat", "owner_lon", "owner_geocode_provider", "owner_geocode_status",
]


def load_existing_rows(xlsx_path: Path) -> dict[str, dict]:
    """Read the previous run's Excel output (if any) into {repo_full_name: row_dict}."""
    if not xlsx_path.exists():
        return {}
    try:
        df = pd.read_excel(xlsx_path)
    except Exception as e:
        print(f"Could not read existing {xlsx_path.name} (starting fresh): {e}")
        return {}
    out: dict[str, dict] = {}
    for _, rec in df.iterrows():
        d = rec.where(pd.notna(rec), "").to_dict()
        key = str(d.get("repo_full_name", "")).strip()
        if key:
            out[key] = d
    return out


def _build_row_from_repo(repo: dict, query_label: str) -> dict:
    """Build one output row from a GitHub repo dict and query label (created/pushed)."""
    owner = repo.get("owner") or {}
    owner_login = owner.get("login") or ""
    owner_url = owner.get("html_url") or ""
    ojson = fetch_owner(owner_login) if owner_login else {}
    o = owner_fields(ojson) if ojson else {
        "owner_name": "", "owner_email": "", "owner_location": "", "owner_blog": "",
        "owner_x": "", "owner_linkedin": "", "owner_extra_links": "",
    }
    geo = geocode_and_normalize(o["owner_location"])
    return {
        "repo_full_name": repo.get("full_name", ""),
        "repo_url": repo.get("html_url", ""),
        "description": repo.get("description", "") or "",
        "language": repo.get("language", "") or "",
        "stars": repo.get("stargazers_count", 0),
        "forks": repo.get("forks_count", 0),
        "open_issues": repo.get("open_issues_count", 0),
        "created_at": repo.get("created_at", ""),
        "updated_at": repo.get("updated_at", ""),
        "pushed_at": repo.get("pushed_at", ""),
        "owner_login": owner_login,
        "owner_url": owner_url,
        "owner_name": o["owner_name"],
        "owner_location": o["owner_location"],
        "owner_email": o["owner_email"],
        "owner_blog": o["owner_blog"],
        "owner_x": o["owner_x"],
        "owner_linkedin": o["owner_linkedin"],
        "owner_extra_links": o["owner_extra_links"],
        **geo,
        "query": query_label,
        "contributors_top": "",
        "contributors_top_n": 0,
        "contributors_emails": "",
        "contributors_with_email_n": 0,
    }


# ---------------- Main ----------------
def main():
    _print_auth_diagnostics()
    _print_rate_limit_snapshot()
    load_geo_cache()

    window_start, window_end = compute_window()
    print("Window:", window_start.isoformat(), "->", window_end.isoformat())
    print("Geocoding provider:", GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim"))
    print("Google API key detected:", "YES" if GOOGLE_MAPS_API_KEY else "NO (will use Nominatim unless GEO_PROVIDER=google)")

    xlsx_path = Path(__file__).with_name(DEFAULT_XLSX)
    existing_by_key = load_existing_rows(xlsx_path)
    existing_keys = set(existing_by_key)
    print(f"Existing rows loaded: {len(existing_keys)} (from {xlsx_path.name if xlsx_path.exists() else 'none'})")

    all_rows: dict[str, dict] = {}

    # Created scan
    created_query = build_created_query(window_start, window_end)
    print("Query (created):", created_query)
    seen = 0
    for repo in search_repositories(created_query):
        row = _build_row_from_repo(repo, "created")
        key = (row.get("repo_full_name") or "").strip()
        if key:
            all_rows[key] = row
        seen += 1
        if seen % 50 == 0:
            save_geo_cache()
            print(f"Progress (created): {seen} repos")
        if seen >= MAX_REPOS:
            break

    # Pushed scan (overwrites same repo with fresher data)
    pushed_query = build_pushed_query(window_start, window_end)
    print("Query (pushed):", pushed_query)
    seen2 = 0
    for repo in search_repositories(pushed_query):
        row = _build_row_from_repo(repo, "pushed")
        key = (row.get("repo_full_name") or "").strip()
        if key:
            all_rows[key] = row
        seen2 += 1
        if seen2 % 50 == 0:
            save_geo_cache()
            print(f"Progress (pushed): {seen2} repos")
        if seen2 >= MAX_REPOS:
            break

    # Contributors: only for NEW repos (not already in the existing file), subject to throttles
    contributors_fetched_count = 0
    contributors_skipped_low_stars = 0
    contributors_skipped_existing_repo = 0
    contributors_with_email_total = 0
    for key, row in all_rows.items():
        if key in existing_keys:
            contributors_skipped_existing_repo += 1
            continue
        if not INCLUDE_CONTRIBUTORS:
            continue
        if (row.get("stars") or 0) < MIN_STARS_FOR_CONTRIB:
            contributors_skipped_low_stars += 1
            continue
        try:
            owner_login = row.get("owner_login", "")
            parts = key.split("/", 1)
            repo_name = parts[1] if len(parts) == 2 else ""
            if owner_login and repo_name:
                data = fetch_top_contributors(owner_login, repo_name, TOP_N_CONTRIBUTORS)
                row["contributors_top"] = data["contributors_top"]
                row["contributors_top_n"] = data["contributors_top_n"]
                row["contributors_emails"] = data["contributors_emails"]
                row["contributors_with_email_n"] = data["contributors_with_email_n"]
                contributors_fetched_count += 1
                contributors_with_email_total += data["contributors_with_email_n"]
        except Exception:
            row["contributors_top"] = ""
            row["contributors_top_n"] = 0
            row["contributors_emails"] = ""
            row["contributors_with_email_n"] = 0
    print(f"Contributors: fetched={contributors_fetched_count}, skipped_low_stars={contributors_skipped_low_stars}, "
          f"skipped_existing_repo={contributors_skipped_existing_repo}, with_email={contributors_with_email_total}")

    run_id = uuid.uuid4().hex[:10]
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for row in all_rows.values():
        row["run_id"] = run_id
        row["run_timestamp_utc"] = run_ts
        row["window_start_utc"] = window_start.isoformat()
        row["window_end_utc"] = window_end.isoformat()

    # Merge into existing rows: new/rescanned repos overwrite, but preserve the existing
    # contributor columns for already-known repos (mirrors lisp.py's upsert_rows behavior),
    # unless REFRESH_CONTRIBUTORS_ON_UPDATE is set.
    num_appended = 0
    num_updated = 0
    final_by_key = dict(existing_by_key)
    for key, row in all_rows.items():
        if key in existing_keys:
            if not REFRESH_CONTRIBUTORS_ON_UPDATE:
                old = existing_by_key.get(key, {})
                for col in CONTRIBUTOR_COLUMNS:
                    old_val = old.get(col, "")
                    if old_val not in ("", None):
                        row[col] = old_val
            num_updated += 1
        else:
            num_appended += 1
        final_by_key[key] = row

    df = pd.DataFrame(list(final_by_key.values()), columns=HEADER)
    write_excel_with_fallback(df, DEFAULT_XLSX)
    print(f"Appended {num_appended} new rows. Updated {num_updated} existing rows. Total rows: {len(final_by_key)}.")

    state = load_state()
    state["last_successful_created_scan_utc"] = window_end.isoformat()
    state["last_successful_pushed_scan_utc"] = window_end.isoformat()
    save_state(state)
    print("Saved last_successful_created_scan_utc and last_successful_pushed_scan_utc:", window_end.isoformat())

    save_geo_cache()
    print(f"Geocode cache saved: {GEO_CACHE_FILE.resolve()}")


if __name__ == "__main__":
    main()
