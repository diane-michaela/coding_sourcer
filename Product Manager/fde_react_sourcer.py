"""
GitHub repo sourcer for Forward Deployed Engineer / PM Engineer / Designer Engineer profiles.

Duplicated from lisp.py (kept untouched — that one feeds the production ML/NLP
Google Sheet, do not repoint it). This version:
- Targets the PhantomBuster stack instead of "lisp": React, TypeScript, Storybook,
  Tailwind CSS, Jest/Cypress, Node/Redis/PostgreSQL, Docker/Ansible, GH Actions/CircleCI
- Runs several BASE_QUERIES (one per stack signal) instead of a single query —
  GitHub repo search returns almost nothing if you AND too many topics at once
- Writes a local Excel file instead of the production Google Sheet — no
  google_service_account.json / gspread dependency needed to run this
- No location filter — owner country/city are still geocoded and included as
  columns for manual review, but no rows are dropped based on them

NOT wired to run automatically. Review BASE_QUERIES below,
then run manually: python fde_react_sourcer.py

Requires:
- requests, pandas, openpyxl
- optional: geopy (only needed if using Nominatim geocoding)

Env:
  GITHUB_TOKEN            (reused from .env, same as lisp.py)
  GEO_PROVIDER            "google" or "nominatim" (optional, default nominatim)
  GOOGLE_MAPS_API_KEY     (optional)
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

# ---------------- Config ----------------
GITHUB_API = "https://api.github.com"

# Free-text README/description search, NOT topic: tags.
# Checked live counts before picking these — topic: is a curated tag most personal
# repos never set (topic:storybook = 3.7k repos vs "storybook" in:readme = 75k+).
# Free text over-matches too though, so each query combines 2-3 stack terms and a
# stars floor to bias toward real, finished projects over one-commit scaffolds.
# Counts below are what each query returned when checked (language:TypeScript, fork:false implicit via owner filter):
BASE_QUERIES = [
    'storybook cypress tailwindcss in:readme,description language:TypeScript stars:>1',   # ~47 repos — tight, full design-system+testing signal
    '"design system" react in:readme,description language:TypeScript stars:>3',           # ~3.7k repos — broader design-system-building signal
    'agent llm react tailwindcss in:readme,description language:TypeScript stars:>3',     # ~212 repos — AI-agentic React builders, closest to the FDE persona
    'react redis postgresql in:readme,description language:TypeScript stars:>5',          # ~1.3k repos — full-stack signal (front+back), not just front-end
]
# Deliberately dropped: a lone "forward deployed engineer" text search — without exact-phrase
# quoting it over-matches (~41k, mostly unrelated), and even quoted it mostly surfaces AI
# startups' own repos/job postings, not individual candidates. Not a usable signal on its own.

STATE_FILE = Path(__file__).with_name("state_fde_react.json")
FIRST_RUN_LOOKBACK_DAYS = 180      # wider than lisp.py's 62 — this stack is less niche, want more history
WINDOW_OVERLAP_HOURS = 12

PER_PAGE = 100
MAX_REPOS_PER_QUERY = 100   # cap per BASE_QUERY (not global) — 5 queries x 100 = 500 repos max per run
TIMEOUT = 20

DEFAULT_XLSX = "github_fde_react_candidates.xlsx"

PAGE_SLEEP_RANGE = (0.2, 0.8)

INCLUDE_CONTRIBUTORS = os.getenv("INCLUDE_CONTRIBUTORS", "true").strip().lower() in ("1", "true", "yes")
TOP_N_CONTRIBUTORS = int(os.getenv("TOP_N_CONTRIBUTORS", "5"))
MIN_STARS_FOR_CONTRIB = int(os.getenv("MIN_STARS_FOR_CONTRIB", "2"))

GEO_PROVIDER = (os.getenv("GEO_PROVIDER") or "").strip().lower()
GOOGLE_MAPS_API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

# Separate cache file — isolated from lisp.py's geocode_cache.json on purpose
GEO_CACHE_FILE = Path(__file__).with_name("geocode_cache_fde.json")
_GEO_CACHE: dict[str, dict] = {}

_BAD_LOCATIONS = {
    "", "remote", "worldwide", "earth", "somewhere", "internet", "everywhere", "global", "online",
    "anywhere", "planet earth", "the internet", "github", "home",
}

# ---------------- Token -----------------
ENV_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/vnd.github+json",
    "User-Agent": "fde-react-sourcer/1.0",
})
if ENV_TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {ENV_TOKEN}"})

_OWNER_CACHE: dict[str, dict] = {}
_CONTRIB_CACHE: dict[str, dict] = {}


# ---------------- Diagnostics ----------------
def print_auth_diagnostics() -> None:
    print(f"Token present: {bool(ENV_TOKEN)}")
    if not ENV_TOKEN:
        print("No GITHUB_TOKEN found — running unauthenticated, very low rate limit.")


def print_rate_limit_snapshot() -> None:
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


# ---------------- Helpers (unchanged from lisp.py) ----------------
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
                val = (cell.value or "").strip()
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
    max_attempts = 6
    base_sleep = 2.0
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)

            if resp.status_code == 401:
                resp.raise_for_status()

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


def build_created_query(base_query: str, window_start: datetime, window_end: datetime) -> str:
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()
    return f"{base_query.strip()} created:{start_date}..{end_date}".strip()


def build_pushed_query(base_query: str, window_start: datetime, window_end: datetime) -> str:
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()
    return f"{base_query.strip()} pushed:{start_date}..{end_date}".strip()


def search_repositories(query: str, max_repos: int) -> t.Iterable[dict]:
    page = 1
    seen = 0
    while True:
        qp = quote_plus(query)
        url = f"{GITHUB_API}/search/repositories?q={qp}&per_page={PER_PAGE}&page={page}"
        data = get(url).json()
        items = data.get("items") or []
        if not items:
            break

        for repo in items:
            yield repo
            seen += 1
            if seen >= max_repos:
                return

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


def fetch_top_contributors(owner_login: str, repo_name: str, top_n: int) -> dict:
    cache_key = f"{owner_login}/{repo_name}:{top_n}"
    if cache_key in _CONTRIB_CACHE:
        return _CONTRIB_CACHE[cache_key]
    url = f"{GITHUB_API}/repos/{owner_login}/{repo_name}/contributors?per_page={top_n}&anon=false"
    try:
        data = get(url).json()
        if not isinstance(data, list):
            result = {"contributors_top": "", "contributors_top_n": 0}
        else:
            logins = [str(c.get("login", "")) for c in data if c.get("login")]
            result = {"contributors_top": "; ".join(logins), "contributors_top_n": len(logins)}
    except Exception:
        result = {"contributors_top": "", "contributors_top_n": 0}
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
        "owner_location": location,
        "owner_bio": bio,
        "owner_blog": blog,
        "owner_x": x_url,
        "owner_linkedin": linkedin,
        "owner_extra_links": extra_links,
    }


# ---------------- Geocoding (cache + providers, unchanged from lisp.py) ----------------
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
        GEO_CACHE_FILE.write_text(json.dumps(_GEO_CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _geo_empty(provider: str = "") -> dict:
    return {
        "owner_location_norm": "", "owner_city": "", "owner_region": "", "owner_country": "",
        "owner_country_code": "", "owner_lat": "", "owner_lon": "",
        "owner_geocode_provider": provider, "owner_geocode_status": "EMPTY",
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
        lat, lon = geom.get("lat"), geom.get("lng")
        comps = top.get("address_components") or []
        comp_map = {}
        for c in comps:
            for ty in (c.get("types") or []):
                comp_map.setdefault(ty, c)

        def _long(ty): return (comp_map.get(ty) or {}).get("long_name") or ""
        def _short(ty): return (comp_map.get(ty) or {}).get("short_name") or ""

        out = {
            "owner_location_norm": formatted,
            "owner_city": _long("locality") or _long("postal_town") or _long("administrative_area_level_3"),
            "owner_region": _long("administrative_area_level_1"),
            "owner_country": _long("country"),
            "owner_country_code": _short("country"),
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
    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        from geopy.exc import GeocoderInsufficientPrivileges

        geolocator = Nominatim(user_agent="diane-rocher-fde-react-sourcer/1.0", timeout=10)
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.2, max_retries=2, error_wait_seconds=2.0)

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
        out = {
            "owner_location_norm": loc.address or "",
            "owner_city": addr.get("city") or addr.get("town") or addr.get("village") or "",
            "owner_region": addr.get("state") or addr.get("region") or "",
            "owner_country": addr.get("country") or "",
            "owner_country_code": (addr.get("country_code") or "").upper(),
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
    provider = GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim")
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
    last = None
    ts = state.get("last_successful_scan_utc")
    if ts:
        try:
            last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            pass
    start = (last - timedelta(hours=WINDOW_OVERLAP_HOURS)) if last else (end - timedelta(days=FIRST_RUN_LOOKBACK_DAYS))
    return start.replace(microsecond=0), end


def _build_row_from_repo(repo: dict, base_query: str, scan_kind: str) -> dict:
    owner = repo.get("owner") or {}
    owner_login = owner.get("login") or ""
    owner_url = owner.get("html_url") or ""
    owner_type = owner.get("type") or ""  # "User" or "Organization" — already in the search payload, no extra call needed
    ojson = fetch_owner(owner_login) if owner_login else {}
    o = owner_fields(ojson) if ojson else {
        "owner_name": "", "owner_email": "", "owner_location": "", "owner_bio": "",
        "owner_blog": "", "owner_x": "", "owner_linkedin": "", "owner_extra_links": "",
    }
    geo = geocode_and_normalize(o["owner_location"])
    return {
        "repo_full_name": repo.get("full_name", ""),
        "repo_url": repo.get("html_url", ""),
        "description": repo.get("description", "") or "",
        "language": repo.get("language", "") or "",
        "stars": repo.get("stargazers_count", 0),
        "forks": repo.get("forks_count", 0),
        "created_at": repo.get("created_at", ""),
        "pushed_at": repo.get("pushed_at", ""),
        "owner_login": owner_login,
        "owner_url": owner_url,
        "owner_type": owner_type,
        "owner_name": o["owner_name"],
        "owner_location": o["owner_location"],
        "owner_bio": o["owner_bio"],
        "owner_email": o["owner_email"],
        "owner_blog": o["owner_blog"],
        "owner_x": o["owner_x"],
        "owner_linkedin": o["owner_linkedin"],
        "owner_extra_links": o["owner_extra_links"],
        **geo,
        "matched_query": base_query,
        "scan_kind": scan_kind,
        "contributors_top": "",
        "contributors_top_n": 0,
    }


def _passes_owner_type_filter(row: dict) -> bool:
    # Company/org repos surface for text searches like design-system or agent
    # keywords (their own docs, not a candidate's personal project) — exclude them.
    return (row.get("owner_type") or "").strip() == "User"


# ---------------- Main ----------------
def main():
    print_auth_diagnostics()
    print_rate_limit_snapshot()
    load_geo_cache()

    window_start, window_end = compute_window()
    print("Window:", window_start.isoformat(), "->", window_end.isoformat())
    print("Geocoding provider:", GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim"))

    all_rows: dict[str, dict] = {}

    for base_query in BASE_QUERIES:
        for scan_kind, build_fn in (("created", build_created_query), ("pushed", build_pushed_query)):
            query = build_fn(base_query, window_start, window_end)
            print(f"\nQuery ({scan_kind}): {query}")
            seen = 0
            for repo in search_repositories(query, MAX_REPOS_PER_QUERY):
                row = _build_row_from_repo(repo, base_query, scan_kind)
                key = (row.get("repo_full_name") or "").strip()
                if key:
                    all_rows[key] = row  # last write wins — pushed scan refreshes created-scan rows
                seen += 1
                if seen % 25 == 0:
                    save_geo_cache()
                    print(f"  progress: {seen} repos")
            print(f"  -> {seen} repos for this query")

    print(f"\nTotal unique repos before filters: {len(all_rows)}")
    user_owned = [r for r in all_rows.values() if _passes_owner_type_filter(r)]
    print(f"After owner-type filter (User only, orgs dropped): {len(user_owned)}")
    kept_rows = user_owned

    # Contributors (optional, rate-limit aware) — only for kept rows
    if INCLUDE_CONTRIBUTORS:
        fetched, skipped_low_stars = 0, 0
        for r in kept_rows:
            if (r.get("stars") or 0) < MIN_STARS_FOR_CONTRIB:
                skipped_low_stars += 1
                continue
            owner_login = r.get("owner_login", "")
            parts = (r.get("repo_full_name") or "").split("/", 1)
            repo_name = parts[1] if len(parts) == 2 else ""
            if owner_login and repo_name:
                data = fetch_top_contributors(owner_login, repo_name, TOP_N_CONTRIBUTORS)
                r["contributors_top"] = data["contributors_top"]
                r["contributors_top_n"] = data["contributors_top_n"]
                fetched += 1
        print(f"Contributors: fetched={fetched}, skipped_low_stars={skipped_low_stars}")

    run_id = uuid.uuid4().hex[:10]
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for r in kept_rows:
        r["run_id"] = run_id
        r["run_timestamp_utc"] = run_ts

    df = pd.DataFrame(kept_rows)
    write_excel_with_fallback(df, DEFAULT_XLSX)

    state = load_state()
    state["last_successful_scan_utc"] = window_end.isoformat()
    save_state(state)
    save_geo_cache()
    print("\nDone. Review the Excel before deciding on next steps — nothing was pushed to Airtable/Sheets/Slack.")


if __name__ == "__main__":
    main()
