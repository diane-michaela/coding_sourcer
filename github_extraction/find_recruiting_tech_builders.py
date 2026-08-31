"""
GitHub sourcer: finds individual GitHub users building hiring/sourcing/recruiter tooling
(ATS integrations, sourcing automation, recruiter CRMs, talent tools, etc.) as a candidate
pool for engineering hires who already understand the RecOps/HR-tech domain.

- Searches GitHub repos per keyword: "hiring", "sourcing", "recruiter"
  (matched in name/description/readme/topics, non-fork, pushed in the lookback window)
- Keeps only individual Users (skips Organizations) and one repo per owner (first match kept)
- Enriches each owner via GET /users/{login}: name, location, email, blog, LinkedIn, X, extra links
- Writes a local Excel file with clickable hyperlinks (falls back to CSV on failure)

No Google Sheets writes — local output only.

Requires: requests, pandas, openpyxl, python-dotenv
Env: GITHUB_TOKEN (loaded from .env in this folder)
"""

import os
import re
import random
import time
import typing as t
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config ----------------
GITHUB_API = "https://api.github.com"

KEYWORDS = [
    "sourcing automation",
    "talent sourcing",
    "candidate sourcing",
    "recruiter crm",
    "recruiting crm",
    "recruitment automation",
    "hiring automation",
    "ats integration",
    "applicant tracking",
    "recruiter tool",
]

# Post-filter (client-side): skip repos whose name+description contain these noise terms.
# NOTE: GitHub's search API does not reliably negate multi-word quoted phrases in full-text
# (readme) search — e.g. `-"event sourcing"` also suppresses plain "sourcing" matches. So
# exclusion is done here in Python instead of via `-"..."` query syntax.
EXCLUDE_TERMS = [
    "event sourcing",
    "leetcode",
    "interview prep",
    "interview preparation",
    "coding interview",
    "cqrs",
]

LOOKBACK_DAYS = 730  # only repos pushed in the last ~2 years (active builders)
PER_PAGE = 100
MAX_REPOS_PER_KEYWORD = 40  # cap per keyword to stay within search rate limits
TIMEOUT = 20

INCLUDE_ORGS = False  # keep individual users only (candidates, not company orgs)

OUTPUT_XLSX = "github_hiring_sourcing_recruiter_candidates.xlsx"

PAGE_SLEEP_RANGE = (0.2, 0.8)

# ---------------- Token ----------------
TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/vnd.github+json",
    "User-Agent": "recruiting-tech-sourcer/1.0",
})
if TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {TOKEN}"})
else:
    print("No GITHUB_TOKEN found in .env — running unauthenticated (very low rate limit).")

_OWNER_CACHE: dict[str, dict] = {}


# ---------------- HTTP helpers ----------------
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


# ---------------- Search ----------------
def build_query(keyword: str, window_start: datetime, window_end: datetime) -> str:
    start_date = window_start.date().isoformat()
    end_date = window_end.date().isoformat()
    keyword_query = f'"{keyword}"' if " " in keyword else keyword
    return (
        f'{keyword_query} in:name,description,readme,topics '
        f'pushed:{start_date}..{end_date} fork:false'
    ).strip()


def is_noise(repo: dict) -> bool:
    text = f"{repo.get('name', '')} {repo.get('description', '') or ''}".lower()
    return any(term in text for term in EXCLUDE_TERMS)


def search_repositories(query: str, max_results: int) -> t.Iterable[dict]:
    page = 1
    seen = 0
    while seen < max_results:
        qp = quote_plus(query)
        url = f"{GITHUB_API}/search/repositories?q={qp}&per_page={PER_PAGE}&page={page}"
        data = get(url).json()
        items = data.get("items") or []
        if not items:
            break
        for repo in items:
            yield repo
            seen += 1
            if seen >= max_results:
                break
        page += 1
        time.sleep(random.uniform(*PAGE_SLEEP_RANGE))


def fetch_owner(login: str) -> dict:
    if not login:
        return {}
    if login in _OWNER_CACHE:
        return _OWNER_CACHE[login]
    try:
        data = get(f"{GITHUB_API}/users/{login}").json()
    except Exception:
        data = {}
    _OWNER_CACHE[login] = data or {}
    return _OWNER_CACHE[login]


def owner_fields(owner_json: dict) -> dict:
    name = (owner_json.get("name") or "").strip()
    email = (owner_json.get("email") or "").strip()
    location = (owner_json.get("location") or "").strip()
    blog_raw = (owner_json.get("blog") or "").strip()
    blog = normalize_url(blog_raw)
    bio = (owner_json.get("bio") or "").strip()
    company = (owner_json.get("company") or "").strip()
    twitter_user = (owner_json.get("twitter_username") or "").strip()
    x_url = normalize_url(f"https://twitter.com/{twitter_user}") if twitter_user else ""
    linkedin = extract_first_linkedin(blog_raw, bio, company)
    extra = urls_from_text(bio)
    for known in [blog, linkedin, x_url]:
        if known and known in extra:
            extra.remove(known)
    return {
        "owner_name": name,
        "owner_email": email,
        "owner_location": location,
        "owner_blog": blog,
        "owner_x": x_url,
        "owner_linkedin": linkedin,
        "owner_extra_links": "; ".join(extra),
    }


def write_excel_with_fallback(df: pd.DataFrame, filename: str) -> Path:
    out = Path(__file__).with_name(filename)
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        wb = load_workbook(out)
        ws = wb.active
        headers = {c.value: i for i, c in enumerate(next(ws.iter_rows(min_row=1, max_row=1)), start=1)}

        link_cols = {
            "profile_url": "GitHub",
            "repo_url": "Repo",
            "blog": "Website",
            "twitter": "X",
            "linkedin": "LinkedIn",
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


# ---------------- Main ----------------
def main():
    print("TOKEN present:", bool(TOKEN))
    window_end = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = window_end - timedelta(days=LOOKBACK_DAYS)
    print(f"Window: {window_start.date()} -> {window_end.date()}")

    seen_owners: set[str] = set()
    rows: list[dict] = []

    for keyword in KEYWORDS:
        query = build_query(keyword, window_start, window_end)
        print(f"\n[{keyword}] query: {query}")
        count_this_keyword = 0
        for repo in search_repositories(query, MAX_REPOS_PER_KEYWORD):
            if is_noise(repo):
                continue
            owner = repo.get("owner") or {}
            login = owner.get("login") or ""
            owner_type = owner.get("type") or ""
            if not login:
                continue
            if not INCLUDE_ORGS and owner_type != "User":
                continue
            if login in seen_owners:
                continue  # first matching repo per owner only
            seen_owners.add(login)

            ojson = fetch_owner(login)
            o = owner_fields(ojson) if ojson else {
                "owner_name": "", "owner_email": "", "owner_location": "", "owner_blog": "",
                "owner_x": "", "owner_linkedin": "", "owner_extra_links": "",
            }

            rows.append({
                "login": login,
                "name": o["owner_name"],
                "location": o["owner_location"],
                "profile_url": owner.get("html_url", ""),
                "matched_keyword": keyword,
                "repo_name": repo.get("full_name", ""),
                "repo_url": repo.get("html_url", ""),
                "repo_description": repo.get("description", "") or "",
                "repo_stars": repo.get("stargazers_count", 0),
                "repo_pushed_at": repo.get("pushed_at", ""),
                "linkedin": o["owner_linkedin"],
                "email": o["owner_email"],
                "twitter": o["owner_x"],
                "blog": o["owner_blog"],
                "extra_links": o["owner_extra_links"],
            })
            count_this_keyword += 1
        print(f"[{keyword}] kept {count_this_keyword} new owners")

    print(f"\nTotal unique candidates: {len(rows)}")
    if not rows:
        print("No results — nothing written.")
        return

    df = pd.DataFrame(rows)
    write_excel_with_fallback(df, OUTPUT_XLSX)


if __name__ == "__main__":
    main()
