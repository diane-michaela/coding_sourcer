"""
One-shot GitHub candidate discovery for the "Designer Engineer" hybrid profile
(Figma + React/TypeScript, real design-token/design-system fluency) — person-centric,
CSV output.

Unlike lisp.py/NLP.py/figma_react.py, this is NOT a recurring rolling-window pipeline:
it runs a fixed set of targeted queries once, enriches every repo's owner + contributors
+ PR authors (no star-count gate — the signal we want, like a real Figma-tooling side
project, often sits at 0 stars), and aggregates results into ONE ROW PER PERSON (not per
repo), so it's easy to scan people directly rather than hunt through a repo list.

Queries were chosen from what actually distinguishes real Figma-to-code practitioners
from generic "figma react" hits: Figma's MCP server, Code Connect, design tokens,
Tailwind, Storybook, and the explicit "design engineer" title.

Run:
  python figma_react_candidates.py
Output:
  figma_react_candidates.csv (in this folder), one row per person.
"""

import os
import re
import csv
import time
import random
from pathlib import Path

import requests
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

from dotenv import load_dotenv
load_dotenv()

GITHUB_API = "https://api.github.com"
TIMEOUT = 20

QUERIES = [
    "figma mcp",
    "figma code connect",
    "claude code figma skill",
    "design tokens figma react",
    "figma variables tailwind",
    "storybook figma",
    "figma design system react",
    "\"design engineer\" figma react",
]
MAX_REPOS_PER_QUERY = 8
TOP_N_CONTRIBUTORS = 5
TOP_N_PR_AUTHORS = 5

OUT_CSV = "figma_react_candidates.csv"

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_NOREPLY_EMAIL_PATTERNS = [
    re.compile(r".*@users\.noreply\.github\.com$"),
    re.compile(r".*noreply.*"),
]

# Curated tech-stack vocabulary for the "keywords" column. Matched case-insensitively
# against repo name + description + README against word boundaries (hyphens/spaces
# treated as equivalent), so "design-tokens" and "design tokens" both hit.
KEYWORD_VOCAB = [
    "figma", "figma mcp", "figma api", "figma plugin", "code connect", "code syntax",
    "design tokens", "design system", "design engineer", "design-to-code",
    "react", "typescript", "tailwind", "tailwindcss", "css variables", "dtcg",
    "storybook", "component library", "ui kit", "next.js", "nextjs", "vue", "svelte",
    "claude code", "claude skill", "agent skill", "mcp server", "model context protocol",
    "ai agent", "chrome extension", "vscode extension", "cursor",
]


def extract_keywords(*texts: str) -> list[str]:
    haystack = " ".join(t for t in texts if t).lower()
    haystack_normalized = re.sub(r"[-_]", " ", haystack)
    found = []
    for term in KEYWORD_VOCAB:
        term_normalized = re.sub(r"[-_]", " ", term.lower())
        pattern = r"\b" + re.escape(term_normalized) + r"\b"
        if re.search(pattern, haystack_normalized):
            found.append(term)
    return found

# ---------------- Token -----------------
ENV_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()
SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/vnd.github+json",
    "User-Agent": "figma-react-candidates-script/1.0",
})
if ENV_TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {ENV_TOKEN}"})
else:
    print("No GITHUB_TOKEN found - running unauthenticated (very low rate limit).")


def get(url: str, params: dict = None) -> requests.Response:
    max_attempts = 6
    base_sleep = 2.0
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=TIMEOUT)
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
                time.sleep(sleep)
                continue
            resp.raise_for_status()
            return resp
        except (ReadTimeout, ConnectionError, HTTPError) as e:
            last_exc = e
            sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 1)
            time.sleep(sleep)
    raise RuntimeError(f"GET failed after retries: {last_exc}")


def search_repos(query: str, max_results: int) -> list[dict]:
    """Best-match relevance order (no sort=stars) so 0-star hidden gems aren't buried."""
    data = get(f"{GITHUB_API}/search/repositories", params={
        "q": query, "per_page": min(max_results, 100),
    }).json()
    return (data.get("items") or [])[:max_results]


_OWNER_CACHE: dict[str, dict] = {}


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


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
    return url


def urls_from_text(text: str) -> list[str]:
    if not text:
        return []
    urls = re.findall(r"(https?://[^\s)]+)", text, flags=re.IGNORECASE)
    out = []
    for u in urls:
        nu = normalize_url(u.rstrip(".,);]}>\"'"))
        if nu and nu not in out:
            out.append(nu)
    return out


def extract_first_linkedin(*fields: str) -> str:
    for field in fields:
        if not field:
            continue
        for u in urls_from_text(field):
            if "linkedin.com" in u.lower():
                return u
    return ""


def is_valid_contact_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    email_lower = email.strip().lower()
    if any(p.match(email_lower) for p in _NOREPLY_EMAIL_PATTERNS):
        return False
    return bool(EMAIL_REGEX.fullmatch(email_lower))


def fetch_events_email(login: str) -> str:
    if not login:
        return ""
    try:
        data = get(f"{GITHUB_API}/users/{login}/events/public", params={"per_page": 100}).json()
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
    text = " ".join(f for f in fields if f)
    for candidate in EMAIL_REGEX.findall(text):
        if is_valid_contact_email(candidate):
            return candidate
    return ""


def resolve_email(login: str, owner_json: dict) -> tuple[str, str]:
    """3-tier fallback: profile email -> PushEvent commit email -> bio/blog regex."""
    profile_email = (owner_json.get("email") or "").strip()
    if profile_email and is_valid_contact_email(profile_email):
        return profile_email, "profile"
    event_email = fetch_events_email(login)
    if event_email:
        return event_email, "push_event"
    bio_email = email_from_text(owner_json.get("bio", "") or "", owner_json.get("blog", "") or "")
    if bio_email:
        return bio_email, "bio_blog"
    return "", ""


def fetch_readme_text(owner_login: str, repo_name: str) -> str:
    """Plain-text README via GitHub's rendering-free raw fetch. Empty string on any failure
    (private README, no README, rate limit) - callers must handle that gracefully."""
    try:
        resp = SESSION.get(
            f"{GITHUB_API}/repos/{owner_login}/{repo_name}/readme",
            headers={"Accept": "application/vnd.github.raw+json"},
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return ""
        return resp.text
    except Exception:
        return ""


_MD_BADGE_LINE = re.compile(r"^\s*(\[!\[|\[!\[.*\]\(.*\)\]\(.*\)|!\[)", re.IGNORECASE)
_MD_HEADING = re.compile(r"^#+\s*")
_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_EMPHASIS = re.compile(r"[*_`]")
_HTML_TAG = re.compile(r"<[^>]+>")


def summarize_readme(readme_text: str, max_len: int = 240) -> str:
    """First substantive paragraph of a README: skip badges, headings, HTML, blank lines."""
    if not readme_text:
        return ""
    for raw_line in readme_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if _MD_BADGE_LINE.match(line) or _HTML_TAG.match(line):
            continue
        line = _MD_HEADING.sub("", line)
        line = _MD_LINK.sub(r"\1", line)
        line = _MD_EMPHASIS.sub("", line)
        line = _HTML_TAG.sub("", line).strip()
        if len(line) < 15:
            continue
        return line[:max_len].rstrip() + ("..." if len(line) > max_len else "")
    return ""


_REPO_DETAIL_CACHE: dict[str, dict] = {}


def fetch_repo_summary_and_keywords(repo: dict) -> dict:
    """Repo-level enrichment, cached per repo since multiple people share the same repo:
    a real summary (GitHub description, falling back to the README's opening paragraph
    when the description is missing or too thin) plus matched tech-stack keywords."""
    full_name = repo.get("full_name", "")
    if full_name in _REPO_DETAIL_CACHE:
        return _REPO_DETAIL_CACHE[full_name]

    description = (repo.get("description") or "").strip()
    topics = repo.get("topics") or []
    owner_login = (repo.get("owner") or {}).get("login", "")
    repo_name = repo.get("name", "") or full_name.split("/")[-1]

    readme_text = ""
    if len(description) < 25 and owner_login and repo_name:
        readme_text = fetch_readme_text(owner_login, repo_name)

    summary = description if len(description) >= 25 else (summarize_readme(readme_text) or description)
    keywords = extract_keywords(full_name, description, "; ".join(topics), readme_text[:3000])

    result = {
        "repo_summary": summary,
        "repo_topics": "; ".join(topics),
        "keywords": "; ".join(keywords),
    }
    _REPO_DETAIL_CACHE[full_name] = result
    return result


def fetch_contributors_and_pr_authors(owner_login: str, repo_name: str) -> list[str]:
    """Merged talent pool: repo contributors + PR authors, deduped, bots excluded."""
    logins, seen = [], set()
    try:
        data = get(f"{GITHUB_API}/repos/{owner_login}/{repo_name}/contributors",
                   params={"per_page": TOP_N_CONTRIBUTORS, "anon": "false"}).json()
        if isinstance(data, list):
            for c in data:
                login = str(c.get("login", "") or "")
                if login and login not in seen and not login.endswith("[bot]"):
                    seen.add(login)
                    logins.append(login)
    except Exception:
        pass
    try:
        data = get(f"{GITHUB_API}/repos/{owner_login}/{repo_name}/pulls",
                   params={"state": "all", "sort": "created", "direction": "desc",
                           "per_page": TOP_N_PR_AUTHORS}).json()
        if isinstance(data, list):
            for pr in data:
                login = ((pr.get("user") or {}).get("login") or "").strip()
                if login and login not in seen and not login.endswith("[bot]"):
                    seen.add(login)
                    logins.append(login)
    except Exception:
        pass
    return logins


def build_person(login: str, role: str, repo: dict, matched_query: str) -> dict:
    owner_json = fetch_owner(login)
    email, email_source = resolve_email(login, owner_json)
    bio = owner_json.get("bio", "") or ""
    blog = normalize_url(owner_json.get("blog", "") or "")
    repo_detail = fetch_repo_summary_and_keywords(repo)
    return {
        "login": login,
        "name": owner_json.get("name", "") or "",
        "roles": {role},
        "email": email,
        "email_source": email_source,
        "location": owner_json.get("location", "") or "",
        "bio": bio,
        "blog": blog,
        "linkedin": extract_first_linkedin(blog, bio),
        "github_url": f"https://github.com/{login}",
        "repos": {repo.get("full_name", "")},
        "max_repo_stars": repo.get("stargazers_count", 0) or 0,
        "top_repo_url": repo.get("html_url", ""),
        "top_repo_summary": repo_detail["repo_summary"],
        "top_repo_topics": repo_detail["repo_topics"],
        "top_repo_keywords": repo_detail["keywords"],
        "matched_queries": {matched_query},
    }


def main():
    print("GITHUB_TOKEN present:", bool(ENV_TOKEN))
    people_by_login: dict[str, dict] = {}

    for query in QUERIES:
        print(f"\n=== Query: {query!r} ===")
        try:
            repos = search_repos(query, MAX_REPOS_PER_QUERY)
        except Exception as e:
            print(f"  search failed: {e}")
            continue
        print(f"  {len(repos)} repos")

        for repo in repos:
            full_name = repo.get("full_name", "")
            owner = repo.get("owner") or {}
            owner_login = owner.get("login") or ""
            print(f"  - {full_name} (stars={repo.get('stargazers_count', 0)})")

            candidates: list[tuple[str, str]] = []
            if owner_login:
                candidates.append((owner_login, "owner"))

            parts = full_name.split("/", 1)
            repo_name = parts[1] if len(parts) == 2 else ""
            if owner_login and repo_name:
                for login in fetch_contributors_and_pr_authors(owner_login, repo_name):
                    if login != owner_login:
                        candidates.append((login, "contributor_or_pr_author"))

            for login, role in candidates:
                new_person = build_person(login, role, repo, query)
                if login in people_by_login:
                    existing = people_by_login[login]
                    existing["roles"] |= new_person["roles"]
                    existing["repos"] |= new_person["repos"]
                    existing["matched_queries"] |= new_person["matched_queries"]
                    if new_person["max_repo_stars"] > existing["max_repo_stars"]:
                        existing["max_repo_stars"] = new_person["max_repo_stars"]
                        existing["top_repo_url"] = new_person["top_repo_url"]
                        existing["top_repo_summary"] = new_person["top_repo_summary"]
                        existing["top_repo_topics"] = new_person["top_repo_topics"]
                        existing["top_repo_keywords"] = new_person["top_repo_keywords"]
                    if not existing["email"] and new_person["email"]:
                        existing["email"] = new_person["email"]
                        existing["email_source"] = new_person["email_source"]
                else:
                    people_by_login[login] = new_person

    people = list(people_by_login.values())
    for p in people:
        p["roles"] = "; ".join(sorted(p["roles"]))
        p["repos"] = "; ".join(sorted(p["repos"]))
        p["matched_queries"] = "; ".join(sorted(p["matched_queries"]))
    people.sort(key=lambda p: ("owner" not in p["roles"], -(p["max_repo_stars"] or 0)))

    out_path = Path(__file__).with_name(OUT_CSV)
    fieldnames = ["login", "name", "roles", "email", "email_source", "location", "bio",
                  "blog", "linkedin", "github_url", "repos", "max_repo_stars",
                  "top_repo_url", "top_repo_summary", "top_repo_topics", "top_repo_keywords",
                  "matched_queries"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(people)

    print(f"\nWrote {len(people)} people to {out_path.resolve()}")
    with_email = sum(1 for p in people if p["email"])
    print(f"With email resolved: {with_email}/{len(people)}")


if __name__ == "__main__":
    main()
