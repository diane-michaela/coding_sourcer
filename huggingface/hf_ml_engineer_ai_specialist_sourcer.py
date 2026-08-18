"""
Hugging Face Hub Sourcer — Machine Learning Engineer / AI Specialist (trial)

Standalone script, separate from huggingface_retrieval_sourcer_excel.py (does not
share code or config with it).

What it does:
- Searches HF Hub models + datasets + Spaces for JD keywords (agentic, bedrock,
  agent framework, RAG, langchain, fine-tuning, prompt engineering, mlops, mlflow)
- Resolves each matched repo's author to an individual (skips org-owned repos
  unless a specific commit contributor can be identified — INDIVIDUALS_ONLY)
- Enriches each candidate: bio, affiliated orgs, github/twitter/linkedin/website
  (scraped from their public profile page — best-effort, HF exposes no bio API),
  best-effort location guess (flagged unreliable), activity recency + prolific-ness
- Scores + summarizes via Claude Haiku Batch API (same pattern as
  phantombuster-api/rank_profiles.py): plain-language "what they built" summary
  + a 0-100 JD-match score, sorted best-first
- Exports a recruiter-friendly Excel (with hyperlinks) + CSV fallback

Explicitly out of scope for this trial (per request):
- No TeamTailor / Airtable cross-check or push
- No automated org location lookup — the org/company name is captured, but its
  location is left as a blank column for manual follow-up
- No fixed target count — the goal is a shortlist small/clean enough to message
  everyone on it, not to hit a number

Usage:
    python hf_ml_engineer_ai_specialist_sourcer.py --dry-run          # free preview, no Claude spend
    python hf_ml_engineer_ai_specialist_sourcer.py                    # full run incl. Claude scoring
    python hf_ml_engineer_ai_specialist_sourcer.py --resume-batch-id <id>

Prerequisites (huggingface/.env):
    HF_TOKEN            — optional but recommended (higher HF rate limits)
    ANTHROPIC_API_KEY   — required unless --dry-run
"""

import argparse
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import anthropic
import pandas as pd
import requests
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request as AnthropicBatchRequest
from openpyxl import load_workbook
from openpyxl.styles import Font
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HF_BASE = "https://huggingface.co"

JD_TITLE = "Machine Learning Engineer / AI Specialist"
JD_KEYWORDS = [
    "agentic",
    "bedrock",
    "agent framework",
    "retrieval augmented generation",
    "RAG",
    "langchain",
    "fine-tuning",
    "prompt engineering",
    "mlops",
    "mlflow",
]
ASSET_KINDS = ["models", "datasets", "spaces"]

SEARCH_LIMIT_PER_QUERY = 100
LOOKBACK_START_YEAR = 2023          # soft bound on search volume, not a candidate filter
INDIVIDUALS_ONLY = True             # skip org-owned repos unless a named contributor is found
MAX_CONTRIBUTORS_PER_ORG_REPO = 3
COMMITS_FETCH_LIMIT = 30
MAX_CANDIDATES = 400                # cost/runtime ceiling before enrichment+scoring
FETCH_README = True
MAX_README_FETCHES = 300
README_SNIPPET_CHARS = 700
FETCH_SOCIAL_LINKS = True
MAX_REPOS_IN_PROMPT = 5

TIMEOUT = 20
REQUEST_SLEEP_RANGE = (0.1, 0.35)

OUTPUT_BASENAME = "hf_ml_engineer_ai_specialist_candidates"
BATCH_ID_FILE = Path(__file__).with_name(".ml_engineer_ai_specialist_last_batch_id")
CANDIDATES_CACHE_FILE = Path(__file__).with_name(".ml_engineer_ai_specialist_candidates_cache.json")

COLUMN_ORDER = [
    "name", "hf_username", "hf_profile_link", "jd_match_score", "summary", "score_reasons",
    "org_company", "org_source", "org_location (manual follow-up — not automated)",
    "location_guess", "location_confidence", "last_activity", "prolific_signal",
    "github_link", "linkedin_link",
    "top_repo", "top_repo_summary",
    "matched_keywords", "sample_repos",
]

# Best-effort, explicitly unreliable location signals (flag emoji + a handful of city names in bios)
FLAG_EMOJI_COUNTRY = {
    "🇫🇷": "France", "🇺🇸": "United States", "🇬🇧": "United Kingdom", "🇩🇪": "Germany",
    "🇪🇸": "Spain", "🇮🇹": "Italy", "🇵🇹": "Portugal", "🇳🇱": "Netherlands", "🇧🇪": "Belgium",
    "🇨🇭": "Switzerland", "🇮🇳": "India", "🇨🇦": "Canada", "🇧🇷": "Brazil", "🇯🇵": "Japan",
    "🇨🇳": "China", "🇰🇷": "South Korea", "🇦🇺": "Australia", "🇸🇬": "Singapore",
    "🇮🇪": "Ireland", "🇸🇪": "Sweden", "🇵🇱": "Poland", "🇲🇽": "Mexico", "🇦🇷": "Argentina",
    "🇮🇱": "Israel", "🇦🇪": "United Arab Emirates", "🇿🇦": "South Africa", "🇳🇬": "Nigeria",
    "🇷🇺": "Russia", "🇺🇦": "Ukraine", "🇹🇷": "Turkey", "🇻🇳": "Vietnam", "🇮🇩": "Indonesia",
}
CITY_HINTS = {
    "paris": "Paris, France", "london": "London, UK", "berlin": "Berlin, Germany",
    "san francisco": "San Francisco, USA", "new york": "New York, USA", "nyc": "New York, USA",
    "bangalore": "Bangalore, India", "bengaluru": "Bangalore, India", "singapore": "Singapore",
    "toronto": "Toronto, Canada", "amsterdam": "Amsterdam, Netherlands", "barcelona": "Barcelona, Spain",
    "madrid": "Madrid, Spain", "lisbon": "Lisbon, Portugal", "montreal": "Montreal, Canada",
    "seattle": "Seattle, USA", "austin": "Austin, USA", "boston": "Boston, USA",
    "tel aviv": "Tel Aviv, Israel", "shanghai": "Shanghai, China", "beijing": "Beijing, China",
    "tokyo": "Tokyo, Japan", "sydney": "Sydney, Australia", "dublin": "Dublin, Ireland",
}

SOCIAL_LINK_RE = re.compile(r'<a class="truncate text-gray-600 hover:underline" href="([^"]+)"')

SCORING_SYSTEM_PROMPT = f"""You are a technical sourcing assistant helping a recruiter evaluate Hugging Face \
Hub contributors against this open role:

{JD_TITLE}
Core signals we're looking for: agentic systems / agent frameworks, AWS Bedrock, retrieval-augmented \
generation (RAG), LangChain, LLM fine-tuning, prompt engineering, MLOps, MLflow.

For each candidate you'll get their HF username, bio, the repos they've published or contributed to that \
matched our search (with tags and a short README excerpt when available), and basic activity stats.

You'll also get their single most recent matched repo highlighted separately (with its README excerpt when \
available) — summarize what THAT specific repo/module actually is and does, distinct from the person-level \
summary.

Respond with ONLY a JSON object, no explanation, no markdown fences:
{{"summary": "<1-2 plain-language sentences describing what this person actually built or worked on, \
written for a non-technical recruiter>", "jd_match_score": <integer 0-100, how well their public HF work \
matches the role above>, "score_reasons": "<one short sentence naming the specific signals that drove the \
score, or the gaps that lowered it>", "repo_summary": "<2-3 short plain-language sentences describing what \
their single highlighted repo/module actually is and does, for a non-technical recruiter>"}}"""


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def load_env():
    env_path = Path(__file__).with_name(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


load_env()

try:
    from token_hf import HF_TOKEN as FILE_TOKEN  # optional legacy fallback shared with the other HF script
except Exception:
    FILE_TOKEN = ""

HF_TOKEN = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACEHUB_API_TOKEN") or FILE_TOKEN or ""
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "pb-recops-hf-sourcer/1.0"})
if HF_TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {HF_TOKEN}"})
else:
    print("No HF token found (HF_TOKEN / HUGGINGFACEHUB_API_TOKEN env var or token_hf.py). Running unauthenticated.")

HTML_SESSION = requests.Session()
HTML_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; RecOpsSourcer/1.0)"})

_NS_CACHE: dict = {}
_README_FETCH_COUNT = 0


# ---------------------------------------------------------------------------
# Resilient GET
# ---------------------------------------------------------------------------

def get(session: requests.Session, url: str, **kwargs):
    max_attempts = 5
    base_sleep = 1.5
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, **kwargs)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 502, 503, 504):
                time.sleep(base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.6))
                continue
            resp.raise_for_status()
            return resp
        except (ReadTimeout, ConnectionError, HTTPError):
            time.sleep(base_sleep * attempt)
    return None


def parse_iso8601(dt: str):
    if not dt:
        return None
    dt = dt.strip()
    if dt.endswith("Z"):
        dt = dt[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def year_in_range(last_modified: str) -> bool:
    d = parse_iso8601(last_modified)
    return bool(d and d.year >= LOOKBACK_START_YEAR)


# ---------------------------------------------------------------------------
# HF Hub search
# ---------------------------------------------------------------------------

def hf_search(kind: str, query: str, limit: int) -> list:
    resp = get(SESSION, f"{HF_BASE}/api/{kind}", params={"search": query, "limit": limit, "full": "true"})
    if resp is None:
        return []
    try:
        return resp.json() or []
    except Exception:
        return []


def search_and_index(keywords: list, asset_kinds: list, limit: int) -> dict:
    repo_index: dict = {}
    for kind in asset_kinds:
        for kw in keywords:
            hits = hf_search(kind, kw, limit)
            print(f"  [{kind}] '{kw}': {len(hits)} hits")
            for h in hits:
                if not year_in_range(h.get("lastModified", "")):
                    continue
                repo_id = h.get("id")
                author = h.get("author") or ""
                if not repo_id or not author:
                    continue
                key = (kind, repo_id)
                if key not in repo_index:
                    repo_index[key] = {
                        "kind": kind,
                        "id": repo_id,
                        "author": author,
                        "lastModified": h.get("lastModified", ""),
                        "likes": h.get("likes") or 0,
                        "downloads": h.get("downloads") or 0,
                        "tags": h.get("tags") or [],
                        "matched_keywords": {kw},
                    }
                else:
                    repo_index[key]["matched_keywords"].add(kw)
            time.sleep(random.uniform(*REQUEST_SLEEP_RANGE))
    return repo_index


# ---------------------------------------------------------------------------
# Author / org resolution
# ---------------------------------------------------------------------------

def classify_namespace(namespace: str):
    """Return ('user'|'org'|'unknown', overview_json)."""
    if namespace in _NS_CACHE:
        return _NS_CACHE[namespace]
    resp = get(SESSION, f"{HF_BASE}/api/users/{quote(namespace, safe='')}/overview")
    if resp is not None:
        result = ("user", resp.json() or {})
        _NS_CACHE[namespace] = result
        return result
    resp = get(SESSION, f"{HF_BASE}/api/organizations/{quote(namespace, safe='')}/overview")
    if resp is not None:
        result = ("org", resp.json() or {})
        _NS_CACHE[namespace] = result
        return result
    result = ("unknown", {})
    _NS_CACHE[namespace] = result
    return result


def fetch_repo_contributors(kind: str, repo_id: str) -> list:
    url = f"{HF_BASE}/api/{kind}/{quote(repo_id, safe='/')}/commits/main"
    resp = get(SESSION, url, params={"limit": COMMITS_FETCH_LIMIT})
    if resp is None:
        return []
    try:
        commits = resp.json() or []
    except Exception:
        return []
    counts = Counter()
    for c in commits:
        for a in c.get("authors", []):
            u = a.get("user")
            if u:
                counts[u] += 1
    return [u for u, _ in counts.most_common(MAX_CONTRIBUTORS_PER_ORG_REPO)]


def build_candidates(repo_index: dict):
    candidates = defaultdict(lambda: {"repos": [], "matched_keywords": set(), "org_from_repo": set()})
    skipped_org = 0
    for (kind, repo_id), rec in repo_index.items():
        namespace = rec["author"]
        ns_type, ns_overview = classify_namespace(namespace)

        if ns_type == "user":
            owners = [namespace]
            org_name = None
        elif ns_type == "org":
            contributors = fetch_repo_contributors(kind, repo_id) if INDIVIDUALS_ONLY else []
            if contributors:
                owners = contributors
                org_name = ns_overview.get("fullname") or namespace
            elif not INDIVIDUALS_ONLY:
                owners = [namespace]
                org_name = ns_overview.get("fullname") or namespace
            else:
                skipped_org += 1
                continue
        else:
            continue

        for owner in owners:
            cand = candidates[owner]
            cand["repos"].append(rec)
            cand["matched_keywords"] |= rec["matched_keywords"]
            if org_name:
                cand["org_from_repo"].add(org_name)
        time.sleep(random.uniform(*REQUEST_SLEEP_RANGE))
    return candidates, skipped_org


# ---------------------------------------------------------------------------
# Enrichment: bio, social links, location guess, README snippets
# ---------------------------------------------------------------------------

def guess_location(bio: str):
    if not bio:
        return "", "no location signal found in bio"
    for flag, country in FLAG_EMOJI_COUNTRY.items():
        if flag in bio:
            return country, "best-effort — inferred from bio flag emoji, unreliable"
    lower = bio.lower()
    for hint, place in CITY_HINTS.items():
        if hint in lower:
            return place, "best-effort — inferred from bio keyword, unreliable"
    return "", "no location signal found in bio"


def fetch_profile_social_links(username: str) -> dict:
    resp = get(HTML_SESSION, f"{HF_BASE}/{quote(username, safe='')}")
    out = {"github": "", "twitter": "", "linkedin": "", "website": ""}
    if resp is None:
        return out
    for url in SOCIAL_LINK_RE.findall(resp.text):
        low = url.lower()
        if "github.com" in low and not out["github"]:
            out["github"] = url
        elif ("twitter.com" in low or "x.com" in low) and not out["twitter"]:
            out["twitter"] = url
        elif "linkedin.com" in low and not out["linkedin"]:
            out["linkedin"] = url
        elif not out["website"] and "huggingface.co" not in low and "bsky.app" not in low:
            out["website"] = url
    return out


def repo_url_path(kind: str, repo_id: str) -> str:
    prefix = {"models": "", "datasets": "datasets/", "spaces": "spaces/"}[kind]
    return f"{prefix}{repo_id}"


def fetch_readme_snippet(kind: str, repo_id: str) -> str:
    global _README_FETCH_COUNT
    if _README_FETCH_COUNT >= MAX_README_FETCHES:
        return ""
    _README_FETCH_COUNT += 1
    path = repo_url_path(kind, repo_id)
    resp = get(SESSION, f"{HF_BASE}/{path}/raw/main/README.md")
    if resp is None:
        return ""
    text = re.sub(r"^---.*?---\s*", "", resp.text, flags=re.DOTALL)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:README_SNIPPET_CHARS]


def enrich_candidates(candidates: dict) -> list:
    kind_label = {"models": "model", "datasets": "dataset", "spaces": "space"}
    rows = []
    for username, cand in candidates.items():
        ns_type, overview = classify_namespace(username)
        fullname = (overview.get("fullname") or "").strip()
        bio = (overview.get("details") or "").strip()

        org_source = ""
        if cand["org_from_repo"]:
            org_company = ", ".join(sorted(cand["org_from_repo"]))
            org_source = "contributed-to-repo"
        else:
            orgs = overview.get("orgs") or []
            names = [o.get("fullname") or o.get("name") for o in orgs[:3] if o.get("fullname") or o.get("name")]
            org_company = ", ".join(names)
            if org_company:
                org_source = "self-listed-affiliation"

        location_guess, location_confidence = guess_location(bio)
        social = fetch_profile_social_links(username) if FETCH_SOCIAL_LINKS else {}

        repos_sorted = sorted(cand["repos"], key=lambda r: r.get("lastModified", ""), reverse=True)
        dates = [d for d in (parse_iso8601(r.get("lastModified", "")) for r in repos_sorted) if d]
        last_activity = max(dates).date().isoformat() if dates else ""
        total_likes = sum(r.get("likes") or 0 for r in cand["repos"])
        total_downloads = sum(r.get("downloads") or 0 for r in cand["repos"])
        hf_totals = (
            f"HF totals: {overview.get('numModels', 0)}M/{overview.get('numDatasets', 0)}D/"
            f"{overview.get('numSpaces', 0)}S, {overview.get('numFollowers', 0)} followers"
        )
        prolific_signal = f"{len(cand['repos'])} matching repo(s) ({total_likes} likes, {total_downloads} downloads) · {hf_totals}"

        prompt_repos = []
        for idx, r in enumerate(repos_sorted[:MAX_REPOS_IN_PROMPT]):
            snippet = fetch_readme_snippet(r["kind"], r["id"]) if (FETCH_README and idx == 0) else ""
            prompt_repos.append({
                "kind": kind_label.get(r["kind"], r["kind"]),
                "id": r["id"],
                "tags": r.get("tags", []),
                "readme_snippet": snippet,
            })

        sample_repos = "; ".join(
            f"{kind_label.get(r['kind'], r['kind'])}:{r['id']}" for r in repos_sorted[:3]
        )
        top_repo = f"{prompt_repos[0]['kind']}:{prompt_repos[0]['id']}" if prompt_repos else ""

        rows.append({
            "name": fullname or username,
            "hf_username": username,
            "hf_profile_link": f"{HF_BASE}/{username}",
            "jd_match_score": "",
            "summary": "",
            "score_reasons": "",
            "org_company": org_company,
            "org_source": org_source,
            "org_location (manual follow-up — not automated)": "",
            "location_guess": location_guess,
            "location_confidence": location_confidence,
            "last_activity": last_activity,
            "prolific_signal": prolific_signal,
            "github_link": social.get("github", ""),
            "linkedin_link": social.get("linkedin", ""),
            "top_repo": top_repo,
            "top_repo_summary": "",
            "matched_keywords": ", ".join(sorted(cand["matched_keywords"])),
            "sample_repos": sample_repos,
            "_repo_count": len(cand["repos"]),
            "_bio": bio,
            "_prompt_repos": prompt_repos,
        })
        time.sleep(random.uniform(*REQUEST_SLEEP_RANGE))
    return rows


def naive_summary_for_row(row: dict) -> str:
    repos = row.get("_prompt_repos") or []
    top = ", ".join(f"{r['kind']} {r['id']}" for r in repos[:2])
    return f"Matched on: {row['matched_keywords']}. Recent work: {top or 'n/a'}."


def naive_repo_summary_for_row(row: dict) -> str:
    repos = row.get("_prompt_repos") or []
    if not repos:
        return ""
    top = repos[0]
    if top.get("readme_snippet"):
        return top["readme_snippet"][:280]
    tags = ", ".join(top.get("tags", [])[:6])
    return f"{top['kind'].capitalize()} tagged: {tags}" if tags else ""


# ---------------------------------------------------------------------------
# Claude Haiku Batch scoring (same pattern as phantombuster-api/rank_profiles.py)
# ---------------------------------------------------------------------------

def build_prompt_body(row: dict) -> str:
    lines = [
        f"Username: {row['hf_username']}",
        f"Display name: {row['name']}",
        f"Bio: {row['_bio'] or '(none)'}",
        f"Matched keywords: {row['matched_keywords']}",
        "Repos:",
    ]
    for r in row["_prompt_repos"]:
        tag_str = ", ".join(r["tags"][:8])
        snippet = f" — {r['readme_snippet']}" if r.get("readme_snippet") else ""
        lines.append(f"- [{r['kind']}] {r['id']} (tags: {tag_str}){snippet}")
    lines.append(f"Activity: {row['prolific_signal']}. Most recent matching activity: {row['last_activity'] or 'unknown'}.")
    return "\n".join(lines)


def build_requests(rows: list) -> list:
    reqs = []
    for i, row in enumerate(rows):
        reqs.append(AnthropicBatchRequest(
            custom_id=str(i),
            params=MessageCreateParamsNonStreaming(
                model="claude-haiku-4-5",
                max_tokens=300,
                system=SCORING_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": build_prompt_body(row)}],
            ),
        ))
    return reqs


def submit_batch(client: anthropic.Anthropic, requests_batch: list) -> str:
    print(f"Submitting batch of {len(requests_batch):,} requests…")
    batch = client.messages.batches.create(requests=requests_batch)
    BATCH_ID_FILE.write_text(batch.id)
    print(f"Batch submitted: {batch.id} (saved to {BATCH_ID_FILE} in case you need to resume)")
    return batch.id


def wait_for_batch(client: anthropic.Anthropic, batch_id: str) -> None:
    print("Waiting for batch to complete (checking every 60s)…")
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        counts = batch.request_counts
        print(f"  Status: {batch.processing_status} | processing={counts.processing} "
              f"succeeded={counts.succeeded} errored={counts.errored}")
        if batch.processing_status == "ended":
            break
        time.sleep(60)
    print("Batch complete.")


def parse_score_json(raw: str) -> dict:
    text = raw.strip()
    text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if m:
        text = m.group(0)
    try:
        parsed = json.loads(text)
        score = max(0, min(100, int(parsed.get("jd_match_score", 0))))
        return {
            "jd_match_score": score,
            "summary": str(parsed.get("summary", "")).strip(),
            "score_reasons": str(parsed.get("score_reasons", "")).strip(),
            "repo_summary": str(parsed.get("repo_summary", "")).strip(),
        }
    except Exception:
        return {"jd_match_score": 0, "summary": "", "score_reasons": "(could not parse model output)", "repo_summary": ""}


def collect_results(client: anthropic.Anthropic, batch_id: str) -> dict:
    results = {}
    for result in client.messages.batches.results(batch_id):
        if result.result.type == "succeeded":
            msg = result.result.message
            raw = next((b.text for b in msg.content if b.type == "text"), "")
            results[result.custom_id] = parse_score_json(raw)
        else:
            results[result.custom_id] = {
                "jd_match_score": 0, "summary": "", "score_reasons": "(batch request errored)", "repo_summary": "",
            }
    return results


def apply_scores(rows: list, results: dict) -> None:
    for i, row in enumerate(rows):
        r = results.get(str(i), {"jd_match_score": 0, "summary": "", "score_reasons": "(no result)", "repo_summary": ""})
        row["jd_match_score"] = r["jd_match_score"]
        row["summary"] = r["summary"] or naive_summary_for_row(row)
        row["score_reasons"] = r["score_reasons"]
        row["top_repo_summary"] = r.get("repo_summary") or naive_repo_summary_for_row(row)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def add_hyperlinks(xlsx_path: Path, sheet_name: str, columns: list) -> None:
    wb = load_workbook(xlsx_path)
    ws = wb[sheet_name]
    header = [c.value for c in ws[1]]
    for col_name in columns:
        if col_name not in header:
            continue
        col_idx = header.index(col_name) + 1
        for row_idx in range(2, ws.max_row + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            url = cell.value
            if url and isinstance(url, str) and url.startswith("http"):
                cell.hyperlink = url
                cell.font = Font(color="0563C1", underline="single")
    wb.save(xlsx_path)


def write_output(rows: list) -> None:
    if not rows:
        print("No candidates to write.")
        return
    df = pd.DataFrame(rows)
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""
    df = df[COLUMN_ORDER]

    xlsx_path = Path(__file__).with_name(f"{OUTPUT_BASENAME}.xlsx")
    csv_path = Path(__file__).with_name(f"{OUTPUT_BASENAME}.csv")
    try:
        df.to_excel(xlsx_path, index=False, sheet_name="candidates")
        add_hyperlinks(xlsx_path, "candidates", ["hf_profile_link", "github_link", "linkedin_link"])
        print(f"Written: {xlsx_path}")
    except PermissionError:
        print(f"Could not write {xlsx_path} (open in Excel?) — CSV still written below.")
    df.to_csv(csv_path, index=False)
    print(f"Written: {csv_path}")
    print(f"\n{len(rows)} candidates, sorted best-first.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=f"Source Hugging Face Hub contributors for: {JD_TITLE}")
    p.add_argument("--dry-run", action="store_true",
                    help="Search + enrich only, skip Claude scoring (no Anthropic spend)")
    p.add_argument("--max-candidates", type=int, default=MAX_CANDIDATES,
                    help=f"Cap on candidates sent to enrichment/scoring (default {MAX_CANDIDATES})")
    p.add_argument("--search-limit", type=int, default=SEARCH_LIMIT_PER_QUERY,
                    help=f"Per-keyword HF search result cap (default {SEARCH_LIMIT_PER_QUERY})")
    p.add_argument("--keywords", type=str, default=None,
                    help="Comma-separated override of the default JD keyword list")
    p.add_argument("--asset-kinds", type=str, default=None,
                    help="Comma-separated override of models,datasets,spaces")
    p.add_argument("--resume-batch-id", type=str, default=None,
                    help="Resume a previously submitted Claude batch using the cached candidates file")
    return p.parse_args()


def main():
    args = parse_args()

    if args.resume_batch_id:
        if not CANDIDATES_CACHE_FILE.exists():
            sys.exit(f"No cached candidates file found at {CANDIDATES_CACHE_FILE}. Cannot resume.")
        rows = json.loads(CANDIDATES_CACHE_FILE.read_text())
        if not ANTHROPIC_KEY:
            sys.exit("ANTHROPIC_API_KEY not set.")
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        wait_for_batch(client, args.resume_batch_id)
        results = collect_results(client, args.resume_batch_id)
        apply_scores(rows, results)
        rows.sort(key=lambda r: r["jd_match_score"] if isinstance(r["jd_match_score"], int) else -1, reverse=True)
        write_output(rows)
        return

    keywords = [k.strip() for k in args.keywords.split(",")] if args.keywords else JD_KEYWORDS
    asset_kinds = [k.strip() for k in args.asset_kinds.split(",")] if args.asset_kinds else ASSET_KINDS

    print(f"Searching Hugging Face Hub for: {JD_TITLE}")
    print(f"Keywords: {', '.join(keywords)}")
    print(f"Asset kinds: {', '.join(asset_kinds)}")

    repo_index = search_and_index(keywords, asset_kinds, args.search_limit)
    print(f"{len(repo_index)} unique repos matched (lastModified >= {LOOKBACK_START_YEAR}).")

    candidates, skipped_org = build_candidates(repo_index)
    print(f"{len(candidates)} candidate individuals identified "
          f"({skipped_org} org-owned repos skipped — no identifiable individual contributor).")

    if not candidates:
        print("Nothing to enrich. Try widening --keywords, --asset-kinds, or LOOKBACK_START_YEAR.")
        return

    if len(candidates) > args.max_candidates:
        print(f"Capping to top {args.max_candidates} by matched-repo count for enrichment/scoring "
              f"— re-run with --max-candidates to widen.")
        candidates = dict(sorted(candidates.items(), key=lambda kv: len(kv[1]["repos"]), reverse=True)[:args.max_candidates])

    print("Enriching candidate profiles (bio, social links, location, README snippets)…")
    rows = enrich_candidates(candidates)
    print(f"Enriched {len(rows)} candidate profiles.")

    if args.dry_run:
        for row in rows:
            row["jd_match_score"] = ""
            row["summary"] = naive_summary_for_row(row)
            row["score_reasons"] = "(dry run — not scored by Claude)"
            row["top_repo_summary"] = naive_repo_summary_for_row(row)
        rows.sort(key=lambda r: r["_repo_count"], reverse=True)
        write_output(rows)
        print("Dry run complete — no Anthropic spend.")
        return

    if not ANTHROPIC_KEY:
        sys.exit("ANTHROPIC_API_KEY not set. Add it to huggingface/.env, or use --dry-run to skip scoring.")

    CANDIDATES_CACHE_FILE.write_text(json.dumps(rows))

    n = len(rows)
    est_input_tokens = n * 350
    est_output_tokens = n * 90
    est_cost = (est_input_tokens / 1_000_000 * 0.50) + (est_output_tokens / 1_000_000 * 2.50)
    print(f"Estimated cost: ${est_cost:.3f} for {n} candidates (Haiku 4.5 batch pricing)")

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    requests_batch = build_requests(rows)
    batch_id = submit_batch(client, requests_batch)
    wait_for_batch(client, batch_id)
    results = collect_results(client, batch_id)
    apply_scores(rows, results)
    rows.sort(key=lambda r: r["jd_match_score"] if isinstance(r["jd_match_score"], int) else -1, reverse=True)
    write_output(rows)


if __name__ == "__main__":
    main()
