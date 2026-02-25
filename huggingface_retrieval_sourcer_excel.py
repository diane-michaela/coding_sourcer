"""
Hugging Face Retrieval/Search Sourcer v2.1
(Models + Datasets + Author Enrichment + README summary + Contributors + Individuals-only + Org ranking)

What it does:
- Searches HF Hub for models/datasets using retrieval/search keywords (multiple small queries)
- Filters assets by lastModified year range (default 2023–2026)
- Enriches author namespaces (user OR organization) + caches
- Extracts a short description from README/model card (best-effort)
- Extracts best-effort contributors (commit authors) + caches
- Produces recruiter-friendly Excel with hyperlinks + CSV fallback
- Adds score + score_reasons columns (talent-signal oriented)
- Individuals-only mode (default ON): exclude org-owned repos unless individual contributors are found
- Ranking: organizations + members (best-effort inferred) via extra Excel sheets

Notes / limitations:
- HF does NOT reliably provide: email, location, real names. Many fields are optional.
- Country is best-effort guess from bio/name/website only.
- Contributors are best-effort inferred from repo commit metadata (may be empty).
"""

import os
import re
import time
import random
import typing as t
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

# ---------------- Config ----------------
HF_BASE = "https://huggingface.co"

# Retrieval-focused query plan (your lists can be pasted here)
MODEL_QUERIES_CORE = [
    "reranker", "ranker", "retrieval", "cross-encoder", "bi-encoder",
    "sentence-transformers", "text-embedding",
]
MODEL_QUERIES_EXTENDED = [
    "dense retrieval", "dual encoder", "colbert", "splade", "contriever", "dpr",
    "semantic search", "semantic similarity", "matryoshka", "binary embedding", "late interaction",
    "recommender", "recommendation", "collaborative filtering", "content-based filtering", "user embedding", "item embedding",
    "NLP", "text classification", "named entity recognition", "question answering", "fine-tuned", "distillation",
    "vector search", "similarity search", "approximate nearest neighbor", "faiss", "milvus",
]

DATASET_QUERIES_CORE = ["retrieval", "ranking", "recommendation"]
DATASET_QUERIES_EXTENDED = [
    "qrels", "hard negatives", "triplets", "query passage", "query-document pairs", "pairwise ranking",
    "user-item", "click-through", "implicit feedback",
    "BEIR", "MTEB", "MS MARCO",
]

USE_EXTENDED_QUERIES = True

# Time filtering based on lastModified
START_YEAR = 2023
END_YEAR = 2026

# Limits
MAX_ASSETS_TOTAL = 1200
LIMIT_PER_QUERY = 200
TIMEOUT = 20
PAGE_SLEEP_RANGE = (0.15, 0.6)

# Individuals-only filter (default ON)
INDIVIDUALS_ONLY = True

# README extraction control
FETCH_README = True
MAX_README_FETCHES = 800
README_TIMEOUT = 15

# Contributors extraction control
FETCH_CONTRIBUTORS = True
COMMITS_LIMIT = 50             # per repo
MAX_COMMITS_FETCHES = 1200     # cap to avoid blowing up runtime

DEFAULT_XLSX = "hf_retrieval_models_datasets_with_author_details.xlsx"

# ---------------- Token -----------------
try:
    from token_hf import HF_TOKEN as FILE_TOKEN  # type: ignore
except Exception:
    FILE_TOKEN = ""

ENV_TOKEN = os.getenv("HF_TOKEN", "") or os.getenv("HUGGINGFACEHUB_API_TOKEN", "") or ""
TOKEN = ENV_TOKEN or FILE_TOKEN or ""

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "hf-retrieval-sourcer-v2.1/1.0",
})
if TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {TOKEN}"})
else:
    print("No HF token found (HF_TOKEN / HUGGINGFACEHUB_API_TOKEN env var or token_hf.py). Running unauthenticated.")


def _print_auth_diagnostics() -> None:
    source = "env:HF_TOKEN/HUGGINGFACEHUB_API_TOKEN" if ENV_TOKEN else ("token_hf.py:HF_TOKEN" if FILE_TOKEN else "none")
    print(f"Token source: {source}")
    print(f"Authorization header present: {'Authorization' in SESSION.headers}")


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
        for u in urls_from_text(field):
            if "linkedin.com" in u.lower():
                return u
        s = field.strip()
        if "linkedin.com" in s.lower():
            idx = s.lower().find("linkedin.com")
            candidate = s[idx:].split()[0].strip().rstrip(".,);]}>\"'")
            return normalize_url(candidate)
    return ""


def parse_iso8601(dt: str) -> t.Optional[datetime]:
    if not dt:
        return None
    dt = dt.strip()
    try:
        if dt.endswith("Z"):
            dt = dt.replace("Z", "+00:00")
        return datetime.fromisoformat(dt)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(dt, fmt)
            except Exception:
                continue
    return None


def year_in_range(last_modified: str) -> bool:
    d = parse_iso8601(last_modified)
    if not d:
        return False
    return START_YEAR <= d.year <= END_YEAR


# ---------------- Resilient GET ----------------
def get(url: str) -> requests.Response:
    max_attempts = 6
    base_sleep = 1.6
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT)

            if resp.status_code == 401:
                raise RuntimeError("Unauthorized (401). Check HF token validity/permissions.")
            if resp.status_code == 404:
                resp.raise_for_status()

            if resp.status_code in (429, 502, 503, 504):
                sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                print(f"Transient {resp.status_code}. Retry {attempt}/{max_attempts} in {sleep:.1f}s")
                time.sleep(sleep)
                continue

            resp.raise_for_status()
            return resp

        except (ReadTimeout, ConnectionError, HTTPError) as e:
            last_exc = e
            sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
            print(f"Error {e}. Retry {attempt}/{max_attempts} in {sleep:.1f}s")
            time.sleep(sleep)

    raise RuntimeError(f"GET failed after retries: {last_exc}")


# ---------------- HF API Calls ----------------
def hf_search(asset_kind: str, query: str, limit: int) -> list[dict]:
    q = quote_plus(query.strip())
    url = f"{HF_BASE}/api/{asset_kind}?search={q}&limit={limit}&full=true"
    return get(url).json() or []


# ---------------- Author enrichment cache ----------------
_AUTHOR_CACHE: dict[str, dict] = {}
_AUTHOR_TYPE_CACHE: dict[str, str] = {}  # 'user'/'org'/'unknown'

def hf_fetch_author(namespace: str) -> dict:
    if not namespace:
        return {}
    if namespace in _AUTHOR_CACHE:
        return _AUTHOR_CACHE[namespace]

    def _fetch_no_retry(url: str) -> dict:
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                return {}
            if r.status_code == 401:
                raise RuntimeError("Unauthorized (401). Check HF token validity/permissions.")
            if r.status_code in (429, 502, 503, 504):
                return get(url).json() or {}
            r.raise_for_status()
            return r.json() or {}
        except Exception:
            return {}

    user_url = f"{HF_BASE}/api/users/{quote_plus(namespace)}"
    org_url = f"{HF_BASE}/api/organizations/{quote_plus(namespace)}"

    data = _fetch_no_retry(user_url)
    if data:
        _AUTHOR_TYPE_CACHE[namespace] = "user"
    else:
        data = _fetch_no_retry(org_url)
        _AUTHOR_TYPE_CACHE[namespace] = "org" if data else "unknown"

    _AUTHOR_CACHE[namespace] = data or {}
    return _AUTHOR_CACHE[namespace]


def is_org(namespace: str) -> bool:
    _ = hf_fetch_author(namespace)
    return _AUTHOR_TYPE_CACHE.get(namespace, "unknown") == "org"


def author_fields(author_json: dict, namespace: str) -> dict:
    display = (author_json.get("name") or author_json.get("fullname") or author_json.get("fullName") or "").strip()
    bio = (author_js_
