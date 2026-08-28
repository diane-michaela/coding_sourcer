"""
Candidate tagging via Claude Haiku
====================================

Reads export/candidates.csv, and for every candidate with resume-summary or
pitch text, asks Haiku for a normalized suggested_title + keyword tags.
Results are cached in tags_cache.json keyed by candidate id + resume-updated-at,
so re-running only pays for candidates that are new or whose resume changed
(needed for "stay current" refreshes).

Usage
-----
    python tag_candidates.py
"""

from __future__ import annotations

import csv
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "talent_radar" / ".env")

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
HAIKU_INPUT_COST = 0.80 / 1_000_000
HAIKU_OUTPUT_COST = 4.00 / 1_000_000
MODEL = "claude-haiku-4-5-20251001"
MAX_WORKERS = 12

# Bump when the extraction schema changes (new fields added to PROMPT) so a
# full re-tag runs even though cache keys (id:resume-updated-at) still match.
TAG_SCHEMA_VERSION = 2

EXPORT_DIR = Path(__file__).parent / "export"
CACHE_PATH = Path(__file__).parent / "tags_cache.json"

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_html(html: str) -> str:
    if not html:
        return ""
    txt = HTML_TAG_RE.sub(" ", html)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    return WS_RE.sub(" ", txt).strip()


SYSTEM = (
    "You are a recruiter assistant that normalizes noisy CV summaries into "
    "structured tags. Respond with valid JSON only, no extra text. "
    "Tag strictly on the CV text provided. Never infer or use health, religion, "
    "ethnicity, sexual orientation, political affiliation, age, or other protected/sensitive "
    "personal attributes, even if a name or summary seems to suggest one."
)

PROMPT = """CV summary for a candidate:
\"\"\"{summary}\"\"\"

Return exactly this JSON:
{{
  "suggested_title": "<a short, normalized professional title, e.g. 'Senior Backend Engineer' — empty string if the summary has no discernible profession>",
  "keywords": [<3-8 short lowercase keyword strings: skills, tools, domains — no duplicates, no sentences>],
  "seniority": "<junior|mid|senior|lead|unknown>",
  "location": "<city and/or country the candidate lives or works in, ONLY if explicitly stated in the text — empty string if not mentioned, never guess from company/nationality cues>",
  "past_companies": [<employer names the summary says the candidate worked at, as written, most recent first — empty list if none are mentioned>]
}}"""


def _load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2))


_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$")


def _strip_fence(text: str) -> str:
    return _FENCE_RE.sub("", text.strip())


def _tag_one(candidate_id: str, summary: str) -> dict | None:
    prompt = PROMPT.format(summary=summary[:4000])
    for attempt in range(2):
        try:
            msg = _client.messages.create(
                model=MODEL,
                max_tokens=400,
                temperature=0.2,
                system=SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            result = json.loads(_strip_fence(msg.content[0].text))
            result["_in"] = msg.usage.input_tokens
            result["_out"] = msg.usage.output_tokens
            result["_v"] = TAG_SCHEMA_VERSION
            return result
        except json.JSONDecodeError:
            if attempt == 1:
                print(f"[tag] skip {candidate_id}: could not parse Claude response")
        except Exception as e:
            print(f"[tag] error on {candidate_id}: {e}")
            return None
    return None


def main() -> None:
    candidates_path = EXPORT_DIR / "candidates.csv"
    with candidates_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} candidates")

    cache = _load_cache()
    to_tag = []
    for row in rows:
        cid = row["id"]
        resume_updated = row.get("resume-updated-at", "")
        cache_key = f"{cid}:{resume_updated}"
        if cache.get(cache_key, {}).get("_v") == TAG_SCHEMA_VERSION:
            continue
        text = clean_html(row.get("resume-summary", "")) or clean_html(row.get("pitch", ""))
        if not text:
            continue
        to_tag.append((cid, cache_key, text))

    print(f"{len(rows) - len(to_tag)} already cached / no text, {len(to_tag)} to tag now")

    total_cost = 0.0
    tagged = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_tag_one, cid, text): (cid, cache_key)
                   for cid, cache_key, text in to_tag}
        for i, fut in enumerate(as_completed(futures), 1):
            cid, cache_key = futures[fut]
            result = fut.result()
            if result is None:
                continue
            cost = result.pop("_in") * HAIKU_INPUT_COST + result.pop("_out") * HAIKU_OUTPUT_COST
            total_cost += cost
            cache[cache_key] = result
            tagged += 1
            if i % 200 == 0:
                print(f"  tagged {i}/{len(to_tag)}  (running cost ${total_cost:.2f})")
                _save_cache(cache)

    _save_cache(cache)
    print(f"\nDone. Tagged {tagged} new candidates this run. Cost: ${total_cost:.2f}")
    print(f"Cache now has {len(cache)} entries -> {CACHE_PATH}")


if __name__ == "__main__":
    main()
