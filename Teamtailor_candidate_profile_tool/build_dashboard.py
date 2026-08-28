"""
Join candidates + tags_cache + job_applications + jobs + stages into a single
flat dataset for the dashboard.

Deliberately excludes: salary, comments, reviews — none of these exist in
Teamtailor's data model (verified against the live API), so they aren't
faked here. Location and past employers ARE included when the LLM tagger
found them stated in the candidate's own CV text (see tag_candidates.py).

Usage
-----
    python build_dashboard.py
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

EXPORT_DIR = Path(__file__).parent / "export"
CACHE_PATH = Path(__file__).parent / "tags_cache.json"
OUT_PATH = Path(__file__).parent / "dashboard_data.json"

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_html(html: str) -> str:
    if not html:
        return ""
    txt = HTML_TAG_RE.sub(" ", html)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    return WS_RE.sub(" ", txt).strip()


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for v in values:
        v = (v or "").strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def load_csv(name: str) -> list[dict]:
    path = EXPORT_DIR / name
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> None:
    candidates = load_csv("candidates.csv")
    applications = load_csv("job_applications.csv")
    jobs = load_csv("jobs.csv")
    stages = load_csv("stages.csv")
    cache = json.loads(CACHE_PATH.read_text()) if CACHE_PATH.exists() else {}

    job_title_by_id = {j["id"]: (j.get("title") or "").strip() for j in jobs}
    stage_name_by_id = {s["id"]: (s.get("name") or "").strip() for s in stages}

    apps_by_candidate: dict[str, list[dict]] = {}
    for app in applications:
        cid = app.get("rel_candidate_id")
        if not cid:
            continue
        apps_by_candidate.setdefault(cid, []).append(app)

    rows = []
    tagged_count = 0
    for c in candidates:
        cid = c["id"]
        cache_key = f"{cid}:{c.get('resume-updated-at', '')}"
        tag = cache.get(cache_key, {})
        if tag:
            tagged_count += 1

        try:
            tags_list = json.loads(c.get("tags") or "[]")
        except json.JSONDecodeError:
            tags_list = []
        tags_list = dedupe_preserve_order(tags_list)

        apps = sorted(apps_by_candidate.get(cid, []), key=lambda a: a.get("created-at") or "")
        roles = []
        seen_roles = set()
        for a in apps:
            role = {
                "job_title": job_title_by_id.get(a.get("rel_job_id"), ""),
                "stage": stage_name_by_id.get(a.get("rel_stage_id"), ""),
                "applied_at": a.get("created-at", ""),
                "rejected_at": a.get("rejected-at", ""),
                "sourced": a.get("sourced", ""),
            }
            # job-applications include=candidate,job,stage has occasionally
            # yielded the same application twice on pagination edges — drop
            # exact repeats rather than showing "Job fit interview" twice.
            key = (role["job_title"], role["stage"], role["applied_at"])
            if key in seen_roles:
                continue
            seen_roles.add(key)
            roles.append(role)
        latest = roles[-1] if roles else None

        cv_text = clean_html(c.get("resume-summary", "")) or clean_html(c.get("pitch", ""))

        rows.append({
            "id": cid,
            "name": f"{c.get('first-name', '')} {c.get('last-name', '')}".strip(),
            "email": c.get("email", ""),
            "phone": c.get("phone", ""),
            "linkedin_url": c.get("linkedin-url") or c.get("linkedin-profile", ""),
            "profile_url": c.get("profile-url", ""),
            "source": c.get("referring-site", ""),
            "connected": c.get("connected", "") == "True",
            "sourced": c.get("sourced", "") == "True",
            "created_at": c.get("created-at", ""),
            "tt_tags": tags_list,
            "suggested_title": tag.get("suggested_title", ""),
            "keywords": tag.get("keywords", []),
            "seniority": tag.get("seniority", ""),
            "location": tag.get("location", ""),
            "past_companies": dedupe_preserve_order(tag.get("past_companies", [])),
            "cv_text": cv_text,
            "latest_role_applied": latest["job_title"] if latest else "",
            "latest_stage": latest["stage"] if latest else "",
            "latest_applied_at": latest["applied_at"] if latest else "",
            "roles_applied": roles,
            "num_applications": len(roles),
        })

    OUT_PATH.write_text(json.dumps(rows, ensure_ascii=False))
    print(f"Wrote {len(rows)} candidates -> {OUT_PATH}")
    print(f"  {tagged_count} have LLM tags, {len(rows) - tagged_count} don't (no resume-summary/pitch text)")
    print(f"  {sum(1 for r in rows if r['num_applications'] > 0)} have at least one job application")
    print(f"  {sum(1 for r in rows if r['location'])} have a CV-stated location (expect low — resumes rarely state addresses)")
    print(f"  {sum(1 for r in rows if r['past_companies'])} have at least one past employer extracted")
    print(f"  {sum(1 for r in rows if r['cv_text'])} have raw CV text stored for full-text search")


if __name__ == "__main__":
    main()
