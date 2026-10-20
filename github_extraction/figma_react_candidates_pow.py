"""
Proof-of-work variant of figma_react_candidates.py (trial, 2026-10-19).

Same goal (Figma + React "Designer Engineer" candidates, one row per person) and the same
8 repo queries, so the two outputs can be compared. What changes is Kathy Baptista's
proof-of-work method (Sourcing Summit 2026, "Proof of Work"): read what people build,
not what they write about themselves.

1. A second discovery route, "build it": GitHub code search for package.json files that
   actually depend on a Figma-to-code tool (Code Connect, the Figma MCP server, Tokens
   Studio transforms, Storybook's Figma addon, Figma plugin typings). Finds repos whose
   name/description never says "figma".
2. Merged PRs only. The old script takes the 5 latest PRs in any state, which includes
   rejected and abandoned ones. Here a PR author only counts if the PR was merged.
3. A third door: issue authors on the Figma-to-code tools' own repos. People who report
   bugs on Code Connect or the Figma MCP use it for real, and often never own a repo.
4. Evidence per person instead of a ranking by stars:
   - merged PRs to other people's repos (reviewed and accepted code), with examples
   - stack read from the top repo's package.json (Figma tooling / UI / quality)
   - project hygiene signals (tests, CI workflows, CONTRIBUTING, Storybook)
   - issues opened on the Figma-to-code tools
   A transparent pow_score adds these up; the columns show why.

v2 (same day, after comparing v1 with the old script): v1's score saturated (284/410 people
hit the 20-PR cap, many PRs going to their employer's own repos) and credited Figma
dependencies to anyone contributing to a repo that has them. v2 scores, in order of weight:
merged PRs with Figma in the title (person-level; v2 first matched the body too, which PR templates polluted), issues opened on the Figma tools, Figma
dependencies only when the person owns the repo or has Figma PRs, and the number of
distinct external project owners that accepted their PRs (breadth, not volume).
Service-account bots are filtered out.

Run:
  python figma_react_candidates_pow.py
Output:
  figma_react_candidates_pow.csv (in this folder, gitignored), one row per person.
"""

import base64
import csv
import json
import re
import time
from pathlib import Path

from figma_react_candidates import (
    GITHUB_API,
    ENV_TOKEN,
    QUERIES,
    MAX_REPOS_PER_QUERY,
    TOP_N_CONTRIBUTORS,
    TOP_N_PR_AUTHORS,
    get,
    search_repos,
    fetch_owner,
    normalize_url,
    extract_first_linkedin,
    resolve_email,
    fetch_repo_summary_and_keywords,
)

OUT_CSV = "figma_react_candidates_pow.csv"

# "Build it" discovery: package.json files that depend on a Figma-to-code tool.
CODE_QUERIES = [
    '"@figma/code-connect" filename:package.json',
    '"figma-developer-mcp" filename:package.json',
    '"@tokens-studio/sd-transforms" filename:package.json',
    '"@storybook/addon-designs" filename:package.json',
    '"@figma/plugin-typings" filename:package.json',
]
MAX_REPOS_PER_CODE_QUERY = MAX_REPOS_PER_QUERY

# Third door: the tools' own repos, where real users open issues.
TOOL_REPOS = [
    "figma/code-connect",
    "GLips/Figma-Context-MCP",
    "tokens-studio/sd-transforms",
    "storybookjs/addon-designs",
]
TOP_N_ISSUE_AUTHORS = 10

# package.json dependency vocabulary, grouped the way Baptista reads a stack.
FIGMA_DEPS = {
    "@figma/code-connect", "@figma/plugin-typings", "@figma/rest-api-spec",
    "@figma/widget-typings", "figma-api", "figma-js", "figma-developer-mcp",
    "@tokens-studio/sd-transforms", "style-dictionary", "@storybook/addon-designs",
    "@modelcontextprotocol/sdk",
}
UI_DEPS = {"react", "next", "vue", "svelte", "typescript", "tailwindcss", "@radix-ui/react-slot",
           "class-variance-authority", "@vanilla-extract/css", "styled-components", "lit"}
QUALITY_DEPS = {"vitest", "jest", "@playwright/test", "@testing-library/react", "@biomejs/biome",
                "eslint", "chromatic", "@changesets/cli", "storybook", "@storybook/react"}

# Search API budgets: 30 req/min (issues/PR search), 10 req/min (code search).
SEARCH_INTERVAL_S = 2.2
CODE_SEARCH_INTERVAL_S = 6.5
_last_call = {"search": 0.0, "code": 0.0}

# Catches "[bot]" apps and service accounts like edx-requirements-bot, snyk-bot, RiotRobot,
# without matching people whose handle merely ends in "bot" (e.g. shawnbot).
_BOT_LOGIN = re.compile(r"(\[bot\]$|[-_]bot$|^bot[-_]|[-_]bot[-_]|robot$)", re.IGNORECASE)


def is_bot(login: str) -> bool:
    return bool(_BOT_LOGIN.search(login or ""))


def throttled_get(kind: str, url: str, params: dict):
    interval = CODE_SEARCH_INTERVAL_S if kind == "code" else SEARCH_INTERVAL_S
    wait = _last_call[kind] + interval - time.time()
    if wait > 0:
        time.sleep(wait)
    _last_call[kind] = time.time()
    return get(url, params=params)


_REPO_CACHE: dict[str, dict] = {}


def fetch_repo(full_name: str) -> dict:
    if full_name not in _REPO_CACHE:
        try:
            _REPO_CACHE[full_name] = get(f"{GITHUB_API}/repos/{full_name}").json()
        except Exception:
            _REPO_CACHE[full_name] = {}
    return _REPO_CACHE[full_name]


def search_code_repos(query: str, max_results: int) -> list[dict]:
    """Repos holding a matching package.json, deduped, forks and the tools' own repos skipped."""
    data = throttled_get("code", f"{GITHUB_API}/search/code",
                         {"q": query, "per_page": 50}).json()
    repos, seen = [], set(TOOL_REPOS)
    for item in data.get("items") or []:
        full_name = (item.get("repository") or {}).get("full_name", "")
        if not full_name or full_name in seen:
            continue
        seen.add(full_name)
        repo = fetch_repo(full_name)
        if repo and not repo.get("fork") and not repo.get("archived"):
            repos.append(repo)
        if len(repos) >= max_results:
            break
    return repos


def fetch_contributors_and_merged_pr_authors(owner_login: str, repo_name: str) -> list[tuple[str, str]]:
    """Contributors (commits on the default branch, so merged by definition) + authors of
    MERGED PRs only. Returns (login, role) pairs, bots excluded."""
    out, seen = [], set()
    try:
        data = get(f"{GITHUB_API}/repos/{owner_login}/{repo_name}/contributors",
                   params={"per_page": TOP_N_CONTRIBUTORS, "anon": "false"}).json()
        for c in data if isinstance(data, list) else []:
            login = str(c.get("login", "") or "")
            if login and login not in seen and not is_bot(login):
                seen.add(login)
                out.append((login, "contributor"))
    except Exception:
        pass
    try:
        data = get(f"{GITHUB_API}/repos/{owner_login}/{repo_name}/pulls",
                   params={"state": "closed", "sort": "updated", "direction": "desc",
                           "per_page": 30}).json()
        n = 0
        for pr in data if isinstance(data, list) else []:
            if not pr.get("merged_at"):
                continue
            login = ((pr.get("user") or {}).get("login") or "").strip()
            if login and login not in seen and not is_bot(login):
                seen.add(login)
                out.append((login, "merged_pr_author"))
                n += 1
                if n >= TOP_N_PR_AUTHORS:
                    break
    except Exception:
        pass
    return out


def fetch_issue_authors(tool_repo: str) -> list[tuple[str, str]]:
    """(login, 'title (url)') for recent issue authors on a tool repo; PRs filtered out."""
    try:
        data = get(f"{GITHUB_API}/repos/{tool_repo}/issues",
                   params={"state": "all", "sort": "created", "direction": "desc",
                           "per_page": 50}).json()
    except Exception as e:
        print(f"  issues fetch failed for {tool_repo}: {e}")
        return []
    out, seen = [], set()
    for issue in data if isinstance(data, list) else []:
        if issue.get("pull_request"):
            continue
        login = ((issue.get("user") or {}).get("login") or "").strip()
        if not login or login in seen or is_bot(login):
            continue
        seen.add(login)
        out.append((login, f"{tool_repo}: {issue.get('title', '')[:80]} ({issue.get('html_url', '')})"))
        if len(out) >= TOP_N_ISSUE_AUTHORS:
            break
    return out


_STACK_CACHE: dict[str, dict] = {}


def read_stack(full_name: str) -> dict:
    """Read the repo root: package.json dependencies + hygiene files. Cached per repo."""
    if full_name in _STACK_CACHE:
        return _STACK_CACHE[full_name]
    result = {"figma": [], "ui": [], "quality": [], "hygiene": []}
    try:
        listing = get(f"{GITHUB_API}/repos/{full_name}/contents/").json()
    except Exception:
        listing = []
    names = {(e.get("name") or "").lower() for e in listing if isinstance(e, dict)}
    if ".github" in names:
        try:
            wf = get(f"{GITHUB_API}/repos/{full_name}/contents/.github/workflows").json()
            if isinstance(wf, list) and wf:
                result["hygiene"].append("ci_workflows")
        except Exception:
            pass
    if "contributing.md" in names:
        result["hygiene"].append("contributing")
    if ".storybook" in names:
        result["hygiene"].append("storybook_config")
    if "package.json" in names:
        try:
            pkg = get(f"{GITHUB_API}/repos/{full_name}/contents/package.json").json()
            content = json.loads(base64.b64decode(pkg.get("content", "")).decode("utf-8", "ignore"))
            deps = set((content.get("dependencies") or {}).keys()) | \
                set((content.get("devDependencies") or {}).keys()) | \
                set((content.get("peerDependencies") or {}).keys())
            result["figma"] = sorted(deps & FIGMA_DEPS)
            result["ui"] = sorted(deps & UI_DEPS)
            result["quality"] = sorted(deps & QUALITY_DEPS)
            if any(d.startswith("@storybook/") for d in deps) and "storybook_config" not in result["hygiene"]:
                result["hygiene"].append("storybook_deps")
        except Exception:
            pass
    _STACK_CACHE[full_name] = result
    return result


def merged_external_prs(login: str) -> tuple[int, int, list[str]]:
    """Merged PRs on repos the person doesn't own. Returns (total, distinct owners among the
    last 100, example repos). Distinct owners measures breadth: 300 PRs into an employer's
    monorepo is one owner; PRs accepted by 8 unrelated projects is eight."""
    try:
        data = throttled_get("search", f"{GITHUB_API}/search/issues", {
            "q": f"type:pr is:merged author:{login} -user:{login}", "per_page": 100,
        }).json()
    except Exception:
        return 0, 0, []
    repos, owners = [], set()
    for item in data.get("items") or []:
        repo = (item.get("repository_url") or "").replace(f"{GITHUB_API}/repos/", "")
        if not repo:
            continue
        owners.add(repo.split("/")[0].lower())
        if repo not in repos:
            repos.append(repo)
    return int(data.get("total_count") or 0), len(owners), repos[:5]


def merged_figma_prs(login: str) -> tuple[int, list[str]]:
    """Merged PRs by this person (any repo) with Figma in the title: proof that they
    personally did Figma-related work, not just that a repo they touch uses Figma. Title
    only, because many repos' PR templates carry a "Figma link" field, which made a body
    match fire on unrelated PRs (seen in v2: a SoundFont PR, Sentry UI PRs)."""
    try:
        data = throttled_get("search", f"{GITHUB_API}/search/issues", {
            "q": f"type:pr is:merged author:{login} figma in:title", "per_page": 5,
        }).json()
    except Exception:
        return 0, []
    examples = [f"{(i.get('title') or '')[:70]} ({i.get('html_url', '')})" for i in data.get("items") or []]
    return int(data.get("total_count") or 0), examples[:3]


def new_person(login: str) -> dict:
    owner_json = fetch_owner(login)
    if owner_json.get("type") == "Bot":
        return {}
    email, email_source = resolve_email(login, owner_json)
    bio = owner_json.get("bio", "") or ""
    blog = normalize_url(owner_json.get("blog", "") or "")
    return {
        "login": login,
        "name": owner_json.get("name", "") or "",
        "company": owner_json.get("company", "") or "",
        "roles": set(),
        "email": email,
        "email_source": email_source,
        "location": owner_json.get("location", "") or "",
        "bio": bio,
        "blog": blog,
        "linkedin": extract_first_linkedin(blog, bio),
        "github_url": f"https://github.com/{login}",
        "repos": set(),
        "discovery": set(),
        "matched_queries": set(),
        "issues_on_tools": [],
        "top_repo": None,
        "max_repo_stars": -1,
    }


def main():
    print("GITHUB_TOKEN present:", bool(ENV_TOKEN))
    people: dict[str, dict] = {}
    skipped: set[str] = set()

    def add(login: str, role: str, discovery: str, query: str, repo: dict | None = None):
        if is_bot(login) or login in skipped:
            return
        if login not in people:
            person = new_person(login)
            if not person:
                skipped.add(login)
                return
            people[login] = person
        p = people[login]
        p["roles"].add(role)
        p["discovery"].add(discovery)
        p["matched_queries"].add(query)
        if repo:
            p["repos"].add(repo.get("full_name", ""))
            stars = repo.get("stargazers_count", 0) or 0
            # Prefer a repo the person owns as their top repo, then stars.
            owns = repo.get("full_name", "").split("/")[0] == login
            rank = (owns, stars)
            if p["top_repo"] is None or rank > p["_rank"]:
                p["top_repo"], p["_rank"], p["max_repo_stars"] = repo, rank, stars

    def harvest_repo(repo: dict, discovery: str, query: str):
        full_name = repo.get("full_name", "")
        owner_login = (repo.get("owner") or {}).get("login") or ""
        repo_name = full_name.split("/", 1)[1] if "/" in full_name else ""
        print(f"  - {full_name} (stars={repo.get('stargazers_count', 0)})")
        if owner_login and (repo.get("owner") or {}).get("type", "User") == "User":
            add(owner_login, "owner", discovery, query, repo)
        if owner_login and repo_name:
            for login, role in fetch_contributors_and_merged_pr_authors(owner_login, repo_name):
                if login != owner_login:
                    add(login, role, discovery, query, repo)

    # Door 1 (same as the old script): repos that describe themselves with the keywords.
    for query in QUERIES:
        print(f"\n=== Repo query (says it): {query!r} ===")
        try:
            repos = search_repos(query, MAX_REPOS_PER_QUERY)
        except Exception as e:
            print(f"  search failed: {e}")
            continue
        for repo in repos:
            harvest_repo(repo, "repo_search", query)

    # Door 2: repos that depend on a Figma-to-code tool in package.json.
    for query in CODE_QUERIES:
        print(f"\n=== Code query (builds it): {query!r} ===")
        try:
            repos = search_code_repos(query, MAX_REPOS_PER_CODE_QUERY)
        except Exception as e:
            print(f"  code search failed: {e}")
            continue
        for repo in repos:
            harvest_repo(repo, "dependency_search", query)

    # Door 3: people who open issues on the tools themselves.
    for tool in TOOL_REPOS:
        print(f"\n=== Issue authors: {tool} ===")
        for login, evidence in fetch_issue_authors(tool):
            add(login, "issue_author", "tool_issues", tool)
            if login in people:
                people[login]["issues_on_tools"].append(evidence)

    # Evidence per person.
    print(f"\nReading proof of work for {len(people)} people...")
    rows = []
    for i, (login, p) in enumerate(people.items(), 1):
        if i % 25 == 0:
            print(f"  {i}/{len(people)}")
        n_ext, n_owners, ext_repos = merged_external_prs(login)
        n_figma_prs, figma_pr_examples = merged_figma_prs(login)
        top = p["top_repo"]
        stack = read_stack(top["full_name"]) if top else {"figma": [], "ui": [], "quality": [], "hygiene": []}
        detail = fetch_repo_summary_and_keywords(top) if top else {"repo_summary": "", "repo_topics": "", "keywords": ""}
        owns_top = bool(top) and top.get("full_name", "").split("/")[0] == login

        # A repo's Figma dependencies only count for the person if they own the repo, or
        # if their own merged PRs show Figma work. Contributing to a big monorepo that
        # happens to depend on a Figma tool proves nothing about the person.
        if owns_top:
            figma_dep_weight = 3.0
        elif n_figma_prs > 0:
            figma_dep_weight = 1.5
        else:
            figma_dep_weight = 0.0
        pow_score = (
            min(n_figma_prs, 10) * 3
            + len(stack["figma"]) * figma_dep_weight
            + min(len(p["issues_on_tools"]), 3) * 4
            + min(n_owners, 10) * 1
            + ((min(len(stack["quality"]), 4) + len(stack["hygiene"])) * 0.5 if owns_top else 0)
        )
        rows.append({
            "login": login,
            "name": p["name"],
            "company": p["company"],
            "roles": "; ".join(sorted(p["roles"])),
            "discovery": "; ".join(sorted(p["discovery"])),
            "pow_score": round(pow_score, 1),
            "merged_figma_prs": n_figma_prs,
            "figma_pr_examples": " | ".join(figma_pr_examples),
            "merged_external_prs": n_ext,
            "external_owners": n_owners,
            "merged_external_pr_repos": "; ".join(ext_repos),
            "top_repo_owned": owns_top,
            "stack_figma": "; ".join(stack["figma"]),
            "stack_ui": "; ".join(stack["ui"]),
            "stack_quality": "; ".join(stack["quality"]),
            "hygiene": "; ".join(stack["hygiene"]),
            "issues_on_tools": " | ".join(p["issues_on_tools"][:3]),
            "email": p["email"],
            "email_source": p["email_source"],
            "location": p["location"],
            "bio": p["bio"],
            "blog": p["blog"],
            "linkedin": p["linkedin"],
            "github_url": p["github_url"],
            "repos": "; ".join(sorted(r for r in p["repos"] if r)),
            "max_repo_stars": max(p["max_repo_stars"], 0),
            "top_repo_url": (top or {}).get("html_url", ""),
            "top_repo_summary": detail["repo_summary"],
            "top_repo_topics": detail["repo_topics"],
            "top_repo_keywords": detail["keywords"],
            "matched_queries": "; ".join(sorted(p["matched_queries"])),
        })

    rows.sort(key=lambda r: -r["pow_score"])
    out_path = Path(__file__).with_name(OUT_CSV)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["login"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} people to {out_path.resolve()}")
    print(f"With email resolved: {sum(1 for r in rows if r['email'])}/{len(rows)}")
    print(f"With >=1 merged external PR: {sum(1 for r in rows if r['merged_external_prs'])}/{len(rows)}")
    print(f"With >=1 merged Figma PR: {sum(1 for r in rows if r['merged_figma_prs'])}/{len(rows)}")


if __name__ == "__main__":
    main()
