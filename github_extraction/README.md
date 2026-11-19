# Coding Sourcer

> Builds a sourcer list of European (or unknown-location) individual GitHub users active in 2024+ on Python repos related to transformers / NLP / RAG / agents, and exports a clean, hyperlink-rich Excel for outreach or analysis.

---

## Architecture

```mermaid
flowchart TB
    A["**ME (Runner)**<br/>Run the script"] --> B["**Repo Search Step**<br/>• GitHub /search/repos<br/>• Filters created range (year)<br/>• Writes repos table"]
    B --> C["**Builds data in memory**<br/>repos, owners, small runs"]
    C --> D["**Owner Enrichment Step**<br/>• GitHub /users/{login}<br/>• Cache in owners table<br/>• Retry/backoff"]
    D --> E["**Export Step**<br/>• join repos+owners<br/>• write XLSX hyperlinks<br/>• CSV fallback"]
    E --> F["**Geocoding enricher** (Apps script)<br/>• Improve data collection<br/>owner_location, city, country"]
```

---

## What it searches

**Repositories that:**

- Are written in **Python**
- Match any of the given **topics** (`topic:transformers`, `topic:nlp`, …) or **keywords** in repo name/description/README (“knowledge graph”, “rag”, “agent”, etc.)
- Are **public**, not archived, and **active in 2024+** (`pushed:>=2024-01-01`)

From those repos, it keeps only the **first repo per owner** (to avoid duplicates).

---

## Who it keeps (owner filter)

| Filter | Behavior |
|--------|----------|
| **Owner type** | Only **Users** (not orgs), unless you flip `INCLUDE_ORGS` |
| **Owner location** | Keeps users whose GitHub profile location is: clearly in **Europe** (matches a big list of country/city tokens), or **blank/unknown** (if `KEEP_UNKNOWN_LOCATION=True`). Excludes users whose location clearly indicates non-Europe (e.g. United States, Austin, Vancouver, Beijing, Singapore, etc.) |

---

## What fields it pulls

**From the user profile:**

- `login`
- Complete name (split into first/surname and rejoined)
- `location`
- `html_url` (profile URL)
- `blog` (normalized to a proper URL)
- `bio`
- `email`
- `twitter_username` (turned into a URL)
- LinkedIn URL (first one detected in blog or bio)
- Extra links (any other URLs found in bio)

**From that user’s first matching repository:**

- `repo_name` (full name)
- `repo_url`
- `repo_updated_at` (prefers `pushed_at`, falls back to `updated_at` / `created_at`)
- `repo_description`


## Proof-of-work variant (figma_react_candidates_pow.py) — trial, 2026-10-19

Same target and the same 8 repo queries as `figma_react_candidates.py`, rebuilt around Kathy
Baptista's proof-of-work method (Sourcing Summit 2026): read what people build, not what they
write about themselves. It reuses the old script's helpers (HTTP, email fallback, README
summary), so the differences below are the whole change.

```mermaid
flowchart LR
    A["Door 1: repo search<br/>(same 8 queries)"] --> P["People<br/>one row per login"]
    B["Door 2: code search<br/>package.json depends on<br/>Code Connect, Figma MCP,<br/>Tokens Studio, addon-designs,<br/>plugin-typings"] --> P
    C["Door 3: issue authors<br/>on the tools' own repos"] --> P
    P --> E["Evidence per person<br/>• merged PRs with Figma in title<br/>• distinct external projects<br/>• top repo package.json stack<br/>• tests / CI / Storybook"]
    E --> S["pow_score + evidence columns"]
```

