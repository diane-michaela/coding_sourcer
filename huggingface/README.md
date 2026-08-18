# Hugging Face Retrieval Sourcer

Sourcing tool that searches the Hugging Face Hub for individuals working on retrieval, ranking, and recommendation — and exports results to a recruiter-friendly Excel file.

## What it does

1. **Searches HF Hub** for models and datasets using retrieval/search keywords (e.g. `reranker`, `bi-encoder`, `colbert`, `semantic search`, `recommendation`, etc.)
2. **Filters by recency** — only assets last modified between 2023 and 2026
3. **Enriches author profiles** — fetches user or organization metadata and caches results
4. **Extracts README summaries** — pulls a short description from each model card (best-effort)
5. **Extracts contributors** — infers commit authors from repo history (best-effort, cached)
6. **Scores candidates** — adds `score` and `score_reasons` columns based on talent signals
7. **Individuals-only mode** (default ON) — skips org-owned repos unless individual contributors are found
8. **Exports to Excel** with hyperlinks, plus a CSV fallback. Includes separate sheets for org rankings.

## Output

- `hf_retrieval_models_datasets_with_author_details.xlsx` — main output with candidate profiles, scores, and links

## Configuration

Edit the constants at the top of the script:

| Variable | Default | Description |
|---|---|---|
| `USE_EXTENDED_QUERIES` | `True` | Include extended keyword list |
| `START_YEAR` / `END_YEAR` | 2023 / 2026 | Filter assets by last modified year |
| `MAX_ASSETS_TOTAL` | 1200 | Cap on total assets fetched |
| `INDIVIDUALS_ONLY` | `True` | Exclude org-owned repos with no individual contributors |
| `FETCH_README` | `True` | Extract model card descriptions |
| `FETCH_CONTRIBUTORS` | `True` | Extract commit contributors |

## Authentication

The script looks for a Hugging Face token in this order:
1. `HF_TOKEN` or `HUGGINGFACEHUB_API_TOKEN` environment variable
2. A local `token_hf.py` file with `HF_TOKEN = "hf_..."`

Running without a token works but may hit rate limits faster.

## Requirements

```
requests
pandas
openpyxl
```

## Limitations

- HF does not reliably expose emails, locations, or real names — many fields are optional
- Country is a best-effort guess from bio/name/website only
- Contributor extraction relies on commit metadata and may return empty results

---

## hf_ml_engineer_ai_specialist_sourcer.py

Standalone trial sourcer built for one req — "Machine Learning Engineer / AI Specialist". Separate
script, separate config, does not read from or write to anything the retrieval sourcer above uses.

### How it works

1. **Search** — queries HF Hub's public search API (`/api/models`, `/api/datasets`, `/api/spaces`)
   for each JD keyword (agentic, bedrock, agent framework, retrieval augmented generation, RAG,
   langchain, fine-tuning, prompt engineering, mlops, mlflow), across all three asset kinds, and
   keeps only results modified since `LOOKBACK_START_YEAR`.
2. **Resolve to an individual** — every matched repo has a namespace (its `author`). If that
   namespace is a personal account, the account itself is the candidate. If it's an organization,
   the script pulls that specific repo's commit history and takes the top commit author(s) instead
   — an org-owned repo with no identifiable individual contributor is skipped rather than kept as a
   faceless "candidate." This is why `org_source` in the output distinguishes `self-listed-affiliation`
   (they joined that org on their own HF profile) from `contributed-to-repo` (they committed to that
   org's repo — not proof they work there; open-source orgs take outside contributions all the time).
3. **Enrich** — for each candidate: fetch their HF bio (`details` field) and org memberships from
   the users/organizations overview API; scrape their public profile page for GitHub/LinkedIn/
   Twitter/website links (HF exposes no bio API for these — they only exist as rendered links on the
   profile page); guess a location from flag emoji or city names in the bio (flagged unreliable —
   in practice this fires on well under 5% of profiles, most HF bios carry no location text at all);
   fetch a README excerpt from their most recent matching repo; compute activity recency and a
   prolific-ness signal (matching repo count, likes/downloads, HF-wide totals).
4. **Score** — every candidate's bio + matched repos + README excerpt is sent to Claude Haiku via
   the Batch API (same pattern as `phantombuster-api/rank_profiles.py`) with the JD spelled out in
   the system prompt. Each candidate comes back with a plain-language summary of what they built, a
   0–100 JD-match score, a one-line reason for that score, and a short summary of their single most
   relevant repo.
5. **Export** — results sorted best-first by JD-match score into a recruiter-friendly Excel (with
   clickable HF/GitHub/LinkedIn links) plus a CSV fallback.

```bash
python hf_ml_engineer_ai_specialist_sourcer.py --dry-run   # free preview, no Anthropic spend
python hf_ml_engineer_ai_specialist_sourcer.py             # full run incl. Claude scoring
python hf_ml_engineer_ai_specialist_sourcer.py --resume-batch-id <id>   # resume an interrupted batch
```

### Output columns

`name`, `hf_username`, `hf_profile_link`, `jd_match_score`, `summary`, `score_reasons`,
`org_company`, `org_source` (see step 2 above), `org_location (manual follow-up — not automated)`
(intentionally left blank — geocoding an org name isn't automated here), `location_guess`,
`location_confidence`, `last_activity`, `prolific_signal`, `github_link`, `linkedin_link`,
`top_repo`, `top_repo_summary`, `matched_keywords`, `sample_repos`.

### Configuration

Edit the constants at the top of the script, or override per-run via CLI flags:

| Constant | CLI flag | Default | Description |
|---|---|---|---|
| `JD_KEYWORDS` | `--keywords` | see script | Search terms |
| `ASSET_KINDS` | `--asset-kinds` | `models,datasets,spaces` | HF repo types to search |
| `SEARCH_LIMIT_PER_QUERY` | `--search-limit` | 100 | Per-keyword HF search result cap |
| `MAX_CANDIDATES` | `--max-candidates` | 400 | Cap on candidates sent to enrichment/scoring |
| `LOOKBACK_START_YEAR` | — | 2023 | Soft recency bound on search volume |
| `INDIVIDUALS_ONLY` | — | `True` | Skip org-owned repos with no named contributor |

### Not in scope for this trial

No TeamTailor/Airtable cross-check, no automated org-location lookup, and no fixed target
headcount — the goal is a shortlist small enough to message everyone on it, not a specific number.

### Requirements & auth

Requires `anthropic` in addition to `requests`/`pandas`/`openpyxl`. Reads `HF_TOKEN` (optional,
raises HF rate limits) and `ANTHROPIC_API_KEY` (required unless `--dry-run`) from a local `.env`
— copy `.env.example` and fill it in, or reuse the same `ANTHROPIC_API_KEY` already set up in
`phantombuster-api/.env` for `rank_profiles.py`. Claude Haiku batch scoring runs on that Anthropic
API account's own billing, separate from any Claude Pro/Code subscription — a full run of ~400
candidates costs roughly $0.15–0.20.
