# Python — RecOps sourcing & hiring automation workspace

Diane's personal workspace at PhantomBuster: sourcing pipelines, TeamTailor ATS tooling,
and the RecOps "second brain." Each top-level folder is close to its own project — check
for a `README.md` or `CLAUDE.md` inside before assuming behavior from folder name alone.

## Layout

| Folder | What it is |
|---|---|
| `phantombuster-api/` | Core PhantomBuster API wrapper + sourcing pipeline (LinkedIn export → filter → enrich → rank via Claude Haiku → Airtable). Has its own detailed `CLAUDE.md` and PB-specific skills under `.claude/skills/` — read that file before touching phantom launches. |
| `talent_radar/` | Separate weekly cron pipeline (PB scrape → keyword filter → Claude Haiku scoring → SQLite → Slack alerts ≥7/10). Gitignored — has its own `.env` with `PB_API_KEY`/`ANTHROPIC_API_KEY`/`SLACK_WEBHOOK_URL`. |
| `Teamtailor_candidate_profile_tool/` | "Candidate Index" — all ~6,400 TeamTailor candidates tagged via Claude Haiku, published as a Claude Artifact with boolean/full-text search. |
| `Teamtailor_quaterly_extraction/` | Quarterly TeamTailor extraction + stage-funnel report. Output CSVs get posted to the Teamtailor Analytics Notion page (one section per quarter). |
| `Teamtailor-sourced-candidates-per-job/` | Per-job sourced-candidate reporting (Make + Python variants). |
| `Teamtailor-make-weekly-analytics/` | Weekly analytics report generation from TeamTailor via Make. |
| `Teamtailor feedback extractions/` | Extracts job feedback from TeamTailor. |
| `teamtailor_career_scrapping/` | Scraping TeamTailor-hosted career pages / company discovery. |
| `coding_sourcer/` | Reusable template scripts for sourcing (GitHub, Hugging Face, Meetup, Make, TeamTailor) — starting points to adapt, not one-off scripts. |
| `github_extraction/` | GitHub-based candidate/repo sourcing (production ML/NLP sourcer `lisp.py`). |
| `Product Manager/` | Exploratory sourcing for PM/Designer/AI-Squad roles — no active req/budget as of 2026-07-27, nothing wired to production. Re-confirm scope before treating as real. |
| `Intake-Meeting-Automation/` | Make.com scenario docs/blueprints for the hiring-intake automation. Live Make scenarios are the source of truth, not the JSON files here. |
| `pb-daily-dashboard/` | Daily PhantomBuster dashboard (scheduled via GitHub Actions, `.github/workflows/daily-dashboard.yml`). |
| `Meetup/` | Meetup member/event extraction scripts. |
| `huggingface/` | Hugging Face profile/model sourcing. |
| `PRD/` | The `/PRD` Claude Code skill (prompt/brief sharpening, author: Pierre-Richard DUPONT) — installed globally via symlink at `~/.claude/skills/PRD`. |
| `LinkedIn Boolean Search/` | Canonical source for the `Irina LinkedIn Lite` / `Irina LinkedIn Recruiter` Claude Code skills (JD/intake-notes → LinkedIn Boolean search, author: Irina Shamaeva) — `.claude/skills/Irina LinkedIn Lite` and `.claude/skills/Irina LinkedIn Recruiter` are symlinks into this folder, same pattern as the PRD skill above. |
| `Intake Meeting Prep/` | Canonical source for the `Vlastelica Intake Prep` Claude Code skill (JD → pre-intake-meeting advisory pass, distilled from John Vlastelica/Recruiting Toolbox's Talent Advisor material) — `.claude/skills/Vlastelica Intake Prep` is a symlink into this folder, same pattern as PRD/Irina above. Fires *before* an intake meeting exists (input is just a JD); complements, doesn't replace, the `Intake-Meeting-Automation/` Make scenarios. |
| `X-Ray Search Beyond LinkedIn/` | Canonical source for the `Agent Bliard` Claude Code skill (X-ray/Google search for sourcing outside LinkedIn — GitHub, Stack Overflow, Behance, Kaggle, Meetup, ADPList, Substack, company team pages, open-web resumes — authors: Benoit Bliard/Search & Go, plus Glen Cathey and Irina Shamaeva) — `.claude/skills/Agent Bliard` is a symlink into this folder, same pattern as PRD/Irina/Vlastelica above. Defers to the Irina skills for anything LinkedIn-specific. |
| `Market Mapping/` | Canonical source for the `Fortin Market Mapping` Claude Code skill (maps a talent market — key employers, ecosystem, talent flow — before candidate-level sourcing starts, distilled from Pierre-André Fortin/Anara's published method) — `.claude/skills/Fortin Market Mapping` is a symlink into this folder, same pattern as PRD/Irina/Vlastelica/Bliard above. Sits upstream of Irina and Agent Bliard: produces a target-company list/market read, then hands off rather than building search strings itself. |
| `LLM-wiki-vault/` | Symlink to the RecOps Obsidian vault (Google Drive) — gitignored, has its own `CLAUDE.md`. |

## Cross-cutting conventions

- **LinkedIn Boolean search**: two tier-specific skills, canonically sourced from
  `LinkedIn Boolean Search/` and symlinked into `.claude/skills/` (named after
  Irina Shamaeva, co-author of the source book), turn a JD or intake-meeting brief into a search
  string — **`Irina LinkedIn Lite`** (Diane's own account: no bulk import, no char-limit cap) and
  **`Irina LinkedIn Recruiter`** (full Recruiter: has bulk CSV import for cross-referencing
  external sources). Both cover the hidden operators (`headline:`, `summary:`, `skills:`...) and
  why to avoid the Seniority/Function/Company-size/-type selection filters (50-80% of profiles
  miss those values). Source material: `linkedin-advanced-search-techniques-ebook.md` in the
  RecOps Obsidian wiki (`LLM-wiki-vault/2026/wiki/insights/`).
- **Airtable writes**: generate a CSV for manual import rather than pushing via API directly,
  and split any import into batches of ≤1000 rows — Airtable silently drops fields beyond
  ~1100 rows in one batch.
- **Secrets**: never hardcode API keys. Each project loads its own `.env`
  (`phantombuster-api/.env`, `talent_radar/.env`, etc.) — ask before assuming a key's value.
  `PB_API_KEY` / `PHANTOMBUSTER_API_KEY` in different `.env` files are the same underlying key.
- **PhantomBuster `launch_phantom(agent_id, args=...)`**: the `args` you pass *replace* the
  phantom's saved argument, they don't merge with it. Always fetch and merge the full saved
  argument first, or you'll silently drop config the phantom needs.
- **PhantomBuster LinkedIn phantoms**: check current pause/wave status in
  `phantombuster-api/CLAUDE.md` (wave table + "DO NOT RUN" section) before launching anything —
  phantoms get paused after cookie-invalidation incidents and shouldn't be auto-resumed.

## Where to look first

For anything PhantomBuster-specific (API functions, sourcing pipeline stages, wave status,
which phantoms are safe to run), `phantombuster-api/CLAUDE.md` is the detailed reference —
this file is only the cross-project map.
