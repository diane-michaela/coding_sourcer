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
| `Intake meeting/` | Make.com scenario docs/blueprints for the hiring-intake automation. Live Make scenarios are the source of truth, not the JSON files here. |
| `pb-daily-dashboard/` | Daily PhantomBuster dashboard (deployed via `render.yaml`). |
| `Meetup/` | Meetup member/event extraction scripts. |
| `huggingface/` | Hugging Face profile/model sourcing. |
| `Agent Pierre-Richard DUPONT (PRD)/` | The `/PRD` Claude Code skill (prompt/brief sharpening) — installed globally via symlink at `~/.claude/skills/PRD`. |
| `LLM-wiki-vault/` | Symlink to the RecOps Obsidian vault (Google Drive) — gitignored, has its own `CLAUDE.md`. |

## Cross-cutting conventions

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
