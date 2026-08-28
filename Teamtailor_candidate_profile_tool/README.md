# Candidate Index

An internal sourcing tool built on top of Teamtailor's full candidate base —
not a quarter slice like `Teamtailor_quaterly_extraction/`, but all 6,447
candidates in the ATS, with a CV-derived suggested title, keyword tags, and
full role/stage history per person. Published as a Claude Artifact
(searchable, filterable table).

## What it includes

| Column | Source |
|---|---|
| Name, email, phone, LinkedIn | Teamtailor `candidates` |
| Suggested title, keywords, seniority, location, past companies | Claude Haiku, run on Teamtailor's own `resume-summary` (an AI bullet summary of the CV Teamtailor already generates) — falls back to `pitch` text when there's no resume-summary |
| Raw CV text (for full-text/boolean search) | Same `resume-summary`/`pitch` text, stored verbatim in the dashboard data |
| Latest role applied, stage, full role history | Teamtailor `job-applications` joined to `jobs` + `stages` |
| Source, sourced/connected flags | Teamtailor `candidates` |

Location and past companies are inferred from the candidate's own CV text —
no LinkedIn scraping, no extra cost, same Haiku pass as title/keyword tagging.
Coverage is uneven by design: past companies hit ~86% of tagged candidates
(resumes are mostly employment history), location hits ~8% (resumes rarely
state an address) — that's a data-shape fact, not a bug.

## What it deliberately excludes

Verified against the live API on 2026-08-03 — these aren't unbuilt, they're
absent from Teamtailor's data model or return no data:

- **Salary** — no field exists on candidates or applications. Not chased via
  custom application questions either; dropped from scope entirely.
- **Comments / reviews** — `/notes` returns HTTP 200 with 0 rows, `/reviews`
  and `/messages` return 404 on this API plan.

See `[[project_teamtailor_candidate_data_model]]` in memory for the full
verification detail.

## Search

The search box supports plain text (implicit AND between bare words),
`"quoted phrases"`, and boolean `AND` / `OR` / `NOT` with nested parens, e.g.
`(react OR vue) AND senior NOT intern`. It matches against name, title,
keywords, CV text, past companies, and Teamtailor tags.

## Pipeline

```
extract_full.py      -> export/{candidates,job_applications,jobs,stages}.csv
tag_candidates.py     -> tags_cache.json   (Claude Haiku, cached by candidate id + resume-updated-at + schema version)
build_dashboard.py    -> dashboard_data.json
generate_html.py      -> dashboard.html
sync_to_airtable.py   -> airtable_deltas/*.csv   (feeds the intake automation's "Already sourced" lookup — see below)
```

`tag_candidates.py` only pays for candidates that are new, whose resume
changed, or whose cached tag predates the current `TAG_SCHEMA_VERSION` (bump
this constant whenever new fields are added to the extraction prompt — it
forces exactly the affected candidates to re-tag on the next run, nothing
more). First full run: 2,727 candidates tagged, **$1.50**. Second full run
(added location + past_companies, forced by the schema bump): 2,720
candidates, **$2.06**. A refresh a week later with no schema change should
cost roughly what a week of new/updated CVs works out to — a few cents to
low single dollars, not another full-base run.

## Refreshing

```bash
export TEAMTAILOR_TOKEN="..."   # same token as Teamtailor_quaterly_extraction/.env
python3 extract_full.py
python3 tag_candidates.py
python3 build_dashboard.py
python3 generate_html.py
```

Then republish `dashboard.html` as the same Artifact (ask Claude to redeploy
it — same file path keeps the same URL).

Requires `ANTHROPIC_API_KEY` (reused from `talent_radar/.env`) for the
tagging step.

## Syncing to Airtable (for the intake automation's "Already sourced" check)

`sync_to_airtable.py` reads `dashboard_data.json` and diffs it against
`airtable_sync_state.json` (the state saved at the end of the previous run)
to produce three outputs in `airtable_deltas/`:

- `*_add_*.csv` — candidates new since the last sync
- `*_update_*.csv` — candidates whose role/stage/tags changed
- `*_removed_ids.txt` — candidate IDs present last time, gone now (Teamtailor
  erasure, account merge, or similar) — **delete these records from Airtable**,
  don't just leave them

It never calls the Airtable API directly — this workspace's convention is
CSV import, not automated pushes, and add/update batches are automatically
split at 900 rows to stay under Airtable's import ceiling either way. In
steady state a weekly delta should be tens to low hundreds of rows, well
under that limit — the splitting logic is a guardrail for an abnormal run
(e.g. a sync skipped for a month), not something that should normally fire.

Run order for a refresh:

```bash
python3 extract_full.py      # full fresh pull — Teamtailor stays the source of truth
python3 tag_candidates.py    # cheap, only pays for new/changed candidates
python3 build_dashboard.py
python3 sync_to_airtable.py  # writes this run's deltas, updates the state file
```

Then import the CSVs in `airtable_deltas/` into the `teamtailor-candidates`
table — https://airtable.com/app5BF5NrOgR0kZIB/tblAJIxcjQogp1Ltz — and apply
the deletions from `*_removed_ids.txt`. That table lives in the same base as
the PhantomBuster-sourced pool (`sourced-targeted-companies`), as a separate
table rather than a separate base — Airtable base creation needs a workspace
permission this account doesn't have; revisit if that's ever worth fixing.
Its columns match this script's CSV headers exactly, so Airtable's importer
auto-matches on upload. A few default fields (Notes, Assignee, Status,
Attachments) are Airtable's blank-table starter fields — harmless, safe to
delete or ignore.

There's no fixed schedule for this yet — run it whenever the candidate pool
needs to be current, weekly being a reasonable default given how the pool
actually moves.

The **first** run has no prior state, so it produces the full backfill
(currently 8 batches, ~900 rows each) rather than a delta — that's expected,
not a bug.

### How the Make side actually uses this table

Scenario `6512042` ("Intake Meeting automation V2") queries `teamtailor-candidates`
live, once per intake meeting, as part of a two-lookup sub-router that runs after the
AI-generated sourcing brief:

- **This table** — Airtable `Search Records` (`airtable:ActionSearchRecords`), formula
  `SEARCH(LOWER("{{role}}"), LOWER({latest_role_applied})) > 0`, capped at 3 matches. Matches
  on `latest_role_applied` specifically (filled for 94.6% of candidates) rather than the
  Haiku-tagged `keywords`/`suggested_title` fields (only ~42% filled) — real applicant history
  is the reliable signal here, tags are a nice-to-have.
- **`sourced-targeted-companies`** (the PhantomBuster pool, same base) — matched on a
  deterministic role-category classifier computed inline in the scenario, sorted by
  `composite_score`.

Both lookups are Make *iterator* modules — 0 matches means 0 Slack posts, no filter logic
needed. Each match becomes its own threaded reply under the intake channel's role-recap
message: "Already in Teamtailor — so-and-so applied for *X* (stage)" for this table, "Already
sourced — so-and-so, title at company" for the PhantomBuster one. Nothing here needs to change
when the candidate pool refreshes — the connection points at the table, not a snapshot.
