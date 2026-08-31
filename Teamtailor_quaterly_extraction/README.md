# Teamtailor Quarterly Extraction

A script that pulls a quarterly snapshot of hiring activity out of
Teamtailor's JSON:API and writes it to CSV, so it can be analyzed outside the
ATS (Notion, Airtable, spreadsheets, etc.) without relying on Teamtailor's
own reporting.

One script, `teamtailor_extract.py`, takes the quarter as an argument instead
of hardcoding it per file — it replaces the old `teamtailor_q1_extract.py`
and `teamtailor_q2_extract.py`, which had started to drift (Q2 quietly
gained endpoints Q1 never got). The extraction logic is Q1's, since that's
the version that was actually run and worked; Q2's notes/reviews/messages
endpoints and HTML-cleaning were folded in as permanent additions.

## Usage

```bash
pip install requests
export TEAMTAILOR_TOKEN="your-admin-token"   # or paste it into the script
python teamtailor_extract.py                 # defaults to the current quarter
python teamtailor_extract.py --year 2026 --quarter 2
```

The script pings the API first to fail fast on a bad token, then writes one
CSV per entity into `teamtailor_qN_YYYY_export/`.

Design:
- Pagination follows the `links.next` cursor (no assumed page-count cap).
- Date filters are applied both server-side (`filter[created-at]` /
  `filter[updated-at]`) and again client-side, so the CSVs are guaranteed to
  only contain rows from that quarter even if the API filter is loose.
- `?include=` is used to pull related entities (department, location,
  recruiter, stage) inline, avoiding N+1 calls.
- On HTTP 429 the script sleeps for `Retry-After` and retries; on 5xx it
  backs off exponentially; on 403 (missing scope) it skips that endpoint
  instead of crashing the run.

## What it extracts

| CSV | Contents |
|---|---|
| `jobs.csv` | Jobs created or updated in the quarter |
| `job_applications.csv` | Applications created in the quarter |
| `candidates.csv` | Candidates created in the quarter |
| `activities.csv` | Audit trail (stage changes, notes, interviews) — tries `/activities` then falls back to `/audits` |
| `notes.csv` | Free-text interview comments (requires `notes:read` scope) |
| `reviews.csv` | Interview scorecards — tries `/reviews` then `/scorecards` (requires `reviews:read`) |
| `messages.csv` | Recruiter ↔ candidate message threads (requires `messages:read`) |
| `users.csv`, `departments.csv`, `locations.csv`, `stages.csv` | Full reference tables (no date filter) |

`notes`/`reviews`/`messages` also strip HTML from the text bodies into a
`*_clean` column (e.g. `note_clean`, `summary_clean`, `body_clean`) so the
text is usable directly in a spreadsheet or Notion without markup noise. If
those three come back empty even with the right scopes, that's a real
finding (interview documentation isn't being written into Teamtailor), not
a bug — the script prints a reminder to check token scopes so you can tell
the two cases apart.

## Stage funnel report (per role)

`stage_funnel_report.py` runs on top of a quarter's export (`job_applications.csv`,
`jobs.csv`) and produces a per-role breakdown: how many applications reached
TAS/Screening, HM/Job-fit interview, Live coding/Test, and Culture fit, plus
an `Outcome` (Hired / N/A) and `Days open` column.

```bash
export TEAMTAILOR_TOKEN="your-admin-token"   # or reuse the .env in this folder
python stage_funnel_report.py --year 2026 --quarter 2
```

It fetches each job's *own* pipeline order live from Teamtailor
(`/jobs/{id}/stages`) rather than assuming one order across every role —
some pipelines combine two categories into one stage (e.g. "Job-fit
interview + test") or skip a category entirely, and a single assumed order
would fabricate "reached" credit for stages that role's pipeline never had.

**Edit `JOB_TITLES` and `EXCLUDED_JOB_IDS` at the top of the script by hand
every quarter** — this is where you merge job IDs that are really the same
role reposted (give them the same label) and drop test/dummy postings. That
part is manual on purpose; nothing else in the script assumes a merge will
look the same next time.

Writes `stage_funnel_report.csv` into that quarter's export folder.

## Notion

Each quarter's CSVs (and a short write-up of what they show) get added to
the [Teamtailor Analytics](https://www.notion.so/thephantomcompany/Teamtailor-Analytics-a63d3fc4251982a5a16181ea88503502?source=copy_link)
Notion page, one section per quarter.
