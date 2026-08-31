# TeamTailor headcount rollup — V2 build spec

Full spec for rebuilding **TeamTailor-report (diane)** (the live weekly Slack
report, mirrored one level up in [`../`](../)) into two Make scenarios that
persist headcount/funnel numbers week over week per role, so a role's process
can end with one cumulative "we hired!" Slack message instead of just the
last weekly snapshot.

Context: recruiting processes run for an unknown number of weeks (could be 4,
could be 10+) depending on when a role gets filled. Weekly reports go out
every Friday to the hiring team / hiring manager / interviewers / CTO in that
role's own Slack channel; once the role is filled, weekly reporting stops and
a single rollup message goes out instead.

## Why two scenarios, two Data Stores

- **Scenario 1** keeps doing what the live scenario does today (funnel
  snapshot → Slack, weekly) but also **saves** that week's numbers, and reads
  its role list from a table instead of hardcoded Python.
- **Scenario 2** is fully independent. It wakes up only when a role's status
  flips to "filled," pulls every saved week for that role, and posts one
  final message. It never touches TeamTailor directly.
- Both read/write the same two **Make Data Stores** — that's what makes
  scenario 2 possible without re-querying TeamTailor after a job gets
  archived (which is also part of why the live scenario currently shows 0
  results — see `../README.md`).

## Data Store 1 — `TT Roles Registry`

Replaces module 1's hardcoded Python array
([`../01_jobs_to_report.py`](../01_jobs_to_report.py)). One row per role
being tracked; add a role by adding a row, stop reporting by editing
`status`.

| field | type | notes |
|---|---|---|
| `job_id` | text | TeamTailor job ID |
| `job_title` | text | filled on first run |
| `channel` | text | e.g. `#product-expert-recruitment-2026` |
| `status` | text | `active` / `filled` |
| `date_added` | date | when tracking starts |
| `date_filled` | date | set when status flips to `filled` |

## Data Store 2 — `TT Weekly Snapshots`

One row per role per week — the history that makes the final report
"reliable" instead of a live TeamTailor re-query.

| field | type | notes |
|---|---|---|
| `job_id` | text | |
| `week_of` | date | run date |
| `total_all` / `total_active` / `total_rejected` | number | |
| `total_sourced` / `total_inbound` | number | |
| `days_open` | number | |
| `alerts` | text | |
| `hired_candidates` | text | comma-separated names, usually empty until the last week |

Sample rows for local testing: [`sample_snapshots.json`](sample_snapshots.json).

## Scenario 1 — `TeamTailor-report V2 (diane)` (People folder)

1. Schedule trigger — same cadence as the live scenario (Fridays 10:00)
2. **DS1: Search Records**, filter `status = active` — replaces module 1
3. Feeder — one bundle per active role (same pattern as module 40)
4. Python — [`02_generate_weekly_report_v2.py`](02_generate_weekly_report_v2.py):
   same TeamTailor fetch/compute as the live module 5, plus two new funnel
   stages (**Reference check**, **Offered**) and candidate names now surfacing
   for those two plus Hired, not just the three interview stages
5. **DS2: Add a Record** — save this week's numbers (the script's `result`
   dict maps directly onto DS2's fields, minus `slack_message`)
6. Slack: Create Message — unchanged, posts the weekly report

Also move `API_TOKEN` out of the code into a Make **Custom Variable**
(`{{API_TOKEN}}`) — flagged in `../README.md` suggestion #1, currently
hardcoded in plaintext in the live scenario.

## Scenario 2 — `TeamTailor-report — Final Summary V2 (diane)` (People folder)

1. **DS1: Watch Records** trigger — fires when a record's `status` changes to `filled`
2. Filter — `status = filled` (guards against firing on unrelated field edits)
3. **DS2: Search Records**, filter `job_id = {trigger job_id}` — every saved week for that role
4. Python — [`03_final_summary_v2.py`](03_final_summary_v2.py): builds the
   final message — thank-you + celebration emoji, who was hired, weeks
   tracked, time to fill, and a simple final headcount overview (no
   per-stage funnel breakdown — DS2 only stores totals, and the confirmed
   scope for this message is "simple")
5. Slack: Create Message → `channel` from the DS1 record — same channel the
   weekly reports for that role went to

Still runnable manually from Make's UI at any time, on top of the automatic
Watch Records trigger — not mutually exclusive.

## One open assumption

`02_generate_weekly_report_v2.py` assumes TeamTailor's **"Offer"** stage was
**renamed to "Offered"**, not that "Offered" is a new stage sitting alongside
"Offer". If both exist as distinct stages in TeamTailor, add `"Offer"` back
into `STAGE_ORDER` before `"Offered"` and add the matching conversion/
pipeline lines — check the live TeamTailor stage list before building.

## Status — built in Make (2026-08-26)

Both scenarios and both Data Stores exist live in Make (team 67084, People folder), both currently **inactive**:

| Object | Make ID |
|---|---|
| Data Store 1 — TT Roles Registry | `172310` (structure `549915`) |
| Data Store 2 — TT Weekly Snapshots | `172312` (structure `549916`) |
| Scenario 1 — TeamTailor-report V2 (diane) | [`7106751`](https://eu1.make.com/67084/scenarios/7106751/edit) |
| Scenario 2 — TeamTailor-report — Final Summary V2 (diane) | [`7106762`](https://eu1.make.com/67084/scenarios/7106762/edit) |

Two deviations from the spec above, found while building against the live Make API:

1. **No "Watch Records" trigger exists in Make's Data Store app** (confirmed via
   `app-modules_list` — only Add/Update/Get/Exist/Delete/DeleteAll/Stats/Search).
   Scenario 2's trigger was changed to **on-demand (manual run)** instead — per
   Diane, it only needs to fire once per role anyway. Idempotency comes from
   reusing the `status` field: Scenario 2 only picks up roles where
   `status = filled`, and its last step flips that role to `status = reported`
   so re-running the scenario later never double-posts.
2. **`datastore:SearchRecord` already returns one bundle per matching record**
   (`returnsMultipleBundles: true`), so the separate "Feeder" module planned
   for Scenario 1 step 3 was unnecessary and was dropped — the DS1 Search
   Records module itself iterates over active roles.

Scenario 2's DS2 lookup (module 2) still returns one bundle per weekly
snapshot, so a `builtin:BasicAggregator` (module 3, not in the original spec)
collects those into the single `snapshots` array `03_final_summary_v2.py`
expects.

### Manual steps still needed before either scenario can run

- **Paste the real TeamTailor API token** into Scenario 1 module 2's code
  (`API_TOKEN = "PASTE_TOKEN_HERE"`) — left as a placeholder since this session
  has no way to write secrets into Make and won't hardcode one sight-unseen.
  There is no exposed "Custom Variable" API, so the plaintext-in-code pattern
  from the live v1 scenario is unavoidable for now unless done by hand in the
  Make UI (Make does support storing it as a connection/variable manually).
- **Add role rows to DS1 (TT Roles Registry)** — `job_id`, `job_title`,
  `channel`, `status: active`, `date_added` — via the Make UI. Nothing was
  seeded; the only job on file (`7463157`) is archived per `../README.md`.
- **Activate Scenario 1** once at least one active role is in DS1 (it was
  created inactive, matching the live scenario's off-by-default posture).
- **Run Scenario 2 manually** from Make whenever a role's DS1 row is flipped
  to `status: filled`.

## Data store update workflow — how the cumulative report gets triggered

DS2 (Weekly Snapshots) needs **no manual editing at all** — Scenario 1 writes
one row into it every Friday, automatically, for every role currently
`active` in DS1. That's the whole point of V2: the weekly numbers are now
accumulating on their own.

DS1 (Roles Registry) is the only thing a human ever touches, and only at two
moments in a role's lifecycle:

| When | What you do in DS1 | Why |
|---|---|---|
| A new role opens | Add a record: `job_id`, `job_title`, `channel`, `status: active`, `date_added: <today>` | Scenario 1 will start reporting on it (and saving its weekly numbers into DS2) from the next Friday run |
| The role gets filled | Edit that record: `status: filled`, `date_filled: <today>` | Marks the role as done reporting weekly, and makes it eligible for the cumulative report |
| — then — | Run Scenario 2 manually (Make → "Run once") | Reads every DS2 row saved for that `job_id`, builds the "🎉 Role Filled" cumulative message, posts it, then flips the record to `status: reported` so it can't fire twice |

Nothing else needs to change by hand: the weekly numbers behind the
cumulative message are already sitting in DS2 the moment you flip `status`
to `filled` — that flip is the only signal Scenario 2 needs to build the
final report from data that's been accumulating since `date_added`.

