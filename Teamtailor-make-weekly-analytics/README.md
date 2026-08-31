# TeamTailor-report (diane) — Make scenario mirror

Local copy of the Python code from the Make scenario **TeamTailor-report (diane)**
(team 67084, scenario 5316616):
https://eu1.make.com/67084/scenarios/5316616/edit

Runs weekly (Fridays, 10:00) and is meant to post a hiring-funnel report to Slack
per open job.

> **Superseded by V2.** A rebuilt version of this scenario (reads its role list
> from a Make Data Store instead of the hardcoded list below, plus a second
> scenario that posts a one-time "role filled" rollup) is live in Make as of
> 2026-08-26 — see [`report/README.md`](report/README.md) for scenario/data
> store IDs and required manual setup steps. This v1 scenario is still off and
> can stay off; it's kept here as the historical mirror.

## Flow

| Module | Type | Role |
|---|---|---|
| 1 | `code:ExecuteCode` (Python) | Hardcoded list of `{job_id, channel}` to report on → [01_jobs_to_report.py](01_jobs_to_report.py) |
| 40 | `builtin:BasicFeeder` | Iterates over module 1's array |
| 5 | `code:ExecuteCode` (Python) | Pulls job + applications from TeamTailor API, computes funnel/stage stats, builds Slack message → [02_generate_weekly_report.py](02_generate_weekly_report.py) |
| 7 | `slack:CreateMessage` | Posts `{{5.result.slack_message}}` to `{{40.channel}}` |

## Why it doesn't run right now

Confirmed both of these directly against the scenario and the TeamTailor API:

1. **The scenario is switched OFF** — `isActive: false`. Even with a valid weekly
   schedule configured, it won't fire until it's turned back on in Make.
2. **The only job hardcoded in module 1 is archived.** Job `7463157`
   ("Product Expert - Customer Care Agent") has `status: archived`. A live check
   of `GET /v1/jobs?filter[status]=open` right now returns **0 open jobs** — so
   even if you turn the scenario on today, module 1's list needs at least one
   currently-open job/channel pair or the report will run against stale/no data.

Fix: once a new role opens, add its `job_id`/Slack channel to module 1's list (or,
better, make that list dynamic — see suggestion #3 below).

## Optimization suggestions

1. **API token is hardcoded in plaintext inside module 5's code.** Anyone with
   access to the scenario (or its exported blueprint) can read
   `tf3fhc18vdMX1LUmmfoi-WSywaASSNOnxCmRGw3w` directly. Move it to a module
   *input* variable at minimum (Code app → "Input" section), or better, store it
   in a Make Data Store / connection so it's not sitting in the code body. This
   local mirror already reads it from `.env` (`TEAMTAILOR_TOKEN`) instead, for
   reference.
2. **No error handling around the TeamTailor HTTP calls in module 5.** A single
   `urllib.error.HTTPError` (closed job, revoked token, rate limit, TeamTailor
   outage) throws uncaught. With `maxErrors: 3` the whole scenario just stops
   after 3 failed runs — silently, since there's no error-route/Slack-alert
   module wired to the flow. Add a basic try/except that posts a short failure
   message to a private/ops Slack channel, or add an error handler route in Make.
3. **Module 1's job list is a manual, hardcoded array.** Every time a role opens
   or closes you have to edit the code inside Make. Consider replacing it with a
   live call to `GET /v1/jobs?filter[status]=open` and a small lookup (e.g. a
   Make Data Store keyed by `job_id → slack_channel`, or a TeamTailor custom
   field on the job holding the channel name) so the scenario tracks whatever is
   actually open without a code edit each time.
4. **Stage names (`STAGE_ORDER`, `INTERVIEW_STAGES`) are hardcoded strings** tied
   to your current pipeline. If a stage gets renamed in TeamTailor, the report
   silently buckets those applications under "Unknown" rather than failing loudly
   — worth logging/alerting when `stage_name == "Unknown"` shows up so a pipeline
   rename doesn't quietly break the stats.
5. Minor: pagination has no rate-limit/backoff handling — fine at current volume,
   but if a job accumulates hundreds of applications this could start hitting
   TeamTailor's rate limits with no retry.

None of the above have been applied to the live Make scenario — this is a
read-only mirror + review. Say the word if you want me to push any of these
changes into the scenario directly.
