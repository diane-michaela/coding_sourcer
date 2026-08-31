# Company reverse-search — Figma+React France (weekly)

Goal: instead of searching for candidates directly, flag **companies** that post job
listings matching a hybrid design/engineer profile (Figma + React/TS + relevant
titles), in France, so we can build a target-company list for sourcing —
analogous to the n8n "LinkedIn Job Finder" (Bright Data + Google Sheets) workflow,
but built on PhantomBuster since that's free for this workspace.

## Mechanism

PhantomBuster's **LinkedIn Job Scraper** phantom only scrapes details from job
URLs you already have — it can't discover jobs by keyword. The phantom that does
keyword+location discovery on LinkedIn Jobs is **LinkedIn Search Export** run with
`category: Jobs` (a normal LinkedIn Jobs search URL as input). That's what this
pipeline uses.

## Search query

Mandatory anchor term `Figma`, combined with stack/title terms as an OR-group
(titles are searched as free-text keywords, not LinkedIn's structured title
filter — deliberate, so postings under any title wording still match):

```
Figma AND (React OR TypeScript OR "Product Designer" OR "Software Engineer"
OR "Frontend Engineer" OR "PM Engineer" OR "Product Manager"
OR "Forward Deployed Engineer" OR "AI Product Lead")
```

Location: France (`geoId=105015875`, confirmed via an actual LinkedIn Jobs search,
not guessed).

Full search URL used as the phantom's input:
```
https://www.linkedin.com/jobs/search-results/?keywords=Figma%20AND%20%28React%20OR%20TypeScript%20OR%20%22Product%20Designer%22%20OR%20%22Software%20Engineer%22%20OR%20%22Frontend%20Engineer%22%20OR%20%22PM%20Engineer%22%20OR%20%22Product%20Manager%22%20OR%20%22Forward%20Deployed%20Engineer%22%20OR%20%22AI%20Product%20Lead%22%29&geoId=105015875
```

Output fields of interest: `companyName`, `companyUrl`, `jobTitle`, `location`.
The "company of interest" list is built by extracting the company field from
matches — not by shortlisting the job postings themselves.

## PhantomBuster config

| Setting | Value |
|---|---|
| Agent name | `Company Reverse-Search — Figma+React France (weekly)` |
| Agent ID | `8280433903384467` |
| Script | LinkedIn Search Export (`scriptId` 3149, org `phantombuster`) |
| Identity | Diane Rocher — Recruiter Lite (`identityId` 4264401258256688) |
| Results per launch | 200 |
| Watcher mode | On — only surfaces newly-appeared postings each run |
| Enrich job-poster profiles | Off |
| Schedule | Weekly, Monday 09:00, Europe/Paris |
| Output | CSV only for now (`company_reverse_search_figma_react_france`), no Airtable/Sheets push yet |
| Dedup across weeks | Not handled by the phantom — needs a separate script (same pattern as `rank_profiles.py`) to diff week-over-week and only flag genuinely new companies |

Phantom URL: https://phantombuster.com/640105445030552/phantoms/8280433903384467

## Credential issue — resolved 2026-08-14

Two manual test launches via the MCP (containers `8823305913853972`,
`8183844040257516`) failed with `No valid credentials found` (exit code 87 / 1) —
a documented PhantomBuster MCP limitation where identity binding via the API is
unreliable. Fixed by reconnecting the LinkedIn identity manually through the
PhantomBuster web UI using the browser extension (pulled the session cookie from
an already-logged-in browser tab, avoiding the CAPTCHA that a fresh automated
login triggered). Third launch (container `4217595102714240`) then succeeded:
`Connected successfully as Diane Rocher`, exit code 0.

## First run results

- Container: `4217595102714240`, run 2026-08-14
- 50 job postings matched, saved to `company_reverse_search_figma_react_france.csv`
  (in this folder) and the equivalent `.json`
- Fields: `jobUrl, jobId, jobTitle, companyName, location, workplaceType,
  isRemote, postedAt, insights, ...`
- First two hits: **Alan** (Nantes, Product Designer), **Nabla** (Paris,
  Product Designer)
- Note: the run log shows "Total results count: 25" reported twice and then
  "Stopping processing: All identities have been exhausted or disconnected" —
  worth watching on the next scheduled run to confirm it's not silently capping
  below the configured 200/launch for a benign reason (e.g. LinkedIn's actual
  match count) vs. an identity hiccup.

## Paused — 2026-08-14

Diane reported being disconnected from LinkedIn / suspecting a flag shortly
after the first successful run. No new phantom launch had actually occurred at
that point (last container `4217595102714240` still shows the 50-result
success from earlier the same day) — but as a precaution, matching the protocol
from the 2026-07-31 LinkedIn cookie-invalidation incident, the agent's
`launchType` was switched from `repeatedly` to `manually` so it will **not**
auto-fire next Monday. The `repeatedLaunchTimes` config (Mon 09:00 Europe/Paris)
is still saved on the agent — switch `launchType` back to `repeatedly` to
resume once the LinkedIn account status is confirmed safe. Do not resume
without explicit go-ahead.

## Alternatives considered

- **Sales Navigator company search** (keyword-in-description + geo filter) —
  more direct "reverse company search," but needs a Sales Navigator seat.
- **Apify** — has more generic/flexible actors (Google Search, tech-stack
  detection, GitHub/job-board scraping) that could catch non-LinkedIn stack
  signals PhantomBuster's catalog doesn't cover, but PhantomBuster is free for
  this workspace and already has the account-safety guardrails + existing
  scripting infra, so it's the default choice for the LinkedIn-shaped part of
  this search.
