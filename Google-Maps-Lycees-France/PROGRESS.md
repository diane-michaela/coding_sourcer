# Google Maps Search Export — Lycées France

## STATUS: CLOSED (2026-08-24) — Diane confirmed stop here, no further enrichment planned

Final deliverables in this folder:
- `LYCEES_FRANCE_FINAL.csv` — 1,906 lycée général schools (pure lycée + lycée/collège combos),
  1,262 with an email (66%), plus phone/website/social links/category/rating/GPS/filière.
- `lycees_no_email_found.csv` — 644 schools for manual follow-up (378 with a website but no
  email recovered after 2 enrichment passes, 266 with no website at all).

Deliberately NOT done (flagged, Diane chose to stop instead): director/headmaster name
enrichment (no standard scraped field, would need custom AI-read, low expected hit rate),
and finding websites for the 266 schools with none.


PhantomBuster agent id `3551854345694373` (shared workspace slot — was "Google Maps Search Export"
for phase 1/2, now REPURPOSED to script #22972 "Data Scraping Crawler" for the email phase, since
the account is capped at 15 Phantoms and all other slots are other active projects — do not delete).

**IMPORTANT — recurring issue**: background bash watchers (`poll_status.py` loops) keep dying
silently when the Claude Code session restarts, with no completion notification. This has happened
3+ times. DO NOT trust "no notification yet" as "still running" — always check directly:
```
cd phantombuster-api && source .env && .venv/bin/python -c "
import phantombuster_api as pb
r = pb.fetch_output('3551854345694373')
print(r.get('status'), r.get('containerId'), r.get('progressLabel'))
"
```

## Phase 1 — Google Maps scrape (DONE, partial)

- Container `3173320731286924`, launched 2026-08-13 11:07 UTC, ran ~5h.
- **Stopped early: hit PhantomBuster's "Maximum run time has been reached" cap**, not an error.
- Completed **81 of 145 queries**, extracting **2,720 places** → `phase1_raw_2720rows.csv`
- Settings used: `specifyLanguage: fr`, `numberOfResultsPerSearch: 120`, `extractCoordinates: true`, `csvName: lycees_france`

### Missing (64 queries) — see `missing_64_queries.json`
Notably **all 20 Paris arrondissements**, Rhône/Lyon, Bas-Rhin/Strasbourg, Var/Toulon,
Vaucluse/Avignon, Seine-Maritime, Seine-et-Marne, Yvelines, Essonne, Hauts-de-Seine,
Seine-Saint-Denis, Val-de-Marne, Val-d'Oise, and all 5 overseas territories
(Guadeloupe, Martinique, Guyane, La Réunion, Mayotte).

## Phase 2 batch 1 — DONE

- Container `2458061461591437`, 32 queries (items 1-32 of `missing_64_queries.json`:
  Hautes-Pyrénées ... Paris 19e arrondissement).
- Finished cleanly (status: finished), **492 new places** found.
- `fileMgmt: mix` confirmed on the agent → results already merged with phase 1's
  2,720 rows in the same cumulative `lycees_france.csv` (nbLaunches: 2).
- **Note**: the background watcher for this batch died mid-wait (Claude Code session
  restarted, killing the bash background process silently — no notification fired).
  Had to re-check status manually. If this happens again, just re-run
  `poll_status.py <agentId> <containerId>` in a loop, or check status directly via
  `pb.fetch_output(agent_id)`.

## Phase 2 batch 2 — DONE

- Container `5172299622545797`. Finished cleanly, 89 min, exit 0.
- Combined with phase 1 + batch 1 (fileMgmt: mix) → **145/145 queries done**,
  **4,004 unique lycées** total in `lycees_france_raw.csv` (downloaded from
  `https://phantombuster.s3.amazonaws.com/VLyWCsB92xw/CB61LYJJkr0IDr4wbjEgzg/lycees_france.csv`).
  No further de-dupe needed — PB's own merge already deduplicated across all 3 launches.

## Classification (DONE)

- Added `type_etablissement` column (Lycée/Collège/Lycée+Collège/Autre) based on
  title+category keyword matching → `lycees_france_classified.csv`.
  Counts: Lycée 2978, Autre 648, Lycée+Collège 232, Collège 146.
- Added `sous_type` column (filière) for the 2978 pure "Lycée" rows: Non précisé 1477,
  Professionnel 734, Polyvalent 297, Agricole 256, Général+Techno 101, Général 85,
  Techno 28.
- **Diane's scope decision**: only "lycée général" wanted. Excludes Professionnel,
  Agricole, AND Polyvalent (mixte — has a pro track too). Includes Non précisé +
  Général + Général+Techno + Techno (absence of pro/agricole/polyvalent keyword in a
  French lycée name is itself a strong signal it's a général/académique track).
  → **1,691 final rows**, **1,405 unique websites** → `lycees_general_final.csv`.
- Collège and Lycée+Collège (232 combined campuses) explicitly deferred — Diane wants
  to look at those in a separate pass later, not now.

## Email enrichment — IN PROGRESS

Agent `3551854345694373` repurposed to script #22972 "Data Scraping Crawler".
**Gotcha found**: this script's `queries` array input does NOT work (always returns
"Input spreadsheet is empty" even with fresh untried URLs) — must use `spreadsheetUrl`
pointing to a real public Google Sheet. `fileMgmt` must be `folders` (not `mix` — mix
caused a false "already scraped" state, likely scanning old Google Maps output files
in the same S3 folder for matching strings).

1,405 sites split into 7 department-clustered batches (~200 each, grouped by the
department mapping in `query_to_department_map.json`) — see `dept_batches_general/manifest.json`
for the department→file→Drive-fileId mapping. Each batch uploaded as its own Google
Sheet into Drive folder `1tk2nFqkkSvOES58BVm-2_-kzPBthy7LM` ("Lycees France - Email
Enrichment Batches"), shared as "Anyone with the link — Viewer" (confirmed via
anonymous curl export test, NOT via get_file_permissions — that tool doesn't surface
link-sharing grants, don't trust it for this check).

Crawler settings: `dataToScrape: ["Email"]`, `exitWhen: ["exitWhenEmailFound","exitWhenDepth"]`,
`exitDepth: 1`, one launch per batch (sequential, same agent).

- Batch 1 (`1585294534707235`): DONE, 203 sites, 50.2 min, **145 emails found (71%)**
  → `email_batches/batch_01_emails.json`
- Batch 2 (`8782284587028975`): DONE, 203 sites, 60.4 min, **115 emails found (57%)**
  → `email_batches/batch_02_emails.json`
- Batch 3 (`7690035788068806`): LAUNCHED, in progress
- Batches 4-7: NOT YET LAUNCHED — launch same way (update agent argument's
  `spreadsheetUrl` + `csvName` to next batch, then `pb.launch_phantom(agent_id)`),
  one at a time, after each previous one finishes.

## Coverage gap found (2026-08-19) — 5 departments hit the Google Maps 120-result cap

Checked raw place-counts per query in `lycees_france_raw.csv` against the 120 cap:
Côtes-d'Armor (120, exact cap), Bouches-du-Rhône (118, even with Marseille/Aix supplement
already in the 145-query list), Isère (118), Cher (113), Ardennes (102). All other queries
were comfortably under 100 — these 5 are the only truncated ones.

**Plan (in order, once batch 7 email-crawl finishes):**
1. Swap agent 3551854345694373 back to script "Google Maps Search Export.js" (org phantombuster)
   temporarily, and run 12 supplemental city-level queries:
   - Côtes-d'Armor: "lycée Saint-Brieuc, France", "lycée Lannion, France", "lycée Dinan, France"
   - Bouches-du-Rhône: "lycée Arles, France", "lycée Martigues, France", "lycée Istres, France"
   - Isère: "lycée Grenoble, France", "lycée Bourgoin-Jallieu, France" (NOT "lycée Vienne, France" —
     that string is already used for department 86 Vienne; reusing it for the city of Vienne in
     Isère would collide/ambiguous-geocode)
   - Cher: "lycée Bourges, France", "lycée Vierzon, France"
   - Ardennes: "lycée Charleville-Mézières, France", "lycée Sedan, France"
   Same settings as before: specifyLanguage fr, numberOfResultsPerSearch 120, extractCoordinates true.
2. Append new rows to lycees_france_raw.csv, re-run classification (type_etablissement + sous_type)
   on just the new rows, filter to général, diff against websites already covered by batches 1-8.
3. Swap agent back to "Data Scraping Crawler.js", run batch 8 (194 Lycée+Collège général sites,
   already uploaded to Drive, id 1akBL98-dgOHPVUrWliiuctZMEUHDxB4z6ecpeYn6IBU) + a new small batch 9
   for whatever new général websites the supplemental Maps pass turns up.
4. Build final merged CSV + `lycees_no_email_found.csv` (schools with website but no email found,
   for manual follow-up — Diane asked for this on 2026-08-19).

## Next steps (not yet done)

1. Launch batches 4-7 (same pattern as above), fetch each result via
   `containers/fetch-result-object`, save to `email_batches/batch_0N_emails.json`.
2. Merge all 7 batches' emails, keyed by website URL, join back onto
   `lycees_general_final.csv` to produce the final deliverable: name, address, phone,
   website, category, rating, lat/long, department, **email**.
3. Director/headmaster name enrichment — requested but NOT a standard scraped field.
   No PhantomBuster phantom extracts this directly; would need a custom AI-read step
   over each school's site, likely with a low hit rate since most public lycée sites
   don't list the "proviseur" by name. Approach still to be decided with Diane.
4. Collège / Lycée+Collège pass (232+146 rows) — deferred, Diane wants this as a
   separate second search later.

## Note on the MCP disconnect

The PhantomBuster MCP tool connection dropped mid-session. Falling back to the raw
PhantomBuster v2 API directly (key in `phantombuster-api/.env`), same approach as
`phantombuster_api.py` in the `phantombuster-api/` project.
