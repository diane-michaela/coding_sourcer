---
name: Fortin Market Mapping
description: Maps a talent market — key employers, their ecosystem, and how people move between them — before any candidate-level sourcing starts. Named after Pierre-André Fortin, founder of Anara (Paris headhunting cabinet), whose published method (anara.fr) is the direct source: a 4-phase build — Investigation (understand the sector's tools/environment) → Cartographie (identify key companies and their ecosystem) → Flow Analysis (typical career paths and talent movement between those companies) → Targeted Sourcing (approach with context) — plus his "Fifth Element" technique (surface a 5th relevant company from 4 already-known ones, e.g. via shared vendor/client lists) for expanding a company list past the obvious names. Default behavior runs Investigation and Cartographie before naming a single target company, the way Fortin's own material insists on — starting from "who's the obvious competitor" and stopping there is exactly the keyword-search trap his Veeva/pharma CRM example is built to illustrate (30 profiles from a keyword search vs. 138 once the vendor's actual client roster was mapped first). Also carries SocialTalent's market-vs-talent-mapping distinction as an explicit fork in the output — market mapping produces an aggregate picture (supply, demand, compensation, competitive dynamics) answering "is this market viable," talent mapping produces a named-candidate list answering "who exactly should we talk to" — so this skill asks which one Diane actually wants rather than assuming. Use when Diane says things like "map this market before we source," "which companies should we even be looking at for X," "give me the lay of the land for this role/sector," or opens a search with no target-company list yet. Produces a company list, flow/mobility read, and (if asked) a market-viability summary — it does NOT build boolean or X-ray query strings itself; once target companies/profiles are named, hand off to Irina LinkedIn Lite/Recruiter (LinkedIn) or Agent Bliard (everywhere else) to actually construct the search.
---

# Fortin Market Mapping — Cartographier un marché avant de sourcer

Named after **Pierre-André Fortin**, founder of **Anara** (Paris headhunting cabinet, est. 2018),
whose published method — [Market Mapping : Révéler les Talents Invisibles](https://anara.fr/market-mapping-reveler-les-talents-invisibles/)
and his [Market Mapping training](https://anara.fr/formation-market-mapping/) — is the direct
source for the phases below. Supplemented by SocialTalent's market-vs-talent-mapping distinction
and the executive-search "name generation" tradition for the Cartographie phase when specific
companies are already named. Full provenance and source-quality notes in `README.md`. The complete
method, the Veeva case study, and the further-reading list live in
`references/market-mapping-method.md` — this file is the workflow; that file is the reference it
draws on.

## Scope boundary — read this first

This skill maps the **market**, not individual candidates. It answers "which companies, and how
does talent move between them" — not "give me a boolean string for this LinkedIn profile." Once
this skill has produced a target-company list or a named-segment description, **hand off**:
LinkedIn sourcing goes to **Irina LinkedIn Lite** or **Irina LinkedIn Recruiter**; sourcing
anywhere else on the open web goes to **Agent Bliard**. Don't build search strings here even
if the next step feels obvious — that's a different skill's job, and duplicating it here would
create two places for that guidance to drift apart.

## Phase A — Investigation: understand the sector before naming a single company

Fortin's own material is explicit that skipping this is the whole reason keyword-only sourcing
plateaus. Before listing any target company, establish:

- **What tools/platforms define this sector's workflow?** (Fortin's own example: Veeva for
  pharma CRM — the tool itself becomes the anchor for everything downstream, because a vendor's
  client list is public in ways a competitor list often isn't.)
- **What's the brief actually optimizing for** — a live search (need names soon), a proactive pool
  (no live req, building ahead of one — see `[[project_pm_designer_engineer_ai_squad]]`-style
  exploratory work), or a market-viability question from a hiring manager/exec ("can we even
  hire 10 of these people in this city")? This determines which output Phase D produces.
- **What's already known** — any named competitors, feeder companies, or a JD/intake brief. Pull
  this the same way `Agent Bliard` reads a JD: don't just take the obvious title, read every
  signal (named tools, named competitors, seniority language, location).

If Diane hands this skill a JD or intake brief rather than a bare sector name, extract the signals
first rather than jumping to company names.

## Phase B — Cartographie: map the companies and their ecosystem

Two techniques, use whichever fits what's already known:

1. **Fortin's "Fifth Element" technique** — when you have ~4 known companies (competitors, or
   companies known to employ the target profile), find a 5th through what connects the four: a
   shared vendor, a shared client, a shared investor, an industry association member list, or a
   conference sponsor list. Repeat outward from each newly found company rather than stopping at
   one pass.
2. **Vendor/client-roster mapping** (the Veeva example, generalizes beyond pharma) — when the
   sector runs on a specific named tool or platform, that vendor's own public client list/case
   studies/logos page is often a faster route to a full company list than guessing competitors from
   memory. Full worked example in the reference file.
3. **Executive-search "name generation"** (when specific target companies are already named, e.g.
   from a JD) — build out the org chart at each named company: verify reporting relationships via
   public sources (press releases, funding announcements, company "team" pages — same technique
   `Agent Bliard` uses for company pages) and infer the rest from title levels and department
   names where public confirmation isn't available. Use this to go deep on a short known list
   rather than wide across an unknown one — it's the complementary move to the Fifth Element
   technique, not a replacement for it.

Set a boundary before the list grows indefinitely — Fortin's own caution: an unbounded mapping
pass is analysis paralysis, not thoroughness. Define what "enough companies" looks like for this
brief before starting (a number, or a saturation signal — the same 3-4 new companies keep
resurfacing).

## Phase C — Flow Analysis: how people actually move between the mapped companies

Once the company list is stable, work out the movement patterns between them, not just the static
list:

- Typical career paths — which companies feed which (A's senior people become B's leads), and in
  which direction the flow mostly runs.
- Mobility hypotheses — where the target profile is more likely to be reachable right now (a
  company with a recent layoff, a leadership change, a funding event) versus a company known for
  low attrition.
- This is what turns a company *list* into a sourcing *plan* — it's the difference between "here
  are 40 companies" and "here's where to start and why."

## Phase D — Choose the output: market picture or named-candidate handoff

Ask, don't assume, which of these Diane wants — they answer different questions and the SocialTalent
distinction is worth naming explicitly if it's not obvious from the ask:

- **Market mapping output** (aggregate, "is this market viable") — summarize supply (roughly how
  many people plausibly fit), demand (who else is hiring for this profile right now), compensation
  range if it's gettable, and competitive dynamics (who's growing/who's shrinking, who exports vs.
  imports this talent). This is a strategic answer, not a candidate list.
- **Talent mapping handoff** (named, "who exactly do we approach") — package the mapped companies
  and flow analysis as a target-company/target-profile brief and hand it to Irina or Agent
  Bliard to build the actual search. Say explicitly that this is the handoff point rather than
  continuing to build queries here.

## Phase E — Talent Intelligence: package it as a decision-ready deliverable

Fortin's training explicitly separates "having a market map" from "having something a hiring
manager or exec can act on." Before handing back the output, structure it as a short deliverable:
the company list (segmented — core targets vs. stretch/fifth-element finds), the flow/mobility
read, and (if Phase D chose market-picture mode) the four-dimension summary — not a raw list of
companies with no read on what it means.

## Output format

1. **What's already known vs. what Investigation surfaced** — brief, so Diane can spot if a signal
   was missed.
2. **The mapped company list**, segmented (core / fifth-element expansions / stretch), with the
   technique that surfaced each segment named.
3. **Flow/mobility read** — where to start and why, not just the list.
4. **Market picture or handoff brief**, per whichever Phase D produced — labeled clearly as one or
   the other.
5. If handing off, say explicitly: "next step is Irina LinkedIn Lite/Recruiter" or "next step is
   Agent Bliard" — don't leave the handoff implicit.

## What this skill does not do

- Does not build boolean or X-ray search strings — that's Irina LinkedIn Lite/Recruiter (LinkedIn)
  or Agent Bliard (everywhere else). This skill's output feeds those, it doesn't replace them.
- Does not do the JD-advisory/contradiction-tagging pass Irina does, or the pre-intake-meeting
  homework Vlastelica Intake Prep does — if either is also needed, run this skill for the market
  layer and bring its output into those.
- Does not treat an unbounded, ever-growing company list as more thorough — Fortin's own material
  treats that as a failure mode (analysis paralysis), not diligence. Set a boundary in Phase B.
- Does not silently pick market-mapping-output vs. talent-mapping-handoff mode — ask when it isn't
  obvious from the brief (Phase D).
