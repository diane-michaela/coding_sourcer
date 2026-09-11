# Intake Meeting Automation — Make.com blueprints

Two Make scenarios that automate the recruiter intake-meeting workflow. Last
synced 2026-09-11 directly from Make's API (equivalent to Export Blueprint in
the UI). V1 is unchanged since the 2026-08-31 export; V2 was re-exported to
capture the already-sourced/already-in-TeamTailor redesign below (see
"Architecture notes").

These JSON files are the reproducible artifact of the automation logic. They are
**not** plug-and-play — see "What you'll need to reconnect" below before importing
into a new Make org/account.

A test clone of V2 ("Intake Meeting automation V2 (Diane) - TEST CLONE (no HM
invite)", scenario id `7226728`) shares module IDs with the production scenario
by design, so a change validated on the clone can be ported to prod by editing
the same module IDs. The clone omits the two HM-invite modules (70/71) so test
runs never message a real hiring manager. Live Make scenarios are the source of
truth — these JSON files are a snapshot, re-export after making changes.

## Files

- `blueprint_v1_pre-intake-prep-question_7009201.json` — **V1, live/production.**
  Triggered when a calendar event's description contains a Notion JD link.
  Fetches the JD, has Claude generate challenge questions for the hiring manager,
  builds a Notion "Intake Meeting Questions" page, posts to Slack, and writes the
  page link back into the calendar event description.

- `blueprint_v2_intake-meeting-automation_6512042.json` — **V2, active build.**
  Triggered by a webhook fed from a Google Drive folder watch (new "Notes by
  Gemini" doc after a Meet-recorded intake call). Extracts role data via Claude
  (role, department, skills, seniority, HM, **category** — see below), then in
  parallel: (a) creates a private Slack hiring channel, auto-invites the hiring
  manager by matching `hm_name` against the workspace member list (diacritic-
  normalized), invites a fixed recruiter user, posts a role recap, then — behind
  a router that splits so one branch's failure can't block the other — (a1) an
  AI-generated sourcing brief (job titles, boolean keywords, GitHub keywords,
  tech-stack alternatives, market intel) plus a keyword search against already-
  in-TeamTailor candidates, and (a2) a Slack message linking to the pre-filtered
  Airtable view of already-sourced candidates for that role's category, (b)
  creates a Notion "TA Screening Kit" page, (c) creates a Notion "JD v2" page
  containing an updated job description merged from the original + the intake
  meeting.

## Architecture notes

- **Already-sourced / already-in-TeamTailor lookup was redesigned on 2026-09-11**
  (module 93, 103, 104→removed, 105, 106, plus a new router 111). The original
  design searched Airtable with a role-title/department/`role_bucket` match
  loosely OR'd together with every extracted skill — a single generic skill
  match (e.g. "react") was enough to surface a candidate regardless of the
  actual role, and the whole thing was gated behind Slack channel creation
  succeeding, which silently killed both lookups whenever a same-day rerun hit
  `name_taken` (see the `name_taken` note below — same root cause, this was the
  second incident it caused). The new design:
  - **Module 93** drops role-title/department matching entirely ("fuck les
    jobs titles, je veux juste une recherche par mots clés" — 2026-09-11). It
    sanitizes the extracted skills, filters out a hardcoded `WEAK_TERMS`
    blocklist of generic/ubiquitous terms (javascript, react, git, agile,
    communication, etc. — validated against real Airtable data: `pulumi`=1,
    `ansible`=2, `aws`=284, `react`=518 out of ~6,400 candidates, confirming
    rare terms are the strong signal), and **AND**s together the top 2-3
    remaining ("strong") skills into the `teamtailor-candidates` formula —
    a candidate must match all 2-3, not just one.
  - **Already-sourced candidates (`sourced-targeted-companies`, module 104) no
    longer runs an Airtable search at all.** That table has no per-candidate
    skill/keyword field worth searching (only `linkedinSkillsLabel`/
    `linkedinHeadline`, sparse); it already has a `role` singleSelect
    "category" field though (13 values: DevOps/SRE/Infrastructure, Backend,
    Frontend/Mobile/Fullstack, AI/ML/Data Science, Other Engineering, Product,
    Design, Marketing/Growth, Sales/AE/BDR/SDR, Customer Success/Enablement,
    Revenue/BizDev/Partnerships, Investor/VC/Advisor, Other/HR/Finance/Unknown).
    Module 3's extraction prompt now also returns `category` (one of that
    fixed list). Module 93 maps `category` → a pre-built Airtable **Interface**
    page URL (base `app5BF5NrOgR0kZIB`, interface `pbd3HEd1NWAnjyxCl` "Sourced
    Candidates by Category", one `visualization`/`grid` page per category with
    a hard-set `recordScopeFilters` on `role`), and module 106 posts that single
    link directly — no search, no per-record messages, no location filter
    (sourcing is already country-scoped at collection time). Module 104 was
    deleted from the blueprint.
  - **A new router 111** splits Route 1 right after module 93 into two
    independent branches: (A) the calendar/Notion JD lookup → Claude sourcing
    brief → TeamTailor keyword search (modules 90/91/92/221/80/82/81/103/105),
    and (B) the sourced-candidates link (module 106). Both branches still sit
    behind Slack channel creation (module 7/14) — intentional now, since both
    post into that per-role channel (`{{7.body.channel.id}}`, changed from the
    old fixed `C0BAD2GUQMR` for modules 105/106) — but a failure in one branch
    can no longer take out the other.
  - `maxRecords` on the TeamTailor search (module 103) raised from the default
    (10) to 50.
- **JD v2 generation (module 23 + 96) rewrites and merges, it doesn't just diff.**
  Claude receives the original JD's content and the intake meeting's extracted
  data, and returns structured `jd_blocks` (typed `{type, text}` entries) that
  reproduce the *entire* JD — boilerplate sections ("About PhantomBuster",
  "Benefits", "Hiring Process") copied verbatim, role-specific sections
  (responsibilities, requirements, team, seniority, tech stack) updated to match
  the meeting. Module 96 (`code:ExecuteCode`) converts `jd_blocks` into Notion
  block objects and appends two more sections: **"What has been changed"** (a
  past-tense changelog of concrete edits, for reviewers who don't want to diff
  the JD by hand) and **"Watch out — possible incoherencies"** (unresolved
  conflicts between the meeting and the original JD, worth a human check before
  publishing). This replaced an earlier design (2026-09-04) that copied the
  original JD verbatim and only appended a "what should be changed" suggestion
  list — the current version actually applies the changes into the JD body.
  Because Claude now regenerates the full JD text instead of a short diff list,
  module 23's `max_tokens` is 4000 (was 1000). Both module 15 (TA screening kit)
  and module 23 (JD v2) explicitly instruct Claude to never name any person —
  hiring manager, current team member, or anyone being replaced/reinforced —
  and to describe the need functionally instead (e.g. "this role covers a
  recent departure"). Added to module 23 on 2026-09-11 after a real generated
  JD v2 named a departing employee; module 15 already had the equivalent rule.
- Both scenarios build Notion page content via a **`json:TransformToJSON`**
  module (feeding a structured `object` mapper) rather than hand-typing raw JSON
  text with `{{}}` interpolations. This matters: Make's raw-body text fields have
  no reliable way to escape a literal `"` or `\` inside a formula string literal —
  there is no `char()` function (despite it looking like it should exist; this
  cost real debugging time on 2026-08-28 and again on 2026-08-31). Always route
  LLM-generated text through `TransformToJSON` before it hits a raw JSON PATCH
  body — Make's own serializer escapes it correctly, no manual escaping needed.
  Module 96 is the one exception: it builds Notion block JSON directly in Python
  (via `json.dumps`) rather than through `TransformToJSON`, since its input is
  already a parsed/validated JSON structure from Claude, not a raw interpolated
  string — no unescaped-quote risk there.
- V2's Slack channel name includes `{{formatDate(now; "YYYY-MM-DD")}}` so
  re-running the automation on a different day doesn't collide with an existing
  channel. Same-day reruns still collide (Slack won't reuse a name even from an
  archived channel — it has to be renamed or deleted first). This matters beyond
  Slack noise: **don't put a shared, filtered upstream step in front of a router
  branch that has no logical dependency on it.** A 2026-09-07 attempt to
  deduplicate the calendar/Notion lookup (modules 90/91/92/221 vs. 20/21/22/220 —
  functionally identical, both used to run once per router branch) by hoisting it
  in front of a single shared branch accidentally routed Slack-channel-creation
  (module 7/14) upstream of JD v2 generation too. JD v2 doesn't post to the
  dynamic Slack channel at all (it posts to a fixed channel), so it has no
  business depending on channel creation — but once the channel already existed
  from a same-day rerun, module 7 failed with `name_taken`, which silently killed
  the entire shared branch including JD v2. Confirmed via a diagnostic run with
  `onerror` handlers stripped (surfaced the real `name_taken` error instead of
  the swallowed empty-body fallback). The dedup was reverted; the two lookup
  chains stay duplicated on purpose so each router branch's failure modes stay
  isolated to that branch.
- V2 modules 90/91 (Slack sourcing-brief branch) and 20/21 (JD v2 branch) both
  gate on finding a calendar event titled "Intake Meeting" **today**, with the
  hiring manager's email in the attendees list. In practice the "Intake Meeting"
  placeholder event Diane creates for herself has no attendees — so the HM has to
  be manually added as a guest on that event for these branches to fire. Known
  gap, not yet structurally fixed (see Notion doc "🔍 Détail module par module —
  Scénario V2" for more).
- Modules 80/81 (V2) have `onerror → Resume` fallbacks added 2026-08-31 so a
  Claude/web_search or Slack post failure surfaces a diagnostic message in the
  thread instead of silently vanishing.
- **Module 82 (V2, added 2026-09-10)** strips LLM reasoning/narration out of the
  sourcing brief before it's posted. Module 80 uses Claude Sonnet with the
  `web_search` tool, and its `textResponse` sometimes includes the model's own
  planning text around the tool call (e.g. "I'll search for current market
  intelligence... Now I'll create the Slack-ready sourcing brief...") — the
  prompt's "Output ONLY the brief, no preamble" instruction isn't reliably
  honored once tool use is involved. Rather than fight this with more prompt
  wording, module 82 (`code:ExecuteCode`) deterministically finds the first
  occurrence of `*Job titles*` (the brief's own required first line) in
  `80.textResponse` and discards everything before it, with a regex fallback
  (first `*bolded heading*`-shaped text) if that exact marker is missing.
  Module 81 now reads `{{82.result.brief_clean}}` instead of
  `{{80.textResponse}}` directly.
- Modules 70/71 (V2, production only — not present on the test clone) auto-invite
  the hiring manager to the new Slack channel: module 70 lists all workspace
  members, module 71 filters on a diacritic-normalized, case-insensitive match
  between the member's real name and `hm_name`, then invites that user. Silent
  no-op (via `onerror`) if no match is found — the recruiter still gets invited
  either way via module 14's fixed user ID.

## What you'll need to reconnect / remap to reproduce elsewhere

Connections (`__IMTCONN__` IDs in the JSON) are account-specific and are **not**
included in the export. On import, Make will prompt to map each of:

| App | Used for |
|---|---|
| Slack | channel creation, invites, all bot messages |
| Notion | JD/TA-kit/questions page reads & writes |
| Google Calendar | reading/updating the intake event |
| Google Drive | watching for new Gemini notes docs, reading JD/transcript files |
| Anthropic (Claude) | role extraction, question/brief generation |
| Airtable | already-sourced / already-in-TeamTailor candidate lookups (V2 only) |

Hardcoded IDs to replace for a different workspace:

- Notion parent pages: `38bd3fc4251980db9253c0899aa483b9` (Interview Kit),
  `389d3fc42519803d9432cabafd45136e` (V2 JDs – Post-Intake),
  `3c1d3fc42519805a8969f37cf784cc11` (Past Intakes)
- Slack: `C0BAD2GUQMR` (fixed notifications channel — TA screening kit/JD v2
  creation pings, modules 17/25 only; the already-sourced/already-in-TeamTailor
  messages post into the dynamic per-role channel instead), `U02D68ST52S`
  (fixed invite user)
- Airtable: base `app5BF5NrOgR0kZIB`, tables `tblAJIxcjQogp1Ltz` (TeamTailor
  candidates) and `tbl01XKJ9ZQuADIcn` (sourced candidates)
- Airtable **Interface** `pbd3HEd1NWAnjyxCl` ("Sourced Candidates by
  Category") — 13 `visualization`/`grid` pages, one per `role` category value
  on `tbl01XKJ9ZQuADIcn`, each with a hard-set `recordScopeFilters`. Module 93
  hardcodes each page's URL in a `CATEGORY_LINKS` dict keyed by category name;
  reproducing this elsewhere means recreating those 13 pages (or an equivalent
  per-category filtered view/link) and updating that dict.
- Google Calendar: `diane.rocher@thephantomcompany.com`
- The webhook (V2, module 31) gets a brand-new URL on import — whatever posts to
  it (currently the Drive folder watch → Gemini notes pipeline) needs repointing.

## How to re-import

1. In Make: **Create a new scenario** → "..." menu → **Import Blueprint** → select
   the JSON file.
2. Map each connection when prompted (see table above).
3. Replace every hardcoded ID listed above with the equivalent in the new
   workspace.
4. For V2, re-point the source that feeds the webhook at the new webhook URL.
5. Leave both scenarios **inactive** until the mappings are verified — a broken
   connection reference will otherwise error on the first real trigger.
