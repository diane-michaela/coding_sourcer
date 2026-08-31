# Intake Meeting Automation — Make.com blueprints

Two Make scenarios that automate the recruiter intake-meeting workflow. Exported
2026-08-31 directly from Make's API (equivalent to Export Blueprint in the UI).

These JSON files are the reproducible artifact of the automation logic. They are
**not** plug-and-play — see "What you'll need to reconnect" below before importing
into a new Make org/account.

## Files

- `blueprint_v1_pre-intake-prep-question_7009201.json` — **V1, live/production.**
  Triggered when a calendar event's description contains a Notion JD link.
  Fetches the JD, has Claude generate challenge questions for the hiring manager,
  builds a Notion "Intake Meeting Questions" page, posts to Slack, and writes the
  page link back into the calendar event description.

- `blueprint_v2_intake-meeting-automation_6512042.json` — **V2, active build.**
  Triggered by a webhook fed from a Google Drive folder watch (new "Notes by
  Gemini" doc after a Meet-recorded intake call). Extracts role data via Claude,
  then in parallel: (a) creates a private Slack hiring channel, invites the HM,
  posts a role recap + AI-generated sourcing brief (booleans, market intel,
  already-sourced/TeamTailor candidates), (b) creates a Notion "TA Screening Kit"
  page, (c) creates a Notion "JD v2" page comparing against the original JD.

## Architecture notes

- Both scenarios build Notion page content via a **`json:TransformToJSON`**
  module (feeding a structured `object` mapper) rather than hand-typing raw JSON
  text with `{{}}` interpolations. This matters: Make's raw-body text fields have
  no reliable way to escape a literal `"` or `\` inside a formula string literal —
  there is no `char()` function (despite it looking like it should exist; this
  cost real debugging time on 2026-08-28 and again on 2026-08-31). Always route
  LLM-generated text through `TransformToJSON` before it hits a raw JSON PATCH
  body — Make's own serializer escapes it correctly, no manual escaping needed.
- V2's Slack channel name includes `{{formatDate(now; "YYYY-MM-DD")}}` so
  re-running the automation on a different day doesn't collide with an existing
  channel. Same-day reruns still collide (Slack won't reuse a name even from an
  archived channel — it has to be renamed or deleted first).
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
- Slack: `C0BAD2GUQMR` (notifications test channel), `U02D68ST52S` (fixed invite
  user)
- Airtable: base `app5BF5NrOgR0kZIB`, tables `tblAJIxcjQogp1Ltz` (TeamTailor
  candidates) and `tbl01XKJ9ZQuADIcn` (sourced candidates)
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
