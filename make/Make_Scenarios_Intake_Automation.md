# Make Automation — Intake Meeting Workflow
**Last updated:** May 2026  
**Owner:** Diane Rocher  
**Stack:** Google Drive · Google Calendar · Anthropic Claude · Notion · Slack · Teamtailor

---

## Overview

Two separate Make scenarios that work in sequence.  
Scenario 1 fires first and builds the entire structure.  
Scenario 2 enriches the Notion page with the HM's original JD and the interview pitch.  
Build and fully test Scenario 1 before touching Scenario 2.

---

## Scenario 1 — Core intake automation

**Trigger:** New file in Google Drive `/Intake Meetings` folder  
**Goal:** Extract the transcript, create the Slack channel, Notion page, and Teamtailor draft  
**Status:** In progress

---

### Module 1 — Google Drive: watch files in a folder

| Field | Value |
|---|---|
| Connection | Your Google account |
| Drive | My Drive |
| Folder | `/Intake Meetings` |
| Watch | New files only |
| Max files returned | 1 |
| Scheduler | Every 15 minutes |

---

### Filter — Is intake meeting?

| Field | Value |
|---|---|
| Condition | File Name · Contains (case insensitive) · `intake` |
| Label | Is intake meeting? |

---

### Module 2 — Google Drive: download a file

| Field | Value |
|---|---|
| File ID | `{{1.id}}` |
| Note | Gemini saves as Google Doc. If content is empty, switch to Get a File Content with Export Format: txt |

---

### Module 3 — Anthropic Claude: create a prompt (extraction)

| Field | Value |
|---|---|
| Model | `claude-sonnet-4-20250514` |
| Max tokens | `1000` |

**Prompt:**
```
You are a recruitment assistant. Analyze this intake meeting transcript and extract the following information. Return ONLY a valid JSON object with no preamble, no markdown, no backticks.

Extract these fields:
- role_title (string): the job title being hired for
- seniority (string): seniority level e.g. Senior, Lead, Junior
- department (string): the team or department
- meeting_date (string): date of the meeting in format YYYY-MM-DD
- hiring_manager_name (string): full name of the person whose title or role is described as a manager, head, director, VP, or lead of a team
- channel_name (string): formatted as hiring_[role_title]_[department]_[meeting_date], all lowercase, spaces replaced with hyphens, no special characters

Transcript:
{{2.data}}
```

---

### Module 4 — JSON: parse JSON

| Field | Value |
|---|---|
| JSON string | `{{3.data.content[].text}}` |
| Outputs | `role_title` · `seniority` · `department` · `meeting_date` · `hiring_manager_name` · `channel_name` |

---

### Module 5 — Anthropic Claude: create a prompt (JD + follow-up)

| Field | Value |
|---|---|
| Model | `claude-sonnet-4-20250514` |
| Max tokens | `1500` |

**Prompt:**
```
You are a senior recruitment specialist. Based on the intake meeting transcript below, generate two outputs.

Return ONLY the following structure, no preamble, no markdown code blocks:

---
JOB DESCRIPTION

[Write a compelling, complete job description for the role. Include: role summary, key responsibilities (5-7 bullet points), required skills and experience, nice-to-have skills, and what makes this role and team unique. Base everything strictly on what was discussed in the transcript.]

---
INTAKE FOLLOW-UP

[Write a structured follow-up of the intake meeting. Include:
- Role summary (2-3 sentences)
- Key requirements discussed
- Must-have skills emphasized by the hiring manager
- Nice-to-have skills
- Team context and culture signals
- Next steps agreed during the meeting]

---

Transcript:
{{2.data}}
```

---

### Module 6 — Notion: create a page

| Field | Value |
|---|---|
| Parent type | Page |
| Parent page ID | [Your "Intake meeting" page ID — copy from Notion URL] |
| Page title | `{{4.role_title}} — {{4.department}} — {{4.meeting_date}}` |
| Content block 1 | Heading: "Job Description" + body: `{{5.jd_section}}` |
| Content block 2 | Heading: "Intake Follow-up" + body: `{{5.followup_section}}` |

> **How to find the Intake meeting page ID:** Open the page in Notion → `...` menu → Copy link → the long string at the end of the URL is your page ID.

---

### Module 7 — Slack: create a channel

| Field | Value |
|---|---|
| Connection | Your Slack connection |
| Channel type | Private channel |
| Channel name | `{{4.channel_name}}` |
| Example output | `hiring_ml-engineer_data_2026-05-21` |

---

### Module 8 — Slack: invite users

| Field | Value |
|---|---|
| Channel ID | `{{7.channelId}}` |
| Users | Your hardcoded Slack user IDs (recruiting team) |
| Note | HM is invited manually — a reminder is posted in the channel |

---

### Module 9 — HTTP: make a request (Teamtailor create job)

| Field | Value |
|---|---|
| URL | `https://api.teamtailor.com/v1/jobs` |
| Method | POST |
| Header: Authorization | `Token YOUR_TEAMTAILOR_API_KEY` |
| Header: X-Api-Version | `20180828` |
| Header: Content-Type | `application/vnd.api+json` |
| Body type | Raw · application/vnd.api+json |

**Body:**
```json
{
  "data": {
    "type": "jobs",
    "attributes": {
      "title": "{{4.role_title}} - {{4.seniority}}",
      "body": "{{5.jd_section}}",
      "status": "draft"
    },
    "relationships": {
      "department": {
        "data": {
          "type": "departments",
          "id": "YOUR_DEPT_ID"
        }
      },
      "template": {
        "data": {
          "type": "jobs",
          "id": "YOUR_TEMPLATE_ID"
        }
      }
    }
  }
}
```

> **How to get your template IDs:** Run `GET https://api.teamtailor.com/v1/jobs?filter[status]=template` once with your API key.  
> **How to get your department IDs:** Run `GET https://api.teamtailor.com/v1/departments` once with your API key.

---

### Module 10 — Slack: send message (pinned recap)

| Field | Value |
|---|---|
| Channel | `{{7.channelId}}` |
| Pin message | Yes |

**Message content:**
```
👋 *New intake meeting processed — {{4.role_title}} · {{4.department}}*

Here's everything you need to get started:

📄 *Notion page:* {{6.url}}
🎥 *Drive recording:* {{1.webViewLink}}
💼 *Teamtailor draft:* [review and publish when ready]

⚠️ *Don't forget to manually invite the Hiring Manager to this channel.*
```

---

### Module 11 — Slack: send message (channel guidelines)

| Field | Value |
|---|---|
| Channel | `{{7.channelId}}` |

**Message content:**
```
📋 *Channel purpose*

This channel is the single source of truth for the {{4.role_title}} search.

Use it to:
• Share candidate profiles for feedback
• Ask the hiring manager questions
• Post sourcing updates and pipeline progress
• Align on interview feedback

The full job description and intake follow-up are in the Notion page linked above.
```

---

### What Scenario 1 produces

| Output | Where |
|---|---|
| Slack channel | `hiring_[role]_[dept]_[date]` — private, team invited |
| Notion page | Inside `/Hiring 2026/Intake meeting/` — JD + follow-up |
| Teamtailor job | Draft — not published, uses your template |
| Pinned Slack message | Recap + links to Drive and Notion |

---

---

## Scenario 2 — JD enrichment + interview pitch

**Trigger:** Same Google Drive `/Intake Meetings` folder (same filter as Scenario 1)  
**Goal:** Fetch the HM's original Notion JD from the calendar invite, save it, and generate the interview pitch  
**Dependency:** Build this only after Scenario 1 is fully tested and stable  
**Status:** To build next

---

### Prerequisites

Before building this scenario:
- Scenario 1 must be fully working
- You must consistently paste the HM's Notion JD link in your Google Calendar intake invite description
- Recommended format in the invite description: `JD: https://notion.so/...`

---

### Module 1 — Google Calendar: search events

| Field | Value |
|---|---|
| Connection | Your Google account |
| Calendar | Your main calendar |
| Search query | `intake` |
| Time range | Last 2 hours (to match the just-completed meeting) |

---

### Module 2 — Text parser: extract Notion URL

| Field | Value |
|---|---|
| Method | Match pattern |
| Pattern | `https://notion\.so/[^\s]+` |
| Input | `{{1.description}}` (calendar event description) |

---

### Module 3 — Notion: get a page

| Field | Value |
|---|---|
| Page URL | `{{2.match}}` (extracted Notion URL) |
| Returns | Full text content of the HM's original JD |

---

### Module 4 — Google Drive: download transcript

| Field | Value |
|---|---|
| Note | Same as Scenario 1 Module 2 — re-fetch the transcript to pass to Claude |
| File ID | Match by filename using the same intake file from Drive |

---

### Module 5 — Anthropic Claude: create a prompt (interview pitch)

| Field | Value |
|---|---|
| Model | `claude-sonnet-4-20250514` |
| Max tokens | `1500` |

**Prompt:**
```
You are an expert technical recruiter. Based on the intake meeting transcript and the hiring manager's original job description below, generate a structured interview pitch document.

Return ONLY the following structure, no preamble, no markdown code blocks:

---
ROLE PITCH

[2-3 sentences a recruiter can say out loud to a candidate to sell the role. Make it compelling, specific, and human. Draw from what the hiring manager said about the team vision and impact.]

---
PROJECT & TEAM

[3-4 bullet points describing the team, the project, the stack or domain if mentioned, and what makes this role unique. Extract directly from both the transcript and the original JD — do not invent details.]

---
5 QUESTIONS TO ASK CANDIDATES

Based on what the hiring manager emphasized, these are the 5 most important things to probe for:

1. [Question + 1 line explaining why this matters based on the transcript]
2. [Question + 1 line explaining why this matters based on the transcript]
3. [Question + 1 line explaining why this matters based on the transcript]
4. [Question + 1 line explaining why this matters based on the transcript]
5. [Question + 1 line explaining why this matters based on the transcript]

---
SUGGESTED JD (enriched by Claude)

[Rewrite and improve the original JD using insights from the transcript. Add context about team culture, priorities, and must-haves that the HM mentioned during the call but didn't include in their original JD. Keep the same structure but make it richer and more candidate-facing.]

---

ORIGINAL JD (as written by hiring manager):
{{3.content}}

INTAKE MEETING TRANSCRIPT:
{{4.data}}
```

---

### Module 6 — Notion: find the page created by Scenario 1

| Field | Value |
|---|---|
| Action | Search objects |
| Search query | `{{role_title}} — {{department}}` |
| Parent | Your "Intake meeting" page |
| Returns | The page ID created by Scenario 1 |

---

### Module 7 — Notion: append blocks to page

| Field | Value |
|---|---|
| Page ID | `{{6.pageId}}` (found in module 6) |
| Block 1 | Heading: "Original JD" + body: `{{3.content}}` |
| Block 2 | Heading: "Suggested JD (Claude)" + body: `{{5.suggested_jd}}` |
| Block 3 | Heading: "Interview Pitch" + body: `{{5.role_pitch}}` |
| Block 4 | Heading: "5 Questions to Ask" + body: `{{5.questions}}` |

---

### Module 8 — Slack: send message (pitch ready)

| Field | Value |
|---|---|
| Channel | Find by name using `{{channel_name}}` from extracted variables |

**Message content:**
```
📋 *Interview pitch is ready*

The Notion page has been updated with:
• Original JD from the hiring manager
• Claude's suggested enriched JD
• Role pitch you can use with candidates
• 5 key questions to ask based on the intake

→ {{6.pageUrl}}
```

---

### What Scenario 2 adds to the Notion page

| Section added | Source |
|---|---|
| Original JD | Fetched from HM's Notion page via calendar link |
| Suggested JD | Claude — enriched from transcript + original JD |
| Role pitch | Claude — 2-3 sentences to use with candidates |
| 5 candidate questions | Claude — extracted from what HM emphasized |

---

### Final Notion page structure (after both scenarios run)

```
Hiring 2026
  └── Intake meeting
        └── ML Engineer — Data — 2026-05-21
              ├── Job Description          (Scenario 1)
              ├── Intake Follow-up         (Scenario 1)
              ├── Original JD              (Scenario 2)
              ├── Suggested JD (Claude)    (Scenario 2)
              ├── Interview Pitch          (Scenario 2)
              └── 5 Questions to Ask       (Scenario 2)
```

---

## Build order

| Step | Action |
|---|---|
| 1 | Build all modules of Scenario 1 |
| 2 | Run a test with a real transcript file |
| 3 | Confirm Slack channel, Notion page, and Teamtailor draft all appear correctly |
| 4 | Fix any prompt or mapping issues |
| 5 | Activate Scenario 1 |
| 6 | Build Scenario 2 |
| 7 | Test with a real calendar event that has a Notion JD link in the description |
| 8 | Confirm Notion page is updated with all new sections |
| 9 | Activate Scenario 2 |

---

## Keys and IDs to collect before building

| What | Where to find it |
|---|---|
| Anthropic API key | console.anthropic.com → API keys |
| Teamtailor API key | Teamtailor → Settings → Integrations → API keys |
| Teamtailor department IDs | `GET https://api.teamtailor.com/v1/departments` |
| Teamtailor template IDs | `GET https://api.teamtailor.com/v1/jobs?filter[status]=template` |
| Notion "Intake meeting" page ID | Open page in Notion → `...` → Copy link → last string in URL |
| Your Slack user IDs (recruiting team) | Slack → click profile → `...` → Copy member ID |
