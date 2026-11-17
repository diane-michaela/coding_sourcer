# LinkedIn Job Search Extractor

A bookmarklet that turns a Google search of LinkedIn job posts into a CSV file.

You write the Google search, the bookmarklet reads every results page for you and
downloads one row per job post: company, role, location, stack seen in the snippet, URL.

This folder has three things:
- this guide: how to write your search string, and how to create the bookmarklet;
- `bookmarklet.js`: the code. The line to copy is at the bottom of the file;
- [`skill/SKILL.md`](skill/SKILL.md): the same reverse search as a Claude skill (see section 4).

## 1. Write your Google search string

Example (AI engineers on AWS Bedrock with an agent framework, in France):

```
site:fr.linkedin.com/jobs/view bedrock (langchain | langgraph | llamaindex | crewai | autogen | dspy | "semantic kernel")
```

How to read it, piece by piece:

| Piece | What it does | Why |
|---|---|---|
| `site:` | Only returns pages from that website. | You want LinkedIn job posts, nothing else. |
| `fr.linkedin.com` | The country version of LinkedIn. | `fr.` keeps posts LinkedIn shows on its French site. Change it for another country: `uk.`, `de.`, `es.`, `it.`, `nl.`, `ca.`... One country per search. |
| `/jobs/view` | Only individual job posts. | Without it, you also get search pages and company pages, not one post per result. |
| `bedrock` | A word that must appear. | Google reads the whole public post, so a tool named in the 7th bullet still matches. |
| `(a \| b \| c)` | At least one of these words. | The vertical bar `\|` means OR. It's shorter than writing `OR`, which matters because Google stops reading after about 32 words. `OR` in capitals also works. |
| `"semantic kernel"` | This exact phrase. | Use double quotes for any tool with two words or more. Single quotes `' '` don't work. |

Rules to remember:
- **There is no AND in Google. A space means AND.** `bedrock langchain` = both words must appear.
- **Group your alternatives in brackets**, then add the required words outside them.
- **Keep it simple.** Search on the stack (the tools), not the job title: a Platform or
  Backend post can list the ML stack too, and a title filter would hide it.

### About the country: `fr.` is not a location filter

The `fr.` subdomain depends on where the recruiter was (their IP address) when they
published the post, not on where the job is. So a `fr.` search can return a few posts
based abroad, and miss a French post published from another country.

If location matters, add it to the search:
- a country: `site:fr.linkedin.com/jobs/view bedrock (langchain | langgraph) france`
- one or several cities: `site:fr.linkedin.com/jobs/view bedrock (langchain | langgraph) (paris | lyon | nantes)`

Or keep the search broad and filter on the **Location** column of the CSV afterwards.

### More examples

```
site:fr.linkedin.com/jobs/view figma (react | "design system")
site:uk.linkedin.com/jobs/view terraform (kubernetes | k8s) london
site:de.linkedin.com/jobs/view (sagemaker | mlflow) (berlin | munich)
```

## 2. Create the bookmarklet

A bookmarklet is a browser bookmark that runs a small script instead of opening a website.
Nothing to install, no extension, no account. Works in Chrome, Edge and Brave, on Mac and Windows.

1. Show the bookmarks bar: `Cmd + Shift + B` (Mac) or `Ctrl + Shift + B` (Windows).
2. Open [`bookmarklet.js`](bookmarklet.js) and scroll to the very bottom, under
   **READY-TO-PASTE BOOKMARKLET**.
3. Copy the whole line that starts with `javascript:`. It's full of `%20`, `%3D`... that's normal.
4. Right-click the bookmarks bar, then **Add page...** (Chrome, Brave) or **Add favorite** (Edge).
5. Name it, for example `LinkedIn jobs to CSV`.
6. In the **URL** field, paste the `javascript:` line. Save.

If you had an older version, delete the old bookmark first.

## 3. Use it

1. Paste your search string in Google and press Enter.
2. Click the `LinkedIn jobs to CSV` bookmark, on the Google results page.
3. Wait. It reads the result pages in the background (up to 30 pages, about 1 second each).
   Nothing moves on screen.
4. A message says **"Done: X unique job listings exported to CSV."**
5. The file `linkedin-jobs-<number>.csv` is in your Downloads. Open it in Google Sheets or Excel.

Columns: ID, Title, Company, Role, Location, Pays, Stack, Source, URL.
Location and Stack only come from what Google shows (title and two lines of text): use them
to sort, then open the post to check. The Pays column (France / Hors France) is built for
French searches: ignore it if you search another country.

If Google shows a CAPTCHA, solve it, wait a minute, and click the bookmark again.

The bookmarklet only reads the Google results page you're on. It doesn't log in to LinkedIn
and doesn't send your data anywhere.

## 4. Or let Claude do it: the `job-reversed-search` skill

[`skill/SKILL.md`](skill/SKILL.md) is a Claude skill that runs the same reverse search for you,
starting from a job description, an intake brief or a list of skills:

1. **Builds the query** (stack focus or role focus) and shows it to you before running it.
   No exclusion (`-stage`, `-alternance`...) unless you ask for one.
2. **Collects from Google only**: either you click this bookmarklet and drop its CSV in the
   chat, or Claude in Chrome runs the same bookmarklet code for you (`skill/scripts/`). It never
   uses a search API, never opens the job posts and never works around a CAPTCHA.
3. **Delivers one CSV**, `postings.csv`: the bookmarklet's columns, unchanged, plus one
   **Company type** column (Product, Staffing or Unsure). No Google Sheet, no Drive file.
4. **Proposes a PhantomBuster chain** on the Product companies: LinkedIn Job Scraper →
   Company Employees Export → Profile Scraper. The Employees Export needs LinkedIn company page
   URLs: they come from the Job Scraper, or from the Company URL Finder if you skip it.

Bookmarklet or skill? The bookmarklet is a one-click CSV with no AI. The skill uses the same
extraction code, then adds the Company type and the PhantomBuster hand-off.

**Install it:** in Claude, go to *Settings → Capabilities → Skills*, then upload a zip of the
`skill` folder. In Claude Code, copy the folder to `~/.claude/skills/job-reversed-search/`.
The skill runs only when you ask for it ("job reversed search", "reverse job search",
"job X-ray").
