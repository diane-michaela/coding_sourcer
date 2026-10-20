# LinkedIn Job Search Extractor

A browser bookmarklet that scans a Google search of LinkedIn job postings
(a query using `site:fr.linkedin.com/jobs/view`), collects every unique
job link on every results page, and exports them to a CSV file you can
open in Excel or Google Sheets.

## What's in this folder

- `bookmarklet.js` -- the tool's source code, with a ready-to-paste
  one-line version at the bottom of the file.
- `README.md` -- this file.

## Requirements

- A Chromium-based browser: Google Chrome, Microsoft Edge, or Brave.
  Works the same way on macOS and Windows.
- A Google search results page using the pattern:
  `site:fr.linkedin.com/jobs/view <your keywords>`

## Installation

### macOS (Google Chrome)

1. Show the bookmarks bar if it's hidden: `Cmd + Shift + B`.
2. Open `bookmarklet.js` in any text editor (TextEdit, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`. It is URL-encoded
   (lots of `%20`, `%3D`...), which is normal.
4. Right-click anywhere on the bookmarks bar and choose "Add Page...".
5. Name it, for example `linkedin jobs`.
6. In the URL field, paste the `javascript:...` line you copied.
7. Save.

If you had an older version installed, delete the old bookmark first.

### Windows (Google Chrome or Microsoft Edge)

1. Show the bookmarks/favorites bar if it's hidden: `Ctrl + Shift + B`.
2. Open `bookmarklet.js` in any text editor (Notepad, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`.
4. Right-click the bookmarks/favorites bar and choose "Add page"
   (Chrome) or "Add favorite" (Edge).
5. Name it, for example `linkedin jobs`.
6. Paste the `javascript:...` line into the URL/address field.
7. Save.

## How to use it

1. Go to Google and run a search such as:
   `site:fr.linkedin.com/jobs/view langchain OR langgraph OR crewai`
2. Click the bookmark you created above.
3. Wait -- it pages silently through the results (roughly 1.5 seconds
   per page, so a 10-15 page search takes 20-40 seconds). Nothing
   visible happens on screen until it's done.
4. A browser alert appears when finished: "Done: X unique job
   listings exported to CSV."
5. A file named `linkedin-jobs-<timestamp>.csv` downloads automatically
   to your Downloads folder.

## CSV columns

| Column | How it's filled |
|---|---|
| ID | Export timestamp + row number (e.g. `1760870000000-001`), so rows from different exports stay distinct after merging. |
| Title | The result's heading text on Google. |
| Company | From the URL slug (see below), else from the title. |
| Role | From the URL slug, else from the title. |
| Location | Best guess from the title (`— Paris`, `hiring ... in ...`), then from bold place names in the snippet, then from a known-places list. Can be blank. |
| Pays | `France`, `Hors France`, or `?`, from a list of French cities vs. places outside France. Results with no place found default to `France` (the search targets `fr.linkedin.com`). |
| Stack | Tools found in the title/snippet, from a fixed list of ~40 (Bedrock, LangChain, AWS, Python, React, Terraform, Kubernetes, Figma...). Edit the `STACK` array to change it. |
| Source | Always `LinkedIn`. |
| URL | The cleaned job link (tracking parameters removed). |

### How Company/Role are extracted

1. **From the URL itself.** LinkedIn job URLs encode
   "<role-slug>-at-<company-slug>-<jobId>" in the path, e.g.
   `/jobs/view/ingénieur-ia-at-kiiro-4465100210`. That slug doesn't
   depend on the result's display language, so it's the primary source.
   Percent-encoded characters (`%C3%A9` for "é") are decoded first.
2. **From Title, as a fallback**, when a URL has no slug or no "-at-"
   segment. Google's title text depends on locale:
   - English: "<Company> hiring <Role> in <Location> | LinkedIn" or
     "<Role> at <Company> | LinkedIn"
   - French: "<Role> chez <Company>" " | LinkedIn", or "<Company> recrute
     [pour un poste de | pour des postes de | un | une] <Role> | LinkedIn"
   - Less often: "<Role> - <Company> | LinkedIn"

   If nothing matches, Company is left blank and Role is the raw title --
   check those rows manually.

## Notes and troubleshooting

- The script checks up to 30 result pages and stops as soon as a page
  returns no new links.
- If Google briefly blocks the requests or shows a CAPTCHA, wait a
  minute before trying again.
- Location, Pays and Stack only use what's visible on the Google results
  page (title, snippet, bold words). They're a first-pass triage, not
  verified data. Work mode, salary and posted date aren't on the results
  page at all and would need a second pass that opens each job page.
- Google's "Translate this page" links (`translate.goog`) are unwrapped
  to the real job URL, so they don't show up as junk rows.
- To change the page cap or delay, edit `maxPages` / `delayMs` in the
  readable code, then rebuild the one-line version: join the non-empty
  lines with spaces, URL-encode the result, and prefix it with
  `javascript:`.
- This bookmarklet only reads the current Google search results page;
  it does not log in to LinkedIn or access any private data.
