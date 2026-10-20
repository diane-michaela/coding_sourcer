# Google Jobs X-Ray Extractors

Two browser bookmarklets that page through a Google search of job
postings, collect every unique job link on every results page, and export
them to a CSV file you can open in Excel or Google Sheets.

| Bookmarklet | Use it on | Keeps |
|---|---|---|
| `bookmarklet.js` (LinkedIn) | `site:fr.linkedin.com/jobs/view <keywords>` | LinkedIn job pages only |
| `any_site_bookmarklet.js` (any site) | any job search, e.g. `site:jobs.lever.co OR site:welcometothejungle.com <keywords>` | every organic result |

## What's in this folder

- `bookmarklet.js` -- the LinkedIn extractor's source code, with a
  ready-to-paste one-line version at the bottom of the file.
- `any_site_bookmarklet.js` -- the any-site extractor, same layout.
- `README.md` -- this file.

## Requirements

- A Chromium-based browser: Google Chrome, Microsoft Edge, or Brave.
  Works the same way on macOS and Windows.
- A Google search results page (see the table above for the query pattern).

## Installation

Repeat for each bookmarklet you want.

### macOS (Google Chrome)

1. Show the bookmarks bar if it's hidden: `Cmd + Shift + B`.
2. Open `bookmarklet.js` (or `any_site_bookmarklet.js`) in any text
   editor (TextEdit, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`. It is URL-encoded
   (lots of `%20`, `%3D`...), which is normal.
4. Right-click anywhere on the bookmarks bar and choose "Add Page...".
5. Name it, for example `linkedin jobs` or `alljobs`.
6. In the URL field, paste the `javascript:...` line you copied.
7. Save.

If you had an older version installed, delete the old bookmark first.

### Windows (Google Chrome or Microsoft Edge)

1. Show the bookmarks/favorites bar if it's hidden: `Ctrl + Shift + B`.
2. Open `bookmarklet.js` (or `any_site_bookmarklet.js`) in any text
   editor (Notepad, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`.
4. Right-click the bookmarks/favorites bar and choose "Add page"
   (Chrome) or "Add favorite" (Edge).
5. Name it, for example `linkedin jobs` or `alljobs`.
6. Paste the `javascript:...` line into the URL/address field.
7. Save.

## How to use it

1. Go to Google and run a search such as:
   `site:fr.linkedin.com/jobs/view langchain OR langgraph OR crewai`
2. Click the bookmark you created above.
3. Wait -- it pages silently through the results (roughly 1.5 seconds
   per page, so a 10-15 page search takes 20-40 seconds). Nothing
   visible happens on screen until it's done.
4. A browser alert appears when finished: "Done: X unique ... exported
   to CSV."
5. A CSV downloads automatically to your Downloads folder:
   `linkedin-jobs-<timestamp>.csv` or `google-jobs-<timestamp>.csv`.

## CSV columns

| Column | LinkedIn | Any site | How it's filled |
|---|---|---|---|
| ID | yes | -- | Export timestamp + row number (e.g. `1760870000000-001`), so rows from different exports stay distinct after merging. |
| Title | yes | yes | The result's heading text on Google. |
| Company | yes | yes | LinkedIn: from the URL slug (see below), else from the title. Any site: from the ATS URL (Lever/Greenhouse/Ashby path, Teamtailor/Recruitee/Personio subdomain, WTTJ `/companies/<name>`...); other sites fall back to the domain name. |
| Role | yes | yes | LinkedIn: URL slug, else title. Any site: the title with the company name and site suffix (`| LinkedIn`, `- Welcome to the Jungle`...) removed. |
| Location | yes | yes | Best guess from the title (`— Paris`, `CDI à Lyon`, `hiring ... in ...`), then from bold place names in the snippet, then from a known-places list. Can be blank. |
| Pays | yes | yes | `France`, `Hors France`, or `?`, from a list of French cities vs. places outside France. LinkedIn results with no place found default to `France` (the search targets `fr.linkedin.com`). |
| Stack | yes | yes | Tools found in the title/snippet, from a fixed list of ~40 (Bedrock, LangChain, AWS, Python, React, Terraform, Kubernetes, Figma...). Edit the `STACK` array to change it. |
| Source | yes | yes | `LinkedIn`, or the ATS / site name (Lever, Greenhouse, Ashby, Teamtailor, Workable, WTTJ, Other...). |
| URL | yes | yes | The cleaned job link (tracking parameters removed). |

### How LinkedIn Company/Role are extracted

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

- Both scripts check up to 30 result pages and stop as soon as a page
  returns no new links.
- If Google briefly blocks the requests or shows a CAPTCHA, wait a
  minute before trying again.
- Location, Pays and Stack only use what's visible on the Google results
  page (title, snippet, bold words). They're a first-pass triage, not
  verified data. Work mode, salary and posted date aren't on the results
  page at all and would need a second pass that opens each job page.
- Google's "Translate this page" links (`translate.goog`) are unwrapped
  to the real job URL, and links pointing back to Google itself are
  dropped, so you don't get junk rows.
- To change the page cap or delay, edit `maxPages` / `delayMs` in the
  readable code, then rebuild the one-line version: join the non-empty
  lines with spaces, URL-encode the result, and prefix it with
  `javascript:`.
- These bookmarklets only read the current Google search results page;
  they do not log in to LinkedIn or access any private data.
