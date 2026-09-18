# LinkedIn Job Search Extractor

A browser bookmarklet that scans a Google search of LinkedIn job postings
(a query using `site:linkedin.com/jobs/view`), collects every unique job
link on every results page, and exports them to a CSV file you can open
in Excel or Google Sheets.

## What's in this folder

- `bookmarklet.js` -- the tool's source code, with a ready-to-paste
  one-line version at the bottom of the file.
- `README.md` -- this file.

## Requirements

- A Chromium-based browser: Google Chrome, Microsoft Edge, or Brave.
  Works the same way on macOS and Windows.
- A Google search results page using the pattern:
  `site:linkedin.com/jobs/view <your keywords>`

## Installation

### macOS (Google Chrome)

1. Show the bookmarks bar if it's hidden: `Cmd + Shift + B`.
2. Open `bookmarklet.js` in any text editor (TextEdit, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`.
4. Right-click anywhere on the bookmarks bar and choose "Add Page...".
5. Name it, for example `Export LinkedIn Jobs CSV`.
6. In the URL field, paste the `javascript:...` line you copied.
7. Save.

### Windows (Google Chrome or Microsoft Edge)

1. Show the bookmarks/favorites bar if it's hidden: `Ctrl + Shift + B`.
2. Open `bookmarklet.js` in any text editor (Notepad, VS Code, etc.).
3. Scroll to the bottom, under "READY-TO-PASTE BOOKMARKLET", and copy
   the entire line that starts with `javascript:`.
4. Right-click the bookmarks/favorites bar and choose "Add page"
   (Chrome) or "Add favorite" (Edge).
5. Name it, for example `Export LinkedIn Jobs CSV`.
6. Paste the `javascript:...` line into the URL/address field.
7. Save.

## How to use it

1. Go to Google and run a search such as:
   `site:linkedin.com/jobs/view langchain OR langgraph OR crewai`
2. Click the bookmark you created above.
3. Wait -- it pages silently through the results (roughly 1.5 seconds
   per page, so a 10-15 page search takes 20-40 seconds). Nothing
   visible happens on screen until it's done.
4. A browser alert appears when finished: "Done: X unique job
   listings exported to CSV."
5. A file named `linkedin-jobs-<timestamp>.csv` downloads automatically
   to your Downloads folder. Open it in Excel or Google Sheets.

## Notes and troubleshooting

- The script checks up to 30 result pages by default and stops on its
  own as soon as a page returns no new links.
- If Google briefly blocks the requests or shows a CAPTCHA, wait a
  minute before trying again -- this can happen after many fast
  automated requests in a row.
- The CSV has four columns: Title (the raw link text from the search
  result), Company, Role, and URL. Company and Role are extracted two
  ways, in this order:
  1. **From the URL itself.** LinkedIn job URLs encode
     "<role-slug>-at-<company-slug>-<jobId>" in the path, e.g.
     `/jobs/view/ingénieur-ia-at-kiiro-4465100210`. That slug is part of
     the canonical URL, not a Google-rendered title, so it's the same
     regardless of the result's display language -- this is the
     primary source and covers English, French, and any other locale
     without needing a language-specific pattern. Accented characters
     and punctuation come through the anchor's raw href still
     percent-encoded (e.g. "%C3%A9" for "é"), so the slug is run through
     `decodeURIComponent` before being split into words -- without that
     step those percent codes would show up literally in Company/Role.
  2. **From Title, as a fallback**, when a URL has no slug (bare
     numeric job IDs) or no "-at-" segment. Google's title text for a
     LinkedIn job listing depends on result locale:
     - English: "<Company> hiring <Role> in <Location> | LinkedIn" or
       "<Role> at <Company> | LinkedIn"
     - French: "<Role> chez <Company>" (sometimes with " -- <Location>"
       appended) " | LinkedIn", or "<Company> recrute [pour un poste de
       | pour des postes de | un | une] <Role> | LinkedIn"
     - Less often: "<Role> - <Company> | LinkedIn"

  If a title doesn't match any of the patterns above and the URL has no
  usable slug either, Company is left blank and Role falls back to the
  full raw title -- check those rows manually. There's no Location
  column: it only ever showed up in a couple of the title patterns
  above and was blank or wrong everywhere else, so it wasn't reliable
  enough to expose as its own field. None of this includes work mode,
  salary, posted date, or framework mentions -- those aren't on the
  Google results page at all and would require a second pass that
  opens each job's own LinkedIn page individually, which this
  bookmarklet doesn't do.
- Google occasionally injects unrelated UI links (e.g. "AI Mode",
  "Translate this page") into the results page whose href happens to
  carry a linkedin.com/jobs/view URL as a tracking/wrapper parameter
  rather than being the job link itself. The script resolves each
  anchor's real target (unwrapping translate.goog links) and only keeps
  it if that target's host is linkedin.com and its path starts with
  /jobs/view, so these show up as their real listing (deduped against
  the direct link) instead of as junk rows.
- To adjust how many pages it checks or how long it waits between
  requests, open `bookmarklet.js`, change the `maxPages` and `delayMs`
  values near the top of the readable source, then regenerate a single
  line `javascript:` version from the edited code (remove line breaks
  and comments) before pasting it into a bookmark.
- This bookmarklet only reads the current Google search results page;
  it does not log in to LinkedIn or access any private data.
