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
- The CSV has five columns: Title (the raw link text from the search
  result), Company, Role, Location, and URL. Company/Role/Location are
  parsed out of Title with a regex, since Google's title text for a
  LinkedIn job listing is normally either "<Company> hiring <Role> in
  <Location> | LinkedIn" or "<Role> - <Company> | LinkedIn". If a title
  doesn't match either pattern, Company and Location are left blank and
  Role falls back to the full raw title -- check those rows manually.
  It does not include work mode or posting date -- that would require
  opening each job listing individually.
- To adjust how many pages it checks or how long it waits between
  requests, open `bookmarklet.js`, change the `maxPages` and `delayMs`
  values near the top of the readable source, then regenerate a single
  line `javascript:` version from the edited code (remove line breaks
  and comments) before pasting it into a bookmark.
- This bookmarklet only reads the current Google search results page;
  it does not log in to LinkedIn or access any private data.
