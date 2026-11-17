---
name: "job-reversed-search"
description: Reverse job search for recruiters and sourcers. Turns a job description, an intake brief or a list of skills into a Google X-ray of LinkedIn job postings, collects results from Google result pages only, delivers one CSV file of postings, each labelled Product, Staffing or Unsure, then proposes a PhantomBuster chain on the Product companies (LinkedIn Job Scraper for full job descriptions and company page URLs, LinkedIn Company Employees Export for the people, LinkedIn Profile Scraper for their profiles). Use this skill whenever someone wants to find companies hiring for a skill set or a role, map a market from job postings, see who is building with a given stack, run a "reverse" or "X-ray" job search, or asks things like "which companies in France are hiring for X", "run it for the UK, Poland and Spain", "find job posts mentioning Y and Z", "build me a company list from job ads". Do NOT use it for people or profile search (that is a LinkedIn Recruiter or people-sourcing task, not this one).
---

# Job Reversed Search

Find the companies that are hiring for a skill set or a role by searching LinkedIn job postings through Google, then hand the heavy lifting to PhantomBuster.

The value of this skill is speed and a clean hand-off. Claude builds a precise query, collects what Google already shows, and stops there. Everything that needs to open job pages one by one belongs to PhantomBuster, which does it faster, at scale and on a schedule.

## Hard limits (read first)

- **Never open job postings and never loop through job descriptions.** No fetching each posting, no guest-API loops, no keyword verification per job. Google's title and preview text are the whole input. If the user wants dates, status, applicant counts, work mode or verified keywords, that is the LinkedIn Job Scraper in Phase D, not Claude.
- **Never solve or work around a CAPTCHA.** If Google shows one, stop and ask the user to solve it in their own browser, then continue.
- **Google only, through the bookmarklet or the connected browser.** Never use Firecrawl, Serper, SerpAPI or any other search API or scraping service, even when a key is available. If no browser is connected, give the user the bookmarklet and wait for their CSV.
- **One CSV file only, always shown in Claude, never Google Drive.** The only output file is `postings.csv`, and it is always delivered here in Claude as a file card (present it), whatever its size. Never leave it only in the browser's Downloads folder. Do not make a separate company list. Never create a Google Sheet, a Google Drive file, an Excel file or a Claude Doc, even when a Drive or Sheets connector is connected, and do not offer to. Do not call any Google Drive or Google Sheets tool while running this skill.
- **This skill does not search people.** LinkedIn Recruiter operators (headline:, skills:, seniority:, yoe:, functions:, etc.) mean nothing to Google. Do not use them here.

## Phase A: Build the query

Ask one question if it is not already clear: **is the focus the stack or the role?**

- **Stack focus** (any job that uses these tools): keywords only, no intitle.
- **Role focus** (a specific job title, whatever the stack): add intitle: terms for the title variants.

Then build the query with the rules in `references/boolean-rules.md`. In short:

| Need | Rule |
|---|---|
| Mandatory term | A space. Never write AND. |
| Alternatives | OR, or the vertical bar `\|` once the query is over 35 characters |
| Exact keyword or phrase | Quotes, only for multi-word phrases or a word Google would swap for a synonym. Never quote everything. |
| Role focus | intitle: repeated per term, joined with `\|`. Never allintitle:. |
| Country | site: on the country LinkedIn subdomain plus /jobs/view (default fr.linkedin.com/jobs/view). Several countries: one query per country, see "Several countries" below |
| Exclusions | Minus sign (-stage, -alternance, -freelance...), **only when the user asks to exclude something**. Never add one by default. |
| Freshness | Add tbs=qdr:m (past month) or qdr:w (past week) to the URL |

Keep the query under about 32 words: Google silently ignores what comes after.

**No exclusions unless asked.** Do not add any minus-sign term on your own, not even common ones like -stage or -alternance: they can drop good postings that mention the word in passing. Add an exclusion only when the user explicitly asks to exclude something (for example "no internships", "remove freelance", "exclude -stage"). If the results show noise afterwards, point it out in the summary and suggest the exclusion, but do not rerun with it until the user says so.

**Turning a JD into a query:** mandatory skills become space-separated terms. Alternative tools for the same need become one `|` group. Desirable-only skills stay out of the query (they shrink results too much) and are listed for later scoring.

Always show the query to the user before running it, with one line saying which terms are mandatory and which are alternatives.

**Example (stack focus, France):**
```
site:fr.linkedin.com/jobs/view Bedrock (langchain | llamaindex | langgraph | crewai | autogen | "semantic kernel" | haystack | dspy)
```
**Same, role focus:** add `(intitle:engineer | intitle:ingénieur | intitle:architect)`.

### Several countries (Europe, or any list of countries)

Keep the same query and change only the country subdomain. Run **one query per country**: Google stops at about 100 to 120 results per query, so one query for the whole of Europe would silently drop most countries.

| Country | Subdomain | Notes |
|---|---|---|
| France | fr.linkedin.com | Default |
| United Kingdom | uk.linkedin.com | Largest market; the code is UK, not GB |
| Poland | pl.linkedin.com | |
| Spain | es.linkedin.com | |
| Portugal | pt.linkedin.com | Small market, few results |
| Lithuania | lt.linkedin.com | Small market, few results |
| Germany, Netherlands, Italy, Ireland, Belgium, Switzerland | de., nl., it., ie., be., ch.linkedin.com | Not tested yet: check page 1 before trusting it |

The six first ones were tested in one run (17 Nov 2026) and all returned postings.

- **Small markets** can share one query to save time: `(site:pt.linkedin.com/jobs/view | site:lt.linkedin.com/jobs/view | site:ie.linkedin.com/jobs/view)` followed by the skill groups. The site: terms eat into the 32-word budget, so shorten the groups if needed.
- **Search by skill, not by title, across languages.** Tool names (vLLM, LoRA, Bedrock, LangGraph...) stay in English everywhere; titles do not. For a role focus, add the local title next to the English one: ingénieur (FR), ingeniero (ES), engenheiro (PT), inżynier (PL), inžinierius (LT). Most tech postings in PL, LT and PT are written in English anyway.
- **Remote roles** ("remote within Europe") are often attached to one country only or to www.linkedin.com. If the user cares about them, add one query on `site:linkedin.com/jobs/view` with `(remote | EMEA | Europe)`; those rows get Country code `?`.
- Show the user the full list of queries (one line per country) before running them.

## Phase B: Collect from Google result pages only

Two ways, depending on the setup:

1. **User runs it (default, works for anyone):** give the user the Google search URL and the bookmarklet in `scripts/bookmarklet.js`. One click on the first results page is enough: it goes through the following Google pages by itself (up to 30, about 1 second apart), stops when a page brings no new job, removes duplicates and downloads a CSV named `linkedin-jobs-<country code>-<timestamp>.csv` with the columns ID, Title, Company, Role, Location, Country code, Stack, Source, URL. **Country code is the code of the LinkedIn subdomain in capitals** (FR, UK, PL, ES, PT, LT...), or `?` when the URL has none (www.linkedin.com). The user then drops that CSV in the chat. For several countries, the user runs the bookmarklet once per country search and drops all the CSVs.
   Install steps to give the user (once):
   1. Show the bookmarks bar: Cmd+Shift+B (Mac) or Ctrl+Shift+B (Windows).
   2. Right-click the bar, then Add page (Chrome, Brave) or Add favorite (Edge). Name it `LinkedIn jobs to CSV`.
   3. In the URL field, paste the whole `javascript:` line found at the very bottom of `scripts/bookmarklet.js`, under READY-TO-PASTE BOOKMARKLET. It is full of `%20`, which is normal. Give this line exactly as written in the file, never rewrite or shorten it.
   4. Open the Google search, wait for the results, click the bookmark once and wait for the "Done" message. If Google shows a CAPTCHA, solve it, then click again.
2. **Claude runs it (when a browser tool is connected):** never rewrite the extraction logic by hand. Use the ready scripts, which hold the bookmarklet's own code, so columns and values are identical to a bookmarklet CSV.
   - **Fast mode (default).** Open the Google URL in a tab, check page 1 loaded without a CAPTCHA, then run `scripts/fast-extract.js` once in that page. It fetches the following pages in the background, so the tab stays on page 1, and returns the row count, pages read and why it stopped. Then bring the rows into Claude with `scripts/readback.js` (see below).
   - **Visible mode (only if the user wants to watch the pages load).** Use `scripts/page-extract.js`: navigate the tab page by page (&start=10, 20, ...) and run it on each page, then its FINALIZE snippet. Slower, about 3 to 5 times longer.
   - **Bringing the rows into Claude (always).** Run `scripts/readback.js` once in the same tab: it makes a compact copy of the rows (constant parts removed), keeps it in `window.jrsCompact` and in sessionStorage under `jrs-compact`, and returns the country, timestamp, URL prefix, row count and length. Read it with `sessionStorage.getItem('jrs-compact').slice(a, b)` in slices of 950 characters, about 10 slices per browser call (browser_batch), and append each call's output to the container straight away. Copy the text exactly; never paraphrase or tidy it. Then close the tab you opened.
   - **Several countries.** Run the countries one after the other in the same tab: navigate to the country's Google URL, check page 1, run `fast-extract.js`, which also saves that country's rows in sessionStorage under `jrs-<country code>` (jrs-uk, jrs-pl...). Then, still before moving on, set `KEY = 'jrs-<country code>'` on the first line of `readback.js`, run it, and save the slices to `<country code>.txt` (uk.txt, pl.txt...) in the container. Read each country back before starting the next one, so a CAPTCHA never costs a country already collected. Pause about 20 seconds between countries: Google blocks fast series of searches.
   - **CAPTCHA in the middle of a multi-country run:** stop, tell the user which countries are already saved and which one is blocked, ask them to reload that page and solve the CAPTCHA in the Chrome window, then rerun the blocked country and continue with the rest.

Either way:
- The company name is in the LinkedIn URL, after `-at-` and before the job ID (for example `...-at-wecasa-4469669619` gives Wecasa). Use it when the Google title does not show the company.
- Google shows 10 results per page. The num=100 parameter no longer works.
- Stop at the notice "In order to show you the most relevant results, we have omitted some entries" (French: "omis certains résultats"). That means the search is complete.
- Stop at a CAPTCHA and hand over to the user.
- Remove duplicates using the job ID (the long number at the end of the posting URL).
- Say plainly that Google sometimes loosens the AND, so a few rows may not truly match. Verification happens in Phase D.

## Phase C: Deliver

Produce one CSV file, `postings.csv`, and nothing else: no company list, no Google Sheet, no Drive file, no Excel file, even if a connector is available. Encode it in UTF-8 with BOM so accents open correctly in Excel.

`postings.csv` is the bookmarklet's CSV, unchanged, with one column added. When the rows came from the connected browser, build it with `scripts/build-postings.py` (compact.txt plus a types.json of company to Company type), then check the row count matches what the browser reported.

**Several countries:** first run `scripts/merge-countries.py --out compact.txt uk.txt pl.txt es.txt ...` (or the user's bookmarklet CSVs). It writes every URL in full, removes postings that appear in two countries (same job ID) and prints the count per country. Then run `build-postings.py` on that compact.txt as usual; the `--prefix` value is not used when every URL is full. Still one file only: `postings.csv`, all countries together, with the Country code column telling them apart. Keep every bookmarklet column, in the same order, with the same values: ID, Title, Company, Role, Location, Country code, Stack, Source, URL. Do not rename, reorder, clean, merge or drop anything. The only change is a new **Company type** column inserted right after Company:

ID, Title, Company, Company type, Role, Location, Country code, Stack, Source, URL.

**Company type** takes exactly one of three values:

- **Product:** the company builds and sells its own product or runs its own service, and the person hired works on it (startups, scale-ups, software vendors, large companies with in-house tech teams).
- **Staffing:** the hire works for someone else's client or is placed by a third party: staffing and recruitment agencies, ESN / IT consulting / SSII, freelance and job platforms, job aggregators.
- **Unsure:** anything you cannot place with confidence. This includes rows where Google shows no company name, unknown small companies, and names you do not recognise. When in doubt, write Unsure. Never guess.

Base the label on what you already know about the company and on the job title and URL. Do not open job postings or company pages to decide. The label never removes a row: Staffing and Unsure rows stay in the file.

Then a short summary: number of postings, number of companies, count per country (Country code column) when several countries were searched, count per company type, the companies with the most postings, and any rows that look suspicious (odd location, volunteer or internship post, stack term probably not in the job description).

Do not invent fields Google does not show. Posting date, open or closed status, applicant count and work mode are not in Google results. Leave them out and point to Phase D.

## Phase D: Propose the PhantomBuster chain (always)

End every run by proposing this chain. Always suggest running it on the **Product** companies of `postings.csv` only: Staffing people work for clients and add noise. Unsure rows join only once the user has checked them and confirmed they are Product. Do not launch anything without the user's go-ahead.

**Golden rule: the people phantoms take LinkedIn company page URLs, never company names.** A company name typed into them returns nothing useful. That is why the order below matters.

1. **[LinkedIn Job Scraper](https://phantombuster.com/automations/linkedin/6772788738377011/linkedin-job-scraper)**
   - Input: the URL column of `postings.csv`, Product rows.
   - Output: the full job description, posting date, whether it is still open, applicant count, work mode, a real check that the stack is in the posting, and `companyUrl` (the LinkedIn company page URL).
   - This step replaces everything Claude must not do in Phases B and C (opening postings, verifying keywords). It also produces the company page URLs that step 2 needs.
2. **[LinkedIn Company Employees Export](https://phantombuster.com/automations/linkedin/3295/linkedin-company-employees-export)**
   - Input: the `companyUrl` column from step 1, Product companies only, with the position filter set to the role's titles (the intitle: variants from Phase A, or the title from the JD).
   - It only works if the Job Scraper ran first: this phantom takes LinkedIn company page URLs, not names.
3. **[LinkedIn Profile Scraper](https://phantombuster.com/automations/linkedin/5589386912058181/linkedin-profile-scraper)**
   - Input: the profile URLs from step 2.
   - Output: headline, About, skills and experience.
   - The pitch line to give the user for this step: "their employer proved the stack for them". These people work at a company whose job postings show it uses the stack, so the stack is credible even when their own profile does not list it.

**Attention: where the company page URLs come from**

- **Job Scraper ran first:** use its `companyUrl` column as the input of step 2.
- **Job Scraper skipped:** run the **LinkedIn Company URL Finder** on the Company column of `postings.csv` first (Product rows, France / fr-FR, duplicates removed). Its `linkedinUrl` column becomes the input of step 2. Tell the user to check its matches before using them: it matches on names, so homonyms and wrong pages happen.
- **Never the LinkedIn Company Scraper for this:** it cannot find URLs from a name, it needs the URL already.

If the PhantomBuster connector is available in the session, offer to set up the chosen phantom with the right column as input. If not, describe each step and the exact column to feed it.

## Output format

1. The query (with the mandatory vs alternatives line).
2. How it was collected (pages scraped, where it stopped and why), per country when there are several.
3. The single CSV file, `postings.csv`, presented here in Claude.
4. The short summary.
5. The PhantomBuster chain (Job Scraper, Company Employees Export, Profile Scraper) on the Product companies, with the company URL warning.
