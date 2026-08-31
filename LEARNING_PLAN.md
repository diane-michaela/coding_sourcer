# Learning Plan — Terminal, DevTools & Debugging (for sessions with Python teacher)

Grounded in the actual gaps found in `teamtailor/scrap-career-page.py` and `teamtailor/batch_scrape_people.py` (hardcoded CSS selectors, no session/auth handling, no retry logic, no JS-rendered page support).

## 1. Terminal

- **Navigation & file ops**: `cd`, `ls -la`, `find`, `mv`/`cp`, reading paths — already doing `OUTPUT_DIR = os.path.join(...)` in Python; do the same moves natively in the shell.
- **Running & controlling scripts**: passing CLI args (`sys.argv[1]` in `scrap-career-page.py`), env vars (`ENRICH=1 python batch_scrape_people.py`), exit codes, `Ctrl+C` to kill a runaway scraper.
- **Piping & redirection**: `python script.py > out.csv 2> errors.log`, `|`, `grep`, `wc -l` to sanity-check output without opening a CSV every time.
- **Process management**: `ps`, `kill`, running long scrapers with `nohup`/`&` and checking on them later — useful once batch jobs run for 20+ minutes.
- **Virtual envs & packages**: `venv`, `pip install -r requirements.txt` (already used in `github_extraction/`) — ask why isolating envs per project matters.
- **git basics**: `status`, `diff`, `add`/`commit`, `.gitignore` — already in use; ask for branching + undoing mistakes (`checkout`, `reset`) since that's the part that causes panic.

## 2. Browser HTML console (DevTools) — for scraping better

- **Elements panel + selector-picking**: right-click → Inspect, hover to find exact selectors (e.g. `div.flex.flex-col.gap-16`) — build these live instead of guessing/copy-pasting from saved HTML files.
- **Console as a scratchpad**: run `document.querySelectorAll(...)` in the browser console to test a CSS/JS selector *before* writing it into BeautifulSoup — replaces the current workflow of saving `html-example.html` to train an LLM on structure.
- **Network tab**: the big one — inspect XHR/fetch calls to find the underlying JSON API a page calls. Many "hard to scrape" pages are just rendering a JSON response client-side; hitting that endpoint directly beats parsing HTML.
- **Reading request headers/cookies**: how auth tokens, cookies, and `User-Agent` get sent — relevant since current scripts fake a `User-Agent` but don't handle login-gated pages.
- **Spotting JS-rendered vs server-rendered pages**: "View Source" vs Elements panel showing different HTML tells you when `requests` + BeautifulSoup will fail and Selenium/Playwright is needed instead.

## 3. Thinking / debugging better

- **Reading a stack trace top-to-bottom vs bottom-to-top** — knowing which line actually threw vs which line is just the caller.
- **Bisection debugging**: `print`/breakpoint at the midpoint of a failing pipeline instead of re-running the whole script — the multi-stage pipeline (`find_teamtailor_companies.py` → `filter_tech_companies.py` → `batch_scrape_people.py`) is a good exercise for isolating which stage produced bad data.
- **Defensive assumptions**: current code already does `if not name and not title: continue` — ask for the general principle of "what can be `None`/missing/malformed here, and what happens if it is."
- **Rate-limit/anti-bot thinking**: why `time.sleep()` + randomized jitter (already in `batch_scrape_people.py`) isn't just politeness — it's what keeps you from getting IP-banned mid-run.

## 4. Case study: `meetup/` project — why this matters concretely

This project is the clearest evidence of why Network tab + JSON + auth understanding matter, because a software engineer had to do the reverse-engineering *for* you. The README says it plainly: the raw browser requests were captured from DevTools into `meetup-request.txt` / `meetup-member-info.txt` before any Python was written. Breaking down what that took:

- **No HTML was scraped at all.** `meetup_request.py` and `meetup_member_info.py` both POST directly to `https://www.meetup.com/gql2`, Meetup's internal GraphQL endpoint — the same endpoint the Meetup web app itself calls. This only works if you can find that endpoint in the first place, which means:
  - Opening the **Network tab** (not the Console — see shortcut note below), loading the attendees list on meetup.com, and watching which request actually returns the member data.
  - Right-clicking that request → **"Copy as fetch"/"Copy as cURL"** to get the exact URL, headers, and JSON payload — that's where every field in the `headers = {...}` dict in `meetup_request.py` (lines 6–40) came from: `sentry-trace`, `sec-ch-ua`, `baggage`, etc. Nobody typed those by hand.
- **Understanding Apollo Persisted Queries.** The payload doesn't send a GraphQL query string — it sends a `sha256Hash` (`extensions.persistedQuery.sha256Hash`, line 59) that the server maps to a stored query called `getEventByIdForAttendees`. To know this exists and matters, you have to actually read the captured request payload and notice the query text is *missing*, not guess.
- **Reading and navigating nested JSON.** The response shape `data["data"]["event"]["rsvps"]["edges"][i]["node"]["member"]` (line 77, `meetup_member_info.py` line 51) has to be discovered by expanding the JSON tree in DevTools' Network → Response/Preview panel *before* writing the Python dict-access chain — guessing at nesting from Python alone would take forever.
- **Cursor-based pagination.** `pageInfo.hasNextPage` / `endCursor` (lines 80–83) is a JSON pagination pattern totally different from the "next page URL" pattern used for the Teamtailor HTML scrapers — recognizing which pagination style an API uses comes from reading multiple sequential Network requests and diffing their payloads.
- **Auth via cookies/JWT, not a login form.** `meetup_member_info.py` needs a `cookie` header with `__meetup_auth_access_token` (a JWT), `MEETUP_SESSION`, `memberId`, and `MEETUP_CSRF` (lines 21–28) — all copied from an authenticated browser session's Network tab. Understanding *why* this is needed (the sidebar profile query is only server-authorized for logged-in sessions) and that the JWT has an `exp` claim that will make it expire and require re-copying, is exactly the "auth/session" gap flagged in section 2.

**Shortcut note:** On Mac Chrome, `⌥⌘J` (Option+Command+J) jumps straight into DevTools' **Console** tab — good for running one-off JS like `document.querySelectorAll(...)` or inspecting a variable. For everything described above (finding the GraphQL request, copying headers/payload, reading the JSON response tree), you want the **Network** tab instead (`⌥⌘I` opens DevTools generally, then click "Network," or just click the tab after `⌥⌘J`). Ask your teacher to walk through: Console vs Network vs Elements — when each one is the right tool, since this project needed Network+Elements far more than Console.

**Practice goal:** be able to reproduce the `meetup/` project's Step 1 (`meetup_request.py`) from scratch on a *different* public Meetup event — i.e., open Network tab yourself, find the `gql2` request, copy the payload/headers, and adapt the pagination loop — without needing someone else to hand you the captured request first.
