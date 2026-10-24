# li > Git / Git > li bookmarklets

Two browser bookmarklets that link a person's LinkedIn profile and GitHub account,
in either direction.

- **li → github**: on a LinkedIn profile, finds and ranks the GitHub accounts that
  most likely belong to that person.
- **github → li**: on a GitHub profile, recovers the person's real name from their
  public commits (the `.patch` trick) and opens the matching LinkedIn search.

## What's in this folder

| File | Role |
|---|---|
| `linkedin_to_github.js` | Readable source of the LinkedIn → GitHub bookmarklet |
| `github_to_linkedin.js` | Readable source of the GitHub → LinkedIn bookmarklet |
| `build.py` | Turns each `.js` into a one-line `javascript:` bookmarklet and writes `install.html` |
| `install.html` | Page with draggable buttons to install both bookmarklets (asks for the token on first use) |
| `*.bookmarklet.txt` | The built one-line versions |
| `.env.example` | Template for the `.env` file that holds your GitHub token |

Not committed (see `.gitignore`): `.env` (holds `GITHUB_TOKEN=...`), and the
`*_local.*` files, a build with the token baked in. LinkedIn wipes unknown
localStorage, so the token has to live inside the bookmark itself.

## Install

1. **GitHub token (needed for li → github, recommended for github → li).** Create one at
   github.com/settings/tokens > Generate new token (classic), **no scope ticked**.
   Copy `.env.example` to `.env` in this folder and paste the token:
   `GITHUB_TOKEN=ghp_...`. `.env` is gitignored and never committed. Without it, the
   bookmarklet asks for the token in a prompt, and LinkedIn may forget it between visits.
2. Run `python3 li-git-bookmarklets/build.py`.
3. Open `install_local.html` (with token) or `install.html` (asks for it) in Chrome,
   show the bookmarks bar (`Cmd+Shift+B`) and **drag** the blue buttons onto it.
   Pasting the code into the address bar doesn't work: Chrome strips `javascript:`.

After any change to a `.js` file: rebuild, then delete and re-drag the bookmark.

## li → github (LinkedIn → GitHub)

On a LinkedIn profile, click the bookmark. The panel reads name, city, company,
headline and the profile URL (vanity), all editable, then:

- lists GitHub links already on the page (open "Contact info" first to catch those);
- **Classer (API)** gathers candidates and ranks them by score;
- offers ready-made GitHub and Google searches as a fallback.

Candidates come from: GitHub user search on full name (with then without location,
then without accents), `<first><last> in:login`, public members of the company's
GitHub org whose name matches the first name, a direct lookup of the LinkedIn
vanity as a GitHub login, and commit search by author name.

| Signal | Points |
|---|---|
| LinkedIn URL in bio / website | +5 |
| GitHub login = LinkedIn vanity | +4 |
| Commits signed with the full name (git author) | +4 |
| Exact display name | +3 |
| Company in bio / company field | +3 |
| Location | +2 |
| Public member of the company's GitHub org | +2 |
| Repo languages match headline tech keywords | +2 |
| Login starts with first+last name | +1 |

## github → li (GitHub → LinkedIn)

On a GitHub profile (github.com/username), click the bookmark. It runs at once:

- LinkedIn links on the page and in the account's social links;
- **real name from commits**: searches the user's public commits
  (`author:<login>`) and lists the git author names and non-noreply emails, the
  same data as opening a commit with `.patch` appended to its URL. When the
  profile shows only a nickname, the most frequent full name replaces it;
- a big **Ouvrir LinkedIn** button (LinkedIn people search on name + company);
- likely LinkedIn URLs (`firstlast`, `first-last`) and Google X-ray searches
  (`site:fr.linkedin.com/in` by default, then `site:linkedin.com/in`).

Works without a token (60 API calls/hour); prompts for one when the limit is hit.

## How it was trained

There's no machine learning. "Training" means tuning the rules against a set of
people whose real answer was already known.

1. **Known-answer set.** Colleagues and candidates whose LinkedIn and GitHub were
   both known (for example a GitHub link in their profile, or confirmed by hand).
   Their profile pages were saved locally as test fixtures, outside the repo.
2. **Replay against the live API.** A Node + JSDOM runner loads each saved page,
   runs the bookmarklet code with a real GitHub token and prints the ranking. The
   question for each person: is the true account found, and at which rank?
3. **Each miss becomes a rule.** Every failure was traced to a cause and fixed
   with a general rule, never a per-person exception:

   | Failure seen | Rule added |
   |---|---|
   | GitHub profile with no location, bio or website | Login = LinkedIn vanity (+4); headline tech vs repo languages (+2) |
   | GitHub display name is only a first name | `<first><last> in:login` search; LinkedIn URL in the website field (+5) |
   | Empty account with no name, numeric suffix on the vanity | Direct lookup of the vanity with and without hyphens |
   | LinkedIn shows only a last-name initial | Public members of the company's GitHub org (+2) |
   | "Greater X Area" / "X Metropolitan Area" locations | Location cleanup before searching |
   | School shown where the company should be | Company line starting with "·" = no company |
   | Account shows a nickname only | Commit search by author name (+4), the `.patch` trick through the API |
   | Commit search on a name with an initial pulled in strangers | Commit search only when every name part has 3+ letters |

4. **Re-run the whole set after every change.** A fix for one person must not push
   another one down.
5. **Test what actually ships.** The built one-line bookmarklet is decoded and
   syntax-checked, then run in headless Chrome on the real GitHub page. That's how
   the build issue below was caught.

Last run: the true account ranked 1st in most known cases. The remaining misses
are structural, not tuning problems:

- several empty accounts under the same name, with nothing to tell them apart;
- accounts whose commits are signed with a nickname, or with no public commits.

## Limits and maintenance

- **No `//` comments in the `.js` files.** `build.py` joins the file into one line,
  so a line comment disables everything after it and the bookmarklet silently does
  nothing.
- Results are probabilities: always check the profile before contacting anyone.
- LinkedIn's page markup changes often; if fields come out empty, the selectors at
  the top of `linkedin_to_github.js` need updating.
- Not yet tested on the Recruiter / Recruiter Lite profile layout.
