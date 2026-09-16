# Reverse search — LinkedIn job postings → target companies/roles

Two independent pipelines, both flipped from "search for candidates" to "search job
postings to find companies/roles worth targeting." Kept in this one folder/README so
both are visible at a glance instead of one hiding behind the other.

| | Pipeline 1 — Company reverse-search | Pipeline 2 — Job Watch (ML & Product) |
|---|---|---|
| Covers | Figma+React France | ML + Product LinkedIn job postings, France |
| Engine | PhantomBuster (LinkedIn Search Export) | Google Custom Search API (X-ray) |
| Cadence | Weekly (Monday 09:00, Europe/Paris) | Monthly (GitHub Action, 1st of month) |
| Output | CSV in this folder | Rows appended to a Google Sheet (2 tabs) |
| Status | Paused 2026-08-14 (LinkedIn disconnect precaution) | Paused (GCP billing blocker) |

---

## Pipeline 1 — Company reverse-search — Figma+React France (weekly)

Goal: instead of searching for candidates directly, flag **companies** that post job
listings matching a hybrid design/engineer profile (Figma + React/TS + relevant
titles), in France, so we can build a target-company list for sourcing —
analogous to the n8n "LinkedIn Job Finder" (Bright Data + Google Sheets) workflow,
but built on PhantomBuster since that's free for this workspace.

### Mechanism

PhantomBuster's **LinkedIn Job Scraper** phantom only scrapes details from job
URLs you already have — it can't discover jobs by keyword. The phantom that does
keyword+location discovery on LinkedIn Jobs is **LinkedIn Search Export** run with
`category: Jobs` (a normal LinkedIn Jobs search URL as input). That's what this
pipeline uses.

### Search query

Mandatory anchor term `Figma`, combined with stack/title terms as an OR-group
(titles are searched as free-text keywords, not LinkedIn's structured title
filter — deliberate, so postings under any title wording still match):

```
Figma AND (React OR TypeScript OR "Product Designer" OR "Software Engineer"
OR "Frontend Engineer" OR "PM Engineer" OR "Product Manager"
OR "Forward Deployed Engineer" OR "AI Product Lead")
```

Location: France (`geoId=105015875`, confirmed via an actual LinkedIn Jobs search,
not guessed).

Full search URL used as the phantom's input:
```
https://www.linkedin.com/jobs/search-results/?keywords=Figma%20AND%20%28React%20OR%20TypeScript%20OR%20%22Product%20Designer%22%20OR%20%22Software%20Engineer%22%20OR%20%22Frontend%20Engineer%22%20OR%20%22PM%20Engineer%22%20OR%20%22Product%20Manager%22%20OR%20%22Forward%20Deployed%20Engineer%22%20OR%20%22AI%20Product%20Lead%22%29&geoId=105015875
```

Output fields of interest: `companyName`, `companyUrl`, `jobTitle`, `location`.
The "company of interest" list is built by extracting the company field from
matches — not by shortlisting the job postings themselves.

### PhantomBuster config

| Setting | Value |
|---|---|
| Agent name | `Company Reverse-Search — Figma+React France (weekly)` |
| Agent ID | `8280433903384467` |
| Script | LinkedIn Search Export (`scriptId` 3149, org `phantombuster`) |
| Identity | Diane Rocher — Recruiter Lite (`identityId` 4264401258256688) |
| Results per launch | 200 |
| Watcher mode | On — only surfaces newly-appeared postings each run |
| Enrich job-poster profiles | Off |
| Schedule | Weekly, Monday 09:00, Europe/Paris |
| Output | CSV only for now (`company_reverse_search_figma_react_france`), no Airtable/Sheets push yet |
| Dedup across weeks | Not handled by the phantom — needs a separate script (same pattern as `rank_profiles.py`) to diff week-over-week and only flag genuinely new companies |

Phantom URL: https://phantombuster.com/640105445030552/phantoms/8280433903384467

### Credential issue — resolved 2026-08-14

Two manual test launches via the MCP (containers `8823305913853972`,
`8183844040257516`) failed with `No valid credentials found` (exit code 87 / 1) —
a documented PhantomBuster MCP limitation where identity binding via the API is
unreliable. Fixed by reconnecting the LinkedIn identity manually through the
PhantomBuster web UI using the browser extension (pulled the session cookie from
an already-logged-in browser tab, avoiding the CAPTCHA that a fresh automated
login triggered). Third launch (container `4217595102714240`) then succeeded:
`Connected successfully as Diane Rocher`.

### First run results

- Container: `4217595102714240`, run 2026-08-14
- 50 job postings matched, saved to `company_reverse_search_figma_react_france.csv`
  (in this folder) and the equivalent `.json`
- Fields: `jobUrl, jobId, jobTitle, companyName, location, workplaceType,
  isRemote, postedAt, insights, ...`
- First two hits: **Alan** (Nantes, Product Designer), **Nabla** (Paris,
  Product Designer)
- Note: the run log shows "Total results count: 25" reported twice and then
  "Stopping processing: All identities have been exhausted or disconnected" —
  worth watching on the next scheduled run to confirm it's not silently capping
  below the configured 200/launch for a benign reason (e.g. LinkedIn's actual
  match count) vs. an identity hiccup.

### Paused — 2026-08-14

Diane reported being disconnected from LinkedIn / suspecting a flag shortly
after the first successful run. No new phantom launch had actually occurred at
that point (last container `4217595102714240` still shows the 50-result
success from earlier the same day) — but as a precaution, matching the protocol
from the 2026-07-31 LinkedIn cookie-invalidation incident, the agent's
`launchType` was switched from `repeatedly` to `manually` so it will **not**
auto-fire next Monday. The `repeatedLaunchTimes` config (Mon 09:00 Europe/Paris)
is still saved on the agent — switch `launchType` back to `repeatedly` to
resume once the LinkedIn account status is confirmed safe. Do not resume
without explicit go-ahead.

### Alternatives considered

- **Sales Navigator company search** (keyword-in-description + geo filter) —
  more direct "reverse company search," but needs a Sales Navigator seat.
- **Apify** — has more generic/flexible actors (Google Search, tech-stack
  detection, GitHub/job-board scraping) that could catch non-LinkedIn stack
  signals PhantomBuster's catalog doesn't cover, but PhantomBuster is free for
  this workspace and already has the account-safety guardrails + existing
  scripting infra, so it's the default choice for the LinkedIn-shaped part of
  this search.

---

## Pipeline 2 — LinkedIn Job Watch — ML & Product (monthly, GitHub Action)

> **Status (2026-09-15): paused.** The pipeline is fully built and all 4 GitHub secrets
> are set, but `GOOGLE_API_KEY` needs a GCP billing account linked to work — the Custom
> Search JSON API returns `403 forbidden` without one, even within the free 100
> queries/day quota. Decided not to add billing for now. The workflow's monthly
> `schedule` trigger is commented out in `.github/workflows/reverse-search-linkedin-xray.yml`
> so it doesn't fail-and-email every month — `workflow_dispatch` (manual run) still works
> for testing. To resume: either link a GCP billing account (real cost should stay $0 at
> this usage) or rework the search step to use PhantomBuster instead, then uncomment the
> `schedule:` block.

Veille mensuelle automatisee : X-ray Google (`site:linkedin.com/jobs/view ...`) via l'API
Google Custom Search, dedoublonnage par ID LinkedIn, ecriture directe dans le Google Sheet
["AI Agent Framework — LinkedIn Job Leads (FR)"](https://docs.google.com/spreadsheets/d/1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA/edit).

Complementaire, pas un remplacement, du Pipeline 1 ci-dessus (PhantomBuster, cote
candidat/entreprise Figma+React) — celui-ci tourne gratuitement sur l'API Google Custom
Search, cote offres ML/Product, et ecrit directement dans un Sheet au lieu d'un CSV.

**Limite a connaitre avant de laisser tourner ca en automatique :** le script applique un
filtrage minimal (dedoublonnage par ID LinkedIn + `dateRestrict` sur le dernier mois). Il ne
reproduit pas le jugement applique a la main (reperer le bleed des blocs "Recherches
similaires", distinguer une vraie offre Product Manager d'une simple mention en sidebar).
Toute nouvelle ligne est donc ajoutee avec le statut `Needs review (auto-added)` plutot que
d'etre silencieusement acceptee — a trancher une fois par mois, pas a relancer a la main.

### Onglets du Sheet

Confirmes sur le Sheet lui-meme : **ML**, **Product**, **Platform**, **Frontend** (colonnes
A-I : Company, Job Title, Location, Work Mode, Salary, Framework(s) Mentioned, Posted, Status,
LinkedIn URL).

Seuls **ML** et **Product** ont une requete definie dans `linkedin-xray-scripts/linkedin_job_watch.py`
pour l'instant — voir "Les recherches" ci-dessous. **Platform** et **Frontend** existent deja
dans le Sheet mais n'ont pas encore de mots-cles definis ici : a completer dans le dict
`TABS` du script une fois decides.

### Les recherches

**Onglet ML**
```
site:linkedin.com/jobs/view (bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR "semantic kernel" OR haystack OR dspy) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Product — requete principale**
```
site:fr.linkedin.com/jobs/view react figma
```

**Onglet Product — requete bonus (roles Product Manager)**
```
site:fr.linkedin.com/jobs/view figma ("product manager" OR "chef de produit") (react OR frontend OR "product engineering")
```

Les trois passent par `dateRestrict=m1` (dernier mois) puisque la tache tourne chaque mois —
pas besoin de re-scanner un an a chaque fois.

### Setup Google Cloud (a faire une fois)

1. **API Key pour Custom Search** : [console.cloud.google.com](https://console.cloud.google.com)
   → active l'API "Custom Search API" → cree une cle API (Identifiants → Creer des identifiants
   → Cle API).
2. **Moteur de recherche personnalise** :
   [programmablesearchengine.google.com](https://programmablesearchengine.google.com/controlpanel/create)
   → cree un moteur → dans ses parametres, active **"Search the entire web"** (sinon il reste
   limite aux sites listes) → recupere le **Search engine ID** (`cx`).
3. **Compte de service pour Sheets** : dans le meme projet GCP, active l'API "Google Sheets API"
   → IAM & Admin → Comptes de service → Creer → genere une cle JSON (bouton "Gerer les cles" →
   Ajouter une cle → JSON).
4. **Partage du Sheet** : ouvre le Google Sheet, clique Partager, ajoute l'adresse e-mail du
   compte de service (visible dans le JSON, champ `client_email`) en **Editeur**.

### Secrets GitHub a creer

Dans le repo → Settings → Secrets and variables → Actions :

| Secret | Valeur |
|---|---|
| `GOOGLE_API_KEY` | La cle API Custom Search (etape 1) |
| `GOOGLE_CSE_ID` | Le `cx` du moteur de recherche (etape 2) |
| `GOOGLE_SHEETS_CREDENTIALS_JSON` | Le contenu **complet** du fichier JSON du compte de service (etape 3), colle tel quel |
| `SHEET_ID` | `1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA` |

Sans ces 4 secrets configures, le workflow `.github/workflows/reverse-search-linkedin-xray.yml`
tourne mais echoue au premier appel API.

### Verifications apres le premier run

- Regarde l'onglet **Actions** du repo pour voir le log (`Resume du run`).
- Dans le Sheet, filtre la colonne Status sur `Needs review (auto-added)` pour ne traiter que
  les nouvelles lignes du mois.
- Les colonnes Company / Job Title sont decoupees automatiquement a partir du titre Google
  (`... chez X`, `... at X`, `... - X`) — imparfait sur certains formats, a corriger a la volee
  si besoin.
- Le quota gratuit de l'API Custom Search est de 100 requetes/jour : avec 3 requetes ×
  ~30 resultats (pagine par 10 = 3 appels API) ca reste tres large pour un run mensuel.

### Pour aller plus loin (optionnel)

- Ajouter un `workflow_dispatch` input pour relancer une recherche ponctuelle avec une requete
  personnalisee.
- Brancher une notification Slack/e-mail en fin de job quand `new_rows` n'est pas vide, plutot
  que d'attendre de consulter le Sheet.
- Definir les requetes des onglets **Platform** et **Frontend** dans `TABS` une fois les
  mots-cles decides.
- Si le parsing Company/Job Title reste trop approximatif, envisager un second appel LLM (API
  Claude) dans le script pour structurer proprement chaque resultat avant l'ecriture.
