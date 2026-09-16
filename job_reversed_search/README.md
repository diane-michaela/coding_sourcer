# LinkedIn Job Watch — ML, Product & Platform (monthly, GitHub Action)

> **Status (2026-09-16): switched search provider to Serper, awaiting first test run.**
> Went Google CSE → Tavily → back to Google CSE → now Serper, in one day. History:
> Tavily's own index/crawler has much lower recall than Google's for this site-scoped
> boolean (a broad Platform-tab query only returned one non-French result), so we reverted
> to Google CSE — but Google's Custom Search JSON API is **closed to new customers and
> shuts down entirely on 2027-01-01**, and unblocking it here would've meant linking a
> personal card to a GCP project tied to a company Google Workspace account (messy if
> Diane ever changes companies). Serper solves both: it returns real Google SERP results
> (not a third-party index like Tavily) over a plain HTTP API, no GCP project/billing
> account involved at all. Needs a `SERPER_API_KEY` (2,500 free queries on signup, no card
> required — plenty at ~4 queries/month). Not yet confirmed working end-to-end — the exact
> request/response format was reconstructed from public docs, not verified live, so the
> monthly `schedule` trigger in `.github/workflows/reverse-search-linkedin-xray.yml` stays
> commented out until a manual `workflow_dispatch` run confirms it.

Veille mensuelle automatisee : X-ray Google (`site:linkedin.com/jobs/view ...`) via l'API
Serper (vrais resultats Google, sans index tiers), dedoublonnage par ID LinkedIn, ecriture
directe dans le Google Sheet
["AI Agent Framework — LinkedIn Job Leads (FR)"](https://docs.google.com/spreadsheets/d/1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA/edit).

**Limite a connaitre avant de laisser tourner ca en automatique :** en plus du `site:` de la
requete, le script filtre aussi cote code les resultats qui ne sont pas des pages
`/jobs/view/` (profils, pages entreprise) et ceux dont le titre/extrait mentionne
explicitement un pays hors France — un garde-fou en plus, pas parfait. Il ne reproduit pas
non plus le jugement applique a la main (reperer le bleed des blocs "Recherches
similaires", distinguer une vraie offre Product Manager d'une simple mention en sidebar).
Toute nouvelle ligne est donc ajoutee avec le statut `Needs review (auto-added)` plutot que
d'etre silencieusement acceptee — a trancher une fois par mois, pas a relancer a la main.

## Onglets du Sheet

Confirmes sur le Sheet lui-meme : **ML**, **Product**, **Platform**, **Frontend** (colonnes
A-I : Company, Job Title, Location, Work Mode, Salary, Framework(s) Mentioned, Posted, Status,
LinkedIn URL).

**ML**, **Product** et **Platform** ont une requete definie dans
`linkedin-xray-scripts/linkedin_job_watch.py` — voir "Les recherches" ci-dessous.
**Frontend** existe deja dans le Sheet mais n'a pas encore de mots-cles definis ici :
a completer dans le dict `TABS` du script une fois decides.

## Les recherches

Toutes utilisent `site:linkedin.com/jobs/view` (restriction reelle puisque Serper renvoie
les vrais resultats Google, a la difference de Tavily) + le meme groupe de mots-cles France
(`FRANCE_KEYWORDS` dans le script). La requete envoie aussi `gl: "fr"` (biais pays cote
Google) en plus des mots-cles.

**Onglet ML**
```
site:linkedin.com/jobs/view (bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR "semantic kernel" OR haystack OR dspy) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Product — requete principale**
```
site:linkedin.com/jobs/view react figma (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Product — requete bonus (roles Product Manager)**
```
site:linkedin.com/jobs/view figma ("product manager" OR "chef de produit") (react OR frontend OR "product engineering") (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Platform** — restriction par sous-domaine `fr.linkedin.com` au lieu du groupe de
mots-cles France (peut rater des offres francaises publiees sous `www.linkedin.com`, comme
certaines lignes de l'onglet ML).
```
site:fr.linkedin.com/jobs/view Node.js TypeScript AWS Redis (Pulumi OR Ansible OR Terraform OR "infrastructure as code") (PostgreSQL OR "relational database" OR Postgres)
```

Toutes passent par `tbs: "qdr:m"` (dernier mois, equivalent Serper du `dateRestrict=m1` de
Google CSE) puisque la tache tourne chaque mois — pas besoin de re-scanner un an a chaque fois.

## Setup (a faire une fois)

1. **Cle API Serper** : [serper.dev](https://serper.dev) → cree un compte (2 500 requetes
   gratuites a l'inscription, pas de carte bancaire requise) → recupere la cle API depuis le
   dashboard.
2. **Compte de service pour Sheets** : dans un projet GCP, active l'API "Google Sheets API"
   → IAM & Admin → Comptes de service → Creer → genere une cle JSON (bouton "Gerer les cles" →
   Ajouter une cle → JSON). Ce projet GCP n'a besoin d'aucune facturation liee — l'API Sheets
   reste gratuite a ce volume et n'est pas concernee par la fermeture de Custom Search.
3. **Partage du Sheet** : ouvre le Google Sheet, clique Partager, ajoute l'adresse e-mail du
   compte de service (visible dans le JSON, champ `client_email`) en **Editeur**.

## Secrets GitHub a creer

Dans le repo → Settings → Secrets and variables → Actions :

| Secret | Valeur |
|---|---|
| `SERPER_API_KEY` | La cle API Serper (etape 1) |
| `GOOGLE_SHEETS_CREDENTIALS_JSON` | Le contenu **complet** du fichier JSON du compte de service (etape 2), colle tel quel |
| `SHEET_ID` | `1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA` |

Sans ces 3 secrets configures, le workflow `.github/workflows/reverse-search-linkedin-xray.yml`
tourne mais echoue au premier appel API.

## Verifications apres le premier run

- Regarde l'onglet **Actions** du repo pour voir le log (`Resume du run`) — premiere chose a
  verifier : que l'appel Serper reussit bien (format de requete reconstruit depuis la doc
  publique, pas teste en conditions reelles avant ce premier run).
- Dans le Sheet, filtre la colonne Status sur `Needs review (auto-added)` pour ne traiter que
  les nouvelles lignes du mois.
- Les colonnes Company / Job Title sont decoupees automatiquement a partir du titre Google
  (`... chez X`, `... at X`, `... - X`) — imparfait sur certains formats, a corriger a la volee
  si besoin.
- Le forfait gratuit Serper (2 500 requetes a l'inscription) est tres large pour ce volume
  (~4 requetes/mois) ; au-dela, facturation a l'usage — a surveiller sur
  [serper.dev](https://serper.dev) si le volume augmente un jour.

## Pour aller plus loin (optionnel)

- Ajouter un `workflow_dispatch` input pour relancer une recherche ponctuelle avec une requete
  personnalisee.
- Brancher une notification Slack/e-mail en fin de job quand `new_rows` n'est pas vide, plutot
  que d'attendre de consulter le Sheet.
- Definir la requete de l'onglet **Frontend** dans `TABS` une fois les mots-cles decides.
- Si le parsing Company/Job Title reste trop approximatif, envisager un second appel LLM (API
  Claude) dans le script pour structurer proprement chaque resultat avant l'ecriture.
