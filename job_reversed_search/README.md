# LinkedIn Job Watch — ML, Product & Platform (monthly, GitHub Action)

> **Status (2026-09-16): live.** Switched the search step from Google Custom Search to
> the Tavily API — Google CSE was blocked on a GCP billing requirement (see git history
> for the old setup). Confirmed working via a manual `workflow_dispatch` run this
> morning, and the monthly `schedule` trigger in
> `.github/workflows/reverse-search-linkedin-xray.yml` is re-enabled.

Veille mensuelle automatisee : X-ray via l'API Tavily (domaine `linkedin.com` uniquement),
dedoublonnage par ID LinkedIn, ecriture directe dans le Google Sheet
["AI Agent Framework — LinkedIn Job Leads (FR)"](https://docs.google.com/spreadsheets/d/1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA/edit).

**Limite a connaitre avant de laisser tourner ca en automatique :** le script filtre les
resultats qui ne sont pas des pages `/jobs/view/` (profils, pages entreprise) et ceux dont
le titre/extrait mentionne explicitement un pays hors France (best-effort seulement — Tavily
n'a pas d'equivalent au `site:` de Google, donc rien ne garantit que 100% des resultats sont
bases en France). Il ne reproduit pas non plus le jugement applique a la main (reperer le
bleed des blocs "Recherches similaires", distinguer une vraie offre Product Manager d'une
simple mention en sidebar). Toute nouvelle ligne est donc ajoutee avec le statut
`Needs review (auto-added)` plutot que d'etre silencieusement acceptee — a trancher une fois
par mois, pas a relancer a la main.

## Onglets du Sheet

Confirmes sur le Sheet lui-meme : **ML**, **Product**, **Platform**, **Frontend** (colonnes
A-I : Company, Job Title, Location, Work Mode, Salary, Framework(s) Mentioned, Posted, Status,
LinkedIn URL).

**ML**, **Product** et **Platform** ont une requete definie dans
`linkedin-xray-scripts/linkedin_job_watch.py` — voir "Les recherches" ci-dessous.
**Frontend** existe deja dans le Sheet mais n'a pas encore de mots-cles definis ici :
a completer dans le dict `TABS` du script une fois decides.

## Les recherches

Toutes utilisent le meme groupe de mots-cles France (`FRANCE_KEYWORDS` dans le script) pour
biaiser le classement par pertinence de Tavily vers des postes bases en France — pas une
garantie, voir la limite ci-dessus.

**Onglet ML**
```
(bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR "semantic kernel" OR haystack OR dspy) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Product — requete principale**
```
react figma (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Product — requete bonus (roles Product Manager)**
```
figma ("product manager" OR "chef de produit") (react OR frontend OR "product engineering") (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

**Onglet Platform**
```
Node.js TypeScript AWS Redis (Pulumi OR Ansible OR Terraform OR "infrastructure as code") (PostgreSQL OR "relational database" OR Postgres) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

Toutes passent par `time_range="month"` (dernier mois) puisque la tache tourne chaque
mois — pas besoin de re-scanner un an a chaque fois.

## Setup (a faire une fois)

1. **Cle API Tavily** : [app.tavily.com](https://app.tavily.com) → cree un compte → recupere
   la cle API depuis le dashboard.
2. **Compte de service pour Sheets** : dans un projet GCP, active l'API "Google Sheets API"
   → IAM & Admin → Comptes de service → Creer → genere une cle JSON (bouton "Gerer les cles" →
   Ajouter une cle → JSON).
3. **Partage du Sheet** : ouvre le Google Sheet, clique Partager, ajoute l'adresse e-mail du
   compte de service (visible dans le JSON, champ `client_email`) en **Editeur**.

## Secrets GitHub a creer

Dans le repo → Settings → Secrets and variables → Actions :

| Secret | Valeur |
|---|---|
| `TAVILY_API_KEY` | La cle API Tavily (etape 1) |
| `GOOGLE_SHEETS_CREDENTIALS_JSON` | Le contenu **complet** du fichier JSON du compte de service (etape 2), colle tel quel |
| `SHEET_ID` | `1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA` |

Sans ces 3 secrets configures, le workflow `.github/workflows/reverse-search-linkedin-xray.yml`
tourne mais echoue au premier appel API.

## Verifications apres le premier run

- Regarde l'onglet **Actions** du repo pour voir le log (`Resume du run`).
- Dans le Sheet, filtre la colonne Status sur `Needs review (auto-added)` pour ne traiter que
  les nouvelles lignes du mois.
- Les colonnes Company / Job Title sont decoupees automatiquement a partir du titre Google
  (`... chez X`, `... at X`, `... - X`) — imparfait sur certains formats, a corriger a la volee
  si besoin.
- Le plan Tavily utilise a un quota de requetes/mois a surveiller sur
  [app.tavily.com](https://app.tavily.com) : avec 3 requetes par run mensuel, ca reste tres
  large a l'usage actuel.

## Pour aller plus loin (optionnel)

- Ajouter un `workflow_dispatch` input pour relancer une recherche ponctuelle avec une requete
  personnalisee.
- Brancher une notification Slack/e-mail en fin de job quand `new_rows` n'est pas vide, plutot
  que d'attendre de consulter le Sheet.
- Definir les requetes des onglets **Platform** et **Frontend** dans `TABS` une fois les
  mots-cles decides.
- Si le parsing Company/Job Title reste trop approximatif, envisager un second appel LLM (API
  Claude) dans le script pour structurer proprement chaque resultat avant l'ecriture.
