# Reverse search — LinkedIn X-ray (monthly, GitHub Action)

> **Status (2026-09-15): paused.** The pipeline is fully built and all 4 GitHub secrets
> are set, but `GOOGLE_API_KEY` needs a GCP billing account linked to work — the Custom
> Search JSON API returns `403 forbidden` without one, even within the free 100
> queries/day quota. Decided not to add billing for now. The workflow's monthly
> `schedule` trigger is commented out in `.github/workflows/reverse-search-linkedin-xray.yml`
> so it doesn't fail-and-email every month — `workflow_dispatch` (manual run) still works
> for testing. To resume: either link a GCP billing account (real cost should stay $0 at
> this usage) or rework the search step to use PhantomBuster instead (see discussion in
> git history / ask Diane), then uncomment the `schedule:` block.

Veille mensuelle automatisee : X-ray Google (`site:linkedin.com/jobs/view ...`) via l'API
Google Custom Search, dedoublonnage par ID LinkedIn, ecriture directe dans le Google Sheet
["AI Agent Framework — LinkedIn Job Leads (FR)"](https://docs.google.com/spreadsheets/d/1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA/edit).

Complementaire, pas un remplacement, du pipeline PhantomBuster de ce meme dossier
([`README.md`](README.md), LinkedIn Search Export, cote candidat/entreprise Figma+React) —
celui-ci tourne gratuitement sur l'API Google Custom Search, cote offres ML/Product, et ecrit
directement dans un Sheet au lieu d'un CSV.

**Limite a connaitre avant de laisser tourner ca en automatique :** le script applique un
filtrage minimal (dedoublonnage par ID LinkedIn + `dateRestrict` sur le dernier mois). Il ne
reproduit pas le jugement applique a la main (reperer le bleed des blocs "Recherches
similaires", distinguer une vraie offre Product Manager d'une simple mention en sidebar).
Toute nouvelle ligne est donc ajoutee avec le statut `Needs review (auto-added)` plutot que
d'etre silencieusement acceptee — a trancher une fois par mois, pas a relancer a la main.

## Onglets du Sheet

Confirmes sur le Sheet lui-meme : **ML**, **Product**, **Platform**, **Frontend** (colonnes
A-I : Company, Job Title, Location, Work Mode, Salary, Framework(s) Mentioned, Posted, Status,
LinkedIn URL).

Seuls **ML** et **Product** ont une requete definie dans `scripts/linkedin_job_watch.py` pour
l'instant — voir la section "1. Les recherches" ci-dessous. **Platform** et **Frontend** existent
deja dans le Sheet mais n'ont pas encore de mots-cles definis ici : a completer dans le dict
`TABS` du script une fois decides.

## 1. Les recherches

### Onglet ML

```
site:linkedin.com/jobs/view (bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR "semantic kernel" OR haystack OR dspy) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")
```

### Onglet Product — requete principale

```
site:fr.linkedin.com/jobs/view react figma
```

### Onglet Product — requete bonus (roles Product Manager)

```
site:fr.linkedin.com/jobs/view figma ("product manager" OR "chef de produit") (react OR frontend OR "product engineering")
```

Les trois passent par `dateRestrict=m1` (dernier mois) puisque la tache tourne chaque mois —
pas besoin de re-scanner un an a chaque fois.

## 2. Setup Google Cloud (a faire une fois)

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

## 3. Secrets GitHub a creer

Dans le repo → Settings → Secrets and variables → Actions :

| Secret | Valeur |
|---|---|
| `GOOGLE_API_KEY` | La cle API Custom Search (etape 2.1) |
| `GOOGLE_CSE_ID` | Le `cx` du moteur de recherche (etape 2.2) |
| `GOOGLE_SHEETS_CREDENTIALS_JSON` | Le contenu **complet** du fichier JSON du compte de service (etape 2.3), colle tel quel |
| `SHEET_ID` | `1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA` |

Sans ces 4 secrets configures, le workflow `.github/workflows/reverse-search-linkedin-xray.yml`
tourne mais echoue au premier appel API.

## 4. Verifications apres le premier run

- Regarde l'onglet **Actions** du repo pour voir le log (`Resume du run`).
- Dans le Sheet, filtre la colonne Status sur `Needs review (auto-added)` pour ne traiter que
  les nouvelles lignes du mois.
- Les colonnes Company / Job Title sont decoupees automatiquement a partir du titre Google
  (`... chez X`, `... at X`, `... - X`) — imparfait sur certains formats, a corriger a la volee
  si besoin.
- Le quota gratuit de l'API Custom Search est de 100 requetes/jour : avec 3 requetes ×
  ~30 resultats (pagine par 10 = 3 appels API) ca reste tres large pour un run mensuel.

## 5. Pour aller plus loin (optionnel)

- Ajouter un `workflow_dispatch` input pour relancer une recherche ponctuelle avec une requete
  personnalisee.
- Brancher une notification Slack/e-mail en fin de job quand `new_rows` n'est pas vide, plutot
  que d'attendre de consulter le Sheet.
- Definir les requetes des onglets **Platform** et **Frontend** dans `TABS` une fois les
  mots-cles decides.
- Si le parsing Company/Job Title reste trop approximatif, envisager un second appel LLM (API
  Claude) dans le script pour structurer proprement chaque resultat avant l'ecriture.
