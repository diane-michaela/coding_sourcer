# LinkedIn Job Watch — ML, Product & Platform (monthly, GitHub Action)

> **Status (2026-09-16): Serper wired up, `site:` and quoted phrases don't work on the
> free tier.** Went Google CSE → Tavily → back to Google CSE → now Serper, all in one day
> (see git history for the Tavily-recall and Google-CSE-closure/billing reasons). Serper's
> free tier rejects any query using the `site:` operator **or** a quoted phrase (`"..."`) —
> both, independently — with `"Query pattern not allowed for free accounts."`, confirmed by
> testing directly against the live API via the workflow's `test_query` input. `OR` and
> parentheses grouping are fine. So all three tabs now query plain keywords (`linkedin
> jobs ...`) instead of `site:linkedin.com/jobs/view`, and multi-word phrases that were
> quoted (e.g. `"semantic kernel"`) are now hyphenated (`semantic-kernel`) to stay a single
> token without needing quotes. This means `is_job_posting_url()` and
> `mentions_non_france_location()` (below) are no longer just a safety net on top of a real
> `site:` restriction — they're now the *only* job/France filtering happening, so expect
> more manual review than before. Confirmed working end-to-end via `test_query` runs;
> re-enable the monthly `schedule` once a full (non-test) `workflow_dispatch` run looks good.

Veille mensuelle automatisee : X-ray Google (mots-cles `linkedin jobs ...`, sans `site:` —
voir la limite ci-dessous) via l'API Serper (vrais resultats Google, sans index tiers),
dedoublonnage par ID LinkedIn, ecriture directe dans le Google Sheet
["AI Agent Framework — LinkedIn Job Leads (FR)"](https://docs.google.com/spreadsheets/d/1fkg2X10EHY6w4H1YLGjzWgZShX_5r6zWqc9dksAuecA/edit).

**Limite a connaitre avant de laisser tourner ca en automatique :** le forfait Serper
gratuit bloque `site:` et les guillemets, donc rien ne restreint la requete elle-meme a
`linkedin.com/jobs/view` — tout le filtrage job/France se fait cote script
(`is_job_posting_url` + `is_non_france_subdomain` + `mentions_non_france_location`), pas
garanti a 100%. Confirme sur le premier vrai run (2026-09-16) : deux offres en Inde ont
été rattrapées par `is_non_france_subdomain` (sous-domaine `in.linkedin.com`, signal fiable
cote URL), mais une offre US (San Jose, sans "United States" dans l'extrait) et une offre UK
(extrait tronque par Serper avant que "United Kingdom" apparaisse en entier) sont quand meme
passees — best-effort, pas parfait. Le script ne reproduit pas non plus le jugement applique
a la main (reperer le bleed des blocs "Recherches similaires", distinguer une vraie offre
Product Manager d'une simple mention en sidebar). Toute nouvelle ligne est donc ajoutee avec
le statut `Needs review (auto-added)` plutot que d'etre silencieusement acceptee — a
trancher une fois par mois, pas a relancer a la main.

## Onglets du Sheet

Confirmes sur le Sheet lui-meme : **ML**, **Product**, **Platform**, **Frontend** (colonnes
A-I : Company, Job Title, Location, Work Mode, Salary, Framework(s) Mentioned, Posted, Status,
LinkedIn URL).

**ML**, **Product** et **Platform** ont une requete definie dans
`linkedin-xray-scripts/linkedin_job_watch.py` — voir "Les recherches" ci-dessous.
**Frontend** existe deja dans le Sheet mais n'a pas encore de mots-cles definis ici :
a completer dans le dict `TABS` du script une fois decides.

## Les recherches

Toutes commencent par `linkedin jobs` (biais mots-cles, pas une restriction reelle — voir
la limite ci-dessus) + le meme groupe de mots-cles France (`FRANCE_KEYWORDS` dans le
script). Les phrases multi-mots qui seraient normalement entre guillemets sont
trait-d'unionnees (`semantic-kernel`, `product-manager`, etc.) pour rester un terme unique
sans guillemets. La requete envoie aussi `gl: "fr"` (biais pays cote Google) en plus des
mots-cles.

**Onglet ML**
```
linkedin jobs (bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR semantic-kernel OR haystack OR dspy) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR île-de-france)
```

**Onglet Product — requete principale**
```
linkedin jobs react figma (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR île-de-france)
```

**Onglet Product — requete bonus (roles Product Manager)**
```
linkedin jobs figma (product-manager OR chef-de-produit) (react OR frontend OR product-engineering) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR île-de-france)
```

**Onglet Platform**
```
linkedin jobs Node.js TypeScript AWS Redis (Pulumi OR Ansible OR Terraform OR infrastructure-as-code) (PostgreSQL OR relational-database OR Postgres) (france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR île-de-france)
```

Toutes passent par `tbs: "qdr:m"` (dernier mois, equivalent Serper du `dateRestrict=m1` de
Google CSE) puisque la tache tourne chaque mois — pas besoin de re-scanner un an a chaque fois.

### Debug : tester une requete sans toucher au Sheet

Dans l'onglet **Actions** → ce workflow → **Run workflow**, le champ optionnel
**test_query** lance une seule requete Serper et affiche juste les resultats dans le log
(aucune ecriture dans le Sheet). Pratique pour verifier qu'une requete passe le filtre
"free account" de Serper avant de l'ajouter a `TABS`.

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

- Regarde l'onglet **Actions** du repo pour voir le log (`Resume du run`).
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
