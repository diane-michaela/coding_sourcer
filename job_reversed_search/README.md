# LinkedIn Job Watch — ML, Product, Platform & Frontend (monthly)

> **Status (2026-09-17): manual-search workflow, Slack-reminded.** The GitHub Actions /
> Serper pipeline below is paused/legacy — see "Historique des essais" for the full trail
> (Google CSE → Tavily → Google CSE → Serper → free-tier `site:`/quotes block). Instead of
> automating the search itself, a monthly Claude routine posts the 4 canonical queries to
> **#hiring-test** on Slack as a reminder; Diane runs them herself in her logged-in browser
> (the only way to get `site:` + quoted phrases, and the only way to get Google's real
> personalization — neither is reproducible via API) and shares the resulting job links
> with Claude to dedupe and append to the Sheet. See "Workflow actuel" below for the full
> loop and the 4 queries.

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

## Workflow actuel (2026-09-17) : veille manuelle + rappel Slack mensuel

Decision (2026-09-17) : plutot que de continuer a boucher l'ecart de recall/precision de
l'automatisation Serper (voir "Historique des essais" ci-dessous), la veille mensuelle
repose desormais sur une recherche manuelle dans le navigateur (session LinkedIn/Google
connectee de Diane — la seule facon d'obtenir `site:` et les phrases entre guillemets, que
Serper gratuit rejette, et la seule a beneficier de la personnalisation Google impossible a
reproduire via API de toute facon, voir point 6 de l'historique) :

1. Un routine Claude planifie (mensuel, le 1er du mois) poste dans **#hiring-test** (Slack)
   les 4 requetes ci-dessous, lues directement depuis ce README pour ne jamais etre
   perimees.
2. Diane lance chacune des 4 requetes elle-meme dans son navigateur (session connectee),
   et partage dans une session Claude Code normale les liens d'offres qu'elle retient,
   groupes par onglet (ML / Product / Platform / Frontend).
3. Claude dedoublonne ces liens contre les IDs LinkedIn deja presents dans le Sheet et
   ajoute les nouvelles lignes (statut `Needs review (auto-added)`, memes colonnes que le
   script — voir "Les recherches (script Serper, legacy/on-demand)" plus bas pour le detail
   des colonnes).

**Le routine Slack** : "LinkedIn Job Watch — Monthly Slack Reminder"
(`trig_013UyDa7fQ9XpEU7Qh6kWfvo`, geree depuis
[claude.ai/code/routines](https://claude.ai/code/routines)), cron `0 6 1 * *` (1er du mois,
06:00 UTC ≈ 08:00 Paris en ete / 07:00 en hiver — meme decalage DST que l'ancien cron GitHub
Actions ci-dessous), poste dans le canal Slack prive `#hiring-test` (`C0BAD2GUQMR`). Elle lit
les 4 requetes directement dans ce README a chaque execution — les modifier ici suffit, pas
besoin de recreer le routine. L'etape 3 (dedoublonnage + ecriture Sheet) n'est pas geree par
le routine : les routines sont des sessions cloud isolees, sans acces au Sheet ni au moment
ou Diane repond — cette etape se fait dans une session Claude Code normale, quand Diane
partage les liens.

Ces 4 requetes utilisent `site:fr.linkedin.com/jobs/view` (donc pas besoin du groupe de
mots-cles France separe : le sous-domaine `fr.` fait deja le filtrage geographique) et des
phrases entre guillemets la ou c'est naturel — les deux sont bloques sur le forfait Serper
gratuit (voir plus bas) mais fonctionnent normalement dans un vrai navigateur.

**ML**
```
site:fr.linkedin.com/jobs/view (bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai OR autogen OR "semantic kernel" OR haystack OR dspy)
```

**Product — requete principale**
```
site:fr.linkedin.com/jobs/view react figma
```

**Product — requete bonus (roles Product Manager)**
```
site:fr.linkedin.com/jobs/view figma ("product manager" OR "chef de produit") (react OR frontend OR "product engineering")
```

**Platform**
```
site:fr.linkedin.com/jobs/view Node.js TypeScript AWS Redis (Pulumi OR Ansible OR Terraform OR "infrastructure as code") (PostgreSQL OR "relational database" OR Postgres)
```

**Frontend**
```
site:fr.linkedin.com/jobs/view (React OR "React.js") TypeScript (Storybook OR "design system") (Jest OR Cypress OR Tailwind) (Node.js OR Redis OR PostgreSQL OR AWS OR Docker OR Ansible)
```

## Les recherches (script Serper, legacy/on-demand)

Le script `linkedin-xray-scripts/linkedin_job_watch.py` et le workflow GitHub Actions
restent en place pour des verifications ponctuelles via `test_query` (voir plus bas), mais
ne couvrent que ML/Product/Platform (pas Frontend) et n'utilisent pas les requetes
ci-dessus telles quelles : Serper gratuit rejette `site:` et les guillemets, donc ces
requetes ont ete reecrites en mots-cles simples pour cet usage precis. Le cron mensuel
qui aurait fait tourner ce script reste desactive (voir "Historique des essais").

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

## Historique des essais (2026-09-16)

Journal complet de la journee ou tout ce qui suit a ete teste, dans l'ordre, pour que
personne n'ait a refaire le meme chemin :

1. **Google Custom Search API (setup d'origine).** Bloque : `GOOGLE_API_KEY` necessite un
   compte de facturation GCP lie au projet, meme pour rester dans le quota gratuit de
   100 requetes/jour (`403 forbidden` sans ca). Decision initiale : ne pas ajouter de
   facturation, `schedule` mis en pause.
2. **Tavily API** (essai pour contourner le blocage GCP). Fonctionnel mais recall bien plus
   faible que Google sur une recherche site-scoped comme celle-ci — Tavily a son propre
   index/crawler, pas celui de Google. Une requete Platform large n'a renvoye qu'un seul
   resultat (et hors France). En plus, `include_domains` ne restreint qu'au niveau du domaine,
   pas du chemin `/jobs/view/` — bleed de pages profil et de pages hors sujet observe en
   sheet (ex. une page profil LinkedIn `/in/...`, une offre Figma basee aux Etats-Unis).
3. **Retour a Google CSE**, en ajoutant `is_job_posting_url` / `mentions_non_france_location`
   comme garde-fous supplementaires. A ce moment-la, deux nouveaux blocages sont decouverts :
   - L'API Custom Search JSON **est fermee aux nouveaux clients et s'arrete completement le
     1er janvier 2027** — solution a duree de vie limitee de toute facon.
   - Le projet GCP/la cle CSE avaient ete crees sous l'adresse e-mail professionnelle de
     Diane (compte Google Workspace de l'entreprise) : lier une carte bancaire personnelle a
     ce projet pose un risque de perte de controle si elle change un jour d'entreprise (l'IT
     recupere/coupe l'acces au compte au depart).
4. **Serper** (proxy vers les vrais resultats Google, pas un index tiers comme Tavily) —
   choisi pour eviter les deux blocages ci-dessus : pas de projet/facturation GCP, 2 500
   requetes gratuites a l'inscription sans carte bancaire. Mais decouverte en testant en
   conditions reelles (via le champ `test_query` du workflow, teste requete par requete
   contre l'API live) : **le forfait gratuit Serper rejette `site:` ET les phrases entre
   guillemets** (`"Query pattern not allowed for free accounts."`), chacun independamment de
   l'autre. `OR` et les parentheses de groupement passent bien. Consequence : toutes les
   requetes ont ete reecrites en mots-cles simples (`linkedin jobs ...`) sans `site:`, et les
   phrases entre guillemets remplacees par des equivalents trait-d'union
   (`semantic-kernel`, `product-manager`, etc.).
5. **Premier vrai run reussi** (8 nouvelles lignes : 1 ML, 5 Product, 2 Platform) mais avec du
   bleed reel puisque plus rien ne restreint la requete a `linkedin.com/jobs/view` :
   2 offres en Inde (`in.linkedin.com`), 1 offre US (San Jose, sans "United States" dans
   l'extrait visible), 1 offre UK (extrait tronque par Serper avant que "United Kingdom"
   apparaisse en entier). Ajout de `is_non_france_subdomain` (signal URL fiable) qui rattrape
   les cas `in.`/`uk.`/etc. — mais pas les cas sans marqueur de pays du tout, ni les extraits
   tronques.
6. **Question de fond : est-ce que payer Serper (ou repasser sur CSE) donnerait exactement
   les memes resultats qu'une recherche Google manuelle dans le navigateur ?** Reponse : non,
   dans aucun des deux cas. Une recherche manuelle est **personnalisee** (compte Google
   connecte, historique, localisation reelle de l'appareil) — ni l'API Serper ni l'API
   Google CSE ne peuvent reproduire cette personnalisation, quel que soit le prix payé. Payer
   Serper leverait bien le blocage `site:`/guillemets (donc recupererait la *forme* de la
   requete d'origine), mais ne donnerait pas une correspondance 1:1 garantie avec le
   navigateur — et CSE a en plus son propre index/classement distinct de la recherche web
   principale de Google, donc un ecart de parite supplementaire qui lui est propre.
7. **Decision (2026-09-16) : mettre le pipeline en pause plutot que de continuer a
   l'optimiser.** Le `schedule` mensuel reste desactive. Plutot que de chercher a boucher
   completement l'ecart de recall/precision (payer Serper, revenir a CSE, etc.), les
   recherches ponctuelles se font desormais a la demande via Claude (mode `test_query` pour
   un aperçu sans ecriture, ou un vrai `workflow_dispatch` pour ecrire dans le Sheet) plutot
   que via un cron automatique.

## Pour aller plus loin (optionnel)

- Ajouter un `workflow_dispatch` input pour relancer une recherche ponctuelle avec une requete
  personnalisee.
- Brancher une notification Slack/e-mail en fin de job quand `new_rows` n'est pas vide, plutot
  que d'attendre de consulter le Sheet.
- Definir la requete de l'onglet **Frontend** dans `TABS` une fois les mots-cles decides.
- Si le parsing Company/Job Title reste trop approximatif, envisager un second appel LLM (API
  Claude) dans le script pour structurer proprement chaque resultat avant l'ecriture.
