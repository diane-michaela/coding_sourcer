"""
Veille mensuelle LinkedIn (X-ray Google via l'API Serper) -> dedoublonnage ->
ajout dans le Google Sheet "AI Agent Framework - LinkedIn Job Leads (FR)".

Serper renvoie les vrais resultats de recherche Google (pas un index tiers comme
Tavily) -- c'est un relais vers Google, pas un moteur de recherche independant.
Utilise a la place de l'API officielle Google Custom Search JSON, fermee aux
nouveaux clients et qui s'arrete le 1er janvier 2027 pour tout le monde.

Le forfait Serper gratuit rejette `site:` et les phrases entre guillemets
("Query pattern not allowed for free accounts.") -- pas de restriction par
domaine/chemin possible dans la requete, tout le filtrage job/France se fait
cote script (voir `is_job_posting_url` / `mentions_non_france_location`).

Toute nouvelle ligne est marquee "Needs review (auto-added)" : le filtrage fin
qu'on applique a la main (bleed des blocs "Recherches similaires", distinguer
une vraie offre d'une simple mention en sidebar) reste a faire a l'oeil, une
fois par mois -> ne jamais accepter/rejeter automatiquement.

Onglets couverts pour l'instant : ML, Product, Platform. Le sheet a aussi un
onglet Frontend qui existe deja mais n'a pas encore de requete definie ici --
a completer dans TABS une fois les mots-cles choisis (voir README.md).
"""

import os
import re
import json
import requests

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
SERPER_API_KEY = os.environ["SERPER_API_KEY"]
SHEETS_CREDENTIALS_JSON = os.environ["GOOGLE_SHEETS_CREDENTIALS_JSON"]

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# IMPORTANT (decouvert 2026-09-16 par tatonnement) : le forfait Serper gratuit rejette
# toute requete utilisant l'operateur `site:` OU une phrase entre guillemets ("...")
# avec "Query pattern not allowed for free accounts.", meme separement l'un de l'autre.
# OR et les parentheses de groupement passent tres bien. Consequence : impossible de
# restreindre par domaine/chemin dans la requete elle-meme -- c'est `is_job_posting_url()`
# et `mentions_non_france_location()` ci-dessous qui font tout le travail de filtrage
# job/France, pas juste un garde-fou en plus comme avant. Les phrases multi-mots qui
# etaient entre guillemets sont remplacees par leur equivalent trait-d'union
# (ex. "semantic kernel" -> semantic-kernel) pour rester un terme unique sans guillemets.
FRANCE_KEYWORDS = "(france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR île-de-france)"

# Noms d'onglet confirmes sur le Sheet lui-meme (tab bar) : ML, Product,
# Platform, Frontend. Colonnes A-I : Company, Job Title, Location, Work Mode,
# Salary, Framework(s) Mentioned, Posted, Status, LinkedIn URL. Adapte les
# noms ici s'ils changent un jour dans le fichier.
TABS = {
    "ML": {
        "range": "ML!A:I",
        "queries": [
            "linkedin jobs (bedrock agentcore OR langchain OR llamaindex "
            "OR langgraph OR crewai OR autogen OR semantic-kernel OR haystack OR dspy) "
            + FRANCE_KEYWORDS
        ],
    },
    "Product": {
        "range": "Product!A:I",
        "queries": [
            f"linkedin jobs react figma {FRANCE_KEYWORDS}",
            "linkedin jobs figma (product-manager OR chef-de-produit) "
            f"(react OR frontend OR product-engineering) {FRANCE_KEYWORDS}",
        ],
    },
    "Platform": {
        "range": "Platform!A:I",
        "queries": [
            "linkedin jobs Node.js TypeScript AWS Redis "
            "(Pulumi OR Ansible OR Terraform OR infrastructure-as-code) "
            f"(PostgreSQL OR relational-database OR Postgres) {FRANCE_KEYWORDS}"
        ],
    },
    # "Frontend": {"range": "Frontend!A:I", "queries": [...]},   # TODO: define queries
}

JOB_ID_RE = re.compile(r"-(\d{6,})(?:[/?#].*)?$")
JOB_URL_RE = re.compile(r"linkedin\.com/jobs/view/", re.IGNORECASE)

# Seul vrai filtre job/France maintenant que `site:` n'est plus utilisable (voir plus
# haut) : rejette les resultats dont le titre/extrait mentionne explicitement un pays
# hors France.
NON_FRANCE_MARKERS = (
    "united states", " usa", "united kingdom", "canada", "germany",
    "spain", "italy", "netherlands", "india", "poland",
)


def extract_job_id(url: str) -> str | None:
    match = JOB_ID_RE.search(url.strip())
    return match.group(1) if match else None


def is_job_posting_url(url: str) -> bool:
    """Rejette les pages LinkedIn qui ne sont pas des offres (profils /in/,
    pages entreprise, etc.). C'est le seul filtre job/pas-job : `site:` n'est
    pas utilisable avec le forfait Serper gratuit (voir plus haut)."""
    return bool(JOB_URL_RE.search(url))


def mentions_non_france_location(title: str, snippet: str) -> bool:
    text = f"{title} {snippet}".lower()
    return any(marker in text for marker in NON_FRANCE_MARKERS)


def get_sheets_service():
    info = json.loads(SHEETS_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_existing_ids(sheets_service, sheet_range: str) -> set[str]:
    resp = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=sheet_range)
        .execute()
    )
    rows = resp.get("values", [])
    ids = set()
    for row in rows:
        if not row:
            continue
        url = row[-1] if len(row) >= 9 else ""
        job_id = extract_job_id(url)
        if job_id:
            ids.add(job_id)
    return ids


def search_all_results(query: str, max_results: int = 30):
    """Recherche Google (vrai SERP, via Serper) sur le dernier mois, France en priorite."""
    response = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
        json={
            "q": query,
            "gl": "fr",
            "num": max_results,
            "tbs": "qdr:m",  # dernier mois, equivalent du dateRestrict=m1 de Google CSE
        },
        timeout=30,
    )
    if not response.ok:
        print(f"[debug] Serper {response.status_code} response body: {response.text}")
    response.raise_for_status()

    results = []
    for item in response.json().get("organic", []):
        link = item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet", "")

        if not is_job_posting_url(link):
            continue
        if mentions_non_france_location(title, snippet):
            continue

        results.append({"title": title, "link": link, "snippet": snippet})

    return results


def guess_company_and_title(raw_title: str) -> tuple[str, str]:
    """Best-effort : les titres LinkedIn varient trop pour un parsing fiable.
    On coupe sur les separateurs les plus frequents et on laisse le reste
    dans Job Title pour verification manuelle."""
    for sep in (" chez ", " at ", " - "):
        if sep in raw_title:
            title, _, company = raw_title.partition(sep)
            return company.strip(" —-|"), title.strip()
    return "", raw_title.strip()


def build_row(item: dict) -> list[str]:
    company, title = guess_company_and_title(item.get("title", ""))
    snippet = item.get("snippet", "").replace("\n", " ")
    return [
        company or "not shown in excerpt (verify on page)",
        title,
        "not shown in excerpt (verify on page)",
        "not shown in excerpt (verify on page)",
        "not shown in excerpt (verify on page)",
        f"(auto — verify) {snippet[:200]}",
        "auto-detected — verify posted date",
        "Needs review (auto-added)",
        item.get("link", ""),
    ]


def run():
    test_query = os.environ.get("TEST_QUERY", "").strip()
    if test_query:
        print(f"[test_query mode] {test_query!r}")
        results = search_all_results(test_query)
        print(f"{len(results)} resultat(s) apres filtrage job/France :")
        for item in results:
            print(f"  - {item['title']} — {item['link']}")
        return

    sheets_service = get_sheets_service()

    summary = {}

    for tab_name, cfg in TABS.items():
        existing_ids = read_existing_ids(sheets_service, cfg["range"])
        seen_this_run = set()
        new_rows = []

        for query in cfg["queries"]:
            for item in search_all_results(query):
                link = item.get("link", "")
                job_id = extract_job_id(link)
                if not job_id or job_id in existing_ids or job_id in seen_this_run:
                    continue
                seen_this_run.add(job_id)
                new_rows.append(build_row(item))

        if new_rows:
            sheets_service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=cfg["range"],
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": new_rows},
            ).execute()

        summary[tab_name] = len(new_rows)

    print("Resume du run :")
    for tab_name, count in summary.items():
        print(f"  - {tab_name}: {count} nouvelle(s) ligne(s) ajoutee(s) (statut Needs review)")


if __name__ == "__main__":
    run()
