"""
Veille mensuelle LinkedIn (X-ray via l'API Tavily) -> dedoublonnage ->
ajout dans le Google Sheet "AI Agent Framework - LinkedIn Job Leads (FR)".

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
import time
import requests

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ["SHEET_ID"]
TAVILY_API_KEY = os.environ["TAVILY_API_KEY"]
SHEETS_CREDENTIALS_JSON = os.environ["GOOGLE_SHEETS_CREDENTIALS_JSON"]

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Note : les prefixes "site:..." d'origine (herites de Google Custom Search) sont
# inertes avec Tavily -- ce n'est pas un operateur qu'il comprend, juste du texte
# libre. Le seul filtre de domaine reel est `include_domains` dans search_all_results.
# Les mots-cles de localisation ci-dessous ne font que biaiser le classement par
# pertinence de Tavily, ils ne garantissent pas l'exclusion des resultats hors France.
FRANCE_KEYWORDS = '(france OR paris OR bordeaux OR nantes OR lyon OR toulouse OR "île-de-france")'

# Noms d'onglet confirmes sur le Sheet lui-meme (tab bar) : ML, Product,
# Platform, Frontend. Colonnes A-I : Company, Job Title, Location, Work Mode,
# Salary, Framework(s) Mentioned, Posted, Status, LinkedIn URL. Adapte les
# noms ici s'ils changent un jour dans le fichier.
TABS = {
    "ML": {
        "range": "ML!A:I",
        "queries": [
            '(bedrock agentcore OR langchain OR llamaindex OR langgraph OR crewai '
            'OR autogen OR "semantic kernel" OR haystack OR dspy) ' + FRANCE_KEYWORDS
        ],
    },
    "Product": {
        "range": "Product!A:I",
        "queries": [
            f"react figma {FRANCE_KEYWORDS}",
            'figma ("product manager" OR "chef de produit") '
            f'(react OR frontend OR "product engineering") {FRANCE_KEYWORDS}',
        ],
    },
    "Platform": {
        "range": "Platform!A:I",
        "queries": [
            'Node.js TypeScript AWS Redis (Pulumi OR Ansible OR Terraform OR "infrastructure as code") '
            f'(PostgreSQL OR "relational database" OR Postgres) {FRANCE_KEYWORDS}'
        ],
    },
    # "Frontend": {"range": "Frontend!A:I", "queries": [...]},   # TODO: define queries
}

JOB_ID_RE = re.compile(r"-(\d{6,})(?:[/?#].*)?$")
JOB_URL_RE = re.compile(r"linkedin\.com/jobs/view/", re.IGNORECASE)

# Best-effort seulement : Tavily n'a pas d'equivalent au `site:` de Google, donc rien
# ne garantit que les resultats sont bases en France. On rejette au moins les cas ou
# un marqueur de pays hors France apparait explicitement dans le titre/l'extrait
# (ex. l'offre Figma US remontee le 2026-09-16 : "... in United States").
NON_FRANCE_MARKERS = (
    "united states", " usa", "united kingdom", "canada", "germany",
    "spain", "italy", "netherlands", "india", "poland",
)


def extract_job_id(url: str) -> str | None:
    match = JOB_ID_RE.search(url.strip())
    return match.group(1) if match else None


def is_job_posting_url(url: str) -> bool:
    """Rejette les pages LinkedIn qui ne sont pas des offres (profils /in/,
    pages entreprise, etc.) -- Tavily ne filtre pas par chemin d'URL."""
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
    """Recherche les offres LinkedIn via Tavily sur le dernier mois."""

    response = requests.post(
        "https://api.tavily.com/search",
        json={
            "api_key": TAVILY_API_KEY,
                "query": query,
            "search_depth": "advanced",
            "max_results": max_results,
            "include_domains": ["linkedin.com"],
            "time_range": "month",
        },
        timeout=30,
    )

    response.raise_for_status()

    results = []

    for item in response.json().get("results", []):
        link = item.get("url", "")
        title = item.get("title", "")
        snippet = item.get("content", "")

        if not is_job_posting_url(link):
            continue
        if mentions_non_france_location(title, snippet):
            continue

        results.append({
            "title": title,
            "link": link,
            "snippet": snippet,
        })

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
