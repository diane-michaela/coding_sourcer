"""
geocode_postprocess.py

Post-traitement (2e passe) :
- Lit un fichier Excel/CSV issu de ta collecte GitHub (avec owner_location brut)
- Nettoie owner_location -> owner_location_clean (règles + mappings)
- Géocode uniquement les locations UNIQUES (Google si key, sinon Nominatim)
- Ajoute/alimente les colonnes normalisées (ville/région/pays/lat/lon/status/provider)
- Réécrit un nouvel Excel (ou CSV si Excel impossible)
- Réutilise geocode_cache.json pour éviter de repayer/requêter

Dépendances:
  pip install pandas openpyxl requests python-dotenv
  pip install geopy  # seulement si tu utilises Nominatim
"""

import os
import re
import json
import time
from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# ---------------- Env ----------------
load_dotenv()

TIMEOUT = 20
GEO_PROVIDER = (os.getenv("GEO_PROVIDER") or "").strip().lower()  # "google" ou "nominatim"
GOOGLE_MAPS_API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

# Cache disque
GEO_CACHE_FILE = Path(__file__).with_name("geocode_cache.json")
_GEO_CACHE: Dict[str, Dict[str, Any]] = {}


# Locations à ignorer (non géocodables)
_BAD_LOCATIONS = {
    "", "remote", "worldwide", "earth", "somewhere", "internet", "everywhere", "global", "online",
    "anywhere", "planet earth", "the internet", "github", "home",
    # Ajouts utiles (ton cas)
    "distributed", "international", "europe", "eu", "european union", "emea", "apac",
}

# Mappings rapides (tu peux compléter selon tes datasets)
# Attention: ces mappings sont des "aides" : adapte si tu fais du sourcing France-only par ex.
_LOCATION_MAP = {

    # --- États-Unis (toujours utile dans GitHub)
    "sf": "San Francisco, CA, USA",
    "bay area": "San Francisco Bay Area, CA, USA",
    "ny": "New York, NY, USA",
    "nyc": "New York, NY, USA",
    "la": "Los Angeles, CA, USA",

    # --- Royaume-Uni
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "england": "England, United Kingdom",
    "scotland": "Scotland, United Kingdom",
    "wales": "Wales, United Kingdom",

    # --- France (abréviations fréquentes)
    "paris": "Paris, France",
    "idf": "Île-de-France, France",
    "paca": "Provence-Alpes-Côte d'Azur, France",
    "75": "Paris, France",
    "lyon": "Lyon, France",
    "marseille": "Marseille, France",
    "toulouse": "Toulouse, France",
    "nantes": "Nantes, France",
    "bordeaux": "Bordeaux, France",

    # --- Allemagne
    "de": "Germany",
    "germany": "Germany",
    "berlin": "Berlin, Germany",
    "munich": "Munich, Germany",
    "hamburg": "Hamburg, Germany",

    # --- Espagne
    "es": "Spain",
    "spain": "Spain",
    "madrid": "Madrid, Spain",
    "barcelona": "Barcelona, Spain",
    "valencia": "Valencia, Spain",

    # --- Italie
    "it": "Italy",
    "italy": "Italy",
    "rome": "Rome, Italy",
    "milano": "Milan, Italy",
    "milan": "Milan, Italy",
    "napoli": "Naples, Italy",

    # --- Pays-Bas
    "nl": "Netherlands",
    "netherlands": "Netherlands",
    "holland": "Netherlands",
    "amsterdam": "Amsterdam, Netherlands",
    "rotterdam": "Rotterdam, Netherlands",

    # --- Belgique
    "be": "Belgium",
    "belgium": "Belgium",
    "brussels": "Brussels, Belgium",
    "bxl": "Brussels, Belgium",

    # --- Portugal
    "pt": "Portugal",
    "portugal": "Portugal",
    "lisbon": "Lisbon, Portugal",
    "porto": "Porto, Portugal",

    # --- Irlande
    "ie": "Ireland",
    "ireland": "Ireland",
    "dublin": "Dublin, Ireland",

    # --- Autriche
    "at": "Austria",
    "austria": "Austria",
    "vienna": "Vienna, Austria",

    # --- Pologne
    "pl": "Poland",
    "poland": "Poland",
    "warsaw": "Warsaw, Poland",
    "krakow": "Krakow, Poland",

    # --- République tchèque
    "cz": "Czech Republic",
    "czech": "Czech Republic",
    "prague": "Prague, Czech Republic",

    # --- Hongrie
    "hu": "Hungary",
    "hungary": "Hungary",
    "budapest": "Budapest, Hungary",

    # --- Roumanie
    "ro": "Romania",
    "romania": "Romania",
    "bucharest": "Bucharest, Romania",

    # --- Bulgarie
    "bg": "Bulgaria",
    "bulgaria": "Bulgaria",
    "sofia": "Sofia, Bulgaria",

    # --- Grèce
    "gr": "Greece",
    "greece": "Greece",
    "athens": "Athens, Greece",

    # --- Suède
    "se": "Sweden",
    "sweden": "Sweden",
    "stockholm": "Stockholm, Sweden",

    # --- Danemark
    "dk": "Denmark",
    "denmark": "Denmark",
    "copenhagen": "Copenhagen, Denmark",

    # --- Finlande
    "fi": "Finland",
    "finland": "Finland",
    "helsinki": "Helsinki, Finland",

    # --- Croatie
    "hr": "Croatia",
    "croatia": "Croatia",
    "zagreb": "Zagreb, Croatia",

    # --- Slovénie
    "si": "Slovenia",
    "slovenia": "Slovenia",
    "ljubljana": "Ljubljana, Slovenia",

    # --- Slovaquie
    "sk": "Slovakia",
    "slovakia": "Slovakia",
    "bratislava": "Bratislava, Slovakia",

    # --- Lituanie
    "lt": "Lithuania",
    "vilnius": "Vilnius, Lithuania",

    # --- Lettonie
    "lv": "Latvia",
    "riga": "Riga, Latvia",

    # --- Estonie
    "ee": "Estonia",
    "tallinn": "Tallinn, Estonia",

    # --- Chypre
    "cy": "Cyprus",
    "cyprus": "Cyprus",

    # --- Malte
    "mt": "Malta",
    "malta": "Malta",
}


# Patterns de nettoyage (retirer du bruit)
_CLEAN_REPLACEMENTS = [
    (re.compile(r"\b(european\s+union|eu)\b", re.IGNORECASE), ""),
    (re.compile(r"\b(remote\s*only|fully\s*remote|remote)\b", re.IGNORECASE), "remote"),
    (re.compile(r"[\u200b\u200c\u200d\ufeff]", re.IGNORECASE), ""),  # zero-width
]


# ---------------- Cache helpers ----------------
def load_geo_cache() -> None:
    global _GEO_CACHE
    if GEO_CACHE_FILE.exists():
        try:
            _GEO_CACHE = json.loads(GEO_CACHE_FILE.read_text(encoding="utf-8"))
            if not isinstance(_GEO_CACHE, dict):
                _GEO_CACHE = {}
        except Exception:
            _GEO_CACHE = {}


def save_geo_cache() -> None:
    try:
        GEO_CACHE_FILE.write_text(
            json.dumps(_GEO_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


# ---------------- Geocode output templates ----------------
def _geo_empty(provider: str = "") -> Dict[str, str]:
    return {
        "owner_location_norm": "",
        "owner_city": "",
        "owner_region": "",
        "owner_country": "",
        "owner_country_code": "",
        "owner_lat": "",
        "owner_lon": "",
        "owner_geocode_provider": provider,
        "owner_geocode_status": "EMPTY",
    }


def _geo_no_match(provider: str) -> Dict[str, str]:
    d = _geo_empty(provider)
    d["owner_geocode_status"] = "NO_MATCH"
    return d


# ---------------- Providers ----------------
def geocode_google(raw: str) -> Dict[str, str]:
    provider = "google"
    raw = (raw or "").strip()
    key = raw.lower()

    if not raw or key in _BAD_LOCATIONS:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "SKIPPED" if raw else "EMPTY"
        return d

    if not GOOGLE_MAPS_API_KEY:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "NO_API_KEY"
        return d

    cache_key = f"{provider}:{key}"
    if cache_key in _GEO_CACHE:
        return _GEO_CACHE[cache_key]

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": raw, "key": GOOGLE_MAPS_API_KEY}

    out = _geo_no_match(provider)
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        data = r.json() if r.content else {}
        status = (data.get("status") or "").upper()

        if status != "OK":
            out["owner_geocode_status"] = status or "ERROR"
            _GEO_CACHE[cache_key] = out
            return out

        results = data.get("results") or []
        if not results:
            _GEO_CACHE[cache_key] = out
            return out

        top = results[0]
        formatted = top.get("formatted_address") or ""
        geom = (top.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lon = geom.get("lng")

        comps = top.get("address_components") or []
        comp_map = {}
        for c in comps:
            for ty in (c.get("types") or []):
                comp_map.setdefault(ty, c)

        def _long(ty: str) -> str:
            return (comp_map.get(ty) or {}).get("long_name") or ""

        def _short(ty: str) -> str:
            return (comp_map.get(ty) or {}).get("short_name") or ""

        city = _long("locality") or _long("postal_town") or _long("administrative_area_level_3")
        region = _long("administrative_area_level_1")
        country = _long("country")
        country_code = _short("country")

        out = {
            "owner_location_norm": formatted,
            "owner_city": city,
            "owner_region": region,
            "owner_country": country,
            "owner_country_code": country_code,
            "owner_lat": "" if lat is None else str(lat),
            "owner_lon": "" if lon is None else str(lon),
            "owner_geocode_provider": provider,
            "owner_geocode_status": "OK",
        }

    except Exception:
        out["owner_geocode_status"] = "ERROR"

    _GEO_CACHE[cache_key] = out
    return out


def geocode_nominatim(raw: str) -> Dict[str, str]:
    provider = "nominatim"
    raw = (raw or "").strip()
    key = raw.lower()

    if not raw or key in _BAD_LOCATIONS:
        d = _geo_empty(provider)
        d["owner_geocode_status"] = "SKIPPED" if raw else "EMPTY"
        return d

    cache_key = f"{provider}:{key}"
    if cache_key in _GEO_CACHE:
        return _GEO_CACHE[cache_key]

    out = _geo_no_match(provider)

    try:
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        from geopy.exc import GeocoderInsufficientPrivileges

        geolocator = Nominatim(
            user_agent="github-postprocess-geocoder/1.0",
            timeout=10
        )

        geocode = RateLimiter(
            geolocator.geocode,
            min_delay_seconds=1.2,
            max_retries=2,
            error_wait_seconds=2.0
        )

        try:
            loc = geocode(raw, addressdetails=True)
        except GeocoderInsufficientPrivileges:
            out["owner_geocode_status"] = "OSM_403_BLOCKED"
            _GEO_CACHE[cache_key] = out
            return out

        if not loc:
            _GEO_CACHE[cache_key] = out
            return out

        addr = (loc.raw or {}).get("address") or {}
        city = addr.get("city") or addr.get("town") or addr.get("village") or ""
        region = addr.get("state") or addr.get("region") or ""
        country = addr.get("country") or ""
        country_code = (addr.get("country_code") or "").upper()

        out = {
            "owner_location_norm": loc.address or "",
            "owner_city": city,
            "owner_region": region,
            "owner_country": country,
            "owner_country_code": country_code,
            "owner_lat": str(loc.latitude),
            "owner_lon": str(loc.longitude),
            "owner_geocode_provider": provider,
            "owner_geocode_status": "OK",
        }

    except Exception:
        out["owner_geocode_status"] = "ERROR"

    _GEO_CACHE[cache_key] = out
    return out


def geocode_and_normalize(raw: str) -> Dict[str, str]:
    provider = GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim")
    if provider == "google":
        return geocode_google(raw)
    if provider == "nominatim":
        return geocode_nominatim(raw)
    d = _geo_empty(provider)
    d["owner_geocode_status"] = "UNKNOWN_PROVIDER"
    return d


# ---------------- Cleaning ----------------
def normalize_location_text(raw: str) -> Tuple[str, str]:
    """
    Retourne (clean, status_hint)
    - status_hint peut valoir: "SKIP" si c'est non géocodable
    """
    if raw is None:
        return "", "EMPTY"

    s = str(raw).strip()
    if not s:
        return "", "EMPTY"

    s_low = s.lower().strip()

    # Si c'est clairement non géocodable
    if s_low in _BAD_LOCATIONS:
        return s_low, "SKIP"

    # Appliquer des remplacements regex
    for pattern, repl in _CLEAN_REPLACEMENTS:
        s = pattern.sub(repl, s)

    # Nettoyage ponctuation / espaces
    s = re.sub(r"\s+", " ", s).strip(" ,;|-").strip()
    s_low = s.lower()

    if not s:
        return "", "EMPTY"

    # Re-check après nettoyage
    if s_low in _BAD_LOCATIONS:
        return s_low, "SKIP"

    # Mappings courts
    if s_low in _LOCATION_MAP:
        return _LOCATION_MAP[s_low], "MAPPED"

    # Cas type "Cyprus, European Union" => "Cyprus"
    s = re.sub(r",\s*(european union|eu)\s*$", "", s, flags=re.IGNORECASE).strip()

    return s, "CLEANED"


# ---------------- IO ----------------
def read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in [".xlsx", ".xlsm", ".xltx", ".xltm"]:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported input file: {path}")


def write_any(df: pd.DataFrame, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if out_path.suffix.lower() in [".xlsx", ".xlsm", ".xltx", ".xltm"]:
            df.to_excel(out_path, index=False)
        else:
            df.to_csv(out_path, index=False)
        return out_path
    except Exception:
        # fallback CSV
        fallback = out_path.with_suffix(".csv")
        df.to_csv(fallback, index=False)
        return fallback


# ---------------- Main postprocess ----------------
def postprocess_file(
    input_path: str,
    output_path: str,
    location_col: str = "owner_location",
) -> None:
    load_geo_cache()

    ip = Path(input_path).expanduser().resolve()
    op = Path(output_path).expanduser().resolve()

    df = read_any(ip).fillna("")

    if location_col not in df.columns:
        raise ValueError(f"Column '{location_col}' not found. Available: {list(df.columns)}")

    # 1) Créer owner_location_clean
    cleans = []
    hints = []
    for v in df[location_col].tolist():
        clean, hint = normalize_location_text(v)
        cleans.append(clean)
        hints.append(hint)

    df["owner_location_clean"] = cleans
    df["owner_location_clean_hint"] = hints  # utile pour debug (MAPPED/SKIP/CLEANED)

    # 2) Géocoder uniquement les uniques non vides
    uniques = sorted(set([c for c in df["owner_location_clean"].tolist() if str(c).strip()]))
    print(f"Unique locations to consider: {len(uniques)}")

    geo_map: Dict[str, Dict[str, str]] = {}
    n = 0

    for loc in uniques:
        # Skip immédiat si bad
        if loc.lower().strip() in _BAD_LOCATIONS:
            geo_map[loc] = _geo_empty(GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim"))
            geo_map[loc]["owner_geocode_status"] = "SKIPPED"
            continue

        geo = geocode_and_normalize(loc)
        geo_map[loc] = geo

        n += 1
        if n % 50 == 0:
            save_geo_cache()
            print(f"Progress geocode: {n}/{len(uniques)} (cache saved)")
            time.sleep(0.2)

    # 3) Appliquer geo_map à chaque ligne (merge sans join)
    geo_cols = [
        "owner_location_norm",
        "owner_city",
        "owner_region",
        "owner_country",
        "owner_country_code",
        "owner_lat",
        "owner_lon",
        "owner_geocode_provider",
        "owner_geocode_status",
    ]

    # Initialiser colonnes si absentes
    for c in geo_cols:
        if c not in df.columns:
            df[c] = ""

    for i, loc in enumerate(df["owner_location_clean"].tolist()):
        loc = str(loc).strip()
        geo = geo_map.get(loc) if loc else None
        if not geo:
            geo = _geo_empty(GEO_PROVIDER or ("google" if GOOGLE_MAPS_API_KEY else "nominatim"))
        for c in geo_cols:
            df.at[i, c] = geo.get(c, "")

    out = write_any(df, op)
    save_geo_cache()

    # petit résumé
    counts = df["owner_geocode_status"].value_counts(dropna=False).to_dict()
    print("Geocode status counts:", counts)
    print(f"Written: {out}")


if __name__ == "__main__":
    # Par défaut, adapte ces chemins à ton fichier brut
    # Exemple:
    #   input  = github_repos_lisp_raw.xlsx
    #   output = github_repos_lisp_enriched.xlsx
    INPUT = os.getenv("INPUT_FILE", "github_repos_lisp_with_owner_details.xlsx")
    OUTPUT = os.getenv("OUTPUT_FILE", "github_repos_lisp_enriched.xlsx")

    postprocess_file(INPUT, OUTPUT, location_col="owner_location")
