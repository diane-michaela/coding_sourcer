import requests
from bs4 import BeautifulSoup
import csv
import os
from urllib.parse import urljoin

URL = "https://careers.phantombuster.com/people"

OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "API")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "phantombuster_people.csv")

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(URL, headers=headers, timeout=20)
response.raise_for_status()

soup = BeautifulSoup(response.text, "html.parser")

results = []

# Example selector guesses — these will need adjustment after inspecting the real card wrapper
cards = soup.select('a[href*="/people/"]')

for card in cards:
    link = urljoin(URL, card.get("href", ""))

    name_el = card.select_one("h2, h3, h4, .text-block-link, .name")
    title_el = card.select_one("p, .text-company, .text-md, .role")
    img_el = card.select_one("img")

    name = name_el.get_text(strip=True) if name_el else None
    title = title_el.get_text(strip=True) if title_el else None
    image = img_el.get("src") if img_el else None

    # Skip empty rows
    if not name and not title:
        continue

    results.append({
        "name": name,
        "title": title,
        "profile_url": link,
        "image_url": image,
    })

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["name", "title", "profile_url", "image_url"]
    )
    writer.writeheader()
    writer.writerows(results)

print(f"Saved {len(results)} rows to {OUTPUT_FILE}")