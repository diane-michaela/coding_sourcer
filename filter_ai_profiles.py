"""
Filter LinkedIn profiles to keep only AI/ML-related roles.
Usage:
    python filter_ai_profiles.py input.csv           # reads CSV, writes filtered_profiles.csv
    python filter_ai_profiles.py --sheet-id SHEET_ID # downloads directly from Google Sheets
"""

import sys
import re
import csv
import argparse
from pathlib import Path


AI_KEYWORDS = [
    r"ai engineer",
    r"ml engineer",
    r"machine learning",
    r"applied scientist",
    r"applied ai",
    r"applied science",
    r"\bml\b",
    r"\bllm\b",
    r"\bllms\b",
    r"agent\b",           # agents, agentic, agent research, etc.
    r"agentic",
    r"ai researcher",
    r"ai scientist",
    r"ai research",
    r"deep learning",
    r"reinforcement learning",
    r"nlp engineer",
    r"computer vision",
    r"foundation model",
    r"large language model",
    r"generative ai",
    r"ai/ml",
    r"ml/ai",
    r"data scientist",
]

PATTERN = re.compile("|".join(AI_KEYWORDS), re.IGNORECASE)


def is_ai_profile(job: str) -> bool:
    return bool(PATTERN.search(job))


def filter_csv(input_path: Path, output_path: Path) -> int:
    with open(input_path, newline="", encoding="utf-8") as infile, \
         open(output_path, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        if not reader.fieldnames:
            print("ERROR: Could not read CSV headers.")
            return 0

        job_col = next(
            (f for f in reader.fieldnames if f.strip().lower() == "job"),
            None
        )
        if not job_col:
            print(f"ERROR: No 'job' column found. Columns: {reader.fieldnames}")
            return 0

        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        matched = 0
        for row in reader:
            if is_ai_profile(row.get(job_col, "")):
                writer.writerow(row)
                matched += 1

    return matched


def download_from_sheets(sheet_id: str, gid: str = "147443030") -> Path:
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' not installed. Run: pip install requests")
        sys.exit(1)

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Downloading from Google Sheets (gid={gid})...")
    response = requests.get(url)

    if response.status_code == 401 or "Sign in" in response.text[:200]:
        print("ERROR: Sheet is private. Please download manually:")
        print("  File → Download → Comma-separated values (.csv)")
        sys.exit(1)

    if response.status_code != 200:
        print(f"ERROR: HTTP {response.status_code} when downloading sheet.")
        sys.exit(1)

    tmp_path = Path("downloaded_profiles.csv")
    tmp_path.write_bytes(response.content)
    print(f"Downloaded to {tmp_path}")
    return tmp_path


def main():
    parser = argparse.ArgumentParser(description="Filter LinkedIn profiles for AI/ML roles")
    parser.add_argument("input", nargs="?", help="Input CSV file path")
    parser.add_argument("--sheet-id", help="Google Sheets file ID (for direct download)")
    parser.add_argument("--gid", default="147443030", help="Sheet tab gid (default: 147443030)")
    parser.add_argument("--output", default="filtered_ai_profiles.csv", help="Output CSV file")
    args = parser.parse_args()

    if args.sheet_id:
        input_path = download_from_sheets(args.sheet_id, args.gid)
    elif args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"ERROR: File not found: {input_path}")
            sys.exit(1)
    else:
        # Try default filenames in current directory
        for candidate in ["profiles.csv", "export.csv", "linkedin.csv"]:
            if Path(candidate).exists():
                input_path = Path(candidate)
                print(f"Using {input_path}")
                break
        else:
            print("Usage:")
            print("  python filter_ai_profiles.py your_file.csv")
            print("  python filter_ai_profiles.py --sheet-id 1dFgGFuN7ccCPU7MXBMkO9Oi1Vr4b9nHvwkbexKtGOek")
            sys.exit(1)

    output_path = Path(args.output)
    count = filter_csv(input_path, output_path)
    print(f"\nDone: {count} AI/ML profiles saved to {output_path}")
    print(f"\nKeywords matched: {', '.join(AI_KEYWORDS)}")


if __name__ == "__main__":
    main()
