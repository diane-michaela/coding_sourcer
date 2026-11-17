"""Merge several countries into one compact.txt, ready for build-postings.py.

Usage:
  python3 merge-countries.py --out compact.txt uk.txt pl.txt es.txt ...
  python3 merge-countries.py --out compact.txt linkedin-jobs-uk-*.csv ...

Each input is either
  - a readback file named <country code>.txt (uk.txt, pl.txt, fr.txt...):
    one posting per line, 7 fields split by U+00A6, the last one being the
    URL tail after https://<cc>.linkedin.com/jobs/view/ (or '!' + full URL);
  - a bookmarklet CSV (ID, Title, Company, Role, Location, Country code, Stack,
    Source, URL), for runs where the user collected the rows.

Every URL is written in full ('!' + URL) so countries can be mixed, and
postings are de-duplicated on the LinkedIn job ID (the number at the end of
the URL), keeping the first country listed. Order of the countries on the
command line = order of the rows in postings.csv.

Then run:
  python3 build-postings.py --ts <any ts> --prefix https://www.linkedin.com/jobs/view/ \
      --compact compact.txt --types types.json --out postings.csv
(the prefix is unused when every URL is full).
"""
import argparse, csv, os, re, sys
from collections import Counter

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("inputs", nargs="+")
o = ap.parse_args()

SEP = "\u00a6"
seen, out, per_country, dups = {}, [], Counter(), []


def job_id(url):
    m = re.search(r"-(\d{6,})/?$", url) or re.search(r"(\d{6,})/?$", url)
    return m.group(1) if m else url


def country_of(url):
    m = re.match(r"https?://([a-z]{2})\.linkedin\.com/", url, re.I)
    return m.group(1).upper() if m else "?"


def add(fields, url):
    jid = job_id(url)
    cc = country_of(url)
    if jid in seen:
        dups.append((cc, seen[jid], fields[1]))
        return
    seen[jid] = cc
    per_country[cc] += 1
    out.append(SEP.join(fields[:6] + ["!" + url]))


for path in o.inputs:
    name = os.path.basename(path)
    if name.lower().endswith(".csv"):
        with open(path, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                add([r["Title"], r["Company"], r["Role"], r["Location"], r.get("Country code", r.get("Pays", "")), r["Stack"]], r["URL"])
        continue
    m = re.match(r"([a-z]{2}|www)\b", name.lower())
    if not m:
        sys.exit(f"{name}: name it <country code>.txt, e.g. uk.txt")
    prefix = f"https://{m.group(1)}.linkedin.com/jobs/view/"
    for i, line in enumerate(open(path, encoding="utf-8").read().split("\n"), 1):
        if not line.strip():
            continue
        p = line.split(SEP)
        if len(p) != 7:
            sys.exit(f"{name} line {i} has {len(p)} fields, expected 7: {line[:80]}")
        url = p[6][1:] if p[6].startswith("!") else prefix + p[6]
        add(p, url)

with open(o.out, "w", encoding="utf-8") as f:
    f.write("\n".join(out) + "\n")
print(f"{len(out)} unique postings written to {o.out}")
print("per country: " + ", ".join(f"{k} {v}" for k, v in per_country.items()))
if dups:
    print(f"{len(dups)} duplicates removed (same job ID): " + "; ".join(f"{a} dup of {b}: {c}" for a, b, c in dups))
