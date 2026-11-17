"""Rebuild postings.csv from the compact read-back, in the container.

Usage:
  python3 build-postings.py --ts <ts> --prefix <url prefix> \
      --compact compact.txt --types types.json --out postings.csv

compact.txt : the readback.js text (one posting per line, fields split by U+00A6:
              Title, Company, Role, Location, Pays, Stack, URL tail).
types.json  : {"Company name": "Product" | "Staffing" | "Unsure", ...}.
              Any company missing from it is written as Unsure.

Output: the bookmarklet's columns, values unchanged, with Company type after
Company. UTF-8 with BOM, every field quoted, CRLF line ends.
"""
import argparse, csv, json

ap = argparse.ArgumentParser()
for a in ("--ts", "--prefix", "--compact", "--types", "--out"):
    ap.add_argument(a, required=True)
o = ap.parse_args()
types = json.load(open(o.types, encoding="utf-8"))
lines = [l for l in open(o.compact, encoding="utf-8").read().split("\n") if l.strip()]
with open(o.out, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow(["ID", "Title", "Company", "Company type", "Role", "Location", "Pays", "Stack", "Source", "URL"])
    for i, l in enumerate(lines, 1):
        p = l.split("\u00a6")
        if len(p) != 7:
            raise SystemExit(f"line {i} has {len(p)} fields, expected 7: {l[:80]}")
        url = p[6][1:] if p[6].startswith("!") else o.prefix + p[6]
        w.writerow([f"{o.ts}-{i:03d}", p[0], p[1], types.get(p[1], "Unsure"), p[2], p[3], p[4], p[5], "LinkedIn", url])
print(f"{len(lines)} rows written to {o.out}")
