"""
Renders dashboard_template.html + dashboard_data.json -> dashboard.html
(the file actually handed to the Artifact publisher).

Usage
-----
    python generate_html.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
DATA_PATH = HERE / "dashboard_data.json"
TEMPLATE_PATH = HERE / "dashboard_template.html"
OUT_PATH = HERE / "dashboard.html"


def main() -> None:
    data = json.loads(DATA_PATH.read_text())
    template = TEMPLATE_PATH.read_text()

    total = len(data)
    tagged = sum(1 for r in data if r["suggested_title"])
    with_application = sum(1 for r in data if r["num_applications"] > 0)
    hired = sum(1 for r in data if any(role["stage"] == "Hired" for role in r["roles_applied"]))
    sourced_not_applied = sum(1 for r in data if r["sourced"] and r["num_applications"] == 0)

    html = (
        template
        .replace("__DATA_JSON__", json.dumps(data, ensure_ascii=False))
        .replace("__TOTAL_CANDIDATES__", f"{total:,}")
        .replace("__TAGGED_PCT__", f"{round(100 * tagged / total)}")
        .replace("__WITH_APPLICATION__", f"{with_application:,}")
        .replace("__HIRED_COUNT__", f"{hired:,}")
        .replace("__SOURCED_COUNT__", f"{sourced_not_applied:,}")
        .replace("__GENERATED_AT__", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    )
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_PATH} ({len(html)/1e6:.2f} MB)")


if __name__ == "__main__":
    main()
