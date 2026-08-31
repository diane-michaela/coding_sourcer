# Product Manager

Sourcing script for the exploratory PM Engineer / Designer Engineer / AI Squad roles discussed with CEO Guillaume Boiret. As of 2026-07-27 this is exploratory only — no active req or budget, so nothing here is wired to run automatically or push to production systems.

## fde_react_sourcer.py

Searches GitHub for candidates matching the "Forward Deployed Engineer" profile: builders who work across the PhantomBuster stack (React, TypeScript, Storybook, Tailwind, Jest/Cypress, Node/Redis/PostgreSQL, Docker/Ansible, GH Actions/CircleCI).

- Forked from `lisp.py` (the production ML/NLP sourcer) — that script is left untouched and still feeds its own Google Sheet.
- Runs several stack-specific GitHub search queries (see `BASE_QUERIES`), since ANDing too many topics at once returns almost nothing.
- Filters candidate owners to France by default (`TARGET_COUNTRIES`), extendable to the rest of Europe.
- Writes results to a local Excel file — no Google Sheets/Airtable credentials required to run it.
- Manual run only: review `BASE_QUERIES` / `TARGET_COUNTRIES`, then `python fde_react_sourcer.py`.

Requires `GITHUB_TOKEN` in `.env` (same one `lisp.py` uses) plus `requests`, `pandas`, `openpyxl`.

Before treating this as a real pipeline, re-confirm scope with Guillaume — the role brief may change once there's an active req.
