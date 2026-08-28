# Agent workflow

Instructions for the AI agent working in this repo.

## Launching a new sourcer for a fresh req

Each req gets its own standalone script, forked from an existing one — this repo does *not*
share config or state between reqs (see `hf_ml_engineer_ai_specialist_sourcer.py` and
`hf_search_llm_ops_sourcer.py`: same architecture, separate `JD_KEYWORDS`, separate
`SCORING_SYSTEM_PROMPT`, no shared files). Do not add a `--jd` flag or generalize this into one
shared script — the whole point is that the scoring rubric is hand-tuned per role, not templated.

When asked to source for a new role, treat the request as:

> Fork `hf_ml_engineer_ai_specialist_sourcer.py` into a new standalone script,
> `hf_[role-slug]_sourcer.py` — same pattern as `hf_search_llm_ops_sourcer.py`. Update two things:
> - `JD_KEYWORDS` → search terms for this role's actual skills.
> - `SCORING_SYSTEM_PROMPT` → rewrite for this JD's real signals (role summary, project context,
>   key responsibilities) — follow the rubric already documented in this folder's README under
>   "Sourcing signal rubric" (a Space outweighs a model/dataset upload, recency and reinforcing
>   signals matter more than tag overlap, location is outreach context not a search filter, never
>   infer demographics from a photo or name).
>
> Leave enrichment, individuals-only resolution, and export columns as they are. Run `--dry-run`
> first — free preview, no Anthropic spend — before a full Claude-scored run.

`hf_ml_engineer_ai_specialist_sourcer.py` is the default fork base (general AI/ML) unless the new
role is specifically retrieval/ranking-flavored, in which case fork `hf_search_llm_ops_sourcer.py`
instead — its scoring prompt is already tuned for that.

This only applies when the role is actually AI/ML — for a non-ML role, this whole workflow is out
of scope, there is nothing here worth forking.

## After every prompt

1. **Update the README** — if the code, behavior, or setup changed in a way users should know
   about, update `README.md` accordingly.
2. **Commit and push** — commit the changes (including any README updates) and push to the
   remote.
