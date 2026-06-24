# Hugging Face Retrieval Sourcer

Sourcing tool that searches the Hugging Face Hub for individuals working on retrieval, ranking, and recommendation — and exports results to a recruiter-friendly Excel file.

## What it does

1. **Searches HF Hub** for models and datasets using retrieval/search keywords (e.g. `reranker`, `bi-encoder`, `colbert`, `semantic search`, `recommendation`, etc.)
2. **Filters by recency** — only assets last modified between 2023 and 2026
3. **Enriches author profiles** — fetches user or organization metadata and caches results
4. **Extracts README summaries** — pulls a short description from each model card (best-effort)
5. **Extracts contributors** — infers commit authors from repo history (best-effort, cached)
6. **Scores candidates** — adds `score` and `score_reasons` columns based on talent signals
7. **Individuals-only mode** (default ON) — skips org-owned repos unless individual contributors are found
8. **Exports to Excel** with hyperlinks, plus a CSV fallback. Includes separate sheets for org rankings.

## Output

- `hf_retrieval_models_datasets_with_author_details.xlsx` — main output with candidate profiles, scores, and links

## Configuration

Edit the constants at the top of the script:

| Variable | Default | Description |
|---|---|---|
| `USE_EXTENDED_QUERIES` | `True` | Include extended keyword list |
| `START_YEAR` / `END_YEAR` | 2023 / 2026 | Filter assets by last modified year |
| `MAX_ASSETS_TOTAL` | 1200 | Cap on total assets fetched |
| `INDIVIDUALS_ONLY` | `True` | Exclude org-owned repos with no individual contributors |
| `FETCH_README` | `True` | Extract model card descriptions |
| `FETCH_CONTRIBUTORS` | `True` | Extract commit contributors |

## Authentication

The script looks for a Hugging Face token in this order:
1. `HF_TOKEN` or `HUGGINGFACEHUB_API_TOKEN` environment variable
2. A local `token_hf.py` file with `HF_TOKEN = "hf_..."`

Running without a token works but may hit rate limits faster.

## Requirements

```
requests
pandas
openpyxl
```

## Limitations

- HF does not reliably expose emails, locations, or real names — many fields are optional
- Country is a best-effort guess from bio/name/website only
- Contributor extraction relies on commit metadata and may return empty results
