"""
Google first-result fetcher — rate-limit aware, scalability test.

For each query in SEARCH_QUERIES, fetches the first Google search result URL.
Designed to surface rate-limit behaviour when scaling from small to large batches.

Strategy:
- Uses googlesearch-python (unofficial Google scraping; no API key needed).
- Adds a configurable delay between queries to reduce bot-detection risk.
- On HTTP 429 / captcha detection, backs off exponentially and records the failure.
- Outputs results to CSV + optional Google Sheets tab ("google_search").
- Prints a live summary so you can see where limits kick in.

Usage:
    python google_search.py

Config env vars (optional):
    GOOGLE_SEARCH_DELAY_SEC   base delay between queries  (default 3)
    GOOGLE_SEARCH_MAX_RETRIES retries on transient errors (default 3)
    GOOGLE_SEARCH_TO_SHEETS   "true" to also write to Google Sheets
"""

import os
import csv
import json
import time
import random
import traceback
from datetime import datetime, timezone
from pathlib import Path

from googlesearch import search as _gsearch

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT_SOURCE         = "google_search"
BASE_DELAY_SEC        = float(os.getenv("GOOGLE_SEARCH_DELAY_SEC",   "3"))
MAX_RETRIES           = int(os.getenv("GOOGLE_SEARCH_MAX_RETRIES",   "3"))
WRITE_TO_SHEETS       = os.getenv("GOOGLE_SEARCH_TO_SHEETS", "false").lower() in ("1","true","yes")

SPREADSHEET_ID        = "1OVr2EigkJ5ZHceilXGn-Zl8xpGTGPVxp8jKIEJnOgmo"
SERVICE_ACCOUNT_FILE  = "google_service_account.json"
SHEET_TAB_NAME        = "google_search"

OUTPUT_CSV            = Path(__file__).with_name("google_search_results.csv")

HEADER = [
    "source", "execution_timestamp", "query_index",
    "query", "first_result_url",
    "status",            # OK | NOT_FOUND | RATE_LIMITED | ERROR
    "error_detail",
    "elapsed_sec",
]

# ── 50 sample queries (LLM / fine-tuning engineer discovery) ────────────────
# Replace or extend this list with whatever you want to search for.
SEARCH_QUERIES: list[str] = [
    # High-signal library / tool authors
    "vLLM GitHub LLM inference engineer",
    "Axolotl fine-tuning GitHub",
    "bitsandbytes quantization GitHub profile",
    "PEFT LoRA Hugging Face GitHub developer",
    "TRL RLHF trainer GitHub",
    "Unsloth LLM fine-tuning GitHub",
    "QLoRA paper author GitHub",
    "DeepSpeed ZeRO GitHub engineer",
    "FlashAttention GitHub contributor",
    "FSDP PyTorch fine-tuning GitHub",
    # Frameworks / stacks
    "AutoGPTQ quantization GitHub",
    "AWQ quantization engineer GitHub",
    "GPTQ-for-LLaMA GitHub",
    "llama.cpp GitHub contributor",
    "llamafile GitHub",
    "exllamav2 GitHub",
    "CTransformers GitHub",
    "mlc-llm GitHub contributor",
    "text-generation-inference GitHub engineer",
    "TensorRT-LLM GitHub contributor",
    # Alignment / RLHF
    "DPO direct preference optimization GitHub",
    "PPO reward model training GitHub",
    "OpenRLHF GitHub",
    "trlX GitHub contributor",
    "alignment-handbook Hugging Face GitHub",
    "SFT supervised fine-tuning LLM GitHub",
    "instruction tuning dataset GitHub",
    "LIMA dataset paper author GitHub",
    "Alpaca fine-tuning GitHub",
    "OpenAssistant dataset GitHub contributor",
    # Inference / serving
    "vllm-project contributor site:github.com",
    "triton kernel LLM inference GitHub",
    "ONNX Runtime LLM GitHub",
    "Optimum Hugging Face GitHub contributor",
    "sglang LLM serving GitHub",
    "lm-eval-harness GitHub contributor",
    "FastChat GitHub contributor",
    "LiteLLM GitHub engineer",
    "Ollama GitHub contributor",
    "llm-foundry MosaicML GitHub",
    # Quantisation deep dives
    "AQLM quantization GitHub",
    "SqueezeLLM quantization GitHub",
    "SpQR quantization GitHub",
    "QuIP quantization GitHub",
    "LoftQ GitHub",
    "IA3 adapter fine-tuning GitHub",
    "prefix tuning LLM GitHub",
    "prompt tuning LLM GitHub",
    # Misc high-signal
    "Megatron-LM GitHub contributor",
    "NeMo framework NVIDIA GitHub engineer",
]

assert len(SEARCH_QUERIES) == 50, f"Expected 50 queries, got {len(SEARCH_QUERIES)}"


# ── Google Sheets helpers ─────────────────────────────────────────────────────
def _gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_tab(tab_name: str, cols: int = len(HEADER)):
    client = _gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    for ws in sh.worksheets():
        if ws.title == tab_name:
            return ws
    return sh.add_worksheet(title=tab_name, rows=200, cols=cols)


def ensure_header(ws) -> None:
    existing = ws.row_values(1)
    if not existing:
        ws.append_row(HEADER)
    else:
        missing = [c for c in HEADER if c not in set(existing)]
        if missing:
            ws.update(values=[existing + missing], range_name="1:1")


def append_to_sheet(ws, rows: list[dict]) -> None:
    if not rows:
        return
    next_row = len(ws.col_values(1)) + 1
    needed   = next_row + len(rows) - 1
    if needed > ws.row_count:
        ws.add_rows(max(len(rows) + 200, 500))
    matrix = [[r.get(col, "") for col in HEADER] for r in rows]
    ws.update(values=matrix, range_name=f"A{next_row}", value_input_option="RAW")


# ── Core fetch ───────────────────────────────────────────────────────────────
def fetch_first_result(query: str) -> tuple[str, str, str]:
    """
    Returns (url, status, error_detail).
    status: OK | NOT_FOUND | RATE_LIMITED | ERROR
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            results = list(_gsearch(query, num_results=1, sleep_interval=0))
            if not results:
                return "", "NOT_FOUND", ""
            return results[0], "OK", ""
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "too many" in msg or "captcha" in msg:
                wait = BASE_DELAY_SEC * (3 ** attempt) + random.uniform(0, 2)
                print(f"  ⚠ Rate-limited (attempt {attempt}/{MAX_RETRIES}). "
                      f"Backing off {wait:.1f}s …")
                time.sleep(wait)
                if attempt == MAX_RETRIES:
                    return "", "RATE_LIMITED", str(exc)
            else:
                if attempt == MAX_RETRIES:
                    return "", "ERROR", str(exc)[:200]
                time.sleep(BASE_DELAY_SEC)
    return "", "ERROR", "max retries"


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    run_ts  = datetime.now(timezone.utc).strftime("%d/%m/%y")
    results: list[dict] = []

    # Stats
    ok_count    = 0
    fail_count  = 0
    rl_count    = 0
    total_time  = 0.0

    print(f"Starting Google search run — {len(SEARCH_QUERIES)} queries")
    print(f"Base delay: {BASE_DELAY_SEC}s | Max retries: {MAX_RETRIES} | Write to Sheets: {WRITE_TO_SHEETS}")
    print("─" * 70)

    for i, query in enumerate(SEARCH_QUERIES, start=1):
        t0 = time.time()
        url, status, err = fetch_first_result(query)
        elapsed = round(time.time() - t0, 2)
        total_time += elapsed

        row = {
            "source":             SCRIPT_SOURCE,
            "execution_timestamp": run_ts,
            "query_index":        i,
            "query":              query,
            "first_result_url":   url,
            "status":             status,
            "error_detail":       err,
            "elapsed_sec":        elapsed,
        }
        results.append(row)

        status_icon = "✓" if status == "OK" else ("⚠" if status == "RATE_LIMITED" else "✗")
        print(f"[{i:02d}/50] {status_icon} {status:<12} {elapsed:5.1f}s  {url[:70] if url else err[:70]}")

        if status == "OK":
            ok_count += 1
        elif status == "RATE_LIMITED":
            rl_count += 1
            fail_count += 1
        else:
            fail_count += 1

        # Polite delay between queries (jitter to reduce fingerprinting)
        if i < len(SEARCH_QUERIES):
            sleep = BASE_DELAY_SEC + random.uniform(0.5, 2.0)
            time.sleep(sleep)

    # ── Write CSV ─────────────────────────────────────────────────────────────
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV saved: {OUTPUT_CSV.resolve()}")

    # ── Write to Sheets (optional) ────────────────────────────────────────────
    if WRITE_TO_SHEETS:
        try:
            ws = get_or_create_tab(SHEET_TAB_NAME)
            ensure_header(ws)
            append_to_sheet(ws, results)
            print(f"Appended {len(results)} rows to Google Sheets tab '{SHEET_TAB_NAME}'")
        except Exception as e:
            print(f"Sheets write failed: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print(f"Summary for {len(SEARCH_QUERIES)} queries:")
    print(f"  OK            : {ok_count}")
    print(f"  Rate-limited  : {rl_count}")
    print(f"  Other failures: {fail_count - rl_count}")
    print(f"  Total time    : {total_time:.1f}s  (avg {total_time/len(SEARCH_QUERIES):.1f}s/query)")
    if rl_count:
        print(f"\n⚠ Rate limits hit {rl_count} times. Increase GOOGLE_SEARCH_DELAY_SEC to reduce.")


if __name__ == "__main__":
    main()
