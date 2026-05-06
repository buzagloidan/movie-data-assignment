#!/usr/bin/env python3
"""Keep resumable Wikipedia enrichment running in one Python process."""

from __future__ import annotations

import os
import time

os.environ.setdefault("REQUEST_SLEEP_SECONDS", "1")
os.environ.setdefault("MAX_RATE_LIMIT_RETRIES", "20")
os.environ.setdefault("ENRICH_BATCH_SIZE", "500")
os.environ.setdefault("ENRICH_SAVE_EVERY", "10")
os.environ.setdefault("ENRICH_RETRY_FAILED", "1")
os.environ.setdefault("WIKI_SEARCH_LIMIT", "8")

import enrich_wikipedia
import make_report

LOOP_SLEEP_SECONDS = int(os.environ.get("ENRICH_LOOP_SLEEP_SECONDS", "10"))


def main() -> None:
    while True:
        attempted = enrich_wikipedia.main()
        make_report.main()
        if attempted == 0:
            print("No unenriched rows left to attempt; stopping enrichment loop.", flush=True)
            break
        time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
