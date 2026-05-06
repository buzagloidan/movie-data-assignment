#!/usr/bin/env python3
"""Incrementally enrich dataset.csv with English Wikipedia fields.

This script is intentionally resumable. It only touches rows that still miss
Wikipedia fields, writes checkpoints, and updates report_stats.json after each
checkpoint. Stop it at any time and rerun the same command to continue.
"""

from __future__ import annotations

import csv
import os
import urllib.parse

import collect_movies


BATCH_SIZE = int(os.environ.get("ENRICH_BATCH_SIZE", "250"))
SAVE_EVERY = int(os.environ.get("ENRICH_SAVE_EVERY", "10"))
SEARCH_LIMIT = int(os.environ.get("WIKI_SEARCH_LIMIT", "1"))
RETRY_FAILED = os.environ.get("ENRICH_RETRY_FAILED", "0").strip().lower() not in {"0", "false", "no"}
STATE_PATH = collect_movies.BASE_DIR / "wiki_enrich_state.json"


def has_wikipedia_fields(row: dict) -> bool:
    return bool(row.get("plot") or row.get("Language") or row.get("Country"))


def row_key(row: dict) -> str:
    return row.get("tconst") or f"{row.get('primaryTitle', '')}:{row.get('startYear', '')}"


def load_state() -> dict:
    state = collect_movies.read_json(STATE_PATH)
    failed = state.get("failed_rows")
    if not isinstance(failed, dict):
        failed = {}
    return {"failed_rows": failed}


def write_state(state: dict) -> None:
    collect_movies.write_json(STATE_PATH, state)


def combined_cache_path(row: dict) -> object:
    query = f"{row['primaryTitle']} {row['startYear']} film"
    return collect_movies.CACHE_DIR / f"wiki_combined_{SEARCH_LIMIT}_{collect_movies.safe_name(query)}.json"


def page_wikitext(page: dict) -> str:
    revisions = page.get("revisions") or []
    if not revisions:
        return ""
    slots = revisions[0].get("slots", {})
    return slots.get("main", {}).get("content", "")


def wikipedia_combined_fields(row: dict) -> tuple[dict, str, bool]:
    query = f"{row['primaryTitle']} {row['startYear']} film"
    params = urllib.parse.urlencode({
        "action": "query",
        "generator": "search",
        "gsrsearch": query,
        "gsrlimit": SEARCH_LIMIT,
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "format": "json",
        "formatversion": 2,
        "redirects": 1,
        "utf8": 1,
    })
    cache_name = f"wiki_combined_{SEARCH_LIMIT}_{collect_movies.safe_name(query)}.json"
    data = collect_movies.get_json(f"https://en.wikipedia.org/w/api.php?{params}", cache_name)
    cache_exists = combined_cache_path(row).exists()
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return {}, "no Wikipedia search result", cache_exists

    empty_titles = []
    for page in sorted(pages, key=lambda item: item.get("index", 999)):
        page_title = page.get("title", "")
        wikitext = page_wikitext(page)
        if not wikitext:
            empty_titles.append(page_title or "unknown page")
            continue

        fields = collect_movies.extract_infobox_fields(wikitext)
        fields["plot"] = collect_movies.extract_plot(wikitext)
        if fields.get("plot") or fields.get("Language") or fields.get("Country"):
            return fields, page_title, True
        empty_titles.append(page_title or "unknown page")

    return {}, f"no usable Wikipedia fields in candidates: {', '.join(empty_titles[:5])}", cache_exists


def write_outputs(rows: list[dict]) -> None:
    collect_movies.write_dataset(rows)
    stats = collect_movies.missing_stats(rows)
    collect_movies.write_json(collect_movies.REPORT_STATS_PATH, stats)


def enrich_row(row: dict) -> tuple[bool, str, bool]:
    wiki_fields, page, should_skip_later = wikipedia_combined_fields(row)
    if not wiki_fields:
        return False, page, should_skip_later

    row["Language"] = wiki_fields.get("Language", row.get("Language", ""))
    row["Country"] = wiki_fields.get("Country", row.get("Country", ""))
    row["budget"] = collect_movies.parse_money_to_millions(wiki_fields.get("budget"))
    row["BoxOffice"] = collect_movies.parse_money_to_millions(wiki_fields.get("BoxOffice"))
    row["plot"] = wiki_fields.get("plot", row.get("plot", ""))
    changed = has_wikipedia_fields(row)
    return changed, "enriched" if changed else f"empty usable fields on {page}", not changed


def main() -> int:
    with collect_movies.DATASET_PATH.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    state = load_state()
    failed_rows = state["failed_rows"]
    before = sum(1 for row in rows if has_wikipedia_fields(row))
    attempted = 0
    enriched = 0
    skipped_failed = 0
    newly_failed = 0
    print(f"Starting Wikipedia enrichment: {before}/{len(rows)} rows already enriched", flush=True)

    for index, row in enumerate(rows, start=1):
        if attempted >= BATCH_SIZE:
            break
        if has_wikipedia_fields(row):
            continue
        key = row_key(row)
        if key in failed_rows and not RETRY_FAILED:
            skipped_failed += 1
            continue
        if RETRY_FAILED and failed_rows.get(key, {}).get("retry_search_limit", 0) >= SEARCH_LIMIT:
            skipped_failed += 1
            continue
        if RETRY_FAILED and key not in failed_rows:
            continue

        attempted += 1
        try:
            changed, reason, should_skip_later = enrich_row(row)
        except Exception as exc:
            print(f"Row {index} {row['primaryTitle']}: {type(exc).__name__}: {exc}", flush=True)
            changed = False
            reason = f"{type(exc).__name__}: {exc}"
            should_skip_later = False

        if changed:
            enriched += 1
            failed_rows.pop(key, None)
        elif should_skip_later:
            failed_rows[key] = {
                "title": row.get("primaryTitle", ""),
                "year": row.get("startYear", ""),
                "reason": reason,
                "retry_search_limit": SEARCH_LIMIT if RETRY_FAILED else 0,
            }
            newly_failed += 1

        if attempted % SAVE_EVERY == 0:
            write_outputs(rows)
            write_state(state)
            total = sum(1 for item in rows if has_wikipedia_fields(item))
            print(
                f"Checkpoint: attempted {attempted}/{BATCH_SIZE}, "
                f"newly enriched {enriched}, newly failed {newly_failed}, "
                f"skipped previous failures {skipped_failed}, "
                f"total enriched {total}/{len(rows)}",
                flush=True,
            )

    write_outputs(rows)
    write_state(state)
    total = sum(1 for row in rows if has_wikipedia_fields(row))
    print(
        f"Finished chunk: attempted {attempted}, newly enriched {enriched}, "
        f"newly failed {newly_failed}, skipped previous failures {skipped_failed}, "
        f"total enriched {total}/{len(rows)}",
        flush=True,
    )
    return attempted


if __name__ == "__main__":
    main()
