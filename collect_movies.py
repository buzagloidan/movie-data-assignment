#!/usr/bin/env python3
"""
Collect the movie dataset for assignment part 1.

The team letter is M, so the base population is IMDb movies whose primary
title starts with M. IMDb's public TSV files provide the required title,
rating, vote and actor-id fields. English Wikipedia pages are used for budget,
box office, language, country and plot enrichment.
"""

from __future__ import annotations

import csv
import gzip
import json
import math
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = Path(os.environ.get("CACHE_DIR", BASE_DIR / "cache"))
RAW_DIR = Path(os.environ.get("RAW_DIR", BASE_DIR / "imdb_data"))
DATASET_PATH = Path(os.environ.get("DATASET_PATH", BASE_DIR / "dataset.csv"))
REPORT_STATS_PATH = Path(os.environ.get("REPORT_STATS_PATH", BASE_DIR / "report_stats.json"))

TEAM_LETTER = os.environ.get("TEAM_LETTER", "M").strip()[:1].upper() or "M"
TARGET_MOVIE_COUNT = int(os.environ.get("TARGET_MOVIE_COUNT", "10000"))
CANDIDATE_POOL_SIZE = int(os.environ.get("CANDIDATE_POOL_SIZE", str(TARGET_MOVIE_COUNT * 3)))
REQUEST_SLEEP_SECONDS = float(os.environ.get("REQUEST_SLEEP_SECONDS", "0.25"))
MAX_RATE_LIMIT_RETRIES = int(os.environ.get("MAX_RATE_LIMIT_RETRIES", "2"))
REQUIRE_WIKIPEDIA_PAGE = os.environ.get("REQUIRE_WIKIPEDIA_PAGE", "0").strip().lower() not in {"0", "false", "no"}
WIKIPEDIA_ENRICH_LIMIT = int(os.environ.get("WIKIPEDIA_ENRICH_LIMIT", str(TARGET_MOVIE_COUNT)))

USER_AGENT = "movie-data-assignment/1.0 (student project)"
RATE_LIMITED_HOSTS: set[str] = set()

IMDB_FILES = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "principals": "title.principals.tsv.gz",
}

FIELDNAMES = [
    "tconst",
    "primaryTitle",
    "startYear",
    "genres",
    "lead_actors_ids",
    "runtimeMinutes",
    "averageRating",
    "Language",
    "Country",
    "numVotes",
    "budget",
    "BoxOffice",
    "plot",
]


def read_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def open_json_request(req: urllib.request.Request, context: ssl.SSLContext | None = None) -> dict:
    if context is None:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    with urllib.request.urlopen(req, timeout=30, context=context) as response:
        return json.loads(response.read().decode("utf-8"))


def get_json(url: str, cache_name: str, sleep: float = REQUEST_SLEEP_SECONDS) -> dict:
    cache_path = CACHE_DIR / cache_name
    cached = read_json(cache_path)
    if cached:
        return cached

    host = urllib.parse.urlparse(url).netloc
    if host in RATE_LIMITED_HOSTS:
        return {}

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
        try:
            payload = open_json_request(req)
            break
        except urllib.error.HTTPError as exc:
            if exc.code != 429:
                raise
            wait_seconds = int(exc.headers.get("Retry-After") or 30 * attempt)
            print(f"Rate limited by {host}; waiting {wait_seconds} seconds before retry", flush=True)
            time.sleep(wait_seconds)
        except urllib.error.URLError as exc:
            if "CERTIFICATE_VERIFY_FAILED" not in str(exc):
                raise
            context = ssl._create_unverified_context()
            try:
                payload = open_json_request(req, context=context)
                break
            except urllib.error.HTTPError as http_exc:
                if http_exc.code != 429:
                    raise
                wait_seconds = int(http_exc.headers.get("Retry-After") or 30 * attempt)
                print(f"Rate limited by {host}; waiting {wait_seconds} seconds before retry", flush=True)
                time.sleep(wait_seconds)
    else:
        return {}

    write_json(cache_path, payload)
    time.sleep(sleep)
    return payload


def download_file(url: str, path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    print(f"Downloading {url}", flush=True)
    with urllib.request.urlopen(req, timeout=120) as response, tmp_path.open("wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    tmp_path.replace(path)


def ensure_imdb_files() -> dict[str, Path]:
    paths = {}
    for key, filename in IMDB_FILES.items():
        path = RAW_DIR / filename
        download_file(f"https://datasets.imdbws.com/{filename}", path)
        paths[key] = path
    return paths


def iter_tsv_gz(path: Path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


def clean_int(value: str | None) -> int | None:
    if not value or value == r"\N" or value == "N/A":
        return None
    match = re.search(r"\d[\d,]*", value)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def clean_float(value: str | None) -> float | None:
    if not value or value == r"\N" or value == "N/A":
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def normalize_list_field(value: str | None) -> str:
    if not value or value == r"\N" or value == "N/A":
        return "[]"
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return json.dumps(parts, ensure_ascii=False)


def first_value(value: str | None) -> str:
    if not value or value == "N/A":
        return ""
    return value.split(",")[0].strip()


def parse_money_to_millions(text: str | None) -> float | None:
    if not text:
        return None

    text = clean_wiki_text(text)
    if "N/A" in text:
        return None

    values = []
    for raw_amount, raw_unit in re.findall(
        r"(?:US\$|\$|USD\s*)\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|m)?",
        text,
        flags=re.I,
    ):
        amount = float(raw_amount.replace(",", ""))
        unit = raw_unit.lower()
        if unit.startswith("b"):
            amount *= 1000
        elif unit in ("", "m") and ("," in raw_amount or amount > 10000):
            amount /= 1_000_000
        values.append(amount)

    if not values:
        return None
    return round(sum(values) / len(values), 2)


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:120]


def build_imdb_candidates(paths: dict[str, Path]) -> list[dict]:
    print(f"Reading IMDb basics for movies starting with {TEAM_LETTER}", flush=True)
    candidates: dict[str, dict] = {}
    for row in iter_tsv_gz(paths["basics"]):
        title = row.get("primaryTitle", "").strip()
        if row.get("titleType") != "movie":
            continue
        if not title.upper().startswith(TEAM_LETTER):
            continue

        year = clean_int(row.get("startYear"))
        runtime = clean_int(row.get("runtimeMinutes"))
        if year is None or year > 2024:
            continue
        if runtime is None or not 60 <= runtime <= 300:
            continue

        candidates[row["tconst"]] = {
            "tconst": row["tconst"],
            "primaryTitle": title,
            "startYear": year,
            "genres": normalize_list_field(row.get("genres")),
            "runtimeMinutes": runtime,
        }

    print(f"Found {len(candidates)} eligible {TEAM_LETTER}-title movies before ratings", flush=True)
    print("Joining IMDb ratings", flush=True)
    for row in iter_tsv_gz(paths["ratings"]):
        candidate = candidates.get(row.get("tconst"))
        if not candidate:
            continue
        candidate["averageRating"] = clean_float(row.get("averageRating"))
        candidate["numVotes"] = clean_int(row.get("numVotes"))

    rated = [
        item for item in candidates.values()
        if item.get("averageRating") is not None and item.get("numVotes") is not None
    ]
    rated.sort(key=lambda item: (-item["numVotes"], item["primaryTitle"].casefold(), item["tconst"]))
    pool = rated[:CANDIDATE_POOL_SIZE]
    print(f"Selected {len(pool)} rated candidates for Wikipedia enrichment", flush=True)
    return pool


def attach_actor_ids(paths: dict[str, Path], candidates: list[dict]) -> None:
    selected_ids = {row["tconst"] for row in candidates}
    actor_ids = {tconst: [] for tconst in selected_ids}
    remaining = set(selected_ids)
    current_selected: str | None = None

    print("Reading IMDb principals for lead actor ids", flush=True)
    for row in iter_tsv_gz(paths["principals"]):
        tconst = row.get("tconst")

        if current_selected and tconst != current_selected:
            remaining.discard(current_selected)
            current_selected = None
            if not remaining:
                break

        if tconst not in remaining:
            continue

        current_selected = tconst
        if row.get("category") != "actor":
            continue
        values = actor_ids[tconst]
        nconst = row.get("nconst", "")
        if nconst and nconst not in values and len(values) < 5:
            values.append(nconst)

    for row in candidates:
        row["lead_actors_ids"] = json.dumps(actor_ids.get(row["tconst"], []), ensure_ascii=False)


def wikipedia_search(title: str, year: int | None) -> str | None:
    query = f'{title} {year} film' if year else f"{title} film"
    params = urllib.parse.urlencode({
        "action": "query",
        "list": "search",
        "srsearch": query,
        "format": "json",
        "srlimit": 3,
        "utf8": 1,
    })
    data = get_json(f"https://en.wikipedia.org/w/api.php?{params}", f"wiki_search_{safe_name(query)}.json")
    for result in data.get("query", {}).get("search", []):
        page_title = result.get("title", "")
        if page_title:
            return page_title
    return None


def wikipedia_page_fields(title: str) -> dict:
    params = urllib.parse.urlencode({
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
        "redirects": 1,
    })
    data = get_json(f"https://en.wikipedia.org/w/api.php?{params}", f"wiki_parse_{safe_name(title)}.json")
    if "error" in data:
        return {}
    wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
    if not wikitext:
        return {}
    fields = extract_infobox_fields(wikitext)
    fields["plot"] = extract_plot(wikitext)
    return fields


def extract_infobox_fields(wikitext: str) -> dict:
    wanted = {
        "budget": "budget",
        "gross": "BoxOffice",
        "language": "Language",
        "country": "Country",
    }
    result = {}
    active_key = None
    active_value: list[str] = []

    def finish_active() -> None:
        if not active_key:
            return
        cleaned = clean_wiki_text(" ".join(active_value))
        if active_key in ("Language", "Country"):
            cleaned = first_value(cleaned)
        result[active_key] = cleaned

    for line in wikitext.splitlines():
        line = line.strip()
        if line.startswith("|") and "=" in line:
            finish_active()
            key, value = line[1:].split("=", 1)
            active_key = wanted.get(key.strip().lower())
            active_value = [value]
            continue
        if active_key and not line.startswith("}}"):
            active_value.append(line)

    finish_active()
    return result


def extract_plot(wikitext: str) -> str:
    patterns = [
        r"^==\s*Plot\s*==\s*$",
        r"^==\s*Synopsis\s*==\s*$",
        r"^==\s*Premise\s*==\s*$",
    ]
    lines = wikitext.splitlines()
    start = None
    for index, line in enumerate(lines):
        if any(re.match(pattern, line.strip(), flags=re.I) for pattern in patterns):
            start = index + 1
            break
    if start is None:
        return ""

    section_lines = []
    for line in lines[start:]:
        if re.match(r"^==[^=].*==\s*$", line.strip()):
            break
        if line.strip().startswith(("{{", "|", "[[File:", "[[Image:")):
            continue
        section_lines.append(line)

    plot = clean_wiki_text(" ".join(section_lines))
    return plot[:2000].strip()


def clean_wiki_text(value: str) -> str:
    value = re.sub(r"<!--.*?-->", "", value, flags=re.S)
    value = re.sub(r"<ref[^>]*>.*?</ref>", "", value, flags=re.I | re.S)
    value = re.sub(r"<ref[^/]*/>", "", value, flags=re.I)
    value = re.sub(r"<br\s*/?>", ", ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"\{\{(?:ubl|plainlist|plain list|unbulleted list)\|", "", value, flags=re.I)
    value = re.sub(r"\{\{nowrap\|([^{}]+)\}\}", r"\1", value)
    value = re.sub(r"\{\{flag\|([^{}|]+).*?\}\}", r"\1", value)
    value = re.sub(r"\{\{convert\|([^{}|]+)\|([^{}|]+)\|?.*?\}\}", r"\1 \2", value, flags=re.I)
    value = re.sub(r"\{\{(?:lang|native name|small|abbr)[^{}]*\}\}", "", value, flags=re.I)
    value = re.sub(r"\{\{[^{}]+\}\}", "", value)
    value = re.sub(r"\{\{[^{}]*", "", value)
    value = re.sub(r"\|group\s*=\s*[^}|]+", "", value)
    value = re.sub(r"\{\{efn\|.*", "", value)
    value = re.sub(r"\[\[[^|\]]+\|([^\]]+)\]\]", r"\1", value)
    value = re.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
    value = re.sub(r"\[[a-z]+://[^\s\]]+\s+([^\]]+)\]", r"\1", value)
    value = re.sub(r"'{2,}", "", value)
    value = value.replace("}}", "")
    value = value.replace("&nbsp;", " ").replace("\xa0", " ")
    value = value.replace("*", ", ")
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ,|")


def build_row(candidate: dict, wiki_fields: dict) -> dict:
    budget = parse_money_to_millions(wiki_fields.get("budget"))
    box_office = parse_money_to_millions(wiki_fields.get("BoxOffice"))
    return {
        "tconst": candidate["tconst"],
        "primaryTitle": candidate["primaryTitle"],
        "startYear": candidate["startYear"],
        "genres": candidate["genres"],
        "lead_actors_ids": candidate.get("lead_actors_ids", "[]"),
        "runtimeMinutes": candidate["runtimeMinutes"],
        "averageRating": candidate["averageRating"],
        "Language": wiki_fields.get("Language", ""),
        "Country": wiki_fields.get("Country", ""),
        "numVotes": candidate["numVotes"],
        "budget": budget,
        "BoxOffice": box_office,
        "plot": wiki_fields.get("plot", ""),
    }


def collect() -> list[dict]:
    paths = ensure_imdb_files()
    candidates = build_imdb_candidates(paths)

    selected: list[tuple[dict, dict]] = []
    for index, candidate in enumerate(candidates, start=1):
        if len(selected) >= TARGET_MOVIE_COUNT:
            break
        if index % 100 == 0:
            print(
                f"Wikipedia enrichment progress: {len(selected)} collected from {index} candidates",
                flush=True,
            )

        wiki_fields = {}
        if index <= WIKIPEDIA_ENRICH_LIMIT:
            page = wikipedia_search(candidate["primaryTitle"], candidate["startYear"])
            if page:
                wiki_fields = wikipedia_page_fields(page)

        if REQUIRE_WIKIPEDIA_PAGE and not wiki_fields:
            continue
        selected.append((candidate, wiki_fields))

    if len(selected) < TARGET_MOVIE_COUNT:
        raise RuntimeError(
            f"Collected only {len(selected)} movies. Increase CANDIDATE_POOL_SIZE "
            f"above {CANDIDATE_POOL_SIZE} and run again."
        )

    selected_candidates = [candidate for candidate, _wiki_fields in selected]
    attach_actor_ids(paths, selected_candidates)
    rows = [build_row(candidate, wiki_fields) for candidate, wiki_fields in selected]
    return rows


def write_dataset(rows: list[dict]) -> None:
    with DATASET_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def missing_stats(rows: list[dict]) -> dict:
    stats = {}
    total = len(rows)
    for field in FIELDNAMES:
        missing = 0
        for row in rows:
            value = row.get(field)
            if value in (None, "", "[]") or (isinstance(value, float) and math.isnan(value)):
                missing += 1
        stats[field] = {
            "missing_count": missing,
            "missing_percent": round((missing / total * 100) if total else 0, 2),
        }
    return {
        "movie_count": total,
        "team_letter": TEAM_LETTER,
        "target_movie_count": TARGET_MOVIE_COUNT,
        "candidate_pool_size": CANDIDATE_POOL_SIZE,
        "require_wikipedia_page": REQUIRE_WIKIPEDIA_PAGE,
        "wikipedia_enrich_limit": WIKIPEDIA_ENRICH_LIMIT,
        "wikipedia_enriched_count": sum(1 for row in rows if row.get("plot") or row.get("Language") or row.get("Country")),
        "missing": stats,
    }


def main() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    rows = collect()
    write_dataset(rows)
    stats = missing_stats(rows)
    write_json(REPORT_STATS_PATH, stats)
    print(f"Wrote {len(rows)} movies to {DATASET_PATH}", flush=True)
    print(json.dumps(stats, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
