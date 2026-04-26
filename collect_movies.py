#!/usr/bin/env python3
"""
Collect a movie dataset for assignment part 1.

The script uses OMDb for the main movie fields, Wikipedia infoboxes for some
extra fields, and Wikidata for matching actor names to IMDb name ids.
"""

from __future__ import annotations

import csv
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


OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "5a3e0143")
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cache"
DATASET_PATH = BASE_DIR / "dataset.csv"
REPORT_STATS_PATH = BASE_DIR / "report_stats.json"
RATE_LIMITED_HOSTS: set[str] = set()


MOVIE_IDS = [
    "tt0111161", "tt0068646", "tt0468569", "tt0071562", "tt0050083",
    "tt0108052", "tt0167260", "tt0110912", "tt0060196", "tt0120737",
    "tt0137523", "tt0109830", "tt1375666", "tt0167261", "tt0080684",
    "tt0133093", "tt0099685", "tt0073486", "tt0114369", "tt0038650",
    "tt0047478", "tt0102926", "tt0317248", "tt0816692", "tt0120815",
    "tt0118799", "tt0120689", "tt6751668", "tt0245429", "tt0120586",
    "tt0110413", "tt0253474", "tt0110357", "tt0172495", "tt0407887",
    "tt0482571", "tt2582802", "tt1675434", "tt1853728", "tt0076759",
    "tt0910970", "tt1345836", "tt0088763", "tt0209144", "tt0034583",
    "tt0095765", "tt4154796", "tt4154756", "tt7286456", "tt4633694",
    "tt2380307", "tt5311514", "tt0993846", "tt1130884", "tt0361748",
    "tt0082971", "tt0081505", "tt0103064", "tt0112573", "tt0268978",
    "tt0266543", "tt0435761", "tt1049413", "tt2096673", "tt0114709",
    "tt2278388", "tt6966692", "tt8579674", "tt8267604", "tt8503618",
    "tt1392190", "tt2488496", "tt3659388", "tt3896198", "tt1825683",
    "tt0469494", "tt0372784", "tt0848228", "tt1745960", "tt9362722",
    "tt15398776", "tt1517268", "tt6718170", "tt15239678", "tt9603212",
    "tt10648342", "tt9114286", "tt1877830", "tt10872600", "tt1160419",
    "tt10272386", "tt11671006", "tt4154796", "tt0121766", "tt0121765",
]


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
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def get_json(url: str, cache_name: str, sleep: float = 0.05) -> dict:
    cache_path = CACHE_DIR / cache_name
    cached = read_json(cache_path)
    if cached:
        return cached

    host = urllib.parse.urlparse(url).netloc
    if host in RATE_LIMITED_HOSTS:
        return {}

    req = urllib.request.Request(url, headers={"User-Agent": "movie-data-assignment/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            RATE_LIMITED_HOSTS.add(host)
            return {}
        raise
    except urllib.error.URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc):
            raise
        context = ssl._create_unverified_context()
        try:
            with urllib.request.urlopen(req, timeout=30, context=context) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as http_exc:
            if http_exc.code == 429:
                RATE_LIMITED_HOSTS.add(host)
                return {}
            raise
    write_json(cache_path, payload)
    time.sleep(sleep)
    return payload


def clean_int(value: str | None) -> int | None:
    if not value or value == "N/A":
        return None
    match = re.search(r"\d[\d,]*", value)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def clean_float(value: str | None) -> float | None:
    if not value or value == "N/A":
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def parse_money_to_millions(text: str | None) -> float | None:
    if not text:
        return None
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("\xa0", " ")
    if "N/A" in text:
        return None

    values = []
    for raw in re.findall(r"(?:US\$|\$|USD\s*)\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|m)?", text, re.I):
        amount = float(raw[0].replace(",", ""))
        unit = raw[1].lower()
        if unit.startswith("b"):
            amount *= 1000
        elif unit in ("", "m"):
            if "," in raw[0] or amount > 10000:
                amount /= 1_000_000
        values.append(amount)

    if not values:
        return None
    return round(sum(values) / len(values), 2)


def normalize_list_field(value: str | None) -> str:
    if not value or value == "N/A":
        return "[]"
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return json.dumps(parts, ensure_ascii=False)


def omdb_movie(imdb_id: str) -> dict:
    params = urllib.parse.urlencode({"i": imdb_id, "apikey": OMDB_API_KEY, "plot": "short"})
    return get_json(f"http://www.omdbapi.com/?{params}", f"omdb_{imdb_id}.json")


def wikipedia_search(title: str, year: str | None) -> str | None:
    query = f"{title} {year} film" if year else f"{title} film"
    params = urllib.parse.urlencode({
        "action": "query",
        "list": "search",
        "srsearch": query,
        "format": "json",
        "srlimit": 1,
    })
    data = get_json(f"https://en.wikipedia.org/w/api.php?{params}", f"wiki_search_{safe_name(query)}.json")
    results = data.get("query", {}).get("search", [])
    return results[0]["title"] if results else None


def wikipedia_infobox(title: str) -> dict:
    params = urllib.parse.urlencode({
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
    })
    data = get_json(f"https://en.wikipedia.org/w/api.php?{params}", f"wiki_parse_{safe_name(title)}.json")
    wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
    return extract_infobox_fields(wikitext)


def extract_infobox_fields(wikitext: str) -> dict:
    wanted = {
        "budget": "budget",
        "gross": "BoxOffice",
        "language": "Language",
        "country": "Country",
    }
    result = {}
    for line in wikitext.splitlines():
        line = line.strip()
        if not line.startswith("|") or "=" not in line:
            continue
        key, value = line[1:].split("=", 1)
        key = key.strip().lower()
        out_key = wanted.get(key)
        if out_key is None:
            continue
        cleaned = clean_wiki_text(value)
        if out_key in ("Language", "Country"):
            cleaned = first_value(cleaned)
        result[out_key] = cleaned
    return result


def clean_wiki_text(value: str) -> str:
    value = re.sub(r"<!--.*?-->", "", value)
    value = re.sub(r"<br\s*/?>", ", ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"\{\{(?:ubl|plainlist|plain list)\|", "", value, flags=re.I)
    value = re.sub(r"\{\{nowrap\|([^{}]+)\}\}", r"\1", value)
    value = re.sub(r"\{\{flag\|([^{}|]+).*?\}\}", r"\1", value)
    value = re.sub(r"\{\{(?:lang|native name)[^{}]*\}\}", "", value, flags=re.I)
    value = re.sub(r"\{\{[^{}]+\}\}", "", value)
    value = re.sub(r"\{\{[^{}]*", "", value)
    value = re.sub(r"\|group\s*=\s*[^}|]+", "", value)
    value = re.sub(r"\{\{efn\|.*", "", value)
    value = re.sub(r"\[\[[^|\]]+\|([^\]]+)\]\]", r"\1", value)
    value = re.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
    value = re.sub(r"<ref[^>]*>.*?</ref>", "", value)
    value = re.sub(r"<ref[^/]*/>", "", value)
    value = re.sub(r"'{2,}", "", value)
    value = value.replace("}}", "")
    value = value.replace("&nbsp;", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ,|")


def wikidata_imdb_name_id(person_name: str) -> str | None:
    params = urllib.parse.urlencode({
        "action": "wbsearchentities",
        "search": person_name,
        "language": "en",
        "format": "json",
        "limit": 1,
    })
    try:
        search = get_json(f"https://www.wikidata.org/w/api.php?{params}", f"wd_search_{safe_name(person_name)}.json")
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            return None
        raise
    hits = search.get("search", [])
    if not hits:
        return None
    qid = hits[0].get("id")
    params = urllib.parse.urlencode({
        "action": "wbgetclaims",
        "entity": qid,
        "property": "P345",
        "format": "json",
    })
    try:
        claims = get_json(f"https://www.wikidata.org/w/api.php?{params}", f"wd_claim_{qid}.json")
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            return None
        raise
    for claim in claims.get("claims", {}).get("P345", []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, str) and value.startswith("nm"):
            return value
    return None


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:120]


def build_row(movie: dict) -> dict | None:
    if movie.get("Response") != "True" or movie.get("Type") != "movie":
        return None

    year = clean_int(movie.get("Year"))
    runtime = clean_int(movie.get("Runtime"))
    if year is None or year > 2024:
        return None
    if runtime is None or not 60 <= runtime <= 300:
        return None

    wiki_fields = {}
    page = wikipedia_search(movie["Title"], movie.get("Year"))
    if page:
        wiki_fields = wikipedia_infobox(page)

    actor_ids = []
    for actor in [a.strip() for a in movie.get("Actors", "").split(",")[:5] if a.strip() and a.strip() != "N/A"]:
        imdb_name_id = wikidata_imdb_name_id(actor)
        if imdb_name_id:
            actor_ids.append(imdb_name_id)

    budget = parse_money_to_millions(wiki_fields.get("budget"))
    box_office = parse_money_to_millions(wiki_fields.get("BoxOffice")) or parse_money_to_millions(movie.get("BoxOffice"))

    return {
        "tconst": movie.get("imdbID", ""),
        "primaryTitle": movie.get("Title", ""),
        "startYear": year,
        "genres": normalize_list_field(movie.get("Genre")),
        "lead_actors_ids": json.dumps(actor_ids, ensure_ascii=False),
        "runtimeMinutes": runtime,
        "averageRating": clean_float(movie.get("imdbRating")),
        "Language": wiki_fields.get("Language") or first_value(movie.get("Language")),
        "Country": wiki_fields.get("Country") or first_value(movie.get("Country")),
        "numVotes": clean_int(movie.get("imdbVotes")),
        "budget": budget,
        "BoxOffice": box_office,
        "plot": movie.get("Plot") if movie.get("Plot") != "N/A" else "",
    }


def first_value(value: str | None) -> str:
    if not value or value == "N/A":
        return ""
    return value.split(",")[0].strip()


def collect() -> list[dict]:
    rows = []
    seen = set()
    for imdb_id in MOVIE_IDS:
        if imdb_id in seen:
            continue
        seen.add(imdb_id)
        movie = omdb_movie(imdb_id)
        row = build_row(movie)
        if row:
            rows.append(row)
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
    return {"movie_count": total, "missing": stats}


def main() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    rows = collect()
    write_dataset(rows)
    stats = missing_stats(rows)
    write_json(REPORT_STATS_PATH, stats)
    print(f"Wrote {len(rows)} movies to {DATASET_PATH}")
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
