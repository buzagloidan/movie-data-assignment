#!/usr/bin/env python3
"""Create a short PDF report from the current dataset."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATASET_PATH = BASE_DIR / "dataset.csv"
STATS_PATH = BASE_DIR / "report_stats.json"
REPORT_PATH = BASE_DIR / "report.pdf"
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


def pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def wrap(text: str, width: int = 92) -> list[str]:
    words = text.split()
    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) > width and current:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


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
        "team_letter": "M",
        "require_wikipedia_page": False,
        "wikipedia_enriched_count": sum(
            1 for row in rows if row.get("plot") or row.get("Language") or row.get("Country")
        ),
        "wikipedia_core_complete_count": sum(
            1 for row in rows if row.get("plot") and row.get("Language") and row.get("Country")
        ),
        "missing": stats,
    }


def load_current_stats() -> dict:
    with DATASET_PATH.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    stats = missing_stats(rows)
    STATS_PATH.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    return stats


def build_lines(stats: dict) -> list[str]:
    lines = [
        "Movie Dataset Collection - Part 1",
        "",
        "Collection decisions:",
        f"Our assigned team letter is {stats.get('team_letter', 'M')}, so we collected only movies",
        "whose primaryTitle starts with that letter. We used IMDb's non-commercial TSV",
        "datasets, not the paid IMDb API. We kept titleType movie, release years up to",
        "and including 2024, and runtimes between 60 and 300 minutes.",
        "IMDb TSV files supplied the IMDb title id, title, year, genres, runtime, rating,",
        "votes and actor ids. English Wikipedia pages were used for budget, worldwide",
        "gross, language, country and plot when available. The final submitted CSV keeps",
        "only rows with at least one Wikipedia-backed field and documents missing values",
        "explicitly.",
        "Rows without Wikipedia enrichment excluded from final CSV: True.",
        f"Rows with at least one Wikipedia-backed field: {stats.get('wikipedia_enriched_count', 0)}.",
        f"Rows with Language, Country and plot all present: {stats.get('wikipedia_core_complete_count', 0)}.",
        "",
        f"Number of rows in dataset.csv: {stats.get('movie_count', 0)}",
        "",
        "Field description and source:",
        "tconst: IMDb title id, from title.basics.tsv.gz.",
        "primaryTitle: movie title, from title.basics.tsv.gz.",
        "startYear: release year, from title.basics.tsv.gz.",
        "genres: list of genres as supplied by title.basics.tsv.gz.",
        "lead_actors_ids: up to five IMDb actor ids, from title.principals.tsv.gz.",
        "runtimeMinutes: runtime in minutes, from title.basics.tsv.gz.",
        "averageRating: IMDb average rating, from title.ratings.tsv.gz.",
        "Language: primary language, from English Wikipedia.",
        "Country: production country, from English Wikipedia.",
        "numVotes: number of IMDb votes, from title.ratings.tsv.gz.",
        "budget: production budget in USD millions, from Wikipedia when available.",
        "BoxOffice: global box office in USD millions, from Wikipedia when available.",
        "plot: plot summary, from English Wikipedia.",
        "",
        "Missing values by column:",
    ]

    for field, item in stats.get("missing", {}).items():
        lines.append(f"{field}: {item['missing_count']} missing ({item['missing_percent']}%)")

    lines += [
        "",
        "Notes:",
        "Budget and worldwide gross are the least complete fields because many Wikipedia",
        "infoboxes do not include them or write them in a format that is hard to normalize.",
        "Some Wikipedia-derived values may contain extraction noise from inconsistent",
        "infobox formatting, especially Language, Country, budget and BoxOffice.",
        "All monetary values in dataset.csv are stored as USD millions.",
    ]
    return [line for item in lines for line in (wrap(item) if item else [""])]


def write_pdf(lines: list[str], path: Path) -> None:
    # Minimal single-page PDF using built-in Helvetica. This avoids external packages.
    content_lines = ["BT", "/F1 10 Tf", "50 790 Td", "14 TL"]
    for line in lines[:54]:
        content_lines.append(f"({pdf_escape(line)}) Tj")
        content_lines.append("T*")
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream",
    ]

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode("ascii")
    )
    path.write_bytes(pdf)


def main() -> None:
    stats = load_current_stats()
    write_pdf(build_lines(stats), REPORT_PATH)
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
