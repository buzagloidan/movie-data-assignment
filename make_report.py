#!/usr/bin/env python3
"""Create a short PDF report from the dataset statistics."""

from __future__ import annotations

import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
STATS_PATH = BASE_DIR / "report_stats.json"
REPORT_PATH = BASE_DIR / "report.pdf"


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


def build_lines(stats: dict) -> list[str]:
    lines = [
        "Movie Dataset Collection - Part 1",
        "",
        "Collection decisions:",
        "We collected movies only, with release years up to and including 2024.",
        "We kept movies with runtime between 60 and 300 minutes. OMDb supplied the IMDb",
        "title id, title, year, genres, runtime, rating, votes and plot. Wikipedia infobox",
        "pages were used for budget, worldwide gross, language and country when available.",
        "Wikidata was used to match the leading actor names to IMDb name identifiers.",
        "",
        f"Number of movies collected: {stats.get('movie_count', 0)}",
        "",
        "Field description and source:",
        "tconst: IMDb title id, from OMDb/IMDb.",
        "primaryTitle: movie title, from OMDb.",
        "startYear: release year, from OMDb.",
        "genres: list of genres as supplied by OMDb.",
        "lead_actors_ids: up to five IMDb actor ids, matched from OMDb actor names with Wikidata.",
        "runtimeMinutes: runtime in minutes, from OMDb.",
        "averageRating: IMDb average rating, from OMDb.",
        "Language: primary language, Wikipedia first, OMDb fallback.",
        "Country: production country, Wikipedia first, OMDb fallback.",
        "numVotes: number of IMDb votes, from OMDb.",
        "budget: production budget in USD millions, from Wikipedia when available.",
        "BoxOffice: global box office in USD millions, Wikipedia first, OMDb fallback.",
        "plot: short plot summary, from OMDb.",
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
    stats = json.loads(STATS_PATH.read_text(encoding="utf-8"))
    write_pdf(build_lines(stats), REPORT_PATH)
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
