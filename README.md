# Movie Dataset Collection - Part 1

This folder contains the files for the first part of the movie data collection
assignment.

## Files

Required submission files:

- `notebook.ipynb` - notebook that runs the collection and report steps.
- `dataset.csv` - final cleaned dataset.
- `report.pdf` - short report with the collection decisions, fields and missing values.
- `README.md` - this file.

Support scripts in this folder are used by the notebook and for reproducible
local runs. They are not separate submission artifacts.

## How to run

Use Python 3.8 or newer.

```bash
cd movie-data-assignment-main
python3 collect_movies.py
python3 make_report.py
```

The collector downloads the free IMDb non-commercial TSV files from:

- `title.basics.tsv.gz`
- `title.ratings.tsv.gz`
- `title.principals.tsv.gz`

No paid IMDb API is used. The files are cached locally in `imdb_data/`.

Useful environment variables:

```bash
TEAM_LETTER=M TARGET_MOVIE_COUNT=10000 python3 collect_movies.py
```

The final `dataset.csv` contains 7,435 IMDb `M` movie rows with Wikipedia
enrichment. All rows have at least one Wikipedia-backed field, and 5,675 have
Language, Country and plot all present. The PDF report is regenerated directly
from `dataset.csv`, so its missing-value counts match the submitted CSV.

Because Wikipedia infoboxes are not fully standardized, a small number of
Wikipedia-derived values may contain extraction noise, especially in `Language`,
`Country`, `budget`, and `BoxOffice`. The report documents missing values
explicitly, and monetary values are stored in USD millions where parsed.

If Wikipedia rate-limits a long run, this command still creates the 10,000 IMDb
`M` rows and uses only existing cached Wikipedia data:

```bash
WIKIPEDIA_ENRICH_LIMIT=0 MAX_RATE_LIMIT_RETRIES=0 python3 collect_movies.py
```

For a long resumable Wikipedia enrichment run, use:

```bash
REQUEST_SLEEP_SECONDS=1 MAX_RATE_LIMIT_RETRIES=20 ENRICH_BATCH_SIZE=500 python3 enrich_wikipedia.py
```

The enrichment script writes checkpoints to `dataset.csv` and `report_stats.json`
after every 10 attempted rows by default, so it can be stopped and restarted.
It records Wikipedia rows with no usable fields in `wiki_enrich_state.json`, so
later runs skip known misses instead of retrying them forever.

For strict lecturer-clarification mode, where rows without English Wikipedia
data are skipped:

```bash
REQUIRE_WIKIPEDIA_PAGE=1 TARGET_MOVIE_COUNT=5000 python3 collect_movies.py
```

## Collection Method

Our assigned team letter is `M`, so the script starts from IMDb movies whose
`primaryTitle` starts with `M`. It keeps only `titleType == movie`, release years
up to 2024, and runtimes between 60 and 300 minutes.

IMDb TSV files provide the required IMDb id, title, year, genres, runtime,
rating, vote count and leading actor ids. The script then searches English
Wikipedia pages to collect budget, box office, language, country and plot where
available. Monetary columns are saved in USD millions.
