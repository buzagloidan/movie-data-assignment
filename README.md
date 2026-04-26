# Movie Dataset Collection - Part 1

This folder contains my files for the first part of the movie data collection
assignment.

## Files

- `notebook.ipynb` - notebook that runs the collection and report steps.
- `collect_movies.py` - Python script used by the notebook.
- `dataset.csv` - final cleaned dataset.
- `report.pdf` - short report with the collection decisions, fields and missing values.
- `report_stats.json` - the missing-value statistics used for the report.

## How to run

Use Python 3.8 or newer.

```bash
cd movie_data_assignment
python3 collect_movies.py
python3 make_report.py
```

The OMDb API key supplied for the assignment is used by default. To override it:

```bash
OMDB_API_KEY=your_key_here python3 collect_movies.py
```

## Collection Method

We started from a list of IMDb movie identifiers for movies released up to 2024.
For each movie, the script gets the main fields from OMDb, then looks for the
matching Wikipedia page to collect budget, box office, language and country when
they are available. Actor names from OMDb are matched to IMDb name identifiers
with Wikidata. The final CSV keeps only movies with runtime between 60 and 300
minutes, and the money columns are saved in USD millions.
