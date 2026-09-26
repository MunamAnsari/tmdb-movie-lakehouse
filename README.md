# TMDB Movie Analytics Lakehouse

End-to-end Medallion Architecture (Bronze → Silver → Gold) pipeline built on
Apache Spark, ingesting movie catalog data from **The Movie Database (TMDB) API**.

## Team
- Munam Ansari 
- Sanan Zahid 

## Project Domain
Entertainment / media analytics — tracking movie budgets, revenue, ratings,
genres, and production companies to answer business questions like
"which genres deliver the best ROI?" and "how has average movie budget
changed over the last decade?"

## Data Source
- **TMDB API v3** — https://developer.themoviedb.org/reference/intro/getting-started
- **Full Load:** `/discover/movie` (paginated, sorted by popularity), enriched
  per-movie with `/movie/{id}?append_to_response=credits,keywords,reviews`
- **Incremental Load:** `/movie/changes` (returns movie IDs changed in a
  configurable date window), re-fetched via `/movie/{id}`

## Sample Data & Volume (actual run results)
- **Full load sample:** 200 movies (10 discover pages), enriched with credits/keywords/reviews
- **Incremental load sample:** found 35,585 movies changed in the last 7 days;
  sample capped to the first 300 for repo size, with 2 IDs skipped (deleted
  between listing and fetch — handled gracefully)
- At full production scale, a 7-day incremental window returning ~35K changed
  IDs shows this pipeline needs to handle non-trivial daily volume once deployed

## Repo Structure
├── data/
│ ├── full_load/
│ └── incremental_load/
├── scripts/
│ ├── fetch_full_load.py
│ ├── fetch_incremental_load.py
│ └── requirements.txt
├── notebooks/
│ └── tmdb_data_fetch.ipynb
└── docs/
└── phase1_proposal.pdf


## Security & PII Note
TMDB movie metadata (titles, cast/crew, genres, financials) is public
catalog data and does not constitute PII. The one exception is the
`author_username` field inside review sub-resources, which we mask before
the Silver layer as a precaution. **API keys are never committed** — the
notebook reads the key from Colab Secrets at runtime rather than storing
it in code.

## Medallion Architecture (planned)
- **Bronze:** Raw JSON as returned by TMDB, partitioned by `load_type` and `ingestion_date`.
- **Silver:** Flattened genre/cast/keyword arrays, typed columns, deduped on `movie_id`, reviewer usernames masked.
- **Gold:** Star schema — `fact_movie_performance` joined with `dim_genre`, `dim_production_company`, `dim_date`.

## Infrastructure / FinOps
Built and tested on **Databricks Community Edition** (free tier). Sample
sizes capped intentionally (200 full-load records, 300 incremental records)
to stay well within free compute/storage limits during pipeline testing.
