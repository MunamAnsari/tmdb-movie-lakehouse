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

## Phase 2: Bronze and Silver Data Contracts

These are the planned contracts for the Phase 2 PySpark pipeline. They describe the tables the pipeline will implement; documentation alone does not mean the tables or transformations are already implemented.

**Planned Databricks namespace:** `workspace.tmdb_lakehouse`  
The pipeline must confirm that this schema exists and that the user has permission to write to it before creating tables.

### General contract rules

- Define input schemas explicitly with PySpark `StructType` and `StructField`; do not use Spark schema inference.
- Every Bronze, Silver, operational, and quarantine table includes a `load_timestamp` (`TimestampType`).
- Keys below are logical keys enforced by pipeline logic; they are not assumed to be enforced by Databricks.
- Preserve source lineage using `source_batch_id`, `source_file`, and `source_extracted_at` in Silver tables.
- Use `source_extracted_at` to select the newest source snapshot. Do not use processing time to decide which snapshot wins, because a backfill processed today may contain older data.
- Reuse the same `batch_id` and `source_extracted_at` when retrying the same input.
- Calculate row_hash from stable business fields, excluding load_timestamp and source metadata (source_batch_id, source_file, source_extracted_at). Update only materially changed rows; unchanged rows retain their existing load_timestamp.
- Keep unreleased movies in Silver with their TMDB `status`. Apply any release-status filtering in a later analytics layer.

### Bronze table

**Table:** `workspace.tmdb_lakehouse.bronze_movie_records`  
**Grain:** One row per movie per input batch  
**Logical key:** (`batch_id`, `movie_id`)

| Column | PySpark type | Description |
|---|---|---|
| `movie_id` | `LongType` | TMDB movie identifier; required |
| `load_type` | `StringType` | `full` or `incremental` |
| `batch_id` | `StringType` | Stable identifier for the input batch; required |
| `source_file` | `StringType` | Source file path or name |
| `source_extracted_at` | `TimestampType` | Time represented by the source snapshot |
| `window_start` | `DateType` | Incremental window start; nullable for full loads |
| `window_end` | `DateType` | Incremental window end; nullable for full loads |
| `ingestion_date` | `DateType` | Ingestion date; may be used for partitioning |
| `load_timestamp` | `TimestampType` | Time this record was first written to Bronze |
| `raw_payload` | `StringType` | Original JSON for the movie record |

The existing sample files do not provide a reliable per-record extraction timestamp. The pipeline must receive `source_extracted_at` as a run parameter for those files; it must not substitute the current processing time. Bronze ingestion must deduplicate its source on (`batch_id`, `movie_id`) before merging.

### Silver movie table

**Table:** `workspace.tmdb_lakehouse.silver_movies_cleansed`  
**Grain:** One current cleaned record per movie  
**Logical key:** `movie_id`

| Column | PySpark type | Description |
|---|---|---|
| `movie_id` | `LongType` | TMDB movie identifier; required |
| `imdb_id` | `StringType` | IMDb identifier |
| `title` | `StringType` | Movie title; required |
| `original_title` | `StringType` | Original title |
| `overview` | `StringType` | Movie description |
| `original_language` | `StringType` | Original language code |
| `release_date` | `DateType` | Parsed release date |
| `runtime` | `IntegerType` | Runtime in minutes |
| `status` | `StringType` | TMDB status; retain unreleased movies |
| `adult` | `BooleanType` | Adult-content flag |
| `video` | `BooleanType` | Video flag |
| `popularity` | `DoubleType` | TMDB popularity score |
| `vote_average` | `DoubleType` | Average vote |
| `vote_count` | `LongType` | Number of votes |
| `budget` | `LongType` | Budget; zero and negative values become null |
| `revenue` | `LongType` | Revenue; zero and negative values become null |
| `profit` | `LongType` | Revenue minus budget, only when both are greater than zero |
| `roi` | `DoubleType` | (revenue - budget) / budget, only when both are greater than zero |
| `budget_tier` | `StringType` | `low` below $10M nominal USD; `medium` from $10M to below $100M; `high` at or above $100M; null if budget is unknown |
| `source_batch_id` | `StringType` | Batch that supplied the current movie state |
| `source_file` | `StringType` | Source file for lineage |
| `source_extracted_at` | `TimestampType` | Source snapshot time |
| `row_hash` | `StringType` | Hash of stable business columns for change detection |
| `load_timestamp` | `TimestampType` | Time this Silver row was created or materially updated |

TMDB uses zero to represent unknown budget or revenue. The pipeline converts zero and negative amounts to null so unknown revenue is not incorrectly treated as a real loss.

### Silver child tables

Each child table has the business columns shown below, plus `source_batch_id StringType`, `source_file StringType`, `source_extracted_at TimestampType`, `row_hash StringType`, and `load_timestamp TimestampType`.

| Table | Grain and logical key | Business columns |
|---|---|---|
| `silver_movie_genres` | One row per movie–genre; (`movie_id`, `genre_id`) | `movie_id LongType`, `genre_id LongType`, `genre_name StringType` |
| `silver_movie_studios` | One row per movie–production company; (`movie_id`, `company_id`) | `movie_id LongType`, `company_id LongType`, `company_name StringType`, `origin_country StringType` |
| `silver_movie_cast` | One row per movie–cast credit; (`movie_id`, `credit_id`) | `movie_id LongType`, `credit_id StringType`, `person_id LongType`, `name StringType`, `character StringType`, `cast_order IntegerType` |
| `silver_movie_keywords` | One row per movie–keyword; (`movie_id`, `keyword_id`) | `movie_id LongType`, `keyword_id LongType`, `keyword_name StringType` |

For a successfully processed movie snapshot, each child collection replaces that movie’s previous child set. Remove stale child rows only for movie IDs successfully processed from a complete, valid collection. A valid empty collection means there are no child rows; a missing or malformed collection must not be treated as empty. Apply the replacement only when the incoming source_extracted_at is newer than the stored snapshot, or when processing a retry of the same batch_id. Cast entries missing `credit_id` are quarantined.

### Silver reviews table

**Table:** `workspace.tmdb_lakehouse.silver_reviews_cleansed`  
**Grain:** One row per movie–review  
**Logical key:** (`movie_id`, `review_id`)

| Column | PySpark type | Description |
|---|---|---|
| `movie_id` | `LongType` | TMDB movie identifier |
| `review_id` | `StringType` | TMDB review identifier |
| `reviewer_hash_id` | `StringType` | Salted SHA-256 hash of `author` or `author_details.username` |
| `rating` | `DoubleType` | Review rating, if provided |
| `content` | `StringType` | Review text after email and URL scrubbing |
| `created_at` | `TimestampType` | Review creation time, explicitly parsed from ISO input |
| `updated_at` | `TimestampType` | Review update time, explicitly parsed from ISO input |
| `weeks_since_release` | `IntegerType` | Whole seven-day periods between release and review dates; null if either date is missing or review predates release |
| `source_batch_id` | `StringType` | Source batch |
| `source_file` | `StringType` | Source file for lineage |
| `source_extracted_at` | `TimestampType` | Source snapshot time |
| `row_hash` | `StringType` | Hash of stable review fields for change detection |
| `load_timestamp` | `TimestampType` | Time this Silver row was created or materially updated |

The salt is stored in a Databricks secret and is never committed to GitHub. Silver does not retain the original username, `author_details.name`, or `avatar_path`. Bronze retains the original payload for traceability and may therefore contain these fields; access to Bronze must be controlled accordingly.
If both author and author_details.username are missing, set reviewer_hash_id to NULL; do not hash an empty or missing value. Keep the review unless another required field is invalid.

### Operational tables

**Execution log table:** `workspace.tmdb_lakehouse.pipeline_execution_logs`  
**Grain:** One row per pipeline execution  
**Logical key:** `run_id`

| Column | PySpark type | Description |
|---|---|---|
| `run_id` | `StringType` | Unique identifier for one execution |
| `layer` | `StringType` | Processing step, such as `Raw-to-Bronze` or `Bronze-to-Silver` |
| `load_type` | `StringType` | `full` or `incremental` |
| `input_parameter` | `StringType` | Input path, folder, or backfill parameter |
| `batch_id` | `StringType` | Input batch identifier |
| `start_time` | `TimestampType` | Execution start time |
| `end_time` | `TimestampType` | Execution end time |
| `status` | `StringType` | `SUCCESS` or `FAILURE` |
| `rows_inserted` | `LongType` | Number of inserted rows |
| `rows_updated` | `LongType` | Number of updated rows |
| `rows_quarantined` | `LongType` | Number of quarantined rows |
| `error_details` | `StringType` | Failure details; null on success |
| `load_timestamp` | `TimestampType` | Time the log record was written |

**Quarantine table:** `workspace.tmdb_lakehouse.quarantined_movie_records`  
**Grain:** One row per rejected record  
**Logical key:** `quarantine_record_id`

| Column | PySpark type | Description |
|---|---|---|
| `quarantine_record_id` | `StringType` | Unique identifier for the quarantine entry |
| `batch_id` | `StringType` | Source batch |
| `movie_id` | `LongType` | Movie identifier; nullable if missing or invalid |
| `source_file` | `StringType` | Source file |
| `reason` | `StringType` | Validation or parsing failure reason |
| `raw_payload` | `StringType` | Original record where available |
| `load_timestamp` | `TimestampType` | Time the record was quarantined |

### Run parameters and backfills

Both pipeline steps accept parameters instead of hardcoded “today” paths:

- Input folder or file path
- `load_type`
- `batch_id`
- `source_extracted_at`
- Optional incremental `window_start` and `window_end`

A backfill supplies the historical input path and its original source extraction time. Retrying the same input reuses its `batch_id` and `source_extracted_at`.

### Schema drift approach

Bronze preserves each original movie JSON record so unrecognized source fields are not lost. The pipeline will test an explicit wrapper schema with `records` declared as `ArrayType(StringType())` against the actual input before relying on it. An explicit movie `StructType` is applied when parsing records for Silver; malformed or non-conforming records are quarantined so a bad record does not fail the whole batch.