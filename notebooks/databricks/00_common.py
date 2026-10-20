# Databricks notebook source
# MAGIC %md
# MAGIC # 00_common: shared schemas, helpers and pipeline functions
# MAGIC Run by the other notebooks with `%run ./00_common`. Nothing executes here except definitions.
# MAGIC
# MAGIC * **Explicit schemas** (`StructType`/`StructField`) for the raw wrapper, the movie record, every Bronze/Silver/ops table. No schema inference anywhere.
# MAGIC * **Idempotent** loads via `MERGE INTO` with a row-hash change check and a "newest source wins" guard.
# MAGIC * **Parameterised**: every pipeline function takes load type / file pattern / date window / reprocess flag / source folder.
# MAGIC * **Drift handling**: new top-level fields evolve the Bronze table; type-violating records are quarantined, not fatal.
# MAGIC * **Audit**: one row per file (and per target table) in `pipeline_execution_logs`.

# COMMAND ----------

import os, re, glob, uuid, fnmatch
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, MapType,
    IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration (overridden by `configure()` from the notebook widgets)

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "tmdb_raw"

BRONZE = "bronze_movies"
SILVER_MOVIES = "silver_movies_cleansed"
SILVER_GENRES = "silver_movie_genres"
SILVER_STUDIOS = "silver_movie_studios"
SILVER_REVIEWS = "silver_reviews_cleansed"
LOGS = "pipeline_execution_logs"
QUARANTINE = "quarantine_records"

L_BRONZE = "Raw-to-Bronze"
L_SILVER = "Bronze-to-Silver"

FOLDERS = {"full": "full_load", "incremental": "incremental_load"}
DEV_SALT = "dev-only-salt-change-me"


def configure(catalog=None, schema=None, volume=None):
    """Override the default catalog / schema / volume (called by the runner notebooks)."""
    global CATALOG, SCHEMA, VOLUME
    CATALOG = catalog or CATALOG
    SCHEMA = schema or SCHEMA
    VOLUME = volume or VOLUME


def tbl(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


def volume_root():
    return f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explicit schemas
# MAGIC **Bronze keeps every source scalar as STRING** (faithful to the JSON text). Spark's JSON parser converts
# MAGIC numbers/booleans/objects to their text form for StringType fields, so a type change at the source
# MAGIC (e.g. `budget` becoming `"N/A"`) can never corrupt or crash the Bronze read.
# MAGIC Casting to real types happens in Silver, where violations are quarantined.

# COMMAND ----------

STR = StringType()


def _s(*names):
    return [StructField(n, STR, True) for n in names]


GENRE_T = StructType(_s("id", "name"))
COMPANY_T = StructType(_s("id", "name", "logo_path", "origin_country"))
CAST_T = StructType(_s("id", "name", "character", "known_for_department"))
CREW_T = StructType(_s("id", "name", "job", "department"))
KEYWORD_T = StructType(_s("id", "name"))
REVIEW_DETAILS_T = StructType(_s("name", "username", "avatar_path", "rating"))
REVIEW_T = StructType(
    _s("author")
    + [StructField("author_details", REVIEW_DETAILS_T, True)]
    + _s("content", "created_at", "id", "updated_at", "url")
)

# Contract for one TMDB movie record (/movie/{id}?append_to_response=credits,keywords,reviews)
MOVIE_SCHEMA = StructType(
    _s("id", "imdb_id", "title", "original_title", "original_language", "overview", "tagline",
       "status", "homepage", "release_date", "runtime", "budget", "revenue", "popularity",
       "vote_average", "vote_count", "adult", "video", "poster_path", "backdrop_path",
       "belongs_to_collection", "production_countries", "spoken_languages", "origin_country")
    + [
        StructField("genres", ArrayType(GENRE_T), True),
        StructField("production_companies", ArrayType(COMPANY_T), True),
        StructField("credits", StructType([
            StructField("cast", ArrayType(CAST_T), True),
            StructField("crew", ArrayType(CREW_T), True),
        ]), True),
        StructField("keywords", StructType([
            StructField("keywords", ArrayType(KEYWORD_T), True),
        ]), True),
        StructField("reviews", StructType([
            StructField("results", ArrayType(REVIEW_T), True),
            StructField("total_results", STR, True),
        ]), True),
    ]
)

# Raw file wrapper written by the fetch step: {load_type, ..., records: [ {movie}, ... ]}
# `records` is read as ARRAY<STRING>: each movie arrives as its raw JSON text (parsed in the next step).
WRAPPER_SCHEMA = StructType(
    _s("load_type", "window_start", "window_end", "total_changed_in_window",
       "ingestion_timestamp_utc", "record_count")
    + [StructField("records", ArrayType(STR), True)]
)

MAP_SS = MapType(StringType(), StringType())

# ---- Bronze table ----
_movie_cols_wo_id = [f for f in MOVIE_SCHEMA.fields if f.name != "id"]
BRONZE_SCHEMA = StructType(
    [StructField("id", STR, False)]
    + _movie_cols_wo_id
    + [
        StructField("_source_file", STR, False),
        StructField("_load_type", STR, False),
        StructField("_ingestion_date", DateType(), False),
        StructField("_source_extracted_at", TimestampType(), False),
        StructField("_record_index", IntegerType(), True),
        StructField("_record_hash", STR, True),
        StructField("_batch_id", STR, True),
        StructField("load_timestamp", TimestampType(), False),
        StructField("_raw_json", STR, True),
    ]
)

# ---- Silver tables ----
def _silver_meta():
    return [
        StructField("source_extracted_at", TimestampType(), True),
        StructField("row_hash", STR, True),
        StructField("batch_id", STR, True),
        StructField("load_timestamp", TimestampType(), False),
    ]


SILVER_MOVIES_SCHEMA = StructType([
    StructField("movie_id", LongType(), False),
    StructField("title", STR, False),
    StructField("original_title", STR, True),
    StructField("original_language", STR, True),
    StructField("status", STR, True),
    StructField("overview", STR, True),
    StructField("release_date", DateType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("budget", DoubleType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("profit", DoubleType(), True),
    StructField("roi", DoubleType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", LongType(), True),
    StructField("adult", BooleanType(), True),
    StructField("source_load_type", STR, True),
    StructField("source_file", STR, True),
] + _silver_meta())

SILVER_GENRES_SCHEMA = StructType([
    StructField("movie_id", LongType(), False),
    StructField("genre_id", IntegerType(), False),
    StructField("genre_name", STR, True),
] + _silver_meta())

SILVER_STUDIOS_SCHEMA = StructType([
    StructField("movie_id", LongType(), False),
    StructField("company_id", LongType(), False),
    StructField("company_name", STR, True),
    StructField("origin_country", STR, True),
] + _silver_meta())

SILVER_REVIEWS_SCHEMA = StructType([
    StructField("movie_id", LongType(), False),
    StructField("review_id", STR, False),
    StructField("reviewer_hash_id", STR, True),
    StructField("rating", DoubleType(), True),
    StructField("review_created_at", TimestampType(), True),
    StructField("content_scrubbed", STR, True),
] + _silver_meta())

# ---- Operational tables ----
LOG_SCHEMA = StructType([
    StructField("run_id", STR, True),
    StructField("layer", STR, True),
    StructField("load_type", STR, True),
    StructField("target_table", STR, True),
    StructField("parameter_processed", STR, True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("status", STR, True),
    StructField("rows_read", LongType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_deleted", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("notes", STR, True),
    StructField("error_message", STR, True),
    StructField("load_timestamp", TimestampType(), True),
])

QUARANTINE_SCHEMA = StructType([
    StructField("layer", STR, True),
    StructField("source_file", STR, True),
    StructField("record_key", STR, True),
    StructField("reason", STR, True),
    StructField("raw_json", STR, True),
    StructField("batch_id", STR, True),
    StructField("load_timestamp", TimestampType(), True),
])

# name -> (schema, partition columns, primary key, comment)
TABLE_SPECS = {
    BRONZE: (BRONZE_SCHEMA, ["_load_type", "_ingestion_date"], ["id", "_source_file"],
             "Bronze: raw TMDB movie records, all source scalars as STRING"),
    SILVER_MOVIES: (SILVER_MOVIES_SCHEMA, ["release_year"], ["movie_id"],
                    "Silver: cleansed, typed, deduplicated movies"),
    SILVER_GENRES: (SILVER_GENRES_SCHEMA, None, ["movie_id", "genre_id"],
                    "Silver: movie to genre bridge"),
    SILVER_STUDIOS: (SILVER_STUDIOS_SCHEMA, None, ["movie_id", "company_id"],
                     "Silver: movie to production company bridge"),
    SILVER_REVIEWS: (SILVER_REVIEWS_SCHEMA, None, ["movie_id", "review_id"],
                     "Silver: reviews with hashed reviewer and scrubbed text"),
    LOGS: (LOG_SCHEMA, None, None, "Ops: one row per file/table processed per run"),
    QUARANTINE: (QUARANTINE_SCHEMA, None, None, "Ops: records rejected by data contracts"),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DDL: tables are created from the StructTypes above (single source of truth)

# COMMAND ----------

def ddl_type(dt):
    """StructType/DataType -> Spark SQL DDL string, with backticked nested field names."""
    if isinstance(dt, StructType):
        return "STRUCT<" + ",".join("`%s`:%s" % (f.name, ddl_type(f.dataType)) for f in dt.fields) + ">"
    if isinstance(dt, ArrayType):
        return "ARRAY<%s>" % ddl_type(dt.elementType)
    return dt.simpleString().upper()


def ensure_tables():
    """Create the volume and every Delta table if missing (idempotent)."""
    spark.sql("CREATE VOLUME IF NOT EXISTS %s.%s.%s" % (CATALOG, SCHEMA, VOLUME))
    for name, (schema, parts, pk, comment) in TABLE_SPECS.items():
        full = tbl(name)
        if spark.catalog.tableExists(full):
            continue
        cols = ", ".join(
            "`%s` %s%s" % (f.name, ddl_type(f.dataType), "" if f.nullable else " NOT NULL")
            for f in schema.fields
        )
        part = (" PARTITIONED BY (%s)" % ", ".join("`%s`" % c for c in parts)) if parts else ""
        spark.sql("CREATE TABLE %s (%s) USING DELTA%s COMMENT '%s'" % (full, cols, part, comment))
        if pk:  # informational primary key (documentation / optimizer hint); uniqueness is enforced by MERGE
            try:
                spark.sql("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)"
                          % (full, name, ", ".join("`%s`" % c for c in pk)))
            except Exception as e:
                print("note: informational PK not added on %s (%s)" % (name, type(e).__name__))
        print("created", full)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit logging, file discovery, quarantine, MERGE helpers

# COMMAND ----------

def now_utc():
    return datetime.now(timezone.utc)


def log_event(run_id, layer, load_type, target, parameter, start, end, status,
              rows_read=0, inserted=0, updated=0, deleted=0, quarantined=0, notes=None, error=None):
    """Append one audit row to pipeline_execution_logs."""
    row = (
        run_id, layer, load_type, target, parameter, start, end,
        float((end - start).total_seconds()), status,
        int(rows_read), int(inserted), int(updated), int(deleted), int(quarantined),
        notes, (str(error)[:2000] if error else None), now_utc(),
    )
    spark.createDataFrame([row], LOG_SCHEMA).write.mode("append").saveAsTable(tbl(LOGS))


def log_skip(run_id, layer, load_type, target, parameter, reason):
    """Every run leaves an audit row, even when nothing needed processing."""
    t = now_utc()
    log_event(run_id, layer, load_type, target, parameter, t, t, "SKIPPED", notes=reason)


def already_succeeded(layer, target, parameter):
    return (
        spark.table(tbl(LOGS))
        .where((F.col("layer") == layer) & (F.col("target_table") == target)
               & (F.col("parameter_processed") == parameter) & (F.col("status") == "SUCCESS"))
        .limit(1).count() > 0
    )


def run_step(run_id, layer, load_type, target, parameter, fn):
    """Run fn() -> dict of stats, writing a SUCCESS or FAILURE audit row. Never raises."""
    start = now_utc()
    try:
        stats = fn() or {}
        log_event(run_id, layer, load_type, target, parameter, start, now_utc(), "SUCCESS",
                  stats.get("rows_read", 0), stats.get("inserted", 0), stats.get("updated", 0),
                  stats.get("deleted", 0), stats.get("quarantined", 0), stats.get("notes"))
        print("  OK   %-26s read=%s ins=%s upd=%s del=%s quar=%s %s" % (
            target, stats.get("rows_read", 0), stats.get("inserted", 0), stats.get("updated", 0),
            stats.get("deleted", 0), stats.get("quarantined", 0), stats.get("notes") or ""))
        return True
    except Exception as e:
        log_event(run_id, layer, load_type, target, parameter, start, now_utc(), "FAILURE", error=e)
        print("  FAIL %-26s %s" % (target, str(e)[:300]))
        return False


_TS_RE = re.compile(r"(\d{8})_(\d{6})")


def file_extracted_at(path):
    """Extraction time from the file name (..._YYYYMMDD_HHMMSS...), else the file's modified time."""
    m = _TS_RE.search(os.path.basename(path))
    if m:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)


def parse_date(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date() if s and s.strip() else None


def list_raw_files(load_type="all", file_pattern="*.json", date_from="", date_to="", source_root=""):
    """Raw files to process, oldest first. Parameters make backfills a one-line change."""
    root = source_root.strip() if source_root and source_root.strip() else volume_root()
    d_from, d_to = parse_date(date_from), parse_date(date_to)
    types = ["full", "incremental"] if load_type == "all" else [load_type]
    out = []
    for lt in types:
        for p in sorted(glob.glob(os.path.join(root, FOLDERS[lt], file_pattern))):
            ts = file_extracted_at(p)
            if d_from and ts.date() < d_from:
                continue
            if d_to and ts.date() > d_to:
                continue
            out.append((lt, p, ts))
    out.sort(key=lambda x: x[2])
    return out


def _sql_str(s):
    return str(s).replace("'", "''")


def write_quarantine(df, layer, source_file):
    """Replace this file's quarantined rows for the layer (keeps reruns idempotent)."""
    spark.sql("DELETE FROM %s WHERE layer = '%s' AND source_file = '%s'"
              % (tbl(QUARANTINE), _sql_str(layer), _sql_str(source_file)))
    df.write.mode("append").saveAsTable(tbl(QUARANTINE))


def align_to_target(df, table_name):
    """Select/cast df to the target table's exact column list; missing columns become NULL."""
    out = []
    for f in spark.table(tbl(table_name)).schema.fields:
        if f.name in df.columns:
            out.append(F.col("`%s`" % f.name).cast(f.dataType).alias(f.name))
        else:
            out.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(*out)


def merge_into(table_name, src_df, keys, update_condition, has_op=False):
    """
    Upsert src_df into a Delta table with MERGE INTO.
      * matched + changed (update_condition)  -> UPDATE
      * not matched                           -> INSERT
      * has_op: source rows carry _op = 'U' (upsert) or 'D' (delete stale child row)
    Returns {inserted, updated, deleted} from the MERGE result metrics.
    """
    target = tbl(table_name)
    cols = [f.name for f in spark.table(target).schema.fields]
    view = "_src_" + uuid.uuid4().hex[:12]
    src_df.createOrReplaceTempView(view)
    on = " AND ".join("t.`%s` = s.`%s`" % (k, k) for k in keys)
    set_clause = ", ".join("t.`%s` = s.`%s`" % (c, c) for c in cols)
    ins_cols = ", ".join("`%s`" % c for c in cols)
    ins_vals = ", ".join("s.`%s`" % c for c in cols)
    if has_op:
        clauses = [
            "WHEN MATCHED AND s.`_op` = 'D' THEN DELETE",
            "WHEN MATCHED AND s.`_op` = 'U' AND (%s) THEN UPDATE SET %s" % (update_condition, set_clause),
            "WHEN NOT MATCHED AND s.`_op` = 'U' THEN INSERT (%s) VALUES (%s)" % (ins_cols, ins_vals),
        ]
    else:
        clauses = [
            "WHEN MATCHED AND (%s) THEN UPDATE SET %s" % (update_condition, set_clause),
            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (ins_cols, ins_vals),
        ]
    sql = "MERGE INTO %s AS t USING %s AS s ON %s %s" % (target, view, on, " ".join(clauses))
    res = spark.sql(sql).collect()
    spark.catalog.dropTempView(view)
    d = res[0].asDict() if res else {}
    return {
        "inserted": int(d.get("num_inserted_rows") or 0),
        "updated": int(d.get("num_updated_rows") or 0),
        "deleted": int(d.get("num_deleted_rows") or 0),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Raw to Bronze

# COMMAND ----------

def read_raw_records(path):
    """Explicit-schema, FAILFAST read of one raw file -> one row per movie (raw JSON text)."""
    raw = (spark.read.schema(WRAPPER_SCHEMA)
           .option("multiLine", True).option("mode", "FAILFAST").json(path))
    return raw.select(F.posexplode("records").alias("_record_index", "_raw_json"))


NESTED_FIELDS = ["genres", "production_companies", "credits", "keywords", "reviews"]


def valid_id_expr():
    return F.coalesce(F.col("_m.id").isNotNull() & (F.trim(F.col("_m.id")) != ""), F.lit(False))


def parse_records(recs):
    """Parse each raw JSON record with the explicit schema and flag contract violations (_violations)."""
    df = (recs.withColumn("_m", F.from_json("_raw_json", MOVIE_SCHEMA))
              .withColumn("_map", F.from_json("_raw_json", MAP_SS)))
    checks = [F.when(~valid_id_expr(), F.lit("missing_or_null_id"))]
    # nested type drift: the key is present with a value, but Spark could not parse it into the declared structure
    checks += [F.when(F.col("_map").getItem(f).isNotNull() & F.col("_m." + f).isNull(),
                      F.lit("nested_type_drift:" + f)) for f in NESTED_FIELDS]
    return df.withColumn("_violations", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def valid_row_expr():
    return F.size(F.col("_violations")) == 0


def detect_new_keys(parsed):
    """Top-level JSON keys present in the data but absent from MOVIE_SCHEMA (schema drift)."""
    expected = set(MOVIE_SCHEMA.fieldNames())
    seen = [r[0] for r in parsed.where(valid_row_expr())
            .select(F.explode(F.map_keys("_map")).alias("k")).distinct().collect()]
    return sorted(k for k in seen if k not in expected)


def build_bronze_df(parsed, key_to_col, load_type, fname, extracted_at, run_id, load_ts):
    extra = [F.col("_map").getItem(k).alias(c) for k, c in key_to_col.items()]
    df = parsed.where(valid_row_expr()).select("_m.*", *extra, "_record_index", "_raw_json")
    df = (df.withColumn("_source_file", F.lit(fname))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.lit(extracted_at.date()))
            .withColumn("_source_extracted_at", F.lit(extracted_at))
            .withColumn("_record_hash", F.sha2(F.col("_raw_json"), 256))
            .withColumn("_batch_id", F.lit(run_id))
            .withColumn("load_timestamp", F.lit(load_ts)))
    # the same movie can appear twice in one file (popularity shifts while paging): keep the last occurrence
    w = Window.partitionBy("id").orderBy(F.col("_record_index").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def evolve_bronze_columns(new_keys):
    """Schema drift (new column): add each unknown key to Bronze as a STRING column. Returns {key: column}."""
    base_lower = {n.lower() for n in BRONZE_SCHEMA.fieldNames()}
    existing_lower = {f.name.lower() for f in spark.table(tbl(BRONZE)).schema.fields}
    key_to_col, added = {}, []
    for k in new_keys:
        col = re.sub(r"[^0-9A-Za-z_]", "_", k) or "unnamed"
        if col.lower() in base_lower:
            col = "x_" + col
        key_to_col[k] = col
        if col.lower() not in existing_lower:
            spark.sql("ALTER TABLE %s ADD COLUMNS (`%s` STRING)" % (tbl(BRONZE), col))
            existing_lower.add(col.lower())
            added.append(col)
    return key_to_col, added


def ingest_file_to_bronze(run_id, load_type, path, extracted_at):
    """One raw file -> Bronze (MERGE) + quarantine for unusable records. Returns stats for the audit log."""
    fname = os.path.basename(path)
    load_ts = now_utc()

    recs = read_raw_records(path)
    rows_read = recs.count()
    parsed = parse_records(recs)

    new_keys = detect_new_keys(parsed)
    key_to_col, added = evolve_bronze_columns(new_keys) if new_keys else ({}, [])

    bad = parsed.where(~valid_row_expr())
    q = bad.select(
        F.lit(L_BRONZE).alias("layer"), F.lit(fname).alias("source_file"),
        F.col("_m.id").alias("record_key"), F.concat_ws(";", F.col("_violations")).alias("reason"),
        F.col("_raw_json").alias("raw_json"), F.lit(run_id).alias("batch_id"),
        F.lit(load_ts).alias("load_timestamp"))
    n_bad = bad.count()
    write_quarantine(q, L_BRONZE, fname)

    bronze_df = build_bronze_df(parsed, key_to_col, load_type, fname, extracted_at, run_id, load_ts)
    valid_count = rows_read - n_bad
    src = align_to_target(bronze_df, BRONZE)
    m = merge_into(BRONZE, src, ["id", "_source_file"], "s.`_record_hash` <> t.`_record_hash`")
    deduped = valid_count - bronze_df.count()

    notes = []
    if deduped:
        notes.append("duplicates_dropped=%d" % deduped)
    if new_keys:
        notes.append("schema_drift_new_fields=%s; columns_added=%s" % (new_keys, added or "already present"))
    return {"rows_read": rows_read, "inserted": m["inserted"], "updated": m["updated"],
            "quarantined": n_bad, "notes": "; ".join(notes) or None}


def run_raw_to_bronze(load_type="all", file_pattern="*.json", date_from="", date_to="",
                      reprocess=False, source_root="", raise_on_failure=True):
    """
    Raw JSON files -> bronze_movies.
      load_type    all | full | incremental
      file_pattern glob inside the load folder, e.g. 'movies_incremental_load_20260926_*.json'
      date_from/to YYYY-MM-DD window on the file's extraction date (backfills)
      reprocess    False = skip files already logged SUCCESS (standard run); True = force (backfill)
      source_root  override the raw folder (default: the Unity Catalog volume)
    """
    ensure_tables()
    run_id = str(uuid.uuid4())
    files = list_raw_files(load_type, file_pattern, date_from, date_to, source_root)
    print("run_id=%s  files selected=%d" % (run_id, len(files)))
    if not files:
        log_skip(run_id, L_BRONZE, load_type, BRONZE,
                 "no files matched: pattern=%s from=%s to=%s" % (file_pattern, date_from or "-", date_to or "-"),
                 "no raw files selected; check the sub-folders full_load/ and incremental_load/ and the parameters")
    failures, processed = 0, 0
    for lt, path, ts in files:
        param = "%s/%s" % (FOLDERS[lt], os.path.basename(path))
        if not reprocess and already_succeeded(L_BRONZE, BRONZE, param):
            print("SKIP (already loaded): %s" % param)
            log_skip(run_id, L_BRONZE, lt, BRONZE, param, "already loaded; set reprocess=true to force")
            continue
        print("PROCESS %s" % param)
        ok = run_step(run_id, L_BRONZE, lt, BRONZE, param,
                      lambda lt=lt, path=path, ts=ts: ingest_file_to_bronze(run_id, lt, path, ts))
        processed += 1
        failures += 0 if ok else 1
    print("done: processed=%d failed=%d" % (processed, failures))
    if failures and raise_on_failure:
        raise RuntimeError("%d file(s) failed; see pipeline_execution_logs (other files were still loaded)" % failures)
    return {"run_id": run_id, "processed": processed, "failed": failures}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bronze to Silver

# COMMAND ----------

# scalar fields that must be castable (Bronze is all STRING); anything non-empty that fails try_cast is type drift
TYPE_CONTRACT = {
    "id": "bigint", "budget": "double", "revenue": "double", "runtime": "int",
    "popularity": "double", "vote_average": "double", "vote_count": "bigint", "adult": "boolean",
}
EMAIL_RE = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
URL_RE = r"(https?://|www\.)\S+"
MOVIE_BIZ_COLS = ["movie_id", "title", "original_title", "original_language", "status", "overview",
                  "release_date", "release_year", "runtime", "budget", "revenue", "profit", "roi",
                  "popularity", "vote_average", "vote_count", "adult"]


def _has(c):
    return F.col("`%s`" % c).isNotNull() & (F.trim(F.col("`%s`" % c)) != "")


def _tc(c, t):
    """try_cast of a (possibly blank) STRING column: NULL instead of an error when it does not fit."""
    return F.expr("try_cast(NULLIF(TRIM(`%s`), '') AS %s)" % (c, t))


def validate_and_type(b):
    """Bronze rows -> (conforming rows, rejected rows). Adds _violations (array<string>)."""
    checks = [F.when(_has(c) & _tc(c, t).isNull(), F.lit("type_drift:" + c)) for c, t in TYPE_CONTRACT.items()]
    checks.append(F.when(~_has("title"), F.lit("missing_title")))
    typed = b.withColumn("_violations", F.filter(F.array(*checks), lambda x: x.isNotNull()))
    return typed.where(F.size("_violations") == 0), typed.where(F.size("_violations") > 0)


def build_silver_movies(good):
    s1 = good.select(
        F.col("genres"), F.col("production_companies"), F.col("reviews"),
        _tc("id", "bigint").alias("movie_id"),
        F.trim(F.col("title")).alias("title"),
        F.col("original_title"), F.col("original_language"), F.col("status"), F.col("overview"),
        F.expr("CAST(try_to_timestamp(NULLIF(TRIM(`release_date`), ''), 'yyyy-MM-dd') AS DATE)").alias("release_date"),
        F.when(_tc("runtime", "int") > 0, _tc("runtime", "int")).alias("runtime"),          # 0 = unknown in TMDB
        F.when(_tc("budget", "double") > 0, _tc("budget", "double")).alias("budget"),       # 0 / negative -> NULL
        F.when(_tc("revenue", "double") > 0, _tc("revenue", "double")).alias("revenue"),
        _tc("popularity", "double").alias("popularity"),
        _tc("vote_average", "double").alias("vote_average"),
        _tc("vote_count", "bigint").alias("vote_count"),
        _tc("adult", "boolean").alias("adult"),
        F.col("_load_type").alias("source_load_type"),
        F.col("_source_file").alias("source_file"),
        F.col("_source_extracted_at").alias("source_extracted_at"),
    )
    s2 = (s1.withColumn("release_year", F.year("release_date"))
            .withColumn("profit", F.col("revenue") - F.col("budget"))
            .withColumn("roi", F.when(F.col("budget") > 0,
                                      (F.col("revenue") - F.col("budget")) / F.col("budget"))))
    s3 = s2.withColumn("row_hash", F.sha2(F.to_json(F.struct(*MOVIE_BIZ_COLS)), 256))
    return s3.dropDuplicates(["movie_id"])


def build_children(to_apply, salt):
    """Explode the nested arrays of the movies being applied into the three child tables."""
    base = ["movie_id", "source_extracted_at"]
    genres = (to_apply.select(*base, F.explode("genres").alias("g"))
              .select(*base, F.expr("try_cast(g.id AS int)").alias("genre_id"),
                      F.trim(F.col("g.name")).alias("genre_name"))
              .where(F.col("genre_id").isNotNull()).dropDuplicates(["movie_id", "genre_id"])
              .withColumn("row_hash", F.sha2(F.to_json(F.struct("genre_name")), 256)))

    studios = (to_apply.select(*base, F.explode("production_companies").alias("c"))
               .select(*base, F.expr("try_cast(c.id AS bigint)").alias("company_id"),
                       F.trim(F.col("c.name")).alias("company_name"),
                       F.col("c.origin_country").alias("origin_country"))
               .where(F.col("company_id").isNotNull()).dropDuplicates(["movie_id", "company_id"])
               .withColumn("row_hash", F.sha2(F.to_json(F.struct("company_name", "origin_country")), 256)))

    reviewer = F.coalesce(F.col("r.author"), F.col("r.author_details.username"))
    scrubbed = F.regexp_replace(F.regexp_replace(F.col("r.content"), EMAIL_RE, "[EMAIL_REDACTED]"),
                                URL_RE, "[URL_REDACTED]")
    reviews = (to_apply.select(*base, F.explode(F.col("reviews.results")).alias("r"))
               .select(*base, F.col("r.id").alias("review_id"),
                       F.when(reviewer.isNotNull(), F.sha2(F.concat(reviewer, F.lit(salt)), 256)).alias("reviewer_hash_id"),
                       F.expr("try_cast(r.author_details.rating AS double)").alias("rating"),
                       F.expr("try_cast(r.created_at AS timestamp)").alias("review_created_at"),
                       scrubbed.alias("content_scrubbed"))
               .where(F.col("review_id").isNotNull()).dropDuplicates(["movie_id", "review_id"])
               .withColumn("row_hash", F.sha2(F.to_json(F.struct(
                   "reviewer_hash_id", "rating", "review_created_at", "content_scrubbed")), 256)))
    return genres, studios, reviews


def merge_child(table_name, new_df, keys, applied_ids, run_id, load_ts):
    """Upsert the new child rows and delete rows that disappeared from the source version (atomic MERGE)."""
    stale = (spark.table(tbl(table_name)).join(applied_ids, "movie_id", "left_semi")
             .join(new_df.select(*keys), keys, "left_anti").select(*keys))
    up = align_to_target(new_df.withColumn("batch_id", F.lit(run_id))
                               .withColumn("load_timestamp", F.lit(load_ts)), table_name
                         ).withColumn("_op", F.lit("U"))
    de = align_to_target(stale, table_name).withColumn("_op", F.lit("D"))
    return merge_into(table_name, up.unionByName(de), keys, "s.`row_hash` <> t.`row_hash`", has_op=True)


def process_file_to_silver(run_id, load_type, source_file, salt):
    """One Bronze source file -> 4 Silver tables (+ quarantine). Logs one audit row per Silver table."""
    param = "bronze:%s" % source_file
    load_ts = now_utc()
    b = spark.table(tbl(BRONZE)).where(F.col("_source_file") == source_file)
    good, bad = validate_and_type(b)
    movies = build_silver_movies(good)

    # newest source wins: an older (back-filled) file never overwrites a newer version already in Silver
    existing = spark.table(tbl(SILVER_MOVIES)).select("movie_id", F.col("source_extracted_at").alias("_existing_ts"))
    to_apply = (movies.join(existing, "movie_id", "left")
                .where(F.col("_existing_ts").isNull() | (F.col("source_extracted_at") >= F.col("_existing_ts")))
                .drop("_existing_ts"))
    applied_ids = to_apply.select("movie_id").distinct()
    genres, studios, reviews = build_children(to_apply, salt)

    state = {}

    def step_movies():
        rows_read = b.count()
        n_bad = bad.count()
        q = bad.select(F.lit(L_SILVER).alias("layer"), F.lit(source_file).alias("source_file"),
                       F.col("id").alias("record_key"), F.concat_ws(";", F.col("_violations")).alias("reason"),
                       F.col("_raw_json").alias("raw_json"), F.lit(run_id).alias("batch_id"),
                       F.lit(load_ts).alias("load_timestamp"))
        write_quarantine(q, L_SILVER, source_file)
        n_good, n_apply = movies.count(), to_apply.count()
        src = align_to_target(to_apply.withColumn("batch_id", F.lit(run_id))
                                      .withColumn("load_timestamp", F.lit(load_ts)), SILVER_MOVIES)
        m = merge_into(SILVER_MOVIES, src, ["movie_id"],
                       "s.`row_hash` <> t.`row_hash` AND s.`source_extracted_at` >= t.`source_extracted_at`")
        stale_skipped = n_good - n_apply
        return {"rows_read": rows_read, "inserted": m["inserted"], "updated": m["updated"],
                "quarantined": n_bad,
                "notes": ("skipped_older_than_silver=%d" % stale_skipped) if stale_skipped else None}

    def child(table, df, keys):
        def _run():
            n = df.count()
            m = merge_child(table, df, keys, applied_ids, run_id, load_ts)
            return {"rows_read": n, "inserted": m["inserted"], "updated": m["updated"], "deleted": m["deleted"]}
        return _run

    steps = [
        (SILVER_MOVIES, step_movies),
        (SILVER_GENRES, child(SILVER_GENRES, genres, ["movie_id", "genre_id"])),
        (SILVER_STUDIOS, child(SILVER_STUDIOS, studios, ["movie_id", "company_id"])),
        (SILVER_REVIEWS, child(SILVER_REVIEWS, reviews, ["movie_id", "review_id"])),
    ]
    for target, fn in steps:
        if not run_step(run_id, L_SILVER, load_type, target, param, fn):
            return False  # stop this file; it is retried on the next run (all steps are idempotent)
    return True


def list_bronze_files(load_type="all", file_pattern="*", date_from="", date_to=""):
    df = spark.table(tbl(BRONZE)).select("_source_file", "_load_type", "_source_extracted_at").distinct()
    if load_type != "all":
        df = df.where(F.col("_load_type") == load_type)
    d_from, d_to = parse_date(date_from), parse_date(date_to)
    out = []
    for r in df.collect():
        d = r["_source_extracted_at"].date()
        if not fnmatch.fnmatch(r["_source_file"], file_pattern):
            continue
        if (d_from and d < d_from) or (d_to and d > d_to):
            continue
        out.append((r["_load_type"], r["_source_file"], r["_source_extracted_at"]))
    out.sort(key=lambda x: x[2])
    return out


def run_bronze_to_silver(load_type="all", file_pattern="*", date_from="", date_to="",
                         reprocess=False, pii_salt=DEV_SALT, raise_on_failure=True):
    """
    bronze_movies -> silver tables, one Bronze source file at a time (oldest first).
      load_type / file_pattern / date_from / date_to   select which Bronze files to process
      reprocess  False = skip files already fully loaded to Silver; True = force (backfill / replay)
      pii_salt   salt for hashing reviewer handles (use a private value outside of a class demo)
    """
    ensure_tables()
    if pii_salt == DEV_SALT:
        print("WARNING: using the default dev salt for reviewer hashing; pass a private pii_salt for real use.")
    run_id = str(uuid.uuid4())
    files = list_bronze_files(load_type, file_pattern, date_from, date_to)
    print("run_id=%s  bronze files selected=%d" % (run_id, len(files)))
    if not files:
        log_skip(run_id, L_SILVER, load_type, SILVER_MOVIES,
                 "no bronze files matched: pattern=%s from=%s to=%s" % (file_pattern, date_from or "-", date_to or "-"),
                 "no Bronze files selected; run 01_raw_to_bronze first or widen the parameters")
    failures, processed = 0, 0
    for lt, fname, ts in files:
        param = "bronze:%s" % fname
        # a file counts as done when its LAST Silver step (reviews) succeeded
        if not reprocess and already_succeeded(L_SILVER, SILVER_REVIEWS, param):
            print("SKIP (already in Silver): %s" % fname)
            log_skip(run_id, L_SILVER, lt, SILVER_MOVIES, param, "already in Silver; set reprocess=true to force")
            continue
        print("PROCESS %s" % fname)
        ok = process_file_to_silver(run_id, lt, fname, pii_salt)
        processed += 1
        failures += 0 if ok else 1
    print("done: processed=%d failed=%d" % (processed, failures))
    if failures and raise_on_failure:
        raise RuntimeError("%d file(s) failed; see pipeline_execution_logs" % failures)
    return {"run_id": run_id, "processed": processed, "failed": failures}


def run_pipeline(load_type="all", file_pattern="*.json", date_from="", date_to="", reprocess=False,
                 source_root="", pii_salt=DEV_SALT, raise_on_failure=True):
    """Convenience: Raw->Bronze then Bronze->Silver with the same parameters."""
    r1 = run_raw_to_bronze(load_type, file_pattern, date_from, date_to, reprocess, source_root, raise_on_failure)
    r2 = run_bronze_to_silver(load_type, file_pattern, date_from, date_to, reprocess, pii_salt, raise_on_failure)
    return r1, r2
