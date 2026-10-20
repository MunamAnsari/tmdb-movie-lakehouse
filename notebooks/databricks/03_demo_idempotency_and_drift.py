# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Demo: idempotency, MERGE upserts, schema drift, quarantine, fault tolerance
# MAGIC Run **01** and **02** first (real data loaded). This notebook then proves the Phase 2 requirements with screenshots-ready output:
# MAGIC
# MAGIC 0. **Explicit schemas**: the StructTypes used for reading, and the typed table schemas.
# MAGIC 1. **Idempotency**: replay every file; row counts must not change and the log must show 0 inserted / 0 updated.
# MAGIC 2. **Backfill**: re-run a past period by date window, without editing any code.
# MAGIC 3. **Schema drift + quarantine**: a crafted file with a new field, type changes, a missing key, a blank title and a duplicate.
# MAGIC 4. **MERGE update**: a later file changes one movie; Silver shows exactly 1 update.
# MAGIC 5. **Fault tolerance**: a corrupt file is logged as FAILURE and does not stop the run.
# MAGIC 6. **Cleanup**: removes every demo artifact (demo movie ids are 900000001+, never real movies).

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json, copy
from datetime import timedelta

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "tmdb_raw", "Volume")
configure(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), dbutils.widgets.get("volume"))
ensure_tables()

DATA_TABLES = [BRONZE, SILVER_MOVIES, SILVER_GENRES, SILVER_STUDIOS, SILVER_REVIEWS]


def table_counts():
    return {t: spark.table(tbl(t)).count() for t in DATA_TABLES}


def cleanup_demo():
    """Remove demo files and every row they created (demo movies use ids 900000001-900000099)."""
    for f in glob.glob(volume_root() + "/incremental_load/*_demo*.json"):
        os.remove(f)
    spark.sql("DELETE FROM %s WHERE `_source_file` LIKE '%%_demo%%'" % tbl(BRONZE))
    spark.sql("DELETE FROM %s WHERE source_file LIKE '%%_demo%%'" % tbl(QUARANTINE))
    for t in [SILVER_MOVIES, SILVER_GENRES, SILVER_STUDIOS, SILVER_REVIEWS]:
        spark.sql("DELETE FROM %s WHERE movie_id BETWEEN 900000001 AND 900000099" % tbl(t))


cleanup_demo()  # start from a clean slate so this notebook can be re-run
print("clean start:", table_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Explicit schemas: defined with StructType/StructField before any file is read (no inferSchema)

# COMMAND ----------

print("RAW FILE WRAPPER (used by spark.read.schema(...)):")
print(WRAPPER_SCHEMA.treeString())
print("MOVIE RECORD (used by from_json(..., MOVIE_SCHEMA)):")
print(MOVIE_SCHEMA.treeString())

# COMMAND ----------

# the typed table schemas that result (Silver casts every field to its proper type)
for t in [SILVER_MOVIES, SILVER_GENRES, SILVER_STUDIOS, SILVER_REVIEWS]:
    print("\n==", t)
    spark.table(tbl(t)).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Idempotency: replay everything, nothing may change

# COMMAND ----------

before = table_counts()
r1 = run_raw_to_bronze(load_type="all", reprocess=True)          # replay every raw file
r2 = run_bronze_to_silver(load_type="all", reprocess=True)       # replay every Bronze file
after = table_counts()

print("\nBEFORE:", before)
print("AFTER: ", after)
print("IDEMPOTENT (counts identical):", before == after)

# COMMAND ----------

# MAGIC %md
# MAGIC The replay's audit rows: `rows_inserted` and `rows_updated` should all be 0.

# COMMAND ----------

display(
    spark.table(tbl(LOGS))
    .where(F.col("run_id").isin(r1["run_id"], r2["run_id"]))
    .select("layer", "target_table", "parameter_processed", "status",
            "rows_read", "rows_inserted", "rows_updated", "rows_deleted", "rows_quarantined", "notes")
    .orderBy("layer", "target_table", "parameter_processed")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Backfill: re-run a past period by parameter (no code edits)
# MAGIC Re-processes only the raw files whose extraction date falls in the window, in both steps. Idempotent, so counts stay the same.

# COMMAND ----------

real_files = list_raw_files("all")
window_day = real_files[0][2].date().isoformat()      # the date of the oldest real file
print("Backfilling window:", window_day, "to", window_day)

b1 = run_raw_to_bronze(load_type="all", date_from=window_day, date_to=window_day, reprocess=True)
b2 = run_bronze_to_silver(load_type="all", date_from=window_day, date_to=window_day, reprocess=True)

display(
    spark.table(tbl(LOGS))
    .where(F.col("run_id").isin(b1["run_id"], b2["run_id"]))
    .select("layer", "load_type", "target_table", "parameter_processed", "status",
            "rows_inserted", "rows_updated", "start_time", "end_time")
    .orderBy("start_time")
)
print("counts unchanged after backfill:", table_counts() == before)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema drift + quarantine: craft a "bad day" incremental file

# COMMAND ----------

full_files = sorted(glob.glob(volume_root() + "/full_load/*.json"))
assert full_files, "No full-load file found in the volume; run the data upload first."
with open(full_files[-1], encoding="utf-8") as fh:
    template = next(r for r in json.load(fh)["records"] if r.get("title") and r.get("id"))


def clone(new_id, **overrides):
    r = copy.deepcopy(template)
    r["id"] = new_id
    r["title"] = "[DEMO] " + str(template.get("title"))
    r["demo_new_field"] = "added-by-source"          # (a) NEW COLUMN: schema drift
    r.update(overrides)
    return r


rec_ok = clone(900000001)
rec_type_drift = clone(900000002, budget="N/A")        # (b) TYPE CHANGE: number became text
rec_blank_title = clone(900000003, title="")           # (c) contract violation: blank title
rec_no_id = clone(900000004); del rec_no_id["id"]      # (d) missing key
rec_dup = clone(900000001, popularity=1.0)             # (e) duplicate id inside the same file
rec_nested = clone(900000005, genres="Action")         # (f) NESTED TYPE CHANGE: array became a string

t0 = now_utc()
ts1 = t0.strftime("%Y%m%d_%H%M%S")
ts2 = (t0 + timedelta(minutes=1)).strftime("%Y%m%d_%H%M%S")
ts3 = (t0 + timedelta(minutes=2)).strftime("%Y%m%d_%H%M%S")
inc_dir = volume_root() + "/incremental_load"


def write_raw(name, records):
    with open("%s/%s" % (inc_dir, name), "w", encoding="utf-8") as f:
        json.dump({"load_type": "incremental", "record_count": len(records), "records": records}, f)


write_raw("movies_incremental_load_%s_demo1.json" % ts1,
          [rec_ok, rec_type_drift, rec_blank_title, rec_no_id, rec_dup, rec_nested])
print("wrote demo1 file")

# COMMAND ----------

run_raw_to_bronze(load_type="incremental", file_pattern="*_demo1.json", reprocess=True)
run_bronze_to_silver(load_type="incremental", file_pattern="*_demo1.json", reprocess=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected:** Bronze logs show `schema_drift_new_fields=['demo_new_field']`, 2 quarantined
# MAGIC (`missing_or_null_id`, `nested_type_drift:genres`) and `duplicates_dropped=1`.
# MAGIC Silver logs show 1 inserted movie (900000001) and 2 quarantined (`type_drift:budget`, `missing_title`).

# COMMAND ----------

display(
    spark.table(tbl(LOGS))
    .where(F.col("parameter_processed").contains("_demo1"))
    .select("layer", "target_table", "status", "rows_read", "rows_inserted", "rows_updated",
            "rows_quarantined", "notes")
    .orderBy("start_time")
)

# COMMAND ----------

display(spark.table(tbl(QUARANTINE)).where(F.col("source_file").contains("_demo"))
        .select("layer", "record_key", "reason", "source_file"))

# COMMAND ----------

# the new column now exists in Bronze (schema evolved, nothing crashed)
display(spark.table(tbl(BRONZE)).where(F.col("_source_file").contains("_demo1"))
        .select("id", "title", "budget", "demo_new_field", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE update: a later file changes one movie

# COMMAND ----------

write_raw("movies_incremental_load_%s_demo2.json" % ts2, [clone(900000001, vote_average=9.9)])
run_raw_to_bronze(load_type="incremental", file_pattern="*_demo2.json", reprocess=True)
run_bronze_to_silver(load_type="incremental", file_pattern="*_demo2.json", reprocess=True)

display(spark.table(tbl(SILVER_MOVIES)).where("movie_id = 900000001")
        .select("movie_id", "title", "vote_average", "source_file", "load_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fault tolerance: a corrupt file is logged as FAILURE, other files are unaffected

# COMMAND ----------

with open("%s/movies_incremental_load_%s_demo_corrupt.json" % (inc_dir, ts3), "w") as f:
    f.write('{"load_type": "incremental", "records": [ {"id": 1,')   # truncated JSON

run_raw_to_bronze(load_type="incremental", file_pattern="*_demo*.json", reprocess=False, raise_on_failure=False)

display(spark.table(tbl(LOGS)).where(F.col("parameter_processed").contains("demo_corrupt"))
        .select("layer", "parameter_processed", "status", "error_message"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cleanup: remove all demo artifacts (real data is untouched)

# COMMAND ----------

cleanup_demo()
print("after cleanup:", table_counts())
print("matches pre-demo counts:", table_counts() == before)
