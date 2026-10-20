# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Bronze to Silver
# MAGIC Casts, validates, deduplicates and upserts Bronze into the Silver tables (`MERGE INTO`, idempotent).
# MAGIC Records that break the data contract go to `quarantine_records` instead of failing the batch.
# MAGIC
# MAGIC | Scenario | Widgets |
# MAGIC |---|---|
# MAGIC | **Standard incremental run** (only Bronze files not yet in Silver) | `load_type=all`, `reprocess=false` |
# MAGIC | **Backfill a date window** | `date_from` / `date_to`, `reprocess=true` |
# MAGIC | **Replay one Bronze file** | `file_pattern=<exact file name>`, `reprocess=true` |
# MAGIC | **Only full or only incremental data** | `load_type=full` or `incremental` |

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental"], "Load type")
dbutils.widgets.text("file_pattern", "*", "Bronze file pattern (glob on file name)")
dbutils.widgets.text("date_from", "", "Backfill from date (YYYY-MM-DD)")
dbutils.widgets.text("date_to", "", "Backfill to date (YYYY-MM-DD)")
dbutils.widgets.dropdown("reprocess", "false", ["false", "true"], "Reprocess files already in Silver")
dbutils.widgets.text("pii_salt", DEV_SALT, "Salt for reviewer hashing")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "tmdb_raw", "Volume")

# COMMAND ----------

configure(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), dbutils.widgets.get("volume"))

result = run_bronze_to_silver(
    load_type=dbutils.widgets.get("load_type"),
    file_pattern=dbutils.widgets.get("file_pattern") or "*",
    date_from=dbutils.widgets.get("date_from"),
    date_to=dbutils.widgets.get("date_to"),
    reprocess=dbutils.widgets.get("reprocess") == "true",
    pii_salt=dbutils.widgets.get("pii_salt") or DEV_SALT,
)
result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit trail for this run

# COMMAND ----------

display(
    spark.table(tbl(LOGS))
    .where(F.col("run_id") == result["run_id"])
    .orderBy("start_time")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver row counts and quarantine summary

# COMMAND ----------

for t in [SILVER_MOVIES, SILVER_GENRES, SILVER_STUDIOS, SILVER_REVIEWS]:
    print("%-28s %d rows" % (t, spark.table(tbl(t)).count()))

display(spark.table(tbl(QUARANTINE)).groupBy("layer", "reason").count())
