# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Raw to Bronze
# MAGIC Loads raw TMDB JSON files from the Unity Catalog volume into `bronze_movies` (Delta, MERGE-based, idempotent).
# MAGIC
# MAGIC | Scenario | Widgets |
# MAGIC |---|---|
# MAGIC | **Standard incremental run** (only files not yet loaded) | `load_type=all`, `reprocess=false`, everything else blank |
# MAGIC | **Backfill a date window** | `date_from` / `date_to`, `reprocess=true` |
# MAGIC | **Reload one specific file** | `file_pattern=<exact file name>`, `reprocess=true` |
# MAGIC | **Different raw folder** | `source_root=/Volumes/<catalog>/<schema>/<volume>` |

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental"], "Load type")
dbutils.widgets.text("file_pattern", "*.json", "File pattern (glob)")
dbutils.widgets.text("date_from", "", "Backfill from date (YYYY-MM-DD)")
dbutils.widgets.text("date_to", "", "Backfill to date (YYYY-MM-DD)")
dbutils.widgets.dropdown("reprocess", "false", ["false", "true"], "Reprocess already-loaded files")
dbutils.widgets.text("source_root", "", "Raw folder override (optional)")
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "tmdb_raw", "Volume")

# COMMAND ----------

configure(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), dbutils.widgets.get("volume"))

result = run_raw_to_bronze(
    load_type=dbutils.widgets.get("load_type"),
    file_pattern=dbutils.widgets.get("file_pattern") or "*.json",
    date_from=dbutils.widgets.get("date_from"),
    date_to=dbutils.widgets.get("date_to"),
    reprocess=dbutils.widgets.get("reprocess") == "true",
    source_root=dbutils.widgets.get("source_root"),
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
# MAGIC ## Bronze row counts per source file

# COMMAND ----------

display(
    spark.table(tbl(BRONZE)).groupBy("_load_type", "_source_file").count().orderBy("_source_file")
)
