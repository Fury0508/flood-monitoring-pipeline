# Databricks notebook source
# MAGIC %md
# MAGIC # 02b - Landing: measures
# MAGIC
# MAGIC One call to `/id/measures` returns every measure (about 7,400): what each station records, its units and interval,
# MAGIC and its latest reading. The response is saved untouched to `landing/raw_files/measures/run_date=<date>/`.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = generate)")
dbutils.widgets.text("expected_api_version", "0.9", "Expected API version")

# COMMAND ----------

# MAGIC %run ../common/00_landing_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"))
RUN_DATE = datetime.now(timezone.utc).date().isoformat()

calls = [{"key": "measures", "path": "/id/measures", "partition": f"run_date={RUN_DATE}", "warn_if_empty": True}]

run_landing("measures", calls, CATALOG, RUN_ID, dbutils.widgets.get("expected_api_version").strip())
