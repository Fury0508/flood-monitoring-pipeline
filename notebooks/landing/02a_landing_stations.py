# Databricks notebook source
# MAGIC %md
# MAGIC # 02a - Landing: stations
# MAGIC
# MAGIC One call to `/id/stations` returns every monitoring station (about 5,500).
# MAGIC The response is saved untouched to `landing/raw_files/stations/run_date=<date>/`.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = generate)")
dbutils.widgets.text("expected_api_version", "0.9", "Expected API version")

# COMMAND ----------

# MAGIC %run ../common/00_landing_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = new_run_id(dbutils.widgets.get("run_id"))
RUN_DATE = datetime.now(timezone.utc).date().isoformat()

calls = [{"key": "stations", "path": "/id/stations", "partition": f"run_date={RUN_DATE}", "warn_if_empty": True}]

run_landing("stations", calls, CATALOG, RUN_ID, dbutils.widgets.get("expected_api_version").strip())
