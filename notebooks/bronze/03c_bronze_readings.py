# Databricks notebook source
# MAGIC %md
# MAGIC # 03c - Bronze: readings
# MAGIC
# MAGIC Loads the landed daily `/data/readings?date=` responses into `bronze.readings`: one row per reading, about
# MAGIC 490,000 per day. Every value is kept as a string, so non-numeric values arrive in Bronze untouched.
# MAGIC
# MAGIC The table is liquid-clustered on `reading_date`, so queries and later merges for a few days skip the rest of the table.
# MAGIC Row counts are reconciled per day against the landing run.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Landing run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_landing_run_id(CATALOG, dbutils.widgets.get("run_id"), "readings")
LANDING = landing_details(CATALOG, RUN_ID, "readings")

READINGS = {
    "table": f"{CATALOG}.bronze.readings",
    "glob": f"/Volumes/{CATALOG}/landing/raw_files/readings/reading_date=*/readings_{RUN_ID}.json",
    "partition_col": "reading_date",
    "cluster": True,
    "comment": "Raw readings from the EA flood monitoring API. All values are strings; typing happens in Silver.",
    # our column name -> API JSON key
    "fields": {"reading_uri": "@id", "measure_uri": "measure", "date_time": "dateTime", "value": "value"},
    # every key we know about; anything else is reported as schema drift
    "expected": ["@id", "date", "dateTime", "measure", "value"],
}

print(f"Loading readings from landing run {RUN_ID}")

# COMMAND ----------

run_bronze("readings", READINGS, CATALOG, RUN_ID, expected_reading_counts(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(
    f"""
    SELECT
      reading_date,
      count(*)                                     AS readings,
      count(DISTINCT measure_uri)                  AS measures_reporting,
      count_if(value IS NULL)                      AS missing_value,
      count_if(try_cast(value AS DOUBLE) IS NULL
               AND value IS NOT NULL)              AS non_numeric_value
    FROM {READINGS['table']}
    WHERE run_id = :run_id
    GROUP BY reading_date
    ORDER BY reading_date
    """,
    args={"run_id": RUN_ID},
))

# COMMAND ----------

# The non-numeric values Silver will need a rule for
display(spark.sql(
    f"SELECT reading_date, measure_uri, date_time, value FROM {READINGS['table']} "
    f"WHERE run_id = :run_id AND try_cast(value AS DOUBLE) IS NULL AND value IS NOT NULL LIMIT 20",
    args={"run_id": RUN_ID},
))
