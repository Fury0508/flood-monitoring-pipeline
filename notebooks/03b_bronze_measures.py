# Databricks notebook source
# MAGIC %md
# MAGIC # 03b - Bronze: measures
# MAGIC
# MAGIC Loads the landed `/id/measures` response into `bronze.measures`: one row per measure (water level, flow, rainfall...),
# MAGIC every value kept as a string.
# MAGIC
# MAGIC `latest_reading` is kept as raw JSON. Later it tells us when a silent station has started reporting again.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Landing run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ./00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_landing_run_id(CATALOG, dbutils.widgets.get("run_id"), "measures")
LANDING = landing_details(CATALOG, RUN_ID, "measures")

MEASURES = {
    "table": f"{CATALOG}.bronze.measures",
    "glob": f"/Volumes/{CATALOG}/landing/raw_files/measures/run_date=*/measures_{RUN_ID}.json",
    "partition_col": "run_date",
    "comment": "Raw measures from the EA flood monitoring API. All values are strings; typing happens in Silver.",
    # our column name -> API JSON key
    "fields": {
        "measure_uri": "@id", "notation": "notation", "label": "label", "station_uri": "station",
        "station_reference": "stationReference", "parameter": "parameter",
        "parameter_name": "parameterName", "qualifier": "qualifier", "period": "period",
        "unit": "unit", "unit_name": "unitName", "value_type": "valueType",
        "datum_type": "datumType", "latest_reading": "latestReading",
    },
    # every key we know about; anything else is reported as schema drift
    "expected": [
        "@id", "datumType", "label", "latestReading", "notation", "parameter", "parameterName",
        "period", "qualifier", "station", "stationReference", "unit", "unitName", "valueType",
    ],
}

print(f"Loading measures from landing run {RUN_ID}")

# COMMAND ----------

run_bronze("measures", MEASURES, CATALOG, RUN_ID, expected_reference_counts(LANDING, "measures"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(
    f"""
    SELECT parameter, count(*) AS measures, count(DISTINCT station_reference) AS stations,
           count_if(period IS NULL) AS missing_period
    FROM {MEASURES['table']}
    WHERE run_id = :run_id
    GROUP BY parameter
    ORDER BY measures DESC
    """,
    args={"run_id": RUN_ID},
))

# COMMAND ----------

display(spark.sql(
    f"SELECT measure_uri, station_reference, parameter, qualifier, period, unit_name, latest_reading "
    f"FROM {MEASURES['table']} WHERE run_id = :run_id LIMIT 20",
    args={"run_id": RUN_ID},
))
