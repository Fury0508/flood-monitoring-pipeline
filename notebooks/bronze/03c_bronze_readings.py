# Databricks notebook source
# MAGIC %md
# MAGIC # 03c - Bronze: readings
# MAGIC
# MAGIC Loads the landed daily readings into `bronze.readings`: one row per reading, about 490,000 per day.
# MAGIC Values stay strings, so non-numeric values arrive untouched and Silver decides what to do with them.
# MAGIC
# MAGIC Row counts are reconciled **per day** against the landing stage, so a partially loaded day fails the run
# MAGIC instead of passing quietly. The table is liquid-clustered on `reading_date`.
# MAGIC
# MAGIC The catch-up job reuses this notebook for readings it fetched for stations that went silent.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful landing run)")

# COMMAND ----------

# MAGIC %run ../common/00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "landing_readings")
LANDING = stage_details(CATALOG, RUN_ID, "landing_readings")
TABLE = f"{CATALOG}.bronze.readings"

EXPECTED_KEYS = ["@id", "date", "dateTime", "measure", "value"]
expected_keys_sql = ", ".join(f"'{k}'" for k in EXPECTED_KEYS)

# Items landed per day, e.g. {"2026-09-17": 487789}
expected_per_day = {k.removeprefix("readings_"): v for k, v in LANDING["items"].items() if k.startswith("readings_")}

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      reading_uri STRING, measure_uri STRING, date_time STRING, value STRING,
      raw_item          STRING COMMENT 'The API item exactly as received',
      unexpected_fields ARRAY<STRING> COMMENT 'Keys not in the expected list: schema drift',
      reading_date      DATE,
      api_version       STRING,
      run_id            STRING NOT NULL,
      source_file       STRING,
      ingested_at       TIMESTAMP
    ) CLUSTER BY (reading_date)
    COMMENT 'Raw readings from the EA flood monitoring API. All values are strings; typing happens in Silver.'
""")

# COMMAND ----------

read_landing(f"/Volumes/{CATALOG}/landing/raw_files/readings/reading_date=*/readings_{RUN_ID}.json")

with log_stage(CATALOG, RUN_ID, "bronze_readings") as log:
    spark.sql(f"""
        INSERT INTO {TABLE} REPLACE WHERE run_id = '{RUN_ID}'
        SELECT
          get_json_object(raw_item, "$['@id']")   AS reading_uri,
          get_json_object(raw_item, '$.measure')  AS measure_uri,
          get_json_object(raw_item, '$.dateTime') AS date_time,
          get_json_object(raw_item, '$.value')    AS value,
          raw_item,
          array_except(json_object_keys(raw_item), array({expected_keys_sql})) AS unexpected_fields,
          to_date(regexp_extract(source_file, 'reading_date=([0-9-]+)', 1)) AS reading_date,
          api_version,
          '{RUN_ID}' AS run_id,
          source_file,
          current_timestamp() AS ingested_at
        FROM landed
    """)

    check_bronze(log, TABLE, RUN_ID, "reading_date", expected_per_day)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT reading_date,
           count(*)                                                           AS readings,
           count(DISTINCT measure_uri)                                        AS measures_reporting,
           count_if(value IS NULL)                                            AS missing_value,
           count_if(value IS NOT NULL AND try_cast(value AS DOUBLE) IS NULL)  AS non_numeric_value
    FROM {TABLE}
    WHERE run_id = '{RUN_ID}'
    GROUP BY reading_date
    ORDER BY reading_date
"""))
