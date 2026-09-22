# Databricks notebook source
# MAGIC %md
# MAGIC # 03b - Bronze: measures
# MAGIC
# MAGIC Loads the landed `/id/measures` response into `bronze.measures`: one row per measure (water level, flow,
# MAGIC rainfall...). Same rules as stations: values stay strings, the raw item is kept, drift is recorded,
# MAGIC and `REPLACE WHERE run_id` makes a rerun safe.
# MAGIC
# MAGIC `latest_reading` is kept as raw JSON. Later it tells us when a silent station has started reporting again.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful landing run)")

# COMMAND ----------

# MAGIC %run ../common/00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "landing_measures")
LANDING = stage_details(CATALOG, RUN_ID, "landing_measures")
TABLE = f"{CATALOG}.bronze.measures"

EXPECTED_KEYS = [
    "@id", "datumType", "label", "latestReading", "notation", "parameter", "parameterName",
    "period", "qualifier", "station", "stationReference", "unit", "unitName", "valueType",
]
expected_keys_sql = ", ".join(f"'{k}'" for k in EXPECTED_KEYS)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      measure_uri STRING, notation STRING, label STRING, station_uri STRING, station_reference STRING,
      parameter STRING, parameter_name STRING, qualifier STRING, period STRING, unit STRING,
      unit_name STRING, value_type STRING, datum_type STRING, latest_reading STRING,
      raw_item          STRING COMMENT 'The API item exactly as received',
      unexpected_fields ARRAY<STRING> COMMENT 'Keys not in the expected list: schema drift',
      run_date          DATE,
      api_version       STRING,
      run_id            STRING NOT NULL,
      source_file       STRING,
      ingested_at       TIMESTAMP
    ) COMMENT 'Raw measures from the EA flood monitoring API. All values are strings; typing happens in Silver.'
""")

# COMMAND ----------

read_landing(f"/Volumes/{CATALOG}/landing/raw_files/measures/run_date=*/measures_{RUN_ID}.json")

with log_stage(CATALOG, RUN_ID, "bronze_measures") as log:
    spark.sql(f"""
        INSERT INTO {TABLE} REPLACE WHERE run_id = '{RUN_ID}'
        SELECT
          get_json_object(raw_item, "$['@id']")          AS measure_uri,
          get_json_object(raw_item, '$.notation')         AS notation,
          get_json_object(raw_item, '$.label')            AS label,
          get_json_object(raw_item, '$.station')          AS station_uri,
          get_json_object(raw_item, '$.stationReference') AS station_reference,
          get_json_object(raw_item, '$.parameter')        AS parameter,
          get_json_object(raw_item, '$.parameterName')    AS parameter_name,
          get_json_object(raw_item, '$.qualifier')        AS qualifier,
          get_json_object(raw_item, '$.period')           AS period,
          get_json_object(raw_item, '$.unit')             AS unit,
          get_json_object(raw_item, '$.unitName')         AS unit_name,
          get_json_object(raw_item, '$.valueType')        AS value_type,
          get_json_object(raw_item, '$.datumType')        AS datum_type,
          get_json_object(raw_item, '$.latestReading')    AS latest_reading,
          raw_item,
          array_except(json_object_keys(raw_item), array({expected_keys_sql})) AS unexpected_fields,
          to_date(regexp_extract(source_file, 'run_date=([0-9-]+)', 1)) AS run_date,
          api_version,
          '{RUN_ID}' AS run_id,
          source_file,
          current_timestamp() AS ingested_at
        FROM landed
    """)

    run_date = LANDING["files"]["measures"].split("run_date=")[1][:10]
    check_bronze(log, TABLE, RUN_ID, "run_date", {run_date: LANDING["items"]["measures"]})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT parameter, count(*) AS measures, count(DISTINCT station_reference) AS stations,
           count_if(period IS NULL) AS missing_period
    FROM {TABLE}
    WHERE run_id = '{RUN_ID}'
    GROUP BY parameter
    ORDER BY measures DESC
"""))
