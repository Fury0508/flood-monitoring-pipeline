# Databricks notebook source
# MAGIC %md
# MAGIC # 03a - Bronze: stations
# MAGIC
# MAGIC Loads the landed `/id/stations` response into `bronze.stations`: one row per station.
# MAGIC
# MAGIC - **Nothing is interpreted.** Every value stays a string and the whole item is kept in `raw_item`,
# MAGIC   so an API change can't corrupt data and any field can be re-parsed later without re-downloading.
# MAGIC - **Schema drift is recorded, not fatal.** Keys the API sends that aren't in the expected list land in
# MAGIC   `unexpected_fields` and are reported as a warning.
# MAGIC - **Idempotent.** `REPLACE WHERE run_id = ...` swaps this run's rows, so a rerun never duplicates.
# MAGIC
# MAGIC Known quirks kept as-is for Silver: `status` is missing for over half the stations and is sometimes a list,
# MAGIC and some stations have no coordinates.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful landing run)")

# COMMAND ----------

# MAGIC %run ../common/00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "landing_stations")
LANDING = stage_details(CATALOG, RUN_ID, "landing_stations")
TABLE = f"{CATALOG}.bronze.stations"

# Every key we know the API sends; anything else is reported as schema drift.
EXPECTED_KEYS = [
    "@id", "RLOIid", "catchmentName", "dateOpened", "datumOffset", "downstageScale", "easting",
    "eaAreaName", "eaRegionName", "gridReference", "label", "lat", "long", "measures", "northing",
    "notation", "riverName", "stageScale", "stationReference", "status", "statusDate",
    "statusReason", "town", "type", "wiskiID",
]
expected_keys_sql = ", ".join(f"'{k}'" for k in EXPECTED_KEYS)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      station_uri STRING, station_reference STRING, notation STRING, label STRING, status STRING,
      latitude STRING, longitude STRING, easting STRING, northing STRING, river_name STRING,
      catchment_name STRING, town STRING, date_opened STRING, station_type STRING, measures STRING,
      raw_item          STRING COMMENT 'The API item exactly as received',
      unexpected_fields ARRAY<STRING> COMMENT 'Keys not in the expected list: schema drift',
      run_date          DATE,
      api_version       STRING,
      run_id            STRING NOT NULL,
      source_file       STRING,
      ingested_at       TIMESTAMP
    ) COMMENT 'Raw stations from the EA flood monitoring API. All values are strings; typing happens in Silver.'
""")

# COMMAND ----------

read_landing(f"/Volumes/{CATALOG}/landing/raw_files/stations/run_date=*/stations_{RUN_ID}.json")

with log_stage(CATALOG, RUN_ID, "bronze_stations") as log:
    spark.sql(f"""
        INSERT INTO {TABLE} REPLACE WHERE run_id = '{RUN_ID}'
        SELECT
          get_json_object(raw_item, "$['@id']")          AS station_uri,
          get_json_object(raw_item, '$.stationReference') AS station_reference,
          get_json_object(raw_item, '$.notation')         AS notation,
          get_json_object(raw_item, '$.label')            AS label,
          get_json_object(raw_item, '$.status')           AS status,
          get_json_object(raw_item, '$.lat')              AS latitude,
          get_json_object(raw_item, '$.long')             AS longitude,
          get_json_object(raw_item, '$.easting')          AS easting,
          get_json_object(raw_item, '$.northing')         AS northing,
          get_json_object(raw_item, '$.riverName')        AS river_name,
          get_json_object(raw_item, '$.catchmentName')    AS catchment_name,
          get_json_object(raw_item, '$.town')             AS town,
          get_json_object(raw_item, '$.dateOpened')       AS date_opened,
          get_json_object(raw_item, '$.type')             AS station_type,
          get_json_object(raw_item, '$.measures')         AS measures,
          raw_item,
          array_except(json_object_keys(raw_item), array({expected_keys_sql})) AS unexpected_fields,
          to_date(regexp_extract(source_file, 'run_date=([0-9-]+)', 1)) AS run_date,
          api_version,
          '{RUN_ID}' AS run_id,
          source_file,
          current_timestamp() AS ingested_at
        FROM landed
    """)

    run_date = LANDING["files"]["stations"].split("run_date=")[1][:10]
    check_bronze(log, TABLE, RUN_ID, "run_date", {run_date: LANDING["items"]["stations"]})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT count(*)                                        AS stations,
           count_if(status IS NULL)                        AS missing_status,
           count_if(status LIKE '[%')                      AS status_as_list,
           count_if(latitude IS NULL OR longitude IS NULL) AS missing_coordinates,
           count_if(size(unexpected_fields) > 0)           AS rows_with_unexpected_fields
    FROM {TABLE}
    WHERE run_id = '{RUN_ID}'
"""))
