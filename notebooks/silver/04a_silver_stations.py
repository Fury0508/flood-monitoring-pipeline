# Databricks notebook source
# MAGIC %md
# MAGIC # 04a - Silver: stations
# MAGIC
# MAGIC Turns `bronze.stations` into a clean current snapshot of every monitoring station.
# MAGIC
# MAGIC | Problem found in exploration | What Silver does |
# MAGIC |---|---|
# MAGIC | `status` missing on over half the stations | Left as null; the station is still loaded |
# MAGIC | `status` sometimes a JSON list, sometimes a URI | Normalised to Active / Closed / Suspended / Unknown |
# MAGIC | 632 stations without coordinates | Kept as null; a station with no map position is still a valid station |
# MAGIC | A station with no reference | Quarantined: without a key it cannot join to anything |
# MAGIC
# MAGIC The table is rebuilt each run, because it is a snapshot of what the API currently publishes.
# MAGIC Every previous version stays in Bronze and in Delta history.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful bronze run)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "bronze_stations")
TABLE = f"{CATALOG}.silver.stations"
ensure_quarantine_table(CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest copy of each station from this run

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW stations_latest AS
    SELECT * FROM (
      SELECT *,
             row_number() OVER (PARTITION BY station_uri ORDER BY ingested_at DESC) AS rn,
             nullif(trim(station_reference), '') IS NULL AS missing_reference
      FROM {CATALOG}.bronze.stations
      WHERE run_id = '{RUN_ID}'
    )
    WHERE rn = 1
""")

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "silver_stations") as log:
    # A station with no reference cannot be joined to its measures, so it is quarantined, not loaded.
    spark.sql(f"""
        INSERT INTO {CATALOG}.silver.quarantine
          REPLACE WHERE run_id = '{RUN_ID}' AND entity = 'stations'
        SELECT 'stations', station_uri, 'missing_station_reference', label, raw_item,
               '{RUN_ID}', current_timestamp()
        FROM stations_latest
        WHERE missing_reference
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE}
        COMMENT 'Current snapshot of EA monitoring stations: status normalised, coordinates typed.'
        AS
        WITH typed AS (
          SELECT *,
                 -- '.../statusActive' or '["...statusActive"]' -> 'Active'
                 regexp_replace(last_segment(first_element(status)), '^status', '') AS status_code
          FROM stations_latest
          WHERE NOT missing_reference
        )
        SELECT
          trim(station_reference)                    AS station_reference,
          station_uri,
          label,
          river_name,
          catchment_name,
          town,
          CASE WHEN status IS NULL OR trim(status_code) = '' THEN NULL
               WHEN status_code IN ('Active', 'Closed', 'Suspended') THEN status_code
               ELSE 'Unknown' END                    AS status,
          status                                     AS status_raw,
          parse_double(latitude)                     AS latitude,
          parse_double(longitude)                    AS longitude,
          try_cast(first_element(easting) AS INT)    AS easting,
          try_cast(first_element(northing) AS INT)   AS northing,
          to_date(try_to_timestamp(first_element(date_opened))) AS date_opened,
          -- '["http://.../SingleLevel"]' -> ['SingleLevel']
          transform(CASE WHEN startswith(station_type, '[') THEN from_json(station_type, 'array<string>')
                         WHEN station_type IS NULL THEN NULL
                         ELSE array(station_type) END,
                    t -> regexp_extract(t, '([^/]+)$', 1)) AS station_types,  -- SQL functions can't run inside a lambda
          size(from_json(measures, 'array<string>')) AS measure_count,
          unexpected_fields,
          run_date,
          run_id,
          ingested_at
        FROM typed
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM stations_latest)                                   AS rows_in,
               (SELECT count(*) FROM {TABLE})                                           AS rows_out,
               (SELECT count(*) FROM stations_latest WHERE missing_reference)            AS rejected,
               (SELECT count_if(latitude IS NULL OR longitude IS NULL) FROM {TABLE})    AS missing_coords
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.rows_out, rows_rejected=counts.rejected)
    log["details"]["missing_coordinates"] = counts.missing_coords
    log["details"]["status_breakdown"] = {r[0] or "null": r[1] for r in spark.sql(
        f"SELECT status, count(*) FROM {TABLE} GROUP BY status").collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT status, count(*) AS stations, count_if(status_raw LIKE '[%') AS arrived_as_list
    FROM {TABLE}
    GROUP BY status
    ORDER BY stations DESC
"""))
