# Databricks notebook source
# MAGIC %md
# MAGIC # 04b - Silver: measures
# MAGIC
# MAGIC Turns `bronze.measures` into a clean current snapshot of every measure (water level, flow, rainfall...).
# MAGIC
# MAGIC | Field | Why it matters later |
# MAGIC |---|---|
# MAGIC | `measure_notation` | The key every reading joins on |
# MAGIC | `station_reference` | Links a reading to its station; taken from the station URI when the field is missing |
# MAGIC | `period_seconds` | 900 = every 15 minutes, 60 = every minute |
# MAGIC | `latest_reading_ts` | When the API last heard from this measure; drives the catch-up for silent stations |
# MAGIC
# MAGIC A measure with no notation is quarantined: readings could never be attached to it.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful bronze run)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "bronze_measures")
TABLE = f"{CATALOG}.silver.measures"
ensure_quarantine_table(CATALOG)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW measures_latest AS
    SELECT *, nullif(trim(measure_notation), '') IS NULL AS missing_notation
    FROM (
      SELECT *,
             coalesce(notation, last_segment(measure_uri)) AS measure_notation,
             row_number() OVER (PARTITION BY measure_uri ORDER BY ingested_at DESC) AS rn
      FROM {CATALOG}.bronze.measures
      WHERE run_id = '{RUN_ID}'
    )
    WHERE rn = 1
""")

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "silver_measures") as log:
    spark.sql(f"""
        INSERT INTO {CATALOG}.silver.quarantine
          REPLACE WHERE run_id = '{RUN_ID}' AND entity = 'measures'
        SELECT 'measures', measure_uri, 'missing_measure_notation', label, raw_item,
               '{RUN_ID}', current_timestamp()
        FROM measures_latest
        WHERE missing_notation
    """)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE}
        COMMENT 'Current snapshot of EA measures: one row per measure, typed and linked to its station.'
        AS
        SELECT
          trim(measure_notation)                                          AS measure_notation,
          measure_uri,
          coalesce(station_reference, last_segment(station_uri))          AS station_reference,
          station_uri,
          label,
          parameter,
          parameter_name,
          qualifier,
          try_cast(first_element(period) AS INT)                          AS period_seconds,
          last_segment(unit)                                              AS unit,
          unit_name,
          value_type,
          datum_type,
          try_to_timestamp(get_json_object(latest_reading, '$.dateTime')) AS latest_reading_ts,
          parse_double(get_json_object(latest_reading, '$.value'))        AS latest_reading_value,
          unexpected_fields,
          run_date,
          run_id,
          ingested_at
        FROM measures_latest
        WHERE NOT missing_notation
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM measures_latest)                              AS rows_in,
               (SELECT count(*) FROM {TABLE})                                      AS rows_out,
               (SELECT count(*) FROM measures_latest WHERE missing_notation)        AS rejected,
               (SELECT count_if(station_reference IS NULL) FROM {TABLE})           AS missing_station
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.rows_out, rows_rejected=counts.rejected)
    log["details"]["missing_station_reference"] = counts.missing_station
    log["details"]["by_parameter"] = {r[0] or "null": r[1] for r in spark.sql(
        f"SELECT parameter, count(*) FROM {TABLE} GROUP BY parameter").collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: how recently has each measure reported, according to the API itself?

# COMMAND ----------

display(spark.sql(f"""
    SELECT CASE
             WHEN latest_reading_ts IS NULL THEN 'never reported'
             WHEN latest_reading_ts > current_timestamp() - INTERVAL 24 HOURS THEN 'within 24 hours'
             WHEN latest_reading_ts > current_timestamp() - INTERVAL 7 DAYS THEN '1 to 7 days ago'
             ELSE 'over 7 days ago'
           END AS last_reported,
           count(*) AS measures
    FROM {TABLE}
    GROUP BY 1
    ORDER BY measures DESC
"""))
