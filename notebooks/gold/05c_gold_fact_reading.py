# Databricks notebook source
# MAGIC %md
# MAGIC # 05c - Gold: fact_reading
# MAGIC
# MAGIC The fact table analysts and data scientists query.
# MAGIC
# MAGIC **Grain: one row per measure per reading timestamp.** Not per station, because a station can record several
# MAGIC series at once (upstream level, downstream level, flow).
# MAGIC
# MAGIC **Keys come from the dimension.** Readings join `gold.dim_measure`, which is one row per measure, so every
# MAGIC fact row carries the same keys as its dimension row.
# MAGIC
# MAGIC **Idempotent MERGE.** Matching on measure and timestamp means re-running a day, or re-fetching a day that
# MAGIC arrived late, updates rows in place instead of duplicating them. `reading_date` in the ON clause lets Delta
# MAGIC skip every file outside the days being loaded.
# MAGIC
# MAGIC **Watermarks.** The latest reading per measure is recorded in `ops.measure_watermark`, which is how a
# MAGIC station that went silent and came back is detected.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful silver run)")

# COMMAND ----------

# MAGIC %run ../common/00_pipeline_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "silver_readings")
TABLE = f"{CATALOG}.gold.fact_reading"

dates = stage_details(CATALOG, RUN_ID, "silver_readings")["dates"]
DATES_SQL = ", ".join(f"DATE'{d}'" for d in dates)
print(f"Publishing {len(dates)} day(s): {dates[0]} to {dates[-1]}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      measure_key       BIGINT    NOT NULL COMMENT 'Joins to dim_measure',
      station_key       BIGINT             COMMENT 'Joins to dim_station.station_key',
      measure_notation  STRING    NOT NULL COMMENT 'Business key, kept for readability',
      station_reference STRING,
      reading_ts        TIMESTAMP NOT NULL COMMENT 'Reading time, UTC',
      reading_date      DATE      NOT NULL,
      value             DOUBLE,
      value_quality     STRING             COMMENT 'ok | recovered_from_list | recovered_from_pair',
      run_id            STRING             COMMENT 'The run that last wrote this row: used for rollback',
      loaded_at         TIMESTAMP
    ) CLUSTER BY (reading_date, measure_notation)
    COMMENT 'Readings from EA monitoring stations. Grain: one row per measure per timestamp.'
""")

# COMMAND ----------

# Silver is already one row per (measure, timestamp) and dim_measure is one row per measure,
# so this source is one row per MERGE key by construction.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW src_reading AS
    SELECT xxhash64(r.measure_notation) AS measure_key,
           m.station_key,
           r.measure_notation,
           m.station_reference,
           r.reading_ts,
           r.reading_date,
           r.value,
           r.value_quality,
           '{RUN_ID}'          AS run_id,
           current_timestamp() AS loaded_at
    FROM {CATALOG}.silver.readings r
    LEFT JOIN {CATALOG}.gold.dim_measure m ON r.measure_notation = m.measure_notation
    WHERE r.reading_date IN ({DATES_SQL})
""")

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "gold_fact_reading") as log:
    before = spark.sql(f"SELECT count(*) FROM {TABLE} WHERE reading_date IN ({DATES_SQL})").first()[0]

    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_reading s
          ON t.reading_date IN ({DATES_SQL})
         AND t.measure_notation = s.measure_notation
         AND t.reading_ts = s.reading_ts
        WHEN MATCHED AND (t.value IS DISTINCT FROM s.value
                          OR t.value_quality IS DISTINCT FROM s.value_quality) THEN
          UPDATE SET t.value = s.value, t.value_quality = s.value_quality,
                     t.run_id = s.run_id, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Latest reading loaded per measure: drives the catch-up for stations that go quiet and come back.
    spark.sql(f"""
        MERGE INTO {CATALOG}.ops.measure_watermark w
        USING (SELECT measure_notation AS measure_id, max(reading_ts) AS last_reading_at
               FROM src_reading GROUP BY measure_notation) s
          ON w.measure_id = s.measure_id
        WHEN MATCHED AND s.last_reading_at > w.last_reading_at THEN
          UPDATE SET w.last_reading_at = s.last_reading_at, w.updated_at = current_timestamp(),
                     w.updated_by_run_id = '{RUN_ID}'
        WHEN NOT MATCHED THEN
          INSERT (measure_id, last_reading_at, updated_at, updated_by_run_id)
          VALUES (s.measure_id, s.last_reading_at, current_timestamp(), '{RUN_ID}')
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM src_reading)                                           AS rows_in,
               (SELECT count(*) FROM {TABLE} WHERE reading_date IN ({DATES_SQL}))          AS in_scope,
               (SELECT count(*) FROM {TABLE})                                              AS total,
               (SELECT count_if(station_reference IS NULL) FROM src_reading)               AS orphans
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.total)
    log["details"].update(dates=dates, rows_inserted=counts.in_scope - before,
                          readings_without_measure=counts.orphans)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# The grain holds: no measure has two rows at the same timestamp. Expect 0, on every run.
display(spark.sql(f"""
    SELECT count(*) AS duplicate_grain_rows FROM (
      SELECT measure_notation, reading_ts FROM {TABLE} GROUP BY 1, 2 HAVING count(*) > 1
    )
"""))

# COMMAND ----------

# What an analyst would ask: the latest river level at each station, highest first
display(spark.sql(f"""
    WITH latest AS (
      SELECT *, row_number() OVER (PARTITION BY measure_key ORDER BY reading_ts DESC) AS rn
      FROM {TABLE}
    )
    SELECT s.station_reference, s.label AS station, s.river_name, s.town,
           m.qualifier, f.reading_ts, f.value, m.unit_name
    FROM latest f
    JOIN {CATALOG}.gold.dim_measure m ON f.measure_key = m.measure_key
    JOIN {CATALOG}.gold.dim_station s ON f.station_key = s.station_key AND s.is_current
    WHERE f.rn = 1 AND m.parameter = 'level' AND s.river_name IS NOT NULL
    ORDER BY f.value DESC
    LIMIT 20
"""))
