# Databricks notebook source
# MAGIC %md
# MAGIC # 05c - Gold: fact_reading
# MAGIC
# MAGIC The fact table analysts and data scientists query.
# MAGIC
# MAGIC **Grain: one row per measure per reading timestamp.** Not per station, because a station can record several things
# MAGIC at once (level upstream, level downstream, flow), and each is its own series.
# MAGIC
# MAGIC **Idempotent MERGE.** Matching on measure and timestamp means re-running a day, or re-fetching a day that
# MAGIC arrived late, updates rows in place instead of duplicating them. The merge condition is also restricted to the days
# MAGIC in scope, so Delta skips the rest of the table instead of scanning it.
# MAGIC
# MAGIC **Watermarks.** After loading, the latest reading per measure is recorded in `ops.measure_watermark`.
# MAGIC That is what later detects a station that has been silent and has now started reporting again.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Silver run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_gold_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_silver_run_id(CATALOG, dbutils.widgets.get("run_id"), "readings")
TABLE = f"{CATALOG}.gold.fact_reading"
WATERMARK = f"{CATALOG}.ops.measure_watermark"
STARTED_AT = datetime.now(timezone.utc)

dates = silver_details(CATALOG, RUN_ID, "readings")["dates"]
date_list = ", ".join(f"'{d}'" for d in dates)
print(f"Publishing {len(dates)} day(s): {dates[0]} to {dates[-1]}")

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Source
# MAGIC Readings for the days in scope, with their measure and station keys attached.

# COMMAND ----------

readings = spark.table(f"{CATALOG}.silver.readings").filter(F.col("reading_date").isin(dates))
measures = spark.table(f"{CATALOG}.silver.measures").select(
    "measure_notation", "station_reference")

source = (readings.join(F.broadcast(measures), "measure_notation", "left")
    .select(
        durable_key("measure_notation").alias("measure_key"),
        durable_key("station_reference").alias("station_key"),
        "measure_notation",
        "station_reference",
        "reading_ts",
        "reading_date",
        "value",
        "value_quality",
        F.lit(RUN_ID).alias("run_id"),
        F.current_timestamp().alias("loaded_at"),
    ))

source.createOrReplaceTempView("src_reading")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load

# COMMAND ----------

details = {"dates": dates}
status = "FAILED"
rows_in = source.count()
rows_out = 0

try:
    before = spark.sql(
        f"SELECT count(*) AS n FROM {TABLE} WHERE reading_date IN ({date_list})").first()["n"]

    # reading_date in the ON clause lets Delta skip every file outside the days being loaded.
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_reading s
          ON t.reading_date IN ({date_list})
         AND t.measure_notation = s.measure_notation
         AND t.reading_ts = s.reading_ts
        WHEN MATCHED AND (t.value IS DISTINCT FROM s.value
                          OR t.value_quality IS DISTINCT FROM s.value_quality) THEN
          UPDATE SET t.value = s.value, t.value_quality = s.value_quality,
                     t.run_id = s.run_id, t.loaded_at = s.loaded_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Latest reading per measure: drives the catch-up for stations that go quiet and come back.
    spark.sql(f"""
        MERGE INTO {WATERMARK} w
        USING (
          SELECT measure_notation AS measure_id, max(reading_ts) AS last_reading_at
          FROM src_reading GROUP BY measure_notation
        ) s
          ON w.measure_id = s.measure_id
        WHEN MATCHED AND s.last_reading_at > w.last_reading_at THEN
          UPDATE SET w.last_reading_at = s.last_reading_at, w.updated_at = current_timestamp(),
                     w.updated_by_run_id = '{RUN_ID}'
        WHEN NOT MATCHED THEN
          INSERT (measure_id, last_reading_at, updated_at, updated_by_run_id)
          VALUES (s.measure_id, s.last_reading_at, current_timestamp(), '{RUN_ID}')
    """)

    after = spark.sql(
        f"SELECT count(*) AS n FROM {TABLE} WHERE reading_date IN ({date_list})").first()["n"]
    rows_out = spark.table(TABLE).count()
    details.update({
        "rows_in": rows_in,
        "rows_in_scope_before": before,
        "rows_in_scope_after": after,
        "rows_inserted": after - before,
        "rows_total": rows_out,
        "orphan_readings": spark.sql(
            f"SELECT count(*) AS n FROM {TABLE} WHERE reading_date IN ({date_list}) "
            f"AND station_reference IS NULL").first()["n"],
        "watermarks": spark.sql(f"SELECT count(*) AS n FROM {WATERMARK}").first()["n"],
    })
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "gold_fact_reading", status, STARTED_AT, rows_in, rows_out, 0, None, details)

print(f"{status}: {rows_out:,} rows in fact_reading "
      f"({details.get('rows_inserted', 0):,} new in the days loaded), "
      f"{details.get('watermarks', 0):,} watermarks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# The grain holds: no measure has two rows at the same timestamp
display(spark.sql(f"""
    SELECT count(*) AS duplicate_grain_rows FROM (
      SELECT measure_notation, reading_ts, count(*) AS n
      FROM {TABLE} GROUP BY measure_notation, reading_ts HAVING count(*) > 1
    )
"""))

# COMMAND ----------

# What an analyst would actually ask: latest river level per station
display(spark.sql(f"""
    SELECT s.station_reference, s.label AS station, s.river_name, s.town,
           m.qualifier, f.reading_ts, f.value, m.unit_name
    FROM {TABLE} f
    JOIN {CATALOG}.gold.dim_measure m ON f.measure_key = m.measure_key
    JOIN {CATALOG}.gold.dim_station s ON f.station_key = s.station_key AND s.is_current
    WHERE m.parameter = 'level'
      AND f.reading_ts = (SELECT max(reading_ts) FROM {TABLE} f2 WHERE f2.measure_key = f.measure_key)
      AND s.river_name IS NOT NULL
    ORDER BY f.value DESC
    LIMIT 20
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT reading_date, count(*) AS readings, count(DISTINCT measure_notation) AS measures,
           count(DISTINCT station_reference) AS stations
    FROM {TABLE} GROUP BY reading_date ORDER BY reading_date
"""))
