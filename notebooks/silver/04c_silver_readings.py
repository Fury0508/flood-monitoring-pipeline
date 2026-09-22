# Databricks notebook source
# MAGIC %md
# MAGIC # 04c - Silver: readings
# MAGIC
# MAGIC Turns `bronze.readings` into typed, deduplicated readings.
# MAGIC
# MAGIC | Problem | What Silver does |
# MAGIC |---|---|
# MAGIC | Values are strings, and some are not plain numbers | `parse_double` recovers JSON lists and pipe-joined pairs, flagged in `value_quality` |
# MAGIC | NaN readings arrive with no value at all | Quarantined as `missing_value` |
# MAGIC | A reading can arrive in several runs | Deduplicated on measure and timestamp, newest copy wins |
# MAGIC | Timestamps are ISO text | Parsed to UTC; unparseable or future ones are quarantined |
# MAGIC
# MAGIC **Rebuild, not append.** The days this Bronze run touched are rebuilt from every Bronze row for those days,
# MAGIC so late-arriving readings are merged in and rerunning is always safe.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful bronze run)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "bronze_readings")
TABLE = f"{CATALOG}.silver.readings"
ensure_quarantine_table(CATALOG)

# Which days did this Bronze run bring in? Only those days are rebuilt.
dates = [str(r[0]) for r in spark.sql(
    f"SELECT DISTINCT reading_date FROM {CATALOG}.bronze.readings WHERE run_id = '{RUN_ID}' ORDER BY 1").collect()]
if not dates:
    raise RuntimeError(f"Bronze run {RUN_ID} has no readings to process.")
DATES_SQL = ", ".join(f"DATE'{d}'" for d in dates)
print(f"Rebuilding {len(dates)} day(s): {dates[0]} to {dates[-1]}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      measure_notation STRING    NOT NULL COMMENT 'Key that joins to silver.measures',
      measure_uri      STRING,
      reading_ts       TIMESTAMP NOT NULL COMMENT 'Reading time, UTC',
      reading_date     DATE      NOT NULL,
      value            DOUBLE,
      value_raw        STRING              COMMENT 'The value exactly as the API sent it',
      value_quality    STRING              COMMENT 'ok | recovered_from_list | recovered_from_pair',
      run_id           STRING,
      ingested_at      TIMESTAMP
    ) CLUSTER BY (reading_date, measure_notation)
    COMMENT 'Typed, deduplicated readings. One row per measure per timestamp.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse every Bronze reading for these days (from every run), keep the newest copy, and check it

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW readings_checked AS
    WITH parsed AS (
      SELECT reading_uri, measure_uri, date_time, value, raw_item, run_id, ingested_at,
             last_segment(measure_uri)   AS measure_notation,
             try_to_timestamp(date_time) AS reading_ts,
             parse_double(value)         AS value_double,
             CASE WHEN value IS NULL          THEN 'missing'
                  WHEN startswith(value, '[') THEN 'recovered_from_list'
                  WHEN contains(value, '|')   THEN 'recovered_from_pair'
                  ELSE 'ok' END          AS value_quality
      FROM {CATALOG}.bronze.readings
      WHERE reading_date IN ({DATES_SQL})
    ),
    latest AS (
      -- The same reading arrives in every run that re-fetches its day: the newest copy wins.
      SELECT * FROM (
        SELECT *, row_number() OVER (PARTITION BY measure_notation, coalesce(cast(reading_ts AS STRING), date_time)
                                     ORDER BY ingested_at DESC) AS rn
        FROM parsed
      )
      WHERE rn = 1
    )
    SELECT *,
           CASE WHEN nullif(measure_notation, '') IS NULL THEN 'missing_measure'
                WHEN reading_ts IS NULL                  THEN 'invalid_timestamp'
                WHEN value IS NULL                       THEN 'missing_value'
                WHEN value_double IS NULL                THEN 'non_numeric_value'
                WHEN reading_ts > current_timestamp() + INTERVAL 1 HOUR THEN 'future_timestamp'
           END AS rejection_reason
    FROM latest
""")

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "silver_readings") as log:
    spark.sql(f"""
        INSERT INTO {CATALOG}.silver.quarantine
          REPLACE WHERE run_id = '{RUN_ID}' AND entity = 'readings'
        SELECT 'readings', reading_uri, rejection_reason, concat_ws(' @ ', value, date_time), raw_item,
               '{RUN_ID}', current_timestamp()
        FROM readings_checked
        WHERE rejection_reason IS NOT NULL
    """)

    # REPLACE WHERE swaps only the days in scope; every other day in the table is untouched.
    spark.sql(f"""
        INSERT INTO {TABLE} REPLACE WHERE reading_date IN ({DATES_SQL})
        SELECT measure_notation, measure_uri, reading_ts, to_date(reading_ts) AS reading_date,
               value_double AS value, value AS value_raw, value_quality, run_id, ingested_at
        FROM readings_checked
        WHERE rejection_reason IS NULL
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM readings_checked)                                 AS rows_in,
               (SELECT count(*) FROM {TABLE} WHERE reading_date IN ({DATES_SQL}))     AS rows_out,
               (SELECT count(*) FROM readings_checked WHERE rejection_reason IS NOT NULL) AS rejected
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.rows_out, rows_rejected=counts.rejected)
    log["details"]["dates"] = dates  # gold_fact_reading merges exactly these days
    log["details"]["value_quality"] = {r[0]: r[1] for r in spark.sql(
        f"SELECT value_quality, count(*) FROM {TABLE} WHERE reading_date IN ({DATES_SQL}) GROUP BY 1").collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT reading_date, count(*) AS readings, count(DISTINCT measure_notation) AS measures
    FROM {TABLE}
    GROUP BY reading_date
    ORDER BY reading_date
"""))

# COMMAND ----------

# What went to quarantine in this run, and why
display(spark.sql(f"""
    SELECT entity, reason, count(*) AS rows
    FROM {CATALOG}.silver.quarantine
    WHERE run_id = '{RUN_ID}'
    GROUP BY entity, reason
    ORDER BY rows DESC
"""))
