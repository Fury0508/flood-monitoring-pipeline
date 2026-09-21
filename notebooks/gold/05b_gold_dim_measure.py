# Databricks notebook source
# MAGIC %md
# MAGIC # 05b - Gold: dim_measure (Type 1)
# MAGIC
# MAGIC Publishes `silver.measures` as the dimension every reading joins to.
# MAGIC
# MAGIC This one is **Type 1**: measure metadata is static reference data (a 15-minute river level gauge in metres stays
# MAGIC exactly that), so keeping history would add joins and confusion for no analytical gain. If a unit or interval ever
# MAGIC does change, the latest value is the right one to report against.
# MAGIC
# MAGIC `latest_reading_ts` comes straight from the API and is what later tells us a silent station has started reporting again.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Silver run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_gold_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_silver_run_id(CATALOG, dbutils.widgets.get("run_id"), "measures")
TABLE = f"{CATALOG}.gold.dim_measure"
STARTED_AT = datetime.now(timezone.utc)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      measure_key          BIGINT NOT NULL COMMENT 'Surrogate key derived from the measure notation',
      measure_notation     STRING NOT NULL COMMENT 'Business key, e.g. 1491TH-level-stage-i-15_min-mASD',
      station_key          BIGINT          COMMENT 'Joins to dim_station.station_key',
      station_reference    STRING,
      label                STRING,
      parameter            STRING          COMMENT 'level | flow | rainfall | wind | temperature ...',
      parameter_name       STRING,
      qualifier            STRING          COMMENT 'Stage, Downstream Stage, Tipping Bucket Raingauge ...',
      period_seconds       INT             COMMENT '900 = every 15 minutes, 60 = every minute',
      unit                 STRING,
      unit_name            STRING,
      value_type           STRING,
      datum_type           STRING,
      latest_reading_ts    TIMESTAMP       COMMENT 'Last reading the API reports for this measure',
      latest_reading_value DOUBLE,
      updated_at           TIMESTAMP,
      run_id               STRING
    ) COMMENT 'Measures taken at monitoring stations. Type 1: always the current definition.'
""")

# COMMAND ----------

raw_source = (spark.table(f"{CATALOG}.silver.measures")
    .select(
        durable_key("measure_notation").alias("measure_key"),
        "measure_notation",
        durable_key("station_reference").alias("station_key"),
        "station_reference", "label", "parameter", "parameter_name", "qualifier",
        "period_seconds", "unit", "unit_name", "value_type", "datum_type",
        "latest_reading_ts", "latest_reading_value",
    )
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID)))

# MERGE needs one row per measure_notation: keep the one that reported most recently.
source, duplicates_resolved = one_row_per_key(
    raw_source,
    ["measure_notation"],
    [F.col("latest_reading_ts").desc_nulls_last(), F.col("station_reference").isNull(), F.col("label")],
)

source.createOrReplaceTempView("src_measure")
print(f"{source.count():,} measures ({duplicates_resolved:,} duplicate notations resolved)")

# COMMAND ----------

details = {}
status = "FAILED"
rows_in = source.count()
rows_out = 0

try:
    before = spark.sql(f"SELECT count(*) AS n FROM {TABLE}").first()["n"]

    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_measure s
          ON t.measure_notation = s.measure_notation
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    rows_out = spark.table(TABLE).count()
    details = {
        "rows_in": rows_in,
        "duplicates_resolved": duplicates_resolved,
        "rows_before": before,
        "rows_after": rows_out,
        "new_measures": rows_out - before,
        "orphans": spark.sql(f"""
            SELECT count(*) AS n FROM {TABLE} m
            LEFT JOIN {CATALOG}.gold.dim_station d
              ON m.station_key = d.station_key AND d.is_current
            WHERE d.station_key IS NULL
        """).first()["n"],
    }
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "gold_dim_measure", status, STARTED_AT, rows_in, rows_out, 0, None, details)

print(f"{status}: {rows_out:,} measures ({details.get('new_measures', 0):,} new), "
      f"{details.get('orphans', 0):,} without a matching current station")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

display(spark.sql(
    f"SELECT m.measure_notation, s.label AS station, m.parameter, m.qualifier, m.period_seconds, "
    f"       m.unit_name, m.latest_reading_ts, m.latest_reading_value "
    f"FROM {TABLE} m LEFT JOIN {CATALOG}.gold.dim_station s "
    f"  ON m.station_key = s.station_key AND s.is_current "
    f"ORDER BY m.station_reference LIMIT 20"
))
