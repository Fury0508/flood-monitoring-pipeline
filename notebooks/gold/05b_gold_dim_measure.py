# Databricks notebook source
# MAGIC %md
# MAGIC # 05b - Gold: dim_measure (Type 1)
# MAGIC
# MAGIC Publishes `silver.measures` as the dimension every reading joins to.
# MAGIC
# MAGIC This one is **Type 1**: measure metadata is static reference data (a 15-minute river level gauge in metres stays
# MAGIC exactly that), so keeping history would add joins and confusion for no analytical gain.
# MAGIC
# MAGIC `latest_reading_ts` comes straight from the API and later tells us a silent station has started reporting again.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful silver run)")

# COMMAND ----------

# MAGIC %run ../common/00_pipeline_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "silver_measures")
TABLE = f"{CATALOG}.gold.dim_measure"

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

# MAGIC %md
# MAGIC ## Source: one row per measure (the one that reported most recently wins)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW src_measure AS
    SELECT measure_key, measure_notation, station_key, station_reference, label, parameter, parameter_name,
           qualifier, period_seconds, unit, unit_name, value_type, datum_type,
           latest_reading_ts, latest_reading_value, updated_at, '{RUN_ID}' AS run_id
    FROM (
      SELECT *,
             xxhash64(measure_notation)  AS measure_key,
             xxhash64(station_reference) AS station_key,
             current_timestamp()         AS updated_at,
             row_number() OVER (PARTITION BY measure_notation
                                ORDER BY latest_reading_ts DESC NULLS LAST, station_reference IS NULL, label) AS rn
      FROM {CATALOG}.silver.measures
    )
    WHERE rn = 1
""")

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "gold_dim_measure") as log:
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_measure s
          ON t.measure_notation = s.measure_notation
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM src_measure) AS rows_in,
               (SELECT count(*) FROM {TABLE})     AS rows_out,
               (SELECT count(*) FROM {TABLE} m
                  LEFT ANTI JOIN {CATALOG}.gold.dim_station d
                  ON m.station_key = d.station_key AND d.is_current) AS orphans
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.rows_out)
    log["details"]["measures_without_current_station"] = counts.orphans

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.measure_notation, s.label AS station, m.parameter, m.qualifier, m.period_seconds,
           m.unit_name, m.latest_reading_ts, m.latest_reading_value
    FROM {TABLE} m
    LEFT JOIN {CATALOG}.gold.dim_station s ON m.station_key = s.station_key AND s.is_current
    ORDER BY m.station_reference
    LIMIT 20
"""))
