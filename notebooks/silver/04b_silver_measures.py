# Databricks notebook source
# MAGIC %md
# MAGIC # 04b - Silver: measures
# MAGIC
# MAGIC Turns `bronze.measures` into a clean current snapshot of every measure (water level, flow, rainfall and the rest).
# MAGIC
# MAGIC | Field | Why it matters later |
# MAGIC |---|---|
# MAGIC | `measure_notation` | The key every reading joins on |
# MAGIC | `station_reference` | Links a reading to its station; derived from the station URI when the field is missing |
# MAGIC | `period_seconds` | 900 = every 15 minutes, 60 = every minute. Used to judge whether a station is silent |
# MAGIC | `latest_reading_ts` | When this measure last reported, straight from the API. Drives the catch-up for silent stations |
# MAGIC
# MAGIC A measure with no notation is quarantined: readings could never be attached to it.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Bronze run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_bronze_run_id(CATALOG, dbutils.widgets.get("run_id"), "measures")
TABLE = f"{CATALOG}.silver.measures"
STARTED_AT = datetime.now(timezone.utc)

bronze = spark.read.table(f"{CATALOG}.bronze.measures").filter(F.col("run_id") == RUN_ID)
print(f"Cleaning {bronze.count():,} measure rows from Bronze run {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean

# COMMAND ----------

deduplicated = (latest_rows(bronze, ["measure_uri"])
                .withColumn("measure_notation",
                            F.coalesce(F.col("notation"), notation_from_uri("measure_uri"))))

rejected = (deduplicated
            .filter(F.col("measure_notation").isNull() | (F.trim("measure_notation") == ""))
            .select(F.col("measure_uri").alias("key_ref"),
                    F.lit("missing_measure_notation").alias("reason"),
                    F.col("label").alias("detail"),
                    "raw_item"))

cleaned = (deduplicated
    .filter(F.col("measure_notation").isNotNull() & (F.trim("measure_notation") != ""))
    .select(
        F.trim("measure_notation").alias("measure_notation"),
        F.col("measure_uri"),
        # stationReference is sometimes absent, so fall back to the station URI
        F.coalesce(F.col("station_reference"), notation_from_uri("station_uri")).alias("station_reference"),
        F.col("station_uri"),
        F.col("label"),
        F.col("parameter"),
        F.col("parameter_name"),
        F.col("qualifier"),
        parse_int("period").alias("period_seconds"),
        notation_from_uri("unit").alias("unit"),
        F.col("unit_name"),
        F.col("value_type"),
        F.col("datum_type"),
        parse_timestamp("get_json_object(latest_reading, '$.dateTime')").alias("latest_reading_ts"),
        parse_double("get_json_object(latest_reading, '$.value')").alias("latest_reading_value"),
        F.col("unexpected_fields"),
        F.col("run_date"),
        F.col("run_id"),
        F.col("ingested_at"),
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

details = {}
status = "FAILED"
rows_in = deduplicated.count()
rows_out = 0
rows_rejected = 0

try:
    rows_rejected = write_quarantine(CATALOG, "measures", RUN_ID, rejected)

    (cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(TABLE))
    spark.sql(f"COMMENT ON TABLE {TABLE} IS "
              f"'Current snapshot of EA measures: one row per measure, typed and linked to its station.'")

    rows_out = spark.table(TABLE).count()
    details = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "by_parameter": {
            r["parameter"] or "null": r["n"]
            for r in spark.sql(f"SELECT parameter, count(*) AS n FROM {TABLE} GROUP BY parameter").collect()
        },
        "missing_station_reference": spark.sql(
            f"SELECT count(*) AS n FROM {TABLE} WHERE station_reference IS NULL").first()["n"],
    }
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "silver_measures", status, STARTED_AT,
                  rows_in, rows_out, rows_rejected, None, details)

print(f"{status}: {rows_out:,} measures loaded, {rows_rejected:,} quarantined")
print(details.get("by_parameter"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

display(spark.sql(
    f"SELECT measure_notation, station_reference, parameter, qualifier, period_seconds, unit_name, "
    f"       latest_reading_ts, latest_reading_value "
    f"FROM {TABLE} ORDER BY station_reference LIMIT 20"
))

# COMMAND ----------

# How stale is each measure right now, according to the API itself?
display(spark.sql(f"""
    SELECT CASE
             WHEN latest_reading_ts IS NULL THEN 'never reported'
             WHEN latest_reading_ts > current_timestamp() - INTERVAL 24 HOURS THEN 'within 24 hours'
             WHEN latest_reading_ts > current_timestamp() - INTERVAL 7 DAYS THEN '1 to 7 days ago'
             ELSE 'over 7 days ago'
           END AS last_reported,
           count(*) AS measures
    FROM {TABLE}
    GROUP BY 1 ORDER BY measures DESC
"""))
