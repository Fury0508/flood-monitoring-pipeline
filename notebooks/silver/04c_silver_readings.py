# Databricks notebook source
# MAGIC %md
# MAGIC # 04c - Silver: readings
# MAGIC
# MAGIC Turns `bronze.readings` into typed, deduplicated readings.
# MAGIC
# MAGIC | Problem | What Silver does |
# MAGIC |---|---|
# MAGIC | Values are strings, and about 1 in 2,500 is not a plain number | `parse_double` recovers JSON lists and pipe-joined pairs (`0.121\|0.122`), flagged in `value_quality` |
# MAGIC | NaN readings arrive with no value at all | Quarantined as `missing_value` |
# MAGIC | A reading can arrive in several runs, because each run re-fetches the last few days | Deduplicated on measure and timestamp, newest copy wins |
# MAGIC | Timestamps are ISO text | Parsed to UTC; unparseable ones are quarantined |
# MAGIC
# MAGIC **Rebuild, not append.** The days touched by this Bronze run are rebuilt from every Bronze row for those days,
# MAGIC so late-arriving readings are merged in and rerunning is always safe.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Bronze run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_bronze_run_id(CATALOG, dbutils.widgets.get("run_id"), "readings")
TABLE = f"{CATALOG}.silver.readings"
BRONZE = f"{CATALOG}.bronze.readings"
STARTED_AT = datetime.now(timezone.utc)

# Which days did this Bronze run bring in?
dates = [r["reading_date"] for r in spark.sql(
    f"SELECT DISTINCT reading_date FROM {BRONZE} WHERE run_id = :run_id ORDER BY reading_date",
    args={"run_id": RUN_ID}).collect()]
if not dates:
    raise RuntimeError(f"Bronze run {RUN_ID} has no readings to process.")

date_list = ", ".join(f"'{d}'" for d in dates)
print(f"Rebuilding {len(dates)} day(s): {dates[0]} to {dates[-1]}")

# COMMAND ----------

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
# MAGIC ## Clean

# COMMAND ----------

# Every Bronze row for these days, from every run, so late arrivals are included.
bronze = spark.table(BRONZE).filter(F.col("reading_date").isin(dates))

parsed = (bronze
    .withColumn("measure_notation", notation_from_uri("measure_uri"))
    .withColumn("reading_ts", parse_timestamp("date_time"))
    .withColumn("value_double", parse_double("value"))
    .withColumn("value_quality",
                F.when(F.col("value").isNull(), F.lit("missing"))
                 .when(F.col("value").startswith("["), F.lit("recovered_from_list"))
                 .when(F.col("value").contains("|"), F.lit("recovered_from_pair"))
                 .otherwise(F.lit("ok"))))

rejection_reason = (
    F.when(F.col("measure_notation").isNull() | (F.trim("measure_notation") == ""), F.lit("missing_measure"))
     .when(F.col("reading_ts").isNull(), F.lit("invalid_timestamp"))
     .when(F.col("value").isNull(), F.lit("missing_value"))
     .when(F.col("value_double").isNull(), F.lit("non_numeric_value"))
     .when(F.col("reading_ts") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR"), F.lit("future_timestamp"))
)

checked = parsed.withColumn("rejection_reason", rejection_reason)

rejected = (checked.filter(F.col("rejection_reason").isNotNull())
            .select(F.col("reading_uri").alias("key_ref"),
                    F.col("rejection_reason").alias("reason"),
                    F.concat_ws(" @ ", F.col("value"), F.col("date_time")).alias("detail"),
                    "raw_item"))

# Newest copy of each reading wins, so a re-fetched day corrects itself.
cleaned = latest_rows(
    checked.filter(F.col("rejection_reason").isNull()),
    ["measure_notation", "reading_ts"],
).select(
    "measure_notation",
    "measure_uri",
    "reading_ts",
    F.to_date("reading_ts").alias("reading_date"),
    F.col("value_double").alias("value"),
    F.col("value").alias("value_raw"),
    "value_quality",
    "run_id",
    "ingested_at",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

details = {"dates": [str(d) for d in dates]}
status = "FAILED"
rows_in = bronze.count()
rows_out = 0
rows_rejected = 0

try:
    rows_rejected = write_quarantine(CATALOG, "readings", RUN_ID, rejected)

    # Replace only the days in scope; every other day in the table is untouched.
    (cleaned.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"reading_date IN ({date_list})")
        .saveAsTable(TABLE))

    per_day = spark.sql(
        f"SELECT reading_date, count(*) AS n, count(DISTINCT measure_notation) AS measures "
        f"FROM {TABLE} WHERE reading_date IN ({date_list}) GROUP BY reading_date ORDER BY reading_date"
    ).collect()
    rows_out = sum(r["n"] for r in per_day)
    details.update({
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "per_day": {str(r["reading_date"]): {"readings": r["n"], "measures": r["measures"]} for r in per_day},
        "value_quality": {
            r["value_quality"]: r["n"] for r in spark.sql(
                f"SELECT value_quality, count(*) AS n FROM {TABLE} WHERE reading_date IN ({date_list}) "
                f"GROUP BY value_quality").collect()
        },
    })
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "silver_readings", status, STARTED_AT,
                  rows_in, rows_out, rows_rejected, None, details)

print(f"{status}: {rows_out:,} readings loaded, {rows_rejected:,} quarantined")
print(details.get("value_quality"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

display(spark.sql(
    f"SELECT reading_date, count(*) AS readings, count(DISTINCT measure_notation) AS measures, "
    f"       round(avg(value), 3) AS avg_value "
    f"FROM {TABLE} GROUP BY reading_date ORDER BY reading_date"
))

# COMMAND ----------

# What ended up in quarantine, and why
display(spark.sql(
    f"SELECT entity, reason, count(*) AS rows FROM {quarantine_table(CATALOG)} "
    f"WHERE run_id = :run_id GROUP BY entity, reason ORDER BY rows DESC",
    args={"run_id": RUN_ID},
))
