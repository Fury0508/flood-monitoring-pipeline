# Databricks notebook source
# MAGIC %md
# MAGIC # 04a - Silver: stations
# MAGIC
# MAGIC Turns `bronze.stations` into a clean current snapshot of every monitoring station.
# MAGIC
# MAGIC | Problem found in exploration | What Silver does |
# MAGIC |---|---|
# MAGIC | `status` missing on over half the stations | Left as null; the station is still loaded |
# MAGIC | `status` sometimes a JSON list, sometimes a URI | Normalised to Active / Closed / Suspended / Unknown, original kept in `status_raw` |
# MAGIC | 632 stations without coordinates | Kept as null; a station with no map position is still a valid station |
# MAGIC | A station with no reference | Quarantined: without a key it cannot join to anything |
# MAGIC
# MAGIC The table is rewritten each run, because it is a snapshot of what the API currently publishes.
# MAGIC Every previous version stays in Bronze and in Delta history.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Bronze run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_silver_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_bronze_run_id(CATALOG, dbutils.widgets.get("run_id"), "stations")
TABLE = f"{CATALOG}.silver.stations"
STARTED_AT = datetime.now(timezone.utc)

bronze = spark.read.table(f"{CATALOG}.bronze.stations").filter(F.col("run_id") == RUN_ID)
print(f"Cleaning {bronze.count():,} station rows from Bronze run {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean

# COMMAND ----------

deduplicated = latest_rows(bronze, ["station_uri"])

# A station with no reference cannot be joined to its measures, so it is quarantined rather than loaded.
rejected = (deduplicated
            .filter(F.col("station_reference").isNull() | (F.trim("station_reference") == ""))
            .select(F.col("station_uri").alias("key_ref"),
                    F.lit("missing_station_reference").alias("reason"),
                    F.col("label").alias("detail"),
                    "raw_item"))

cleaned = (with_clean_status(deduplicated.filter(F.col("station_reference").isNotNull()
                                                 & (F.trim("station_reference") != "")))
    .select(
        F.trim("station_reference").alias("station_reference"),
        F.col("station_uri"),
        F.col("label"),
        F.col("river_name"),
        F.col("catchment_name"),
        F.col("town"),
        F.col("status"),
        F.col("status_raw"),
        parse_double("latitude").alias("latitude"),
        parse_double("longitude").alias("longitude"),
        parse_int("easting").alias("easting"),
        parse_int("northing").alias("northing"),
        parse_date("date_opened").alias("date_opened"),
        uri_list_to_names("station_type").alias("station_types"),
        F.expr("size(from_json(measures, 'array<string>'))").alias("measure_count"),
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
    rows_rejected = write_quarantine(CATALOG, "stations", RUN_ID, rejected)

    (cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(TABLE))
    spark.sql(f"COMMENT ON TABLE {TABLE} IS "
              f"'Current snapshot of EA monitoring stations: status normalised, coordinates typed.'")

    rows_out = spark.table(TABLE).count()
    details = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "status_breakdown": {
            r["status"] or "null": r["n"]
            for r in spark.sql(f"SELECT status, count(*) AS n FROM {TABLE} GROUP BY status").collect()
        },
        "missing_coordinates": spark.sql(
            f"SELECT count(*) AS n FROM {TABLE} WHERE latitude IS NULL OR longitude IS NULL").first()["n"],
    }
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "silver_stations", status, STARTED_AT,
                  rows_in, rows_out, rows_rejected, None, details)

print(f"{status}: {rows_out:,} stations loaded, {rows_rejected:,} quarantined")
print(details.get("status_breakdown"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

display(spark.sql(
    f"SELECT station_reference, label, status, status_raw, latitude, longitude, river_name, town, "
    f"       station_types, measure_count "
    f"FROM {TABLE} ORDER BY station_reference LIMIT 20"
))

# COMMAND ----------

# Stations whose status needed the list handling, and any that stayed Unknown
display(spark.sql(
    f"SELECT status, status_raw, count(*) AS stations FROM {TABLE} "
    f"WHERE status = 'Unknown' OR status_raw LIKE '[%' GROUP BY status, status_raw ORDER BY stations DESC"
))
