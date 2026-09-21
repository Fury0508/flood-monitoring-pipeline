# Databricks notebook source
# MAGIC %md
# MAGIC # 05a - Gold: dim_station (SCD Type 2)
# MAGIC
# MAGIC Publishes `silver.stations` as a dimension that keeps history.
# MAGIC
# MAGIC Station attributes do change: a station is suspended, a label is corrected, a river name is filled in.
# MAGIC Flood analysis is historical, so we need to know what a station looked like **at the time of a reading**,
# MAGIC not just today. That is why this dimension is Type 2 while `dim_measure` is Type 1.
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `station_sk` | One row per version of a station |
# MAGIC | `station_key` | Same for every version, so facts can join without following version changes |
# MAGIC | `valid_from`, `valid_to`, `is_current` | The window in which this version was the published truth |
# MAGIC | `attr_hash` | MD5 of the tracked attributes; a change in the hash is what opens a new version |
# MAGIC
# MAGIC Loading is two MERGEs: close the versions whose attributes changed, then insert the new versions.
# MAGIC Rerunning with the same data changes nothing, because the hashes match.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Silver run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ../common/00_gold_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_silver_run_id(CATALOG, dbutils.widgets.get("run_id"), "stations")
TABLE = f"{CATALOG}.gold.dim_station"
STARTED_AT = datetime.now(timezone.utc)

TRACKED = ["label", "river_name", "catchment_name", "town", "status",
           "latitude", "longitude", "easting", "northing", "station_types"]

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      station_sk        BIGINT    NOT NULL COMMENT 'Surrogate key: one row per version of a station',
      station_key       BIGINT    NOT NULL COMMENT 'Durable key: the same for every version of a station',
      station_reference STRING    NOT NULL COMMENT 'Business key, e.g. 1029TH',
      label             STRING,
      river_name        STRING,
      catchment_name    STRING,
      town              STRING,
      status            STRING             COMMENT 'Active | Closed | Suspended | Unknown | null',
      latitude          DOUBLE,
      longitude         DOUBLE,
      easting           INT,
      northing          INT,
      date_opened       DATE,
      station_types     ARRAY<STRING>,
      measure_count     INT,
      attr_hash         STRING             COMMENT 'MD5 of the tracked attributes',
      valid_from        TIMESTAMP NOT NULL,
      valid_to          TIMESTAMP          COMMENT 'Null while this version is current',
      is_current        BOOLEAN   NOT NULL,
      run_id            STRING
    ) COMMENT 'Monitoring stations, slowly changing dimension type 2'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source

# COMMAND ----------

source = (spark.table(f"{CATALOG}.silver.stations")
    .select("station_reference", "label", "river_name", "catchment_name", "town", "status",
            "latitude", "longitude", "easting", "northing", "date_opened", "station_types",
            "measure_count", "ingested_at")
    .withColumn("station_key", durable_key("station_reference"))
    .withColumn("attr_hash", attr_hash(TRACKED))
    .withColumnRenamed("ingested_at", "snapshot_ts"))

source.createOrReplaceTempView("src_station")
print(f"{source.count():,} stations in Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC Step 1 closes versions whose attributes changed. Step 2 inserts the new version, and any station seen
# MAGIC for the first time. Step 3 closes stations the API has stopped publishing.

# COMMAND ----------

details = {}
status = "FAILED"
rows_in = source.count()
rows_out = 0

try:
    before = spark.sql(f"SELECT count(*) AS n FROM {TABLE} WHERE is_current").first()["n"]

    # 1. Close the current version of any station whose tracked attributes changed
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_station s
          ON t.station_reference = s.station_reference AND t.is_current
        WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN
          UPDATE SET t.is_current = false, t.valid_to = s.snapshot_ts
    """)

    # 2. Insert the new version (and brand new stations). Rows closed in step 1 are no longer current,
    #    so they no longer match and fall into NOT MATCHED.
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_station s
          ON t.station_reference = s.station_reference AND t.is_current
        WHEN NOT MATCHED THEN INSERT (
          station_sk, station_key, station_reference, label, river_name, catchment_name, town, status,
          latitude, longitude, easting, northing, date_opened, station_types, measure_count,
          attr_hash, valid_from, valid_to, is_current, run_id
        ) VALUES (
          xxhash64(concat(s.station_reference, '|', cast(s.snapshot_ts AS STRING))), s.station_key,
          s.station_reference, s.label, s.river_name, s.catchment_name, s.town, s.status,
          s.latitude, s.longitude, s.easting, s.northing, s.date_opened, s.station_types, s.measure_count,
          s.attr_hash, s.snapshot_ts, NULL, true, '{RUN_ID}'
        )
    """)

    # 3. A station the API no longer publishes keeps its history but stops being current
    retired = spark.sql(f"""
        MERGE INTO {TABLE} t
        USING (SELECT station_reference FROM src_station) s
          ON t.station_reference = s.station_reference
        WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
          UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
    """)

    after = spark.sql(f"SELECT count(*) AS n FROM {TABLE} WHERE is_current").first()["n"]
    rows_out = spark.table(TABLE).count()
    details = {
        "rows_in": rows_in,
        "current_before": before,
        "current_after": after,
        "total_versions": rows_out,
        "versions_closed_today": spark.sql(
            f"SELECT count(*) AS n FROM {TABLE} WHERE valid_to >= current_date()").first()["n"],
    }
    status = "SUCCEEDED"

except Exception as exc:
    details["error"] = f"{type(exc).__name__}: {exc}"
    raise

finally:
    write_run_log(CATALOG, RUN_ID, "gold_dim_station", status, STARTED_AT, rows_in, rows_out, 0, None, details)

print(f"{status}: {details.get('current_after'):,} current stations, {rows_out:,} rows including history")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

display(spark.sql(
    f"SELECT station_reference, label, status, river_name, town, latitude, longitude, "
    f"       valid_from, valid_to, is_current "
    f"FROM {TABLE} WHERE is_current ORDER BY station_reference LIMIT 20"
))

# COMMAND ----------

# Any station with more than one version: this is the Type 2 history in action.
# On a first load there will be none, which is correct.
display(spark.sql(f"""
    SELECT station_reference, count(*) AS versions, min(valid_from) AS first_seen, max(valid_from) AS latest_version
    FROM {TABLE}
    GROUP BY station_reference
    HAVING count(*) > 1
    ORDER BY versions DESC
    LIMIT 20
"""))
