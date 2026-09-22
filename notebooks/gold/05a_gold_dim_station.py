# Databricks notebook source
# MAGIC %md
# MAGIC # 05a - Gold: dim_station (SCD Type 2)
# MAGIC
# MAGIC Publishes `silver.stations` as a dimension that keeps history.
# MAGIC
# MAGIC Station attributes do change: a station is suspended, a label is corrected, a river name is filled in.
# MAGIC Flood analysis is historical, so analysts need to know what a station looked like **at the time of a
# MAGIC reading**, not just today. That is why this dimension is Type 2 while `dim_measure` is Type 1.
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `station_sk` | One row per version of a station |
# MAGIC | `station_key` | The same for every version, so facts can join without following version changes |
# MAGIC | `valid_from`, `valid_to`, `is_current` | The window in which this version was the published truth |
# MAGIC | `attr_hash` | MD5 of the tracked attributes; a change in the hash opens a new version |
# MAGIC
# MAGIC Rerunning with the same data changes nothing, because the hashes match.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = latest successful silver run)")

# COMMAND ----------

# MAGIC %run ../common/00_pipeline_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"), "silver_stations")
TABLE = f"{CATALOG}.gold.dim_station"

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
# MAGIC ## Source: one row per station
# MAGIC A few station URIs share a `station_reference`, and MERGE needs exactly one source row per key, so the most
# MAGIC complete one wins: newest snapshot, then a known status, then coordinates present, then the URI as a tie-break.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW src_station AS
    SELECT * FROM (
      SELECT station_reference, label, river_name, catchment_name, town, status,
             latitude, longitude, easting, northing, date_opened, station_types, measure_count,
             ingested_at               AS snapshot_ts,
             xxhash64(station_reference) AS station_key,
             md5(concat_ws('||',
               coalesce(cast(label          AS STRING), '~null~'),
               coalesce(cast(river_name     AS STRING), '~null~'),
               coalesce(cast(catchment_name AS STRING), '~null~'),
               coalesce(cast(town           AS STRING), '~null~'),
               coalesce(cast(status         AS STRING), '~null~'),
               coalesce(cast(latitude       AS STRING), '~null~'),
               coalesce(cast(longitude      AS STRING), '~null~'),
               coalesce(cast(easting        AS STRING), '~null~'),
               coalesce(cast(northing       AS STRING), '~null~'),
               coalesce(cast(station_types  AS STRING), '~null~'))) AS attr_hash,
             row_number() OVER (PARTITION BY station_reference
                                ORDER BY ingested_at DESC, status IS NULL, latitude IS NULL, station_uri) AS rn
      FROM {CATALOG}.silver.stations
    )
    WHERE rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC 1. Close the current version of any station whose attributes changed.
# MAGIC 2. Insert the new version, and any station seen for the first time.
# MAGIC 3. Close stations the API has stopped publishing; their history stays.

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "gold_dim_station") as log:
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING src_station s
          ON t.station_reference = s.station_reference AND t.is_current
        WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN
          UPDATE SET t.is_current = false, t.valid_to = s.snapshot_ts
    """)

    # Versions closed in step 1 are no longer current, so they fall into NOT MATCHED here.
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

    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING (SELECT station_reference FROM src_station) s
          ON t.station_reference = s.station_reference
        WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
          UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
    """)

    counts = spark.sql(f"""
        SELECT (SELECT count(*) FROM src_station)                              AS rows_in,
               (SELECT count(*) FROM {TABLE})                                  AS versions,
               (SELECT count_if(is_current) FROM {TABLE})                      AS current_rows,
               (SELECT count_if(valid_to >= current_date()) FROM {TABLE})      AS closed_today
    """).first()
    log.update(rows_in=counts.rows_in, rows_out=counts.versions)
    log["details"].update(current_stations=counts.current_rows, versions_closed_today=counts.closed_today)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: stations with more than one version (the Type 2 history; none on a first load)

# COMMAND ----------

display(spark.sql(f"""
    SELECT station_reference, count(*) AS versions, min(valid_from) AS first_seen, max(valid_from) AS latest_version
    FROM {TABLE}
    GROUP BY station_reference
    HAVING count(*) > 1
    ORDER BY versions DESC
    LIMIT 20
"""))
