# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Catch-up: stations that went silent and came back
# MAGIC
# MAGIC The daily pulls re-fetch the last three days, which covers normal late data. They do **not** cover a station
# MAGIC that stopped reporting for longer and then sent its backlog, because those readings fall outside the window.
# MAGIC
# MAGIC 1. **Detect.** Compare when the API last heard from each measure (`dim_measure.latest_reading_ts`) with what
# MAGIC    we have loaded (`ops.measure_watermark`). A measure needs catching up when the API is ahead of us *and*
# MAGIC    the gap starts before the daily window.
# MAGIC 2. **Fetch.** Pull each affected measure with `since=<watermark>` and `_limit=10000`, checking for truncation.
# MAGIC 3. **Land.** Save the readings to the landing volume, one file per day, exactly like the daily readings.
# MAGIC
# MAGIC The catch-up job then runs the **same** Bronze, Silver and Gold readings notebooks on them, so there is only
# MAGIC one set of cleaning rules and one MERGE. When nothing needs catching up, those tasks are skipped.
# MAGIC
# MAGIC Gaps older than the API's four-week retention can't be recovered from the live API, so they are reported for
# MAGIC an archive backfill (`/archive/readings-<date>.csv`) instead of being silently ignored.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Run id (blank = generate)")
dbutils.widgets.text("lookback_days", "3", "Days already covered by the daily pulls")
dbutils.widgets.text("max_measures", "200", "Maximum measures to catch up in one run")
dbutils.widgets.text("retention_days", "28", "How far back the live API keeps readings")

# COMMAND ----------

# MAGIC %run ../common/00_landing_common

# COMMAND ----------

from collections import defaultdict

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"))
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
MAX_MEASURES = int(dbutils.widgets.get("max_measures"))
RETENTION_DAYS = int(dbutils.widgets.get("retention_days"))
READING_LIMIT = 10000  # the API's documented maximum

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Detect the gaps

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW measure_gaps AS
    SELECT m.measure_notation, m.station_reference, m.parameter, s.status,
           w.last_reading_at,
           m.latest_reading_ts,
           -- where our data ends: the watermark, or the edge of API retention if never loaded
           coalesce(w.last_reading_at, current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS) AS gap_start,
           timestampdiff(HOUR, coalesce(w.last_reading_at, current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS),
                         m.latest_reading_ts) AS gap_hours,
           coalesce(w.last_reading_at < current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS, false)
             AS beyond_retention
    FROM {CATALOG}.gold.dim_measure m
    LEFT JOIN {CATALOG}.gold.dim_station s ON m.station_key = s.station_key AND s.is_current
    LEFT JOIN {CATALOG}.ops.measure_watermark w ON w.measure_id = m.measure_notation
    WHERE m.latest_reading_ts IS NOT NULL
      -- the API has readings we do not...
      AND m.latest_reading_ts > coalesce(w.last_reading_at, current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS)
      -- ...and the gap starts before the window the daily pulls already re-fetch
      AND coalesce(w.last_reading_at, current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS)
          < current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
""")

display(spark.sql("SELECT * FROM measure_gaps ORDER BY gap_hours DESC LIMIT 20"))

to_fetch = spark.sql(f"""
    SELECT measure_notation, date_format(gap_start, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS since
    FROM measure_gaps WHERE NOT beyond_retention
    ORDER BY gap_hours DESC LIMIT {MAX_MEASURES}
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 and 3. Fetch each measure, then land the readings one file per day

# COMMAND ----------

with log_stage(CATALOG, RUN_ID, "landing_readings") as log:
    gaps = spark.sql("SELECT count(*) AS behind, count_if(beyond_retention) AS beyond FROM measure_gaps").first()
    by_day, truncated, failed, versions, no_timestamp = defaultdict(list), [], [], set(), 0

    for row in to_fetch:
        try:
            _, body = fetch(f"/id/measures/{row.measure_notation}/readings",
                            {"since": row.since, "_limit": READING_LIMIT})
        except Exception as exc:  # one bad measure must not stop the rest
            failed.append(f"{row.measure_notation}: {type(exc).__name__}")
            continue
        if len(body["items"]) >= READING_LIMIT:
            truncated.append(row.measure_notation)  # the next run continues from the new watermark
        versions.add((body.get("meta") or {}).get("version"))
        for item in body["items"]:
            day = (item.get("dateTime") or "")[:10]
            if day:
                by_day[day].append(item)
            else:
                no_timestamp += 1  # cannot be placed on a day; counted in the run log rather than dropped silently

    files, items = {}, {}
    for day, day_items in sorted(by_day.items()):
        raw = json.dumps({"meta": {"version": ",".join(sorted(v for v in versions if v))}, "items": day_items})
        files[f"readings_{day}"] = save_raw(raw.encode(), CATALOG, "readings", f"reading_date={day}", RUN_ID)
        items[f"readings_{day}"] = len(day_items)

    warnings = []
    if gaps.beyond:
        warnings.append(f"{gaps.beyond} measures have gaps older than {RETENTION_DAYS} days: "
                        "backfill them from the daily archive CSVs")
    if truncated:
        warnings.append(f"{len(truncated)} measures hit the {READING_LIMIT}-row limit and continue next run")
    if no_timestamp:
        warnings.append(f"{no_timestamp} readings had no dateTime and were not landed")
    if failed:
        warnings.append(f"{len(failed)} measures could not be fetched and will be retried next run")

    log["rows_in"] = log["rows_out"] = sum(items.values())
    log["details"] = {"source": "catchup", "files": files, "items": items, "warnings": warnings,
                      "measures_behind": gaps.behind, "measures_fetched": len(to_fetch),
                      "beyond_retention": gaps.beyond, "no_timestamp": no_timestamp,
                      "truncated": truncated[:50], "failed": failed[:50]}

# The job skips Bronze, Silver and Gold when nothing was landed.
dbutils.jobs.taskValues.set(key="readings_landed", value=log["rows_out"])
print(f"{log['rows_out']:,} readings landed for {len(to_fetch)} measures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stale measures
# MAGIC Silence is expected from a suspended or closed station and a problem from an active one, so the view keeps
# MAGIC that distinction and alerting only needs `WHERE alertable`.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.vw_stale_measures
    COMMENT 'Measures with no reading for over 24 hours. alertable = the station is active, so silence is unexpected.'
    AS
    SELECT m.measure_notation, m.station_reference, s.label AS station, s.river_name, s.status, m.parameter,
           w.last_reading_at,
           m.latest_reading_ts AS api_latest_reading_ts,
           timestampdiff(HOUR, w.last_reading_at, current_timestamp()) AS hours_since_last_reading,
           coalesce(s.status, 'Active') = 'Active' AS alertable
    FROM {CATALOG}.gold.dim_measure m
    LEFT JOIN {CATALOG}.gold.dim_station s ON m.station_key = s.station_key AND s.is_current
    LEFT JOIN {CATALOG}.ops.measure_watermark w ON w.measure_id = m.measure_notation
    WHERE w.last_reading_at IS NULL OR w.last_reading_at < current_timestamp() - INTERVAL 24 HOURS
""")

display(spark.sql(f"""
    SELECT alertable, status, count(*) AS measures
    FROM {CATALOG}.gold.vw_stale_measures
    GROUP BY alertable, status
    ORDER BY measures DESC
"""))
