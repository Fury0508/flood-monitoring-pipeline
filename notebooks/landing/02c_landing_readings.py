# Databricks notebook source
# MAGIC %md
# MAGIC # 02c - Landing: readings
# MAGIC
# MAGIC One call per day to `/data/readings?date=<date>` returns every reading from every station for that day
# MAGIC (about 490,000 rows). Each day is saved untouched to `landing/raw_files/readings/reading_date=<date>/`.
# MAGIC
# MAGIC | Mode | Days fetched | Why |
# MAGIC |---|---|---|
# MAGIC | `backfill` | Last 7 (including today) | The task asks for the past week |
# MAGIC | `incremental` | Last 3 (including today) | Stations send data once or twice a day, so readings arrive late. Re-fetching a short window catches them; Gold's MERGE makes the overlap harmless. |

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.dropdown("mode", "incremental", ["backfill", "incremental"], "Mode")
dbutils.widgets.text("backfill_days", "7", "Backfill days")
dbutils.widgets.text("lookback_days", "3", "Incremental lookback days")
dbutils.widgets.text("run_id", "", "Run id (blank = generate)")
dbutils.widgets.text("expected_api_version", "0.9", "Expected API version")

# COMMAND ----------

# MAGIC %run ../common/00_landing_common

# COMMAND ----------

from datetime import timedelta

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_run_id(CATALOG, dbutils.widgets.get("run_id"))
MODE = dbutils.widgets.get("mode")
DAYS = int(dbutils.widgets.get("backfill_days" if MODE == "backfill" else "lookback_days"))
if DAYS < 1:
    raise ValueError(f"Number of days to load must be at least 1, got {DAYS}")

today = datetime.now(timezone.utc).date()
reading_dates = sorted(today - timedelta(days=i) for i in range(DAYS))

calls = [
    {
        "key": f"readings_{d.isoformat()}",
        "path": "/data/readings",
        "params": {"date": d.isoformat()},
        "partition": f"reading_date={d.isoformat()}",
        "warn_if_empty": d < today,  # a completed day should never be empty; today may be early
    }
    for d in reading_dates
]

print(f"mode={MODE} | {len(calls)} days: {reading_dates[0]} to {reading_dates[-1]}\n")
run_landing("readings", calls, CATALOG, RUN_ID, dbutils.widgets.get("expected_api_version").strip())
