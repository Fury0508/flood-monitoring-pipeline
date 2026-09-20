# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Explore the Environment Agency Flood Monitoring API
# MAGIC
# MAGIC **Purpose:** measure real volumes, limits and data quirks before designing the pipeline.
# MAGIC Every number printed here feeds a design decision, so note them down.
# MAGIC
# MAGIC API docs: https://environment.data.gov.uk/flood-monitoring/doc/reference

# COMMAND ----------

import io
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

BASE = "https://environment.data.gov.uk/flood-monitoring"
session = requests.Session()  # reuses connections; follows redirects by default


def get_json(path: str, params: dict | None = None) -> dict:
    resp = session.get(f"{BASE}{path}", params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Stations: how many, and is the list truncated?
# MAGIC If `meta.limit` is set, the API cut the list short and we must page with `_limit` / `_offset`.

# COMMAND ----------

stations_resp = get_json("/id/stations")
stations = stations_resp["items"]
meta = stations_resp["meta"]

print(f"Stations returned : {len(stations)}")
print(f"meta.limit        : {meta.get('limit')}  (None = no truncation)")
print(f"API version       : {meta.get('version')}")

status_counts = Counter(str(s.get("status", "missing")).split("/")[-1] for s in stations)
print(f"Status breakdown  : {dict(status_counts)}")

missing = {f: sum(1 for s in stations if s.get(f) in (None, "")) for f in ["lat", "long", "riverName", "town", "catchmentName"]}
print(f"Missing optional fields: {missing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measures: how many, and what types?

# COMMAND ----------

measures_resp = get_json("/id/measures")
measures = measures_resp["items"]

print(f"Measures returned : {len(measures)}")
print(f"meta.limit        : {measures_resp['meta'].get('limit')}")
print(f"By parameter      : {dict(Counter(m.get('parameter') for m in measures))}")
print(f"By period (secs)  : {dict(Counter(m.get('period') for m in measures).most_common(5))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One day from the archive CSV: size, columns and quirks
# MAGIC Uses the day before yesterday so the archive file is guaranteed to exist.
# MAGIC Values are read as strings on purpose, so we can see anything that isn't a clean number.

# COMMAND ----------

archive_day = (date.today() - timedelta(days=2)).isoformat()
archive_url = f"{BASE}/archive/readings-{archive_day}.csv"

resp = session.get(archive_url, timeout=600)
resp.raise_for_status()
print(f"Archive {archive_day}: {len(resp.content) / 1e6:.1f} MB")

day_df = pd.read_csv(io.BytesIO(resp.content), dtype=str)
print(f"Rows: {len(day_df):,}")
print(f"Columns: {day_df.columns.tolist()}")
display(day_df.head(10))

# COMMAND ----------

# Adjust these if the column names printed above are different
MEASURE_COL, DATETIME_COL, VALUE_COL = "measure", "dateTime", "value"

numeric = pd.to_numeric(day_df[VALUE_COL], errors="coerce")
non_numeric = day_df[numeric.isna() & day_df[VALUE_COL].notna()]
print(f"Non-numeric values : {len(non_numeric):,}")
display(non_numeric.head(10))

print(f"Null values        : {day_df[VALUE_COL].isna().sum():,}")
print(f"Duplicate (measure, dateTime) rows: {day_df.duplicated([MEASURE_COL, DATETIME_COL]).sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. High-volume measures: how uneven is the load?
# MAGIC A 15-minute measure gives ~96 readings/day. Anything far above that is a "high-volume" measure.

# COMMAND ----------

per_measure = day_df.groupby(MEASURE_COL).size()
print(per_measure.describe())
print("\nTop 10 busiest measures:")
display(per_measure.sort_values(ascending=False).head(10).reset_index(name="readings_per_day"))

print(f"\nEstimated rows for 7 days: {len(day_df) * 7:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Retention and late data for one measure
# MAGIC Confirms how far back the live API goes (docs say up to ~4 weeks).

# COMMAND ----------

sample_measure = "1491TH-level-stage-i-15_min-mASD"  # King's Mill, River Cherwell (example from the docs)
since = (datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%dT%H:%M:%SZ")

hist = get_json(f"/id/measures/{sample_measure}/readings", {"since": since, "_limit": 10000})["items"]
times = sorted(r["dateTime"] for r in hist)
print(f"Readings returned : {len(hist):,}")
print(f"Oldest available  : {times[0] if times else None}")
print(f"Newest available  : {times[-1] if times else None}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. One-day JSON pull for all stations (candidate incremental method)
# MAGIC Docs: a `date` query without `_sorted` / measure has no default limit.
# MAGIC Checks the size and speed of pulling a full day in one call.

# COMMAND ----------

start = datetime.now()
yesterday = (date.today() - timedelta(days=1)).isoformat()
day_json = get_json("/data/readings", {"date": yesterday})
elapsed = (datetime.now() - start).total_seconds()

items = day_json["items"]
print(f"Readings for {yesterday}: {len(items):,} in {elapsed:.0f}s")
print(f"meta.limit: {day_json['meta'].get('limit')}")
print(f"Items missing 'value' (NaN readings): {sum(1 for i in items if 'value' not in i):,}")
print(f"Items where value is not a number: {sum(1 for i in items if 'value' in i and not isinstance(i['value'], (int, float))):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings (fill in after running)
# MAGIC
# MAGIC | Question | Result |
# MAGIC |---|---|
# MAGIC | Number of stations / measures | |
# MAGIC | Is the stations list truncated? | |
# MAGIC | Archive CSV size per day | |
# MAGIC | Rows per day / estimated 7 days | |
# MAGIC | Non-numeric values found? | |
# MAGIC | Duplicate (measure, dateTime) rows? | |
# MAGIC | Busiest measure (readings/day) | |
# MAGIC | Oldest reading available via API | |
# MAGIC | One-day JSON pull: rows and seconds | |
