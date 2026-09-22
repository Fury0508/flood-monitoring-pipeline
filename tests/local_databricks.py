"""
Runs the real Databricks notebooks locally on open-source Spark, with the API replaced by fixtures.

The notebooks are executed unchanged. Only the Delta/Databricks-specific statements are emulated here:
INSERT ... REPLACE WHERE (including Delta's check that every new row satisfies the predicate),
CREATE OR REPLACE TABLE AS, CLUSTER BY, and MERGE (syntax-checked by Spark's parser, then applied with the
same upsert semantics). Everything else, including every SELECT and SQL function, runs on real Spark.
"""
import json
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import fixtures as fx
from pyspark.sql import SparkSession

REPO = Path(__file__).resolve().parents[1]
CATALOG = "spark_catalog"
VOL = Path(f"/Volumes/{CATALOG}/landing/raw_files")

real = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="warehouse-")).getOrCreate())
real.sparkContext.setLogLevel("ERROR")
LOG = []


def overwrite(table, rows):
    df = real.createDataFrame(rows, real.table(table).schema)
    df.cache().count()
    tmp = table + "_tmp_swap"
    df.write.mode("overwrite").saveAsTable(tmp)
    real.sql(f"DROP TABLE {table}")
    real.sql(f"ALTER TABLE {tmp} RENAME TO {table.split('.', 1)[1] if table.count('.') == 2 else table}")


def strip_ddl(sql):
    sql = re.sub(r"CLUSTER BY \([^)]*\)", "", sql)
    return re.sub(r"\bNOT NULL\b", "", sql)


def emulate_merge(sql):
    real._jsparkSession.sessionState().sqlParser().parsePlan(sql)  # syntax check
    target = re.search(r"MERGE INTO\s+(\S+)", sql).group(1)
    src = re.search(r"USING\s+(\w+)\s+s\b", sql)
    if target.endswith("gold.dim_station"):
        if "WHEN MATCHED AND t.attr_hash" in sql:
            changed = real.sql(f"SELECT count(*) FROM src_station s JOIN ({'SELECT * FROM ' + target + ' WHERE is_current'}) t "
                               "ON s.station_reference = t.station_reference WHERE s.attr_hash <> t.attr_hash").first()[0]
            LOG.append(f"dim_station changed versions: {changed}")
            assert changed == 0, "attr_hash changed for identical data"
        elif "WHEN NOT MATCHED THEN INSERT" in sql:
            new = real.sql(f"""SELECT xxhash64(concat(s.station_reference, '|', cast(s.snapshot_ts AS STRING))), s.station_key,
                s.station_reference, s.label, s.river_name, s.catchment_name, s.town, s.status, s.latitude, s.longitude,
                s.easting, s.northing, s.date_opened, s.station_types, s.measure_count, s.attr_hash, s.snapshot_ts,
                CAST(NULL AS TIMESTAMP), true, 'r' FROM src_station s LEFT ANTI JOIN
                (SELECT * FROM {target} WHERE is_current) t ON s.station_reference = t.station_reference""").collect()
            overwrite(target, real.table(target).collect() + new)
        return
    keys = {"gold.dim_measure": ["measure_notation"], "gold.fact_reading": ["measure_notation", "reading_ts"],
            "ops.measure_watermark": ["measure_id"]}[target.split(".", 1)[1]]
    if target.endswith("measure_watermark"):
        s = real.sql("SELECT measure_notation AS measure_id, max(reading_ts) AS last_reading_at, current_timestamp() AS updated_at,"
                     " 'r' AS updated_by_run_id FROM src_reading GROUP BY measure_notation")
        s = (s.unionByName(real.table(target)).groupBy("measure_id").agg({"last_reading_at": "max"})
             .selectExpr("measure_id", "`max(last_reading_at)` AS last_reading_at",
                         "current_timestamp() AS updated_at", "'r' AS updated_by_run_id"))
        overwrite(target, s.collect())
        return
    s = real.table(src.group(1))
    assert s.columns == real.table(target).columns, f"INSERT * column mismatch for {target}: {s.columns}"
    kept = real.table(target).join(s.select(*keys), keys, "left_anti").select(*real.table(target).columns)
    overwrite(target, kept.collect() + s.collect())


def emulate_replace_where(sql):
    m = re.search(r"INSERT INTO\s+(\S+)\s+REPLACE WHERE\s+(.*?)\s+(SELECT\b.*)$", sql, re.S)
    table, pred, query = m.groups()
    new = real.sql(query)
    new = new.toDF(*real.table(table).columns)
    new.cache().count()
    new.createOrReplaceTempView("_new_rows")
    bad = real.sql(f"SELECT count(*) FROM _new_rows WHERE NOT coalesce({pred}, false)").first()[0]
    assert bad == 0, f"{bad} rows violate REPLACE WHERE {pred} (Delta would fail)"
    kept = real.sql(f"SELECT * FROM {table} WHERE NOT coalesce({pred}, false)").collect()
    overwrite(table, kept + new.collect())


class SparkProxy:
    def __getattr__(self, name):
        return getattr(real, name)

    def sql(self, sql, args=None, **kw):
        s = sql.strip()
        if s.startswith("MERGE INTO"):
            return emulate_merge(s)
        if "REPLACE WHERE" in s and s.startswith("INSERT INTO"):
            return emulate_replace_where(s)
        if s.startswith("CREATE OR REPLACE TABLE"):
            name = re.search(r"CREATE OR REPLACE TABLE\s+(\S+)", s).group(1)
            real.sql(f"DROP TABLE IF EXISTS {name}")
            s = s.replace("CREATE OR REPLACE TABLE", "CREATE TABLE", 1)
        if s.startswith("CREATE TABLE"):
            s = strip_ddl(s)
        return real.sql(s, args=args, **kw) if args else real.sql(s, **kw)


class Widgets:
    def __init__(self, values): self.values, self.defaults = values, {}
    def text(self, name, default, label=None): self.defaults[name] = default
    def dropdown(self, name, default, choices, label=None): self.defaults[name] = default
    def get(self, name): return str(self.values.get(name, self.defaults.get(name, "")))


class TaskValues:
    store = {}
    def set(self, key, value): TaskValues.store[key] = value


class DBUtils:
    def __init__(self, values):
        self.widgets = Widgets(values)
        self.jobs = type("J", (), {"taskValues": TaskValues()})()


def fake_fetch(path, params=None):
    """Stands in for the landing notebooks' fetch(): same return shape, fixture data."""
    if path == "/id/stations":
        items = fx.STATIONS
    elif path == "/id/measures":
        items = fx.MEASURES
    elif path == "/data/readings":
        items = fx.day_readings(params["date"], correction=os.environ.get("CORRECTION") == "1")
    elif path.startswith("/id/measures/") and path.endswith("/readings"):
        items = fx.catchup_readings() if fx.M3 in path else []
    else:
        raise ValueError(path)
    body = {"meta": {"version": "0.9"}, "items": items}
    return json.dumps(body).encode(), body


class FrozenDatetime(datetime):
    """What the notebooks see as `datetime`: now() is fixtures.NOW, so the days they fetch are fixed."""

    @classmethod
    def now(cls, tz=None):
        return fx.NOW if tz else fx.NOW.replace(tzinfo=None)


def run_notebook(path: Path, g: dict):
    cells = path.read_text().split("# COMMAND ----------")
    for cell in cells:
        lines = [line for line in cell.strip().splitlines() if line.strip()]
        if lines and lines[0].startswith("# Databricks notebook source"):
            lines = lines[1:]
        if not lines:
            continue
        if all(line.startswith("# MAGIC") for line in lines):
            first = lines[0][len("# MAGIC"):].strip()
            if first.startswith("%run"):
                run_notebook((path.parent / first.split()[1]).with_suffix(".py").resolve(), g)
                if "fetch" in g:
                    g["fetch"] = fake_fetch
                if "datetime" in g:
                    g["datetime"] = FrozenDatetime
            continue
        exec(compile("\n".join(lines), str(path), "exec"), g)


def run(rel, **widgets):
    g = {"spark": SparkProxy(), "dbutils": DBUtils({"catalog": CATALOG, **widgets}),
         "display": lambda df: df.show(20, truncate=60), "__name__": "__nb__"}
    print(f"\n######## {rel} {widgets}")
    run_notebook(REPO / "notebooks" / rel, g)
    return g


def setup():
    shutil.rmtree(f"/Volumes/{CATALOG}", ignore_errors=True)
    VOL.mkdir(parents=True, exist_ok=True)
    for s in ["landing", "bronze", "silver", "gold", "ops"]:
        real.sql(f"CREATE DATABASE IF NOT EXISTS {s}")
    real.sql("""CREATE TABLE ops.pipeline_run_log (run_id STRING, stage STRING, status STRING, started_at TIMESTAMP,
        finished_at TIMESTAMP, rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT, api_version STRING, details STRING)""")
    real.sql("CREATE TABLE ops.measure_watermark "
             "(measure_id STRING, last_reading_at TIMESTAMP, updated_at TIMESTAMP, updated_by_run_id STRING)")
