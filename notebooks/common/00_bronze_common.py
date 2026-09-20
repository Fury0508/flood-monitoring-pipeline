# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared Bronze helpers
# MAGIC
# MAGIC Not run on its own. Each Bronze notebook loads these functions with `%run ./00_bronze_common`.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `resolve_landing_run_id` | Which landing run to load: widget, job task value, or latest successful run |
# MAGIC | `landing_details`, `expected_*_counts` | What the landing run saved, for reconciliation |
# MAGIC | `run_bronze` | Create table, load, reconcile against landing counts, detect drift, write the run log |
# MAGIC
# MAGIC The run log helpers come from `00_pipeline_common`, which this notebook loads with `%run`.

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


class ReconciliationError(Exception):
    """Bronze row counts do not match what the landing run saved."""


# Declaring items as ARRAY<STRING> makes Spark keep each JSON object as its raw text.
LANDING_FILE_SCHEMA = StructType([
    StructField("meta", StructType([StructField("version", StringType())])),
    StructField("items", ArrayType(StringType())),
])

# COMMAND ----------

def landing_stages(entity: str) -> list[str]:
    """Run-log stage names that can hold this entity's landing run ('landing' was the original combined notebook)."""
    return [f"landing_{entity}", "landing"]


def resolve_landing_run_id(catalog: str, widget_value: str, entity: str) -> str:
    """Widget value, else the value passed by this entity's landing task, else its latest successful landing run."""
    if widget_value.strip():
        return widget_value.strip()
    try:
        run_id = dbutils.jobs.taskValues.get(taskKey=f"landing_{entity}", key="run_id", debugValue="")
    except Exception:  # no upstream task when running interactively
        run_id = ""
    if run_id:
        return run_id
    row = spark.sql(
        f"SELECT run_id FROM {run_log_table(catalog)} "
        f"WHERE stage IN (:s1, :s2) AND status = 'SUCCEEDED' ORDER BY finished_at DESC LIMIT 1",
        args=dict(zip(["s1", "s2"], landing_stages(entity))),
    ).first()
    if row is None:
        raise RuntimeError(f"No successful {entity} landing run found. Run the landing_{entity} notebook first.")
    return row.run_id


def landing_details(catalog: str, run_id: str, entity: str) -> dict:
    """What this entity's landing run saved (files and item counts), used to reconcile Bronze."""
    row = spark.sql(
        f"SELECT details FROM {run_log_table(catalog)} "
        f"WHERE run_id = :run_id AND stage IN (:s1, :s2) AND status = 'SUCCEEDED' "
        f"ORDER BY finished_at DESC LIMIT 1",
        args={"run_id": run_id, **dict(zip(["s1", "s2"], landing_stages(entity)))},
    ).first()
    if row is None:
        raise RuntimeError(f"No successful {entity} landing run {run_id}, so there is nothing to load.")
    return json.loads(row.details)


def expected_reference_counts(landing: dict, entity: str) -> dict:
    """Expected rows for stations or measures, keyed by the run_date in the landed file path."""
    run_date = re.search(r"run_date=(\d{4}-\d{2}-\d{2})", landing["files"][entity]).group(1)
    return {run_date: landing["items"][entity]}


def expected_reading_counts(landing: dict) -> dict:
    """Expected rows per reading_date, as saved by the landing run."""
    return {k.removeprefix("readings_"): v for k, v in landing["items"].items() if k.startswith("readings_")}

# COMMAND ----------

def ensure_bronze_table(cfg: dict) -> None:
    """Create the Bronze table from the field mapping, so DDL and load can never drift apart."""
    part = cfg["partition_col"]
    columns = [f"{col} STRING" for col in cfg["fields"]] + [
        "raw_item STRING COMMENT 'The API item exactly as received'",
        "unexpected_fields ARRAY<STRING> COMMENT 'Keys not in the expected list: schema drift'",
        f"{part} DATE",
        "api_version STRING",
        "run_id STRING NOT NULL",
        "source_file STRING",
        "ingested_at TIMESTAMP",
    ]
    cluster = f"CLUSTER BY ({part})" if cfg.get("cluster") else ""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {cfg['table']} (\n  " + ",\n  ".join(columns) + f"\n) {cluster} "
        f"COMMENT '{cfg['comment']}'"
    )


def build_bronze(cfg: dict, run_id: str) -> DataFrame:
    """Read the landing JSON, keep each item raw, and extract the known fields as strings."""
    item_schema = StructType([StructField(key, StringType()) for key in cfg["fields"].values()])
    part = cfg["partition_col"]
    part_pattern = rf"{part}=(\d{{4}}-\d{{2}}-\d{{2}})"

    return (
        spark.read.schema(LANDING_FILE_SCHEMA).option("multiLine", "true").json(cfg["glob"])
        .select(
            F.col("meta.version").alias("api_version"),
            F.col("_metadata.file_path").alias("source_file"),
            F.explode("items").alias("raw_item"),
        )
        .withColumn("_parsed", F.from_json("raw_item", item_schema))
        .select(
            *[F.col("_parsed").getField(key).alias(col) for col, key in cfg["fields"].items()],
            "raw_item",
            F.array_except(
                F.expr("json_object_keys(raw_item)"),
                F.array(*[F.lit(k) for k in cfg["expected"]]),
            ).alias("unexpected_fields"),
            F.to_date(F.regexp_extract("source_file", part_pattern, 1)).alias(part),
            "api_version",
            F.lit(run_id).alias("run_id"),
            "source_file",
            F.current_timestamp().alias("ingested_at"),
        )
    )


def write_bronze(df: DataFrame, table: str, run_id: str) -> None:
    """Replace this run's rows, so a rerun never duplicates data."""
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"run_id = '{run_id}'")
       .saveAsTable(table))


def reconcile(cfg: dict, run_id: str, expected: dict) -> dict:
    """Compare Bronze row counts per partition with the counts the landing run logged."""
    part = cfg["partition_col"]
    actual = {
        str(r[part]): r["n"]
        for r in spark.sql(
            f"SELECT {part}, count(*) AS n FROM {cfg['table']} WHERE run_id = :run_id GROUP BY {part}",
            args={"run_id": run_id},
        ).collect()
    }
    mismatches = {k: {"landing": v, "bronze": actual.get(k, 0)} for k, v in expected.items() if actual.get(k, 0) != v}
    return {"rows": sum(actual.values()), "by_partition": actual, "mismatches": mismatches}


def drift_summary(table: str, run_id: str) -> dict:
    """Count how often each unexpected field appeared in this run."""
    rows = spark.sql(
        f"SELECT field, count(*) AS n FROM (SELECT explode(unexpected_fields) AS field FROM {table} "
        f"WHERE run_id = :run_id) GROUP BY field ORDER BY n DESC",
        args={"run_id": run_id},
    ).collect()
    return {r["field"]: r["n"] for r in rows}

# COMMAND ----------

def run_bronze(entity: str, cfg: dict, catalog: str, run_id: str, expected_counts: dict) -> dict:
    """Full Bronze load for one entity: create, load, reconcile, detect drift, log the run."""
    started_at = datetime.now(timezone.utc)
    stage = f"bronze_{entity}"
    details = {"warnings": []}
    rows_loaded = 0
    api_version = None
    status = "FAILED"

    try:
        ensure_bronze_table(cfg)
        write_bronze(build_bronze(cfg, run_id), cfg["table"], run_id)

        result = reconcile(cfg, run_id, expected_counts)
        rows_loaded = result["rows"]
        details.update(result)

        drift = drift_summary(cfg["table"], run_id)
        if drift:
            details["drift"] = drift
            details["warnings"].append(f"Unexpected fields: {sorted(drift)}")

        versions = [r.api_version for r in spark.sql(
            f"SELECT DISTINCT api_version FROM {cfg['table']} WHERE run_id = :run_id", args={"run_id": run_id}
        ).collect() if r.api_version]
        api_version = ",".join(sorted(versions)) or None

        if result["mismatches"]:
            raise ReconciliationError(f"{entity}: Bronze counts differ from landing: {result['mismatches']}")
        status = "SUCCEEDED"

    except Exception as exc:
        details["error"] = f"{type(exc).__name__}: {exc}"
        raise

    finally:
        write_run_log(catalog, run_id, stage, status, started_at,
                      sum(expected_counts.values()), rows_loaded, 0, api_version, details)

    print(f"{status}: {rows_loaded:,} {entity} rows loaded into {cfg['table']}")
    for warning in details["warnings"]:
        print(f"WARNING: {warning}")
    return details
