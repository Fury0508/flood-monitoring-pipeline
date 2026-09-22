# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Bronze helpers
# MAGIC
# MAGIC Not run on its own. Each Bronze notebook loads these with `%run ../common/00_bronze_common`.
# MAGIC
# MAGIC | Helper | Purpose |
# MAGIC |---|---|
# MAGIC | `read_landing` | Exposes the landed JSON files as a view `landed`: one row per API item, kept as raw text |
# MAGIC | `check_bronze` | Reconciles row counts against what landing saved, and reports unexpected fields (schema drift) |
# MAGIC
# MAGIC The load itself is plain SQL in each notebook.

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------


class ReconciliationError(Exception):
    """Bronze row counts do not match what the landing stage saved."""


def read_landing(path_glob: str) -> None:
    """
    Register the view `landed` (source_file, api_version, raw_item).
    Declaring items as ARRAY<STRING> makes Spark keep each JSON object as its original text,
    so nothing is interpreted or lost before Silver.
    """
    (spark.read.schema("meta STRUCT<version: STRING>, items ARRAY<STRING>")
        .option("multiLine", "true")
        .json(path_glob)
        .selectExpr("_metadata.file_path AS source_file", "meta.version AS api_version",
                    "explode(items) AS raw_item")
        .createOrReplaceTempView("landed"))


def check_bronze(log: dict, table: str, run_id: str, partition_col: str, expected: dict) -> None:
    """
    Compare row counts per partition with what landing saved, and count unexpected fields.
    A count mismatch fails the stage; drift is a warning, because a new field must never break a load.
    """
    actual = {str(r[0]): r[1] for r in spark.sql(
        f"SELECT {partition_col}, count(*) FROM {table} WHERE run_id = :run_id GROUP BY 1",
        args={"run_id": run_id}).collect()}
    drift = {r[0]: r[1] for r in spark.sql(
        f"SELECT field, count(*) FROM {table} LATERAL VIEW explode(unexpected_fields) AS field "
        "WHERE run_id = :run_id GROUP BY field", args={"run_id": run_id}).collect()}
    # Every partition must match, including ones landing never mentioned: extra rows mean files from another run.
    mismatches = {k: {"landing": expected.get(k, 0), "bronze": actual.get(k, 0)}
                  for k in sorted(set(expected) | set(actual)) if actual.get(k, 0) != expected.get(k, 0)}

    log["rows_in"] = sum(expected.values())
    log["rows_out"] = sum(actual.values())
    log["details"].update({"by_partition": actual, "drift": drift, "mismatches": mismatches, "warnings": []})
    if drift:
        log["details"]["warnings"].append(f"Unexpected fields from the API: {sorted(drift)}")
    if mismatches:
        raise ReconciliationError(f"{table}: Bronze counts differ from landing: {mismatches}")
