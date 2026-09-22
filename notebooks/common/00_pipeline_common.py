# Databricks notebook source
# MAGIC %md
# MAGIC # Shared pipeline helpers
# MAGIC
# MAGIC Loaded by every notebook with `%run`. Three helpers:
# MAGIC
# MAGIC | Helper | Purpose |
# MAGIC |---|---|
# MAGIC | `resolve_run_id` | The run to process: the one the job passes in, or the latest successful run when run by hand |
# MAGIC | `stage_details` | What an earlier stage of a run recorded, e.g. the days landing fetched |
# MAGIC | `log_stage` | Writes one row to `ops.pipeline_run_log` for every stage, whether it succeeds or fails |

# COMMAND ----------

import json
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("stage", StringType(), False),
    StructField("status", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), True),
    StructField("rows_in", LongType(), True),
    StructField("rows_out", LongType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("api_version", StringType(), True),
    StructField("details", StringType(), True),
])


def resolve_run_id(catalog: str, widget_value: str, stage: str | None = None) -> str:
    """
    The run id passed in by the job. When a notebook is run by hand with no run id:
    a landing notebook (stage=None) starts a new run; any other notebook picks up the latest
    successful run of the stage it reads from.
    """
    run_id = widget_value.strip()
    if not run_id:
        if stage is None:
            return str(uuid.uuid4())
        row = spark.sql(
            f"SELECT run_id FROM {catalog}.ops.pipeline_run_log "
            "WHERE stage = :stage AND status = 'SUCCEEDED' ORDER BY finished_at DESC LIMIT 1",
            args={"stage": stage},
        ).first()
        if row is None:
            raise RuntimeError(f"No successful {stage} run found. Run that stage first.")
        run_id = row.run_id
    # run_id is interpolated into SQL, so only accept the characters job and uuid ids use.
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError(f"Invalid run_id: {run_id!r}")
    return run_id


def stage_details(catalog: str, run_id: str, stage: str) -> dict:
    """The details JSON a successful stage wrote to the run log."""
    row = spark.sql(
        f"SELECT details FROM {catalog}.ops.pipeline_run_log "
        "WHERE run_id = :run_id AND stage = :stage AND status = 'SUCCEEDED' ORDER BY finished_at DESC LIMIT 1",
        args={"run_id": run_id, "stage": stage},
    ).first()
    if row is None:
        raise RuntimeError(f"No successful {stage} stage for run {run_id}.")
    return json.loads(row.details)


@contextmanager
def log_stage(catalog: str, run_id: str, stage: str):
    """
    Records a stage in ops.pipeline_run_log whether it succeeds or fails:

        with log_stage(CATALOG, RUN_ID, "silver_readings") as log:
            ...
            log["rows_out"] = n

    A failure is recorded with its error and then re-raised, so the job task still fails and retries.
    """
    log = {"rows_in": None, "rows_out": None, "rows_rejected": 0, "api_version": None, "details": {}}
    started_at = datetime.now(timezone.utc)
    status = "FAILED"
    try:
        yield log
        status = "SUCCEEDED"
    except Exception as exc:
        log["details"]["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        row = (run_id, stage, status, started_at, datetime.now(timezone.utc),
               log["rows_in"], log["rows_out"], log["rows_rejected"], log["api_version"],
               json.dumps(log["details"], default=str))
        spark.createDataFrame([row], RUN_LOG_SCHEMA).write.mode("append").saveAsTable(
            f"{catalog}.ops.pipeline_run_log")
        print(f"{stage} {status}: rows_in={log['rows_in']} rows_out={log['rows_out']} "
              f"rejected={log['rows_rejected']}")
        for warning in log["details"].get("warnings", []):
            print(f"WARNING: {warning}")
