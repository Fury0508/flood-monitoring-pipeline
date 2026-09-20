# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared pipeline helpers
# MAGIC
# MAGIC Not run on its own. Loaded by `00_landing_common` and `00_bronze_common` with `%run`.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `new_run_id` | Use the run id passed in (the job run id in production), or generate one |
# MAGIC | `write_run_log` | Append one row per stage per run to `ops.pipeline_run_log` |

# COMMAND ----------

import json
import re
import uuid
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


def run_log_table(catalog: str) -> str:
    return f"{catalog}.ops.pipeline_run_log"


def new_run_id(widget_value: str) -> str:
    """In a job, all tasks receive the same id ({{job.run_id}}). Interactively, a new id is generated."""
    return widget_value.strip() or str(uuid.uuid4())


def write_run_log(catalog: str, run_id: str, stage: str, status: str, started_at: datetime,
                  rows_in: int, rows_out: int, rows_rejected: int,
                  api_version: str | None, details: dict) -> None:
    """Append one row describing a pipeline stage to ops.pipeline_run_log."""
    row = (run_id, stage, status, started_at, datetime.now(timezone.utc),
           rows_in, rows_out, rows_rejected, api_version, json.dumps(details, default=str))
    spark.createDataFrame([row], RUN_LOG_SCHEMA).write.mode("append").saveAsTable(run_log_table(catalog))
