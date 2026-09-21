# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared Gold helpers
# MAGIC
# MAGIC Not run on its own. Each Gold notebook loads these with `%run ./00_gold_common`.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `resolve_silver_run_id` | Which Silver run to publish |
# MAGIC | `silver_details` | What that Silver run produced, including the days it rebuilt |
# MAGIC | `attr_hash` | MD5 of the attributes that matter, so Type 2 changes are detected cheaply |
# MAGIC | `durable_key` | Deterministic surrogate key from a business key, so no sequence is needed |

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------

from pyspark.sql import Column
from pyspark.sql import functions as F


def resolve_silver_run_id(catalog: str, widget_value: str, entity: str) -> str:
    """Widget value, else the value passed by this entity's Silver task, else its latest successful Silver run."""
    if widget_value.strip():
        return widget_value.strip()
    try:
        run_id = dbutils.jobs.taskValues.get(taskKey=f"silver_{entity}", key="run_id", debugValue="")
    except Exception:  # no upstream task when running interactively
        run_id = ""
    if run_id:
        return run_id
    row = spark.sql(
        f"SELECT run_id FROM {run_log_table(catalog)} WHERE stage = :stage AND status = 'SUCCEEDED' "
        f"ORDER BY finished_at DESC LIMIT 1",
        args={"stage": f"silver_{entity}"},
    ).first()
    if row is None:
        raise RuntimeError(f"No successful silver_{entity} run found. Run the Silver notebook first.")
    return row.run_id


def silver_details(catalog: str, run_id: str, entity: str) -> dict:
    row = spark.sql(
        f"SELECT details FROM {run_log_table(catalog)} "
        f"WHERE run_id = :run_id AND stage = :stage AND status = 'SUCCEEDED' "
        f"ORDER BY finished_at DESC LIMIT 1",
        args={"run_id": run_id, "stage": f"silver_{entity}"},
    ).first()
    if row is None:
        raise RuntimeError(f"No successful silver_{entity} run {run_id} to publish.")
    return json.loads(row.details)


def attr_hash(columns: list[str]) -> Column:
    """
    MD5 of the attributes we track for change. Nulls become a placeholder so that
    'value -> null' is still seen as a change rather than being ignored.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~null~")) for c in columns]
    return F.md5(F.concat_ws("||", *parts))


def durable_key(column: str) -> Column:
    """Deterministic surrogate key: the same business key always gives the same number, in any environment."""
    return F.xxhash64(F.col(column))
