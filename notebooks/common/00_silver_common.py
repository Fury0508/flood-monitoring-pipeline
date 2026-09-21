# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared Silver helpers
# MAGIC
# MAGIC Not run on its own. Each Silver notebook loads these with `%run ./00_silver_common`.
# MAGIC
# MAGIC Silver is where the raw strings from Bronze become typed, trustworthy data. Three rules:
# MAGIC
# MAGIC 1. **Clean what can be cleaned.** A value wrapped in a JSON list, or two values separated by `|`, is recovered rather than thrown away.
# MAGIC 2. **Quarantine what cannot.** A bad row goes to `silver.quarantine` with a reason; the rest of the batch still loads.
# MAGIC 3. **Keep the newest version.** The same reading can arrive in several runs, so the latest copy wins.
# MAGIC
# MAGIC The cleaning rules are small named functions so they can be tested on their own.

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


# --- SQL fragments shared by the cleaning rules -------------------------------------------------

def first_element_sql(col: str) -> str:
    """If the value arrived as a JSON list (the API does this for some fields), take its first element."""
    return f"CASE WHEN startswith({col}, '[') THEN get_json_object({col}, '$[0]') ELSE {col} END"


def last_uri_segment_sql(sql: str) -> str:
    """http://.../id/stations/1029TH -> 1029TH"""
    return f"regexp_extract({sql}, '([^/]+)$', 1)"


# --- Cleaning rules ----------------------------------------------------------------------------

def parse_double(col: str) -> Column:
    """
    Value to double, recovering the two shapes the API uses for awkward values:
    a JSON list, and two readings joined with a pipe ('0.121|0.122'). Anything else becomes null,
    which the caller quarantines.
    """
    value = first_element_sql(col)
    return F.expr(f"try_cast(trim(split({value}, '\\\\|')[0] ) AS DOUBLE)")


def parse_timestamp(col: str) -> Column:
    """ISO 8601 text to a UTC timestamp; null when it cannot be parsed."""
    return F.expr(f"try_to_timestamp({col})")


def parse_date(col: str) -> Column:
    return F.expr(f"try_to_date({first_element_sql(col)})")


def parse_int(col: str) -> Column:
    return F.expr(f"try_cast({first_element_sql(col)} AS INT)")


def notation_from_uri(col: str) -> Column:
    return F.expr(last_uri_segment_sql(col))


def uri_list_to_names(col: str) -> Column:
    """'["http://.../SingleLevel"]' -> ['SingleLevel']; a single URI -> ['SingleLevel']."""
    return F.expr(
        f"transform("
        f"  CASE WHEN startswith({col}, '[') THEN from_json({col}, 'array<string>') "
        f"       WHEN {col} IS NULL THEN NULL ELSE array({col}) END,"
        f"  x -> regexp_extract(x, '([^/]+)$', 1))"
    )


def with_clean_status(df: DataFrame, col: str = "status") -> DataFrame:
    """
    Station status arrives three ways: missing, a URI ('.../statusActive'), or a JSON list of URIs.
    Normalise to Active / Closed / Suspended / Unknown, and keep the original in status_raw.
    """
    code = F.expr(f"regexp_replace({last_uri_segment_sql(first_element_sql(col))}, '^status', '')")
    return (
        df.withColumn("status_raw", F.col(col))
          .withColumn("_code", code)
          .withColumn(
              "status",
              F.when(F.col(col).isNull() | (F.trim(F.col("_code")) == ""), F.lit(None).cast("string"))
               .when(F.col("_code").isin("Active", "Closed", "Suspended"), F.col("_code"))
               .otherwise(F.lit("Unknown")),
          )
          .drop("_code")
    )


def latest_rows(df: DataFrame, keys: list[str], order_col: str = "ingested_at") -> DataFrame:
    """One row per key: the most recently ingested copy wins."""
    window = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# --- Quarantine --------------------------------------------------------------------------------

def quarantine_table(catalog: str) -> str:
    return f"{catalog}.silver.quarantine"


def ensure_quarantine_table(catalog: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table(catalog)} (
          entity          STRING  COMMENT 'stations | measures | readings',
          key_ref         STRING  COMMENT 'Station reference, measure notation or reading URI, when known',
          reason          STRING  COMMENT 'Why the row was rejected',
          detail          STRING  COMMENT 'The offending value',
          raw_item        STRING  COMMENT 'The API item exactly as received',
          run_id          STRING,
          quarantined_at  TIMESTAMP
        ) COMMENT 'Rows Silver could not trust. Nothing is deleted: bad rows are kept here with a reason.'
    """)


def write_quarantine(catalog: str, entity: str, run_id: str, rejected: DataFrame) -> int:
    """`rejected` needs the columns key_ref, reason, detail and raw_item."""
    ensure_quarantine_table(catalog)
    # No .cache() here: serverless compute rejects persist/cache with
    # NOT_SUPPORTED_WITH_SERVERLESS. The rejected rows are recomputed for the
    # count and the write, which is cheap - a quarantine batch is small by
    # definition, being only the rows that failed validation.
    out = (rejected
           .select(F.lit(entity).alias("entity"), "key_ref", "reason", "detail", "raw_item",
                   F.lit(run_id).alias("run_id"), F.current_timestamp().alias("quarantined_at")))
    count = out.count()
    if count:
        out.write.mode("append").saveAsTable(quarantine_table(catalog))
    return count

# COMMAND ----------

def resolve_bronze_run_id(catalog: str, widget_value: str, entity: str) -> str:
    """Widget value, else the value passed by this entity's Bronze task, else its latest successful Bronze run."""
    if widget_value.strip():
        return widget_value.strip()
    try:
        run_id = dbutils.jobs.taskValues.get(taskKey=f"bronze_{entity}", key="run_id", debugValue="")
    except Exception:  # no upstream task when running interactively
        run_id = ""
    if run_id:
        return run_id
    row = spark.sql(
        f"SELECT run_id FROM {run_log_table(catalog)} WHERE stage = :stage AND status = 'SUCCEEDED' "
        f"ORDER BY finished_at DESC LIMIT 1",
        args={"stage": f"bronze_{entity}"},
    ).first()
    if row is None:
        raise RuntimeError(f"No successful bronze_{entity} run found. Run the Bronze notebook first.")
    return row.run_id
