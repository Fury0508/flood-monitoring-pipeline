# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Silver helpers
# MAGIC
# MAGIC Not run on its own. Each Silver notebook loads this with `%run ../common/00_silver_common`.
# MAGIC
# MAGIC Silver turns Bronze's raw strings into typed, trustworthy data. Three rules:
# MAGIC
# MAGIC 1. **Clean what can be cleaned.** A value wrapped in a JSON list, or two readings joined with `|`,
# MAGIC    is recovered rather than thrown away.
# MAGIC 2. **Quarantine what cannot.** A bad row goes to `silver.quarantine` with a reason; the rest of the batch loads.
# MAGIC 3. **Keep the newest version.** The same row can arrive in several runs, so the latest copy wins.
# MAGIC
# MAGIC The cleaning rules are SQL functions, so every Silver notebook applies them in plain SQL.

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------

# The API sometimes wraps a value in a JSON list: '["http://.../statusActive"]'. Take the first element.
spark.sql("""
    CREATE OR REPLACE TEMPORARY FUNCTION first_element(v STRING) RETURNS STRING
    RETURN CASE WHEN startswith(v, '[') THEN get_json_object(v, '$[0]') ELSE v END
""")

# Number from text, recovering a JSON list or two readings joined with a pipe ('0.121|0.122').
# Anything still unparseable becomes null, which the caller quarantines.
spark.sql("""
    CREATE OR REPLACE TEMPORARY FUNCTION parse_double(v STRING) RETURNS DOUBLE
    RETURN try_cast(trim(split_part(
      CASE WHEN startswith(v, '[') THEN get_json_object(v, '$[0]') ELSE v END, '|', 1)) AS DOUBLE)
""")

# http://environment.data.gov.uk/flood-monitoring/id/stations/1029TH -> 1029TH
spark.sql("""
    CREATE OR REPLACE TEMPORARY FUNCTION last_segment(uri STRING) RETURNS STRING
    RETURN regexp_extract(uri, '([^/]+)$', 1)
""")

# COMMAND ----------


def ensure_quarantine_table(catalog: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.quarantine (
          entity          STRING  COMMENT 'stations | measures | readings',
          key_ref         STRING  COMMENT 'Station URI, measure URI or reading URI, when known',
          reason          STRING  COMMENT 'Why the row was rejected',
          detail          STRING  COMMENT 'The offending value',
          raw_item        STRING  COMMENT 'The API item exactly as received',
          run_id          STRING,
          quarantined_at  TIMESTAMP
        ) COMMENT 'Rows Silver could not trust. Nothing is deleted: bad rows are kept here with a reason.'
    """)
