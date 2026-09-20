-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Unity Catalog setup
-- MAGIC
-- MAGIC Creates the catalog, medallion schemas, landing volume, operational tables and access grants.
-- MAGIC Every statement is idempotent, so this notebook is safe to rerun.
-- MAGIC
-- MAGIC | Schema | Purpose | Who reads it |
-- MAGIC |---|---|---|
-- MAGIC | `landing` | Raw API responses as files (volume) | Data engineers |
-- MAGIC | `bronze` | Raw data in Delta + ingestion metadata | Data engineers |
-- MAGIC | `silver` | Typed, validated, deduplicated data | Engineers, data scientists |
-- MAGIC | `gold` | Dimensional model | Engineers, analysts, data scientists |
-- MAGIC | `ops` | Run log and watermarks | Data engineers |
-- MAGIC | `sandbox` | Data scientists' own tables | Data scientists |

-- COMMAND ----------

-- If your workspace does not allow CREATE CATALOG, use the existing workspace catalog
-- and replace `flood_monitoring` throughout the project.
CREATE CATALOG IF NOT EXISTS flood_monitoring
COMMENT 'Environment Agency real-time flood monitoring data. Source: https://environment.data.gov.uk/flood-monitoring. Licence: Open Government Licence v3.0. Contains no personal data.';

-- COMMAND ----------

USE CATALOG flood_monitoring;

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Raw API responses stored exactly as received, for replay and audit';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw data loaded into Delta with ingestion metadata; no business rules applied';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Typed, validated and deduplicated data; rejected rows go to quarantine tables';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Dimensional model (dim_station, dim_measure, fact_reading) for analysis';
CREATE SCHEMA IF NOT EXISTS ops     COMMENT 'Pipeline control and monitoring tables';
CREATE SCHEMA IF NOT EXISTS sandbox COMMENT 'Workspace for data scientists to create their own tables and features';

-- COMMAND ----------

-- Landing zone layout: /stations/<run_date>/, /measures/<run_date>/, /readings/<reading_date>/
CREATE VOLUME IF NOT EXISTS landing.raw_files
COMMENT 'Landing zone for raw JSON files downloaded from the API';

-- COMMAND ----------

-- One row per stage per run. Feeds monitoring, alerting and the Q8 silent-failure checks.
CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
  run_id         STRING    NOT NULL COMMENT 'Unique id shared by all stages of one job run',
  stage          STRING    NOT NULL COMMENT 'landing | bronze | silver | gold',
  status         STRING    NOT NULL COMMENT 'STARTED | SUCCEEDED | FAILED',
  started_at     TIMESTAMP NOT NULL,
  finished_at    TIMESTAMP,
  rows_in        BIGINT,
  rows_out       BIGINT,
  rows_rejected  BIGINT,
  api_version    STRING             COMMENT 'meta.version returned by the API; a change triggers an alert',
  details        STRING             COMMENT 'JSON: dates loaded, errors, schema drift, silent measures'
)
COMMENT 'Pipeline run log: one row per stage per run';

-- COMMAND ----------

-- Latest reading loaded per measure. Drives catch-up for stations silent longer than the lookback window.
CREATE TABLE IF NOT EXISTS ops.measure_watermark (
  measure_id         STRING    NOT NULL,
  last_reading_at    TIMESTAMP NOT NULL,
  updated_at         TIMESTAMP NOT NULL,
  updated_by_run_id  STRING
)
COMMENT 'Latest reading timestamp loaded per measure';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access control (RBAC)
-- MAGIC Least privilege: analysts see only Gold; data scientists also see Silver and own a sandbox.
-- MAGIC
-- MAGIC The grants below are left in place as documentation but are commented out.
-- MAGIC Databricks Free Edition cannot create account groups, so `data_engineers`,
-- MAGIC `data_analysts` and `data_scientists` do not exist and every GRANT fails with
-- MAGIC a principal-not-found error. On a workspace where the groups can be created
-- MAGIC (**Settings → Identity and access → Groups**), uncomment this cell and run it.

-- COMMAND ----------

-- GRANT ALL PRIVILEGES ON CATALOG flood_monitoring TO `data_engineers`;

-- GRANT USE CATALOG ON CATALOG flood_monitoring TO `data_analysts`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA flood_monitoring.gold TO `data_analysts`;

-- GRANT USE CATALOG ON CATALOG flood_monitoring TO `data_scientists`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA flood_monitoring.silver TO `data_scientists`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA flood_monitoring.gold TO `data_scientists`;
-- GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA flood_monitoring.sandbox TO `data_scientists`;

-- Free Edition has a single user, who already owns the catalog, so nothing is
-- lost by skipping these locally.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify

-- COMMAND ----------

SHOW SCHEMAS IN flood_monitoring;

