# Databricks notebook source
# MAGIC %md
# MAGIC # 03a - Bronze: stations
# MAGIC
# MAGIC Loads the landed `/id/stations` response into `bronze.stations`: one row per station, every value kept as a string.
# MAGIC
# MAGIC Known quirks kept as-is for Silver to handle: `status` is missing for over half the stations and is sometimes a list,
# MAGIC and some stations have no coordinates.

# COMMAND ----------

dbutils.widgets.text("catalog", "flood_monitoring", "Catalog")
dbutils.widgets.text("run_id", "", "Landing run id (blank = latest successful)")

# COMMAND ----------

# MAGIC %run ./00_bronze_common

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
RUN_ID = resolve_landing_run_id(CATALOG, dbutils.widgets.get("run_id"), "stations")
LANDING = landing_details(CATALOG, RUN_ID, "stations")

STATIONS = {
    "table": f"{CATALOG}.bronze.stations",
    "glob": f"/Volumes/{CATALOG}/landing/raw_files/stations/run_date=*/stations_{RUN_ID}.json",
    "partition_col": "run_date",
    "comment": "Raw stations from the EA flood monitoring API. All values are strings; typing happens in Silver.",
    # our column name -> API JSON key
    "fields": {
        "station_uri": "@id", "station_reference": "stationReference", "notation": "notation",
        "label": "label", "status": "status", "latitude": "lat", "longitude": "long",
        "easting": "easting", "northing": "northing", "river_name": "riverName",
        "catchment_name": "catchmentName", "town": "town", "date_opened": "dateOpened",
        "station_type": "type", "measures": "measures",
    },
    # every key we know about; anything else is reported as schema drift
    "expected": [
        "@id", "RLOIid", "catchmentName", "dateOpened", "datumOffset", "downstageScale", "easting",
        "eaAreaName", "eaRegionName", "gridReference", "label", "lat", "long", "measures", "northing",
        "notation", "riverName", "stageScale", "stationReference", "status", "statusDate",
        "statusReason", "town", "type", "wiskiID",
    ],
}

print(f"Loading stations from landing run {RUN_ID}")

# COMMAND ----------

run_bronze("stations", STATIONS, CATALOG, RUN_ID, expected_reference_counts(LANDING, "stations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(
    f"""
    SELECT
      count(*)                                          AS stations,
      count_if(status IS NULL)                          AS missing_status,
      count_if(status LIKE '[%')                        AS status_as_list,
      count_if(latitude IS NULL OR longitude IS NULL)   AS missing_coordinates,
      count_if(size(unexpected_fields) > 0)             AS rows_with_unexpected_fields
    FROM {STATIONS['table']}
    WHERE run_id = :run_id
    """,
    args={"run_id": RUN_ID},
))

# COMMAND ----------

display(spark.sql(
    f"SELECT station_reference, label, status, latitude, longitude, river_name, town, unexpected_fields "
    f"FROM {STATIONS['table']} WHERE run_id = :run_id LIMIT 20",
    args={"run_id": RUN_ID},
))
