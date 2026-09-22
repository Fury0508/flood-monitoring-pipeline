"""
End-to-end tests: every notebook runs unchanged against fixture API responses that reproduce the quirks found in
the real API. Scenario: a first run, a second run that re-fetches the same days (one corrected value, one late
reading each day), then a catch-up run for a station that went silent. Failure tests come last.
"""
import json
import os

import fixtures as fx
import local_databricks as ldb
import pytest
from local_databricks import real as spark
from local_databricks import run

NOTEBOOKS = [
    "landing/02a_landing_stations.py", "landing/02b_landing_measures.py", "landing/02c_landing_readings.py",
    "bronze/03a_bronze_stations.py", "bronze/03b_bronze_measures.py", "bronze/03c_bronze_readings.py",
    "silver/04a_silver_stations.py", "silver/04b_silver_measures.py", "silver/04c_silver_readings.py",
    "gold/05a_gold_dim_station.py", "gold/05b_gold_dim_measure.py", "gold/05c_gold_fact_reading.py",
]
READINGS_CHAIN = ["bronze/03c_bronze_readings.py", "silver/04c_silver_readings.py", "gold/05c_gold_fact_reading.py"]
GRAIN_DUPLICATES = "SELECT count(*) FROM (SELECT measure_notation, reading_ts FROM gold.fact_reading GROUP BY 1, 2 HAVING count(*) > 1)"


def one(sql):
    return spark.sql(sql).first()[0]


@pytest.fixture(scope="module")
def state():
    ldb.setup()
    os.environ.pop("CORRECTION", None)
    for nb in NOTEBOOKS:
        run(nb, run_id="run1")
    s = {"fact_run1": one("SELECT count(*) FROM gold.fact_reading"),
         "silver_run1": one("SELECT count(*) FROM silver.readings"),
         "statuses": {r[0]: r[1] for r in spark.sql("SELECT station_uri, status FROM silver.stations").collect()},
         "quarantine_run1": {(r[0], r[1]): r[2] for r in spark.sql(
             "SELECT entity, reason, count(*) FROM silver.quarantine WHERE run_id = 'run1' GROUP BY 1, 2").collect()},
         "duplicates_run1": one(GRAIN_DUPLICATES)}

    os.environ["CORRECTION"] = "1"
    for nb in NOTEBOOKS:
        run(nb, run_id="run2")
    s["fact_run2"] = one("SELECT count(*) FROM gold.fact_reading")
    s["silver_before_rerun"] = one("SELECT count(*) FROM silver.readings")
    run("silver/04c_silver_readings.py", run_id="run2")
    s["silver_after_rerun"] = one("SELECT count(*) FROM silver.readings")

    # Deliberately given the daily run's id, as happened in the workspace: catch-up must still use its own.
    s["bronze_run2_before_catchup"] = one("SELECT count(*) FROM bronze.readings WHERE run_id = 'run2'")
    run("catchup/06_catchup_silent_stations.py", run_id="run2")
    s["catchup_landed"] = ldb.TaskValues.store["readings_landed"]
    s["catchup_run_id"] = ldb.TaskValues.store["run_id"]
    for nb in READINGS_CHAIN:
        run(nb, run_id=s["catchup_run_id"])
    os.environ.pop("CORRECTION", None)
    return s


# ---------------------------------------------------------------------------------------------- data quirks

def test_status_is_normalised_whether_text_or_list(state):
    st = state["statuses"]
    assert st[f"{fx.U}/id/stations/1029TH"] == "Active"
    assert st[f"{fx.U}/id/stations/E1234"] == "Suspended"   # arrived as a JSON list
    assert st[f"{fx.U}/id/stations/2001"] == "Unknown"      # unrecognised status value


def test_station_without_coordinates_is_kept(state):
    assert one("SELECT count(*) FROM silver.stations WHERE station_reference = 'E1234' AND latitude IS NULL") == 1


def test_unexpected_api_field_is_recorded_as_drift(state):
    assert [r[0] for r in spark.sql("SELECT unexpected_fields FROM bronze.stations WHERE run_id = 'run1' "
                                    "AND size(unexpected_fields) > 0").collect()] == [["newApiField"]]


def test_measure_notation_and_station_fall_back_to_uris(state):
    row = spark.sql("SELECT measure_notation, station_reference FROM silver.measures WHERE measure_uri LIKE '%E1234-flow%'").first()
    assert tuple(row) == (fx.M2, "E1234")


def test_odd_values_are_recovered_not_dropped(state):
    assert one("SELECT count(*) FROM silver.readings WHERE value_quality = 'recovered_from_pair' AND value = 0.121") == 3
    assert one("SELECT count(*) FROM silver.readings WHERE value_quality = 'recovered_from_list' AND value = 0.5") == 3


def test_bad_rows_are_quarantined_with_a_reason(state):
    q = state["quarantine_run1"]
    assert q[("stations", "missing_station_reference")] == 1
    assert q[("measures", "missing_measure_notation")] == 1
    assert q[("readings", "missing_value")] == 3
    assert q[("readings", "non_numeric_value")] == 3
    assert q[("readings", "invalid_timestamp")] == 1   # the identical bad item appears in 3 files: quarantined once


# ---------------------------------------------------------------------------------------------- model

def test_duplicate_station_reference_gives_one_current_station(state):
    assert one("SELECT count(*) FROM gold.dim_station WHERE is_current") == 3


def test_every_fact_row_has_its_dimension_keys(state):
    assert state["fact_run1"] == state["silver_run1"]
    assert one("SELECT count(*) FROM gold.fact_reading WHERE station_key IS NULL") == 0


def test_grain_holds_after_every_run(state):
    assert state["duplicates_run1"] == 0
    assert one(GRAIN_DUPLICATES) == 0


# ---------------------------------------------------------------------------------------------- incremental behaviour

def test_refetched_days_add_late_readings_without_duplicating(state):
    assert state["fact_run2"] == state["fact_run1"] + 3


def test_corrected_value_wins(state):
    assert one("SELECT count(*) FROM gold.fact_reading WHERE value = -9.99") == 3


def test_unchanged_stations_do_not_open_new_scd2_versions(state):
    assert one("SELECT count(*) FROM gold.dim_station") == 3


def test_rerunning_a_stage_is_idempotent(state):
    assert state["silver_after_rerun"] == state["silver_before_rerun"]
    assert one("SELECT count(*) FROM silver.quarantine WHERE run_id = 'run2' AND entity = 'readings'") == 7


def test_silent_station_is_caught_up_through_the_same_notebooks(state):
    assert state["catchup_landed"] == 4
    assert one(f"SELECT count(*) FROM gold.fact_reading WHERE measure_notation = '{fx.M3}'") == 4
    assert one(f"SELECT count(*) FROM ops.measure_watermark WHERE measure_id = '{fx.M3}'") == 1


def test_catchup_never_shares_the_daily_run_id(state):
    assert state["catchup_run_id"] == "run2-catchup"
    assert one("SELECT count(*) FROM bronze.readings WHERE run_id = 'run2-catchup'") == state["catchup_landed"]
    assert one("SELECT count(*) FROM bronze.readings WHERE run_id = 'run2'") == state["bronze_run2_before_catchup"]


def test_every_stage_is_in_the_run_log(state):
    stages = {r[0] for r in spark.sql("SELECT DISTINCT stage FROM ops.pipeline_run_log WHERE status = 'SUCCEEDED'").collect()}
    assert {"landing_readings", "bronze_readings", "silver_readings", "gold_fact_reading", "gold_dim_station"} <= stages


# ---------------------------------------------------------------------------------------------- failure conditions

def test_truncated_api_response_fails_instead_of_loading_partial_data(state):
    g = {"spark": ldb.SparkProxy(), "dbutils": ldb.DBUtils({}), "display": print}
    ldb.run_notebook(ldb.REPO / "notebooks/common/00_landing_common.py", g)

    class Truncated:
        content = json.dumps({"meta": {"limit": 500}, "items": [{}] * 500}).encode()
        def raise_for_status(self): pass

    g["SESSION"].get = lambda *a, **k: Truncated()
    with pytest.raises(g["IncompleteResponseError"], match="meta.limit"):
        g["fetch"]("/id/stations/1029TH/readings")


def test_bronze_count_mismatch_fails_and_is_logged(state):
    run("landing/02c_landing_readings.py", run_id="run3")
    path = json.loads(one("SELECT details FROM ops.pipeline_run_log WHERE run_id = 'run3' AND stage = 'landing_readings'"))["files"]
    first = sorted(path.values())[0]
    body = json.load(open(first))
    body["items"] = body["items"][:-1]            # one reading lost between landing and bronze
    json.dump(body, open(first, "w"))

    with pytest.raises(Exception, match="differ from landing"):
        run("bronze/03c_bronze_readings.py", run_id="run3")
    row = spark.sql("SELECT status, details FROM ops.pipeline_run_log WHERE run_id = 'run3' AND stage = 'bronze_readings'").first()
    assert row.status == "FAILED" and "ReconciliationError" in row.details
