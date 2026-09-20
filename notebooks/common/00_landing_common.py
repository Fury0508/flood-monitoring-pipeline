# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Shared landing helpers
# MAGIC
# MAGIC Not run on its own. Each landing notebook loads these functions with `%run ./00_landing_common`.
# MAGIC
# MAGIC Every API response is checked before it is saved:
# MAGIC - **Truncation:** if the item count reaches `meta.limit`, the run fails instead of saving partial data.
# MAGIC - **Shape:** the response must be valid JSON with `items` as a list.
# MAGIC - **API version:** `meta.version` is recorded, and a change from the expected version is flagged.
# MAGIC
# MAGIC Transient errors (timeouts, 429, 5xx) are retried with exponential backoff.

# COMMAND ----------

# MAGIC %run ./00_pipeline_common

# COMMAND ----------

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://environment.data.gov.uk/flood-monitoring"
REQUEST_TIMEOUT_SECS = 300  # one full day of readings is roughly 100 MB of JSON


class IncompleteResponseError(Exception):
    """The API returned a response that must not be loaded (truncated or malformed)."""


def build_session() -> requests.Session:
    """HTTP session that retries transient failures with exponential backoff (2s, 4s, 8s...)."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session = requests.Session()  # follows redirects by default, as the API docs require
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"Accept": "application/json", "User-Agent": "flood-monitoring-pipeline/1.0"})
    return session


SESSION = build_session()


def fetch(path: str, params: dict | None = None) -> tuple[bytes, dict]:
    """GET an endpoint, validate the response, and return (raw bytes, parsed body)."""
    resp = SESSION.get(f"{BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT_SECS)
    resp.raise_for_status()

    raw = resp.content
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise IncompleteResponseError(f"{path} {params}: response is not valid JSON") from exc

    items = body.get("items")
    if not isinstance(items, list):
        raise IncompleteResponseError(f"{path} {params}: 'items' is missing or is not a list")

    # The API truncates silently: meta.limit is the only signal that rows are missing.
    limit = (body.get("meta") or {}).get("limit")
    if limit is not None and len(items) >= int(limit):
        raise IncompleteResponseError(
            f"{path} {params}: returned {len(items)} items, which reaches meta.limit={limit}. "
            "The result is truncated, so nothing was saved."
        )
    return raw, body


def save_raw(raw: bytes, catalog: str, entity: str, partition: str, run_id: str) -> str:
    """Write the response bytes untouched to the landing volume and return the file path."""
    folder = f"/Volumes/{catalog}/landing/raw_files/{entity}/{partition}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{entity}_{run_id}.json"
    with open(path, "wb") as f:
        f.write(raw)
    return path

# COMMAND ----------

def run_landing(entity: str, calls: list[dict], catalog: str, run_id: str, expected_api_version: str) -> dict:
    """
    Make each API call, save the raw response, and log the run as stage landing_<entity>.

    Each call is a dict with:
      key            name used in the run log, e.g. "stations" or "readings_2026-09-17"
      path, params   the endpoint to call
      partition      landing folder, e.g. "run_date=2026-09-18"
      warn_if_empty  True when an empty response is suspicious (a completed day of readings)
    """
    started_at = datetime.now(timezone.utc)
    details = {"files": {}, "items": {}, "warnings": []}
    versions: set[str] = set()
    rows_saved = 0
    status = "FAILED"

    try:
        for call in calls:
            raw, body = fetch(call["path"], call.get("params"))
            n = len(body["items"])
            if n == 0 and call.get("warn_if_empty"):
                details["warnings"].append(f"{call['key']}: no items returned")
            file_path = save_raw(raw, catalog, entity, call["partition"], run_id)
            versions.add(body.get("meta", {}).get("version"))
            details["files"][call["key"]] = file_path
            details["items"][call["key"]] = n
            rows_saved += n
            print(f"{call['key']:<22}{n:>10,} items -> {file_path}")
            del raw, body  # free driver memory before the next call
        status = "SUCCEEDED"

    except Exception as exc:
        details["error"] = f"{type(exc).__name__}: {exc}"
        raise

    finally:
        versions.discard(None)
        api_version = ",".join(sorted(versions)) or None
        if expected_api_version and versions and versions != {expected_api_version}:
            details["warnings"].append(f"API version changed: expected {expected_api_version}, got {api_version}")
        write_run_log(catalog, run_id, f"landing_{entity}", status, started_at,
                      rows_saved, rows_saved, 0, api_version, details)

    print(f"\n{status}: {rows_saved:,} {entity} items landed (run_id {run_id})")
    for warning in details["warnings"]:
        print(f"WARNING: {warning}")

    # Lets the downstream Bronze task load exactly this run. A no-op when run interactively.
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
    return details
