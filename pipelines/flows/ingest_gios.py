"""
Ingest GIOS (Polish Chief Inspectorate of Environmental Protection) air quality data.

API: https://api.gios.gov.pl/pjp-api/swagger-ui/  (v1, JSON-LD)
- /v1/rest/station/findAll                  -> all monitoring stations (paginated)
- /v1/rest/station/sensors/{stationId}      -> sensors per station
- /v1/rest/data/getData/{sensorId}          -> hourly measurements per sensor (last ~3 days)

The API response keys are in Polish; this module maps them to English snake_case
so downstream consumers (dbt, dashboard) only see English column names.

Flow stages:
  1. Fetch stations (slow-changing dim, daily snapshot)
  2. For each station, fetch its sensors
  3. For each sensor, fetch latest measurements
  4. Land normalized records to local parquet OR BigQuery raw layer
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from prefect import flow, get_run_logger, task
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

GIOS_BASE = "https://api.gios.gov.pl/pjp-api/v1/rest"
TIMEOUT = httpx.Timeout(30.0, connect=10.0)
PAGE_SIZE = 100  # GIOS API returns HTTP 500 for size > 100

KEY_STATIONS = "Lista stacji pomiarowych"
KEY_SENSORS = "Lista stanowisk pomiarowych dla podanej stacji"
KEY_DATA = "Lista danych pomiarowych"

STATION_MAP = {
    "Identyfikator stacji": "station_id",
    "Kod stacji": "station_code",
    "Nazwa stacji": "station_name",
    "WGS84 φ N": "latitude",
    "WGS84 λ E": "longitude",
    "Identyfikator miasta": "city_id",
    "Nazwa miasta": "city_name",
    "Gmina": "commune",
    "Powiat": "district",
    "Województwo": "province",
    "Ulica": "street",
}

SENSOR_MAP = {
    "Identyfikator stanowiska": "sensor_id",
    "Identyfikator stacji": "station_id",
    "Wskaźnik": "pollutant_name",
    "Wskaźnik - wzór": "pollutant_formula",
    "Wskaźnik - kod": "pollutant_code",
    "Id wskaźnika": "pollutant_id",
}

DATA_MAP = {
    "Kod stanowiska": "sensor_code",
    "Data": "measured_at",
    "Wartość": "value",
}


class _GiosServerError(Exception):
    """GIOS API returned 5xx. Known to occur sporadically on certain pagination pages."""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get(client: httpx.Client, path: str, params: dict | None = None) -> Any:
    resp = client.get(f"{GIOS_BASE}{path}", params=params)
    if 500 <= resp.status_code < 600:
        # GIOS API has documented sporadic 500s on some pages; raise as recoverable.
        raise _GiosServerError(f"{resp.status_code} {resp.url}")
    resp.raise_for_status()
    return resp.json()


def _normalize(records: list[dict], mapping: dict[str, str]) -> list[dict]:
    """Rename Polish JSON keys to English snake_case."""
    return [{mapping.get(k, k): v for k, v in r.items()} for r in records]


@task(retries=2, retry_delay_seconds=30)
def fetch_stations(max_records: int | None = None) -> pd.DataFrame:
    """Fetch stations across paginated responses, skipping pages that fail with 5xx.

    When `max_records` is set, pagination short-circuits once we have enough rows.
    """
    logger = get_run_logger()
    all_records: list[dict] = []
    skipped_pages: list[int] = []
    page = 0
    total_pages = 1

    with httpx.Client(timeout=TIMEOUT) as client:
        while page < total_pages:
            if max_records is not None and len(all_records) >= max_records:
                break
            try:
                payload = _get(client, "/station/findAll", params={"size": PAGE_SIZE, "page": page})
            except (_GiosServerError, RetryError) as e:
                logger.warning(f"Skipping page {page} after retries: {e}")
                skipped_pages.append(page)
                page += 1
                continue

            all_records.extend(payload.get(KEY_STATIONS, []))
            total_pages = payload.get("totalPages", total_pages)
            page += 1

    if skipped_pages:
        logger.warning(f"Skipped {len(skipped_pages)} unrecoverable pages: {skipped_pages}")

    df = pd.DataFrame(_normalize(all_records, STATION_MAP))
    if not df.empty:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["_ingested_at"] = datetime.now(UTC)
    logger.info(f"Fetched {len(df)} stations across {page} pages")
    return df


@task(retries=2, retry_delay_seconds=30)
def fetch_sensors_for_station(station_id: int) -> list[dict]:
    with httpx.Client(timeout=TIMEOUT) as client:
        payload = _get(client, f"/station/sensors/{station_id}", params={"size": PAGE_SIZE})
    return _normalize(payload.get(KEY_SENSORS, []), SENSOR_MAP)


@task(retries=2, retry_delay_seconds=30)
def fetch_measurements_for_sensor(sensor_id: int) -> list[dict]:
    with httpx.Client(timeout=TIMEOUT) as client:
        payload = _get(client, f"/data/getData/{sensor_id}", params={"size": PAGE_SIZE})
    return _normalize(payload.get(KEY_DATA, []), DATA_MAP)


@task
def write_parquet(df: pd.DataFrame, name: str, base_dir: Path) -> Path:
    logger = get_run_logger()
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    out_dir = base_dir / name / f"dt={datetime.now(UTC).date().isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}_{ts}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info(f"Wrote {len(df)} rows -> {out_path}")
    return out_path


@flow(name="ingest-gios", log_prints=True)
def ingest_gios(local: bool = True, max_stations: int | None = None) -> dict:
    """Main ingest flow.

    Args:
        local: If True, write to local parquet. Else, write to BigQuery raw layer.
        max_stations: Cap on station count (dev/testing).
    """
    logger = get_run_logger()
    base_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data"))

    stations = fetch_stations(max_records=max_stations)
    if max_stations:
        stations = stations.head(max_stations)
        logger.info(f"Capped to {len(stations)} stations for dev run")

    sensors_all: list[dict] = []
    measurements_all: list[dict] = []

    failed_stations = 0
    failed_sensors = 0
    for station_id in stations["station_id"].tolist():
        try:
            sensors = fetch_sensors_for_station(station_id)
        except Exception as e:
            logger.warning(f"Skipping station {station_id} (sensors fetch failed): {e}")
            failed_stations += 1
            continue
        sensors_all.extend(sensors)

        for sensor in sensors:
            sid = sensor["sensor_id"]
            try:
                rows = fetch_measurements_for_sensor(sid)
            except Exception as e:
                logger.warning(f"Skipping sensor {sid} (data fetch failed): {e}")
                failed_sensors += 1
                continue
            for row in rows:
                measurements_all.append(
                    {
                        "sensor_id": sid,
                        "station_id": station_id,
                        "sensor_code": row.get("sensor_code"),
                        "pollutant_code": sensor.get("pollutant_code"),
                        "measured_at": row.get("measured_at"),
                        "value": row.get("value"),
                    }
                )

    if failed_stations or failed_sensors:
        logger.warning(
            f"Soft failures: stations skipped={failed_stations}, sensors skipped={failed_sensors}"
        )

    sensors_df = pd.DataFrame(sensors_all)
    sensors_df["_ingested_at"] = datetime.now(UTC)

    measurements_df = pd.DataFrame(measurements_all)
    if not measurements_df.empty:
        measurements_df["measured_at"] = pd.to_datetime(measurements_df["measured_at"])
        measurements_df["_ingested_at"] = datetime.now(UTC)

    write_parquet(stations, "stations", base_dir)
    write_parquet(sensors_df, "sensors", base_dir)
    if not measurements_df.empty:
        write_parquet(measurements_df, "measurements", base_dir)
    if not local:
        logger.info("BigQuery sink not active. The dbt project reads the parquet directly via dbt-duckdb.")

    return {
        "stations": len(stations),
        "sensors": len(sensors_df),
        "measurements": len(measurements_df),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", default=True)
    parser.add_argument("--max-stations", type=int, default=3,
                        help="Cap stations for dev (default: 3)")
    args = parser.parse_args()

    result = ingest_gios(local=args.local, max_stations=args.max_stations)
    print(f"\nDone: {result}")
