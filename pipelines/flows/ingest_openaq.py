"""
Ingest OpenAQ v3 air quality data (global, includes PL stations).

API docs: https://docs.openaq.org/
Requires API key (free): https://explore.openaq.org/register

Why both GIOS + OpenAQ?
- GIOS is the ground truth for PL stations (official source)
- OpenAQ has wider coverage (mobile sensors, low-cost devices), useful for
  completeness checks against GIOS.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import httpx
import pandas as pd
from prefect import flow, get_run_logger, task
from tenacity import retry, stop_after_attempt, wait_exponential

OPENAQ_BASE = "https://api.openaq.org/v3"
TIMEOUT = httpx.Timeout(30.0)


def _headers() -> dict[str, str]:
    key = os.getenv("OPENAQ_API_KEY")
    if not key:
        raise RuntimeError("OPENAQ_API_KEY not set in environment")
    return {"X-API-Key": key}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get(client: httpx.Client, path: str, params: dict | None = None) -> dict:
    resp = client.get(f"{OPENAQ_BASE}{path}", params=params)
    resp.raise_for_status()
    return resp.json()


@task(retries=2)
def fetch_pl_locations(limit_per_page: int = 1000) -> pd.DataFrame:
    """Fetch all PL air quality monitoring locations."""
    logger = get_run_logger()
    all_results: list[dict] = []
    page = 1

    with httpx.Client(timeout=TIMEOUT, headers=_headers()) as client:
        while True:
            data = _get(
                client,
                "/locations",
                params={"iso": "PL", "limit": limit_per_page, "page": page},
            )
            results = data.get("results", [])
            all_results.extend(results)
            if len(results) < limit_per_page:
                break
            page += 1

    df = pd.json_normalize(all_results)
    df["_ingested_at"] = datetime.now(UTC)
    logger.info(f"Fetched {len(df)} PL locations from OpenAQ")
    return df


@flow(name="ingest-openaq", log_prints=True)
def ingest_openaq(local: bool = True) -> dict:
    locations = fetch_pl_locations()
    # TODO M2.5: fetch latest measurements per location
    return {"locations": len(locations)}


if __name__ == "__main__":
    print(ingest_openaq(local=True))
