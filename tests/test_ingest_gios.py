"""Smoke tests for GIOS ingest flow."""

import logging
from unittest.mock import patch

import pytest

from pipelines.flows.ingest_gios import KEY_STATIONS, fetch_stations


@pytest.fixture(autouse=True)
def _stub_prefect_logger():
    """Bypass Prefect's run-context logger when calling tasks via .fn() in unit tests."""
    with patch("pipelines.flows.ingest_gios.get_run_logger", return_value=logging.getLogger("test")):
        yield


def test_imports():
    """Flow module imports cleanly."""
    from pipelines.flows import ingest_gios  # noqa: F401


@patch("pipelines.flows.ingest_gios._get")
def test_fetch_stations_normalizes_polish_keys(mock_get):
    """Polish JSON-LD keys are renamed to English snake_case."""
    mock_get.return_value = {
        KEY_STATIONS: [
            {
                "Identyfikator stacji": 11,
                "Kod stacji": "DsCzerStraza",
                "Nazwa stacji": "Czerniawa",
                "WGS84 φ N": "50.912475",
                "WGS84 λ E": "15.312190",
                "Województwo": "DOLNOŚLĄSKIE",
            },
            {
                "Identyfikator stacji": 16,
                "Kod stacji": "DsDziePilsud",
                "Nazwa stacji": "Dzierżoniów",
                "WGS84 φ N": "50.732817",
                "WGS84 λ E": "16.648050",
                "Województwo": "DOLNOŚLĄSKIE",
            },
        ],
        "totalPages": 1,
    }
    df = fetch_stations.fn()
    assert len(df) == 2
    assert "_ingested_at" in df.columns
    assert set(df["station_id"]) == {11, 16}
    assert df["latitude"].dtype.kind == "f"  # converted to float
    assert df.loc[0, "latitude"] == 50.912475


@patch("pipelines.flows.ingest_gios._get")
def test_fetch_stations_skips_failed_pages(mock_get):
    """If a page raises _GiosServerError after retries, ingest continues on next page."""
    from pipelines.flows.ingest_gios import _GiosServerError

    calls = {"n": 0}

    def side_effect(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                KEY_STATIONS: [{"Identyfikator stacji": 1, "WGS84 φ N": "50", "WGS84 λ E": "20"}],
                "totalPages": 3,
            }
        if calls["n"] == 2:
            raise _GiosServerError("500 simulated")
        return {
            KEY_STATIONS: [{"Identyfikator stacji": 3, "WGS84 φ N": "51", "WGS84 λ E": "21"}],
            "totalPages": 3,
        }

    mock_get.side_effect = side_effect
    df = fetch_stations.fn()
    # Page 0 (id=1) + page 2 (id=3) should land; page 1 is skipped
    assert set(df["station_id"]) == {1, 3}
