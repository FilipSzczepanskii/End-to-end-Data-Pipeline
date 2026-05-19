"""
Streamlit dashboard for the Air Quality PL pipeline.

Run:
    streamlit run dashboard/app.py

Data sources (in priority order):
    1. BigQuery marts (if GCP_PROJECT_ID is set)
    2. Local parquet files in ./data/ (dev fallback)
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

st.set_page_config(
    page_title="Air Quality PL",
    page_icon=":fog:",
    layout="wide",
)


@st.cache_data(ttl=600)
def load_measurements() -> pd.DataFrame:
    """Load the latest measurements. Uses BigQuery if configured, parquet otherwise."""
    project = os.getenv("GCP_PROJECT_ID")
    if project:
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        query = """
            select *
            from `air_quality_marts.fct_measurements_hourly`
            where measured_date >= current_date() - 7
        """
        return client.query(query).to_dataframe()

    data_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")) / "measurements"
    if not data_dir.exists():
        return pd.DataFrame()
    files = list(data_dir.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def main() -> None:
    st.title("Air Quality Poland: Live Dashboard")
    st.caption("Source: GIOS + OpenAQ. Refreshed hourly. Code: GitHub.")

    df = load_measurements()
    if df.empty:
        st.warning(
            "No data found. Run `python -m pipelines.flows.ingest_gios --max-stations 5` to load some."
        )
        return

    pollutants = sorted(df["pollutant_code"].dropna().unique())
    pollutant = st.sidebar.selectbox("Pollutant", pollutants, index=0)

    df_p = df[df["pollutant_code"] == pollutant]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stations", df_p["station_id"].nunique())
    c2.metric("Measurements (7d)", f"{len(df_p):,}")
    c3.metric("Mean", f"{df_p['value'].mean():.1f} ug/m3")
    c4.metric("Max", f"{df_p['value'].max():.1f} ug/m3")

    st.subheader(f"Map: latest {pollutant}")
    latest = (
        df_p.sort_values("measured_at")
        .groupby("station_id")
        .tail(1)
        .dropna(subset=["latitude", "longitude"])
    )
    if not latest.empty:
        st.pydeck_chart(
            pdk.Deck(
                map_style="light",
                initial_view_state=pdk.ViewState(
                    latitude=52.0, longitude=19.5, zoom=5.2, pitch=0
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        latest,
                        get_position=["longitude", "latitude"],
                        get_radius="value * 100",
                        get_fill_color="[255, 80, 80, 160]",
                        pickable=True,
                    )
                ],
                tooltip={"text": "{station_name}\n{pollutant_code}: {value} ug/m3"},
            )
        )

    st.subheader("7-day trend")
    fig = px.line(
        df_p.groupby(df_p["measured_at"].dt.floor("h"))["value"]
            .mean().reset_index(),
        x="measured_at",
        y="value",
        labels={"value": f"{pollutant} (ug/m3)", "measured_at": "Hour"},
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
