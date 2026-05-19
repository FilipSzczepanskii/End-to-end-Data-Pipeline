"""
Streamlit dashboard for the air quality pipeline.

Reads the dbt marts from a local DuckDB file by default. If the file does
not exist yet, falls back to reading the parquet ingest directly so the
dashboard works as soon as the ingest has run, before dbt has built the marts.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

st.set_page_config(page_title="Air Quality PL", layout="wide")


@st.cache_data(ttl=600)
def load_measurements() -> pd.DataFrame:
    """Read the last 7 days of measurements from the dbt mart, or fall back to parquet."""
    duckdb_path = Path(os.getenv("DUCKDB_PATH", "./air_quality.duckdb"))
    if duckdb_path.exists():
        con = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            return con.execute(
                """
                select *
                from marts.fct_measurements_hourly
                where measured_date >= current_date - interval 7 day
                """
            ).df()
        except duckdb.CatalogException:
            pass
        finally:
            con.close()

    data_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")) / "measurements"
    if not data_dir.exists():
        return pd.DataFrame()
    files = list(data_dir.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    # The fallback path does not have station coordinates joined in; pull them
    # from the stations parquet if present.
    stations_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")) / "stations"
    if stations_dir.exists():
        sf = list(stations_dir.rglob("*.parquet"))
        if sf:
            s = pd.concat([pd.read_parquet(f) for f in sf], ignore_index=True)
            s = s.drop_duplicates("station_id", keep="last")[
                ["station_id", "station_name", "latitude", "longitude"]
            ]
            df = df.merge(s, on="station_id", how="left")
    return df


def main() -> None:
    st.title("Air Quality Poland")
    st.caption("GIOS hourly readings, ingested with Prefect, modeled with dbt, served from DuckDB.")

    df = load_measurements()
    if df.empty:
        st.warning(
            "No data yet. Run `python -m pipelines.flows.ingest_gios --max-stations 5` "
            "and then `dbt run` to populate the marts."
        )
        return

    pollutants = sorted(df["pollutant_code"].dropna().unique())
    pollutant = st.sidebar.selectbox("Pollutant", pollutants, index=0)
    df_p = df[df["pollutant_code"] == pollutant]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stations", df_p["station_id"].nunique())
    c2.metric("Readings (7d)", f"{len(df_p):,}")
    c3.metric("Mean", f"{df_p['value'].mean():.1f} ug/m3")
    c4.metric("Max", f"{df_p['value'].max():.1f} ug/m3")

    st.subheader(f"Latest {pollutant} by station")
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
    trend = (
        df_p.assign(hour=df_p["measured_at"].dt.floor("h"))
        .groupby("hour")["value"]
        .mean()
        .reset_index()
    )
    fig = px.line(
        trend, x="hour", y="value",
        labels={"value": f"{pollutant} (ug/m3)", "hour": "Hour"},
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
