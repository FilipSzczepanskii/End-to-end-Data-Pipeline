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
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

# ---------------------------------------------------------------------------
# WHO 2021 annual / 24-h guideline values (ug/m3)
# ---------------------------------------------------------------------------
WHO_LIMITS: dict[str, float] = {
    "PM2.5": 15.0,
    "PM10": 45.0,
    "NO2": 25.0,
    "O3": 100.0,
}

POLLUTANT_LABELS: dict[str, str] = {
    "PM2.5": "Fine particulate matter",
    "PM10": "Coarse particulate matter",
    "NO2": "Nitrogen dioxide",
    "NO": "Nitric oxide",
    "NOX": "Nitrogen oxides",
    "SO2": "Sulfur dioxide",
    "O3": "Ozone",
    "CO": "Carbon monoxide",
    "C6H6": "Benzene",
    "HG(TGM)": "Mercury (TGM)",
}

# ---------------------------------------------------------------------------
# Page config + CSS
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Air Quality Poland",
    page_icon="🌿",
    layout="wide",
)

st.markdown(
    """
    <style>
    /* Hide default Streamlit chrome */
    #MainMenu  { visibility: hidden; }
    footer     { visibility: hidden; }
    header     { visibility: hidden; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: linear-gradient(160deg, #161b27 0%, #1c2130 100%);
        border: 1px solid #2a3148;
        border-radius: 12px;
        padding: 18px 22px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.35);
    }
    [data-testid="stMetricValue"] {
        font-size: 1.75rem !important;
        font-weight: 700;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #8b949e !important;
    }

    /* Section sub-headers */
    h3 {
        font-size: 0.78rem !important;
        font-weight: 600;
        color: #8b949e !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 2rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #0d1117;
        border-right: 1px solid #21262d;
    }

    /* Thin divider */
    hr {
        border: none;
        border-top: 1px solid #21262d;
        margin: 1.2rem 0;
    }

    /* Caption text */
    [data-testid="stCaptionContainer"] p {
        color: #6e7681;
        font-size: 0.78rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _resolve_duckdb_path() -> Path:
    """Pick the first DuckDB file that exists."""
    here = Path(__file__).parent
    candidates = [
        Path(os.getenv("DUCKDB_PATH")) if os.getenv("DUCKDB_PATH") else None,
        here / "air_quality.duckdb",
        Path.cwd() / "air_quality.duckdb",
        here.parent / "air_quality.duckdb",
    ]
    for c in candidates:
        if c and c.exists():
            return c
    return here / "air_quality.duckdb"


@st.cache_data(ttl=600)
def load_measurements() -> pd.DataFrame:
    """Read measurements from the dbt mart, or fall back to parquet."""
    duckdb_path = _resolve_duckdb_path()
    if duckdb_path.exists():
        con = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            for schema in ("main_marts", "marts"):
                try:
                    return con.execute(
                        f"select * from {schema}.fct_measurements_hourly"
                    ).df()
                except duckdb.CatalogException:
                    continue
        finally:
            con.close()

    data_dir = Path(os.getenv("LOCAL_DATA_DIR", "./data")) / "measurements"
    if not data_dir.exists():
        return pd.DataFrame()
    files = list(data_dir.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
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


# ---------------------------------------------------------------------------
# Map helpers
# ---------------------------------------------------------------------------

def _concentration_color(norm: float, alpha: int = 230) -> list[int]:
    """Three-stop ramp: green -> amber -> red, tuned for a light basemap."""
    GREEN = (40, 195, 40)
    AMBER = (255, 140, 0)
    RED   = (215, 20,  20)
    if norm < 0.5:
        t = norm * 2
        c = [int(GREEN[i] + t * (AMBER[i] - GREEN[i])) for i in range(3)]
    else:
        t = (norm - 0.5) * 2
        c = [int(AMBER[i] + t * (RED[i] - AMBER[i])) for i in range(3)]
    return c + [alpha]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # --- Sidebar -----------------------------------------------------------
    with st.sidebar:
        st.markdown("## 🌿 Air Quality PL")
        st.markdown(
            "Hourly readings from ~700 GIOS monitoring stations across Poland. "
            "Ingested with **Prefect**, modeled with **dbt**, served from **DuckDB**."
        )
        st.divider()

        df = load_measurements()
        if df.empty:
            st.warning(
                "No data yet. Run `python -m pipelines.flows.ingest_gios` "
                "and then `dbt run` to populate the marts."
            )
            return

        pollutants = sorted(df["pollutant_code"].dropna().unique())
        pollutant = st.selectbox(
            "Pollutant",
            pollutants,
            format_func=lambda p: f"{p} - {POLLUTANT_LABELS.get(p, p)}",
        )

        limit = WHO_LIMITS.get(pollutant)
        if limit:
            st.markdown(
                f"**WHO 24-h guideline:** {limit:.0f} μg/m³",
            )

        st.divider()
        df_p = df[df["pollutant_code"] == pollutant].copy()
        date_min = df_p["measured_at"].min()
        date_max = df_p["measured_at"].max()
        if pd.notna(date_min):
            st.caption(
                f"Data: {date_min.strftime('%b %d')} - {date_max.strftime('%b %d, %Y')}"
            )
        st.caption("Source: GIOS / powietrze.gios.gov.pl")

    # --- Main area ---------------------------------------------------------
    st.markdown("# Air Quality Poland")
    st.markdown(
        "Real-time monitoring data from Polish environmental stations, "
        f"showing **{pollutant}** - {POLLUTANT_LABELS.get(pollutant, '')}."
    )

    st.divider()

    # KPI row
    mean_val = df_p["value"].mean()
    max_val = df_p["value"].max()
    n_stations = df_p["station_id"].nunique()
    n_readings = len(df_p)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Monitoring stations", n_stations)
    col2.metric("Total readings", f"{n_readings:,}")

    if limit:
        delta_mean = mean_val - limit
        col3.metric(
            "Mean concentration",
            f"{mean_val:.1f} μg/m³",
            delta=f"{delta_mean:+.1f} vs WHO limit",
            delta_color="inverse",
        )
        delta_max = max_val - limit
        col4.metric(
            "Peak concentration",
            f"{max_val:.1f} μg/m³",
            delta=f"{delta_max:+.1f} vs WHO limit",
            delta_color="inverse",
        )
    else:
        col3.metric("Mean concentration", f"{mean_val:.1f} μg/m³")
        col4.metric("Peak concentration", f"{max_val:.1f} μg/m³")

    st.divider()

    # Map + trend side by side
    map_col, chart_col = st.columns([1, 1], gap="large")

    # -- Map ----------------------------------------------------------------
    with map_col:
        st.subheader("Latest readings by station")
        latest = (
            df_p.sort_values("measured_at")
            .groupby("station_id")
            .tail(1)
            .dropna(subset=["latitude", "longitude", "value"])
        )
        if not latest.empty:
            latest = latest.copy()
            v_min, v_max = latest["value"].min(), latest["value"].max()
            norm_series = (
                (latest["value"] - v_min) / (v_max - v_min)
                if v_max > v_min
                else pd.Series(0.5, index=latest.index)
            )

            # Core dot: solid, sized by relative concentration
            latest["_color"]      = [_concentration_color(float(n), alpha=230) for n in norm_series]
            # Outer halo: same RGB, very transparent - creates a soft glow ring
            latest["_color_halo"] = [_concentration_color(float(n), alpha=45)  for n in norm_series]

            st.pydeck_chart(
                pdk.Deck(
                    # Positron: clean white basemap, roads and borders are subtle gray
                    map_style=(
                        "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
                    ),
                    initial_view_state=pdk.ViewState(
                        latitude=52.1,
                        longitude=19.5,
                        zoom=5.2,
                        pitch=0,
                        bearing=0,
                    ),
                    layers=[
                        # Layer 1 - soft outer halo (no interaction)
                        pdk.Layer(
                            "ScatterplotLayer",
                            latest,
                            get_position=["longitude", "latitude"],
                            get_radius=55_000,
                            get_fill_color="_color_halo",
                            radius_min_pixels=28,
                            radius_max_pixels=90,
                            pickable=False,
                        ),
                        # Layer 2 - solid core dot with white ring
                        pdk.Layer(
                            "ScatterplotLayer",
                            latest,
                            get_position=["longitude", "latitude"],
                            get_radius=22_000,
                            get_fill_color="_color",
                            stroked=True,
                            get_line_color=[255, 255, 255, 220],
                            line_width_min_pixels=2,
                            radius_min_pixels=12,
                            radius_max_pixels=44,
                            pickable=True,
                        ),
                    ],
                    tooltip={
                        "html": (
                            "<b>{station_name}</b><br/>"
                            "{city_name}<br/>"
                            "{pollutant_code}: <b>{value}</b> μg/m³"
                        ),
                        "style": {
                            "backgroundColor": "#1c2130",
                            "color": "#e6edf3",
                            "border": "1px solid #2a3148",
                            "borderRadius": "8px",
                            "fontSize": "13px",
                            "padding": "8px 14px",
                            "boxShadow": "0 4px 16px rgba(0,0,0,0.45)",
                        },
                    },
                ),
                use_container_width=True,
            )
            st.caption(
                "Green = low concentration, amber = medium, red = high "
                "(relative to stations shown)."
            )

    # -- Trend chart --------------------------------------------------------
    with chart_col:
        st.subheader("Hourly trend")
        trend = (
            df_p.assign(hour=df_p["measured_at"].dt.floor("h"))
            .groupby("hour")["value"]
            .mean()
            .reset_index()
        )

        fig = go.Figure()

        # WHO limit reference line
        if limit:
            fig.add_hline(
                y=limit,
                line_dash="dot",
                line_color="#ff6b6b",
                line_width=1.5,
                annotation_text=f"WHO limit ({limit:.0f})",
                annotation_position="top right",
                annotation_font_color="#ff6b6b",
                annotation_font_size=11,
            )

        # Area + line
        fig.add_trace(
            go.Scatter(
                x=trend["hour"],
                y=trend["value"],
                mode="lines",
                line=dict(color="#3d9eff", width=2),
                fill="tozeroy",
                fillcolor="rgba(61, 158, 255, 0.12)",
                name=pollutant,
                hovertemplate="%{x|%b %d %H:%M}<br><b>%{y:.1f} μg/m³</b><extra></extra>",
            )
        )

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=8, b=0),
            height=370,
            showlegend=False,
            xaxis=dict(
                gridcolor="#21262d",
                showgrid=True,
                zeroline=False,
                title=None,
            ),
            yaxis=dict(
                gridcolor="#21262d",
                showgrid=True,
                zeroline=False,
                title=f"{pollutant} (μg/m³)",
                title_font_size=12,
            ),
            hoverlabel=dict(
                bgcolor="#161b27",
                bordercolor="#2a3148",
                font_color="#e6edf3",
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    # -- Station table ------------------------------------------------------
    st.divider()
    st.subheader("Station breakdown")

    station_summary = (
        df_p.groupby(["station_id", "station_name", "city_name"], as_index=False)
        .agg(
            readings=("value", "count"),
            mean=("value", "mean"),
            max=("value", "max"),
            last_reading=("measured_at", "max"),
        )
        .sort_values("mean", ascending=False)
        .reset_index(drop=True)
    )
    station_summary["mean"] = station_summary["mean"].round(1)
    station_summary["max"] = station_summary["max"].round(1)
    station_summary = station_summary.rename(
        columns={
            "station_name": "Station",
            "city_name": "City",
            "readings": "Readings",
            "mean": f"Mean ({pollutant})",
            "max": "Peak",
            "last_reading": "Latest reading",
        }
    ).drop(columns=["station_id"])

    st.dataframe(
        station_summary,
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
