"""
Air Quality Dashboard
One source of truth: public_marts.mart_air_quality_summary
- Pollutant breakdown: latest ingestion_date rows only (today's snapshot)
- Trend chart: all rows accumulated over daily DAG runs
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

st.set_page_config(page_title="Is the Air Safe?", page_icon="🌤️", layout="wide")

def get_config(key, default=None):
    """Read from Streamlit secrets first (Cloud), fall back to env vars (local)."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.environ.get(key, default)

PG_HOST     = get_config("PG_HOST", "localhost")
PG_PORT     = get_config("PG_PORT", "5432")
PG_DB       = get_config("PG_DB", "airflow")
PG_USER     = get_config("PG_USER", "airflow")
PG_PASSWORD = get_config("PG_PASSWORD", "airflow")

PARAMETER_INFO = {
    "pm25":             {"label": "Fine Particles (PM2.5)",  "desc": "Tiny particles that get deep into your lungs — mainly from vehicles, dust, and burning."},
    "pm10":             {"label": "Coarse Dust (PM10)",      "desc": "Larger dust and pollen particles — irritates eyes, nose, and throat."},
    "no2":              {"label": "Nitrogen Dioxide",        "desc": "Mainly from car and truck exhaust — can worsen asthma."},
    "so2":              {"label": "Sulfur Dioxide",          "desc": "From burning fuel/coal — can irritate airways."},
    "co":               {"label": "Carbon Monoxide",         "desc": "A gas from vehicle exhaust — reduces oxygen in blood at high levels."},
    "o3":               {"label": "Ground-Level Ozone",      "desc": "Forms in sunlight from pollution — can cause breathing difficulty."},
    "no":               {"label": "Nitric Oxide",            "desc": "Related to vehicle/industrial emissions."},
    "nox":              {"label": "Nitrogen Oxides",         "desc": "Combined NO + NO2 — vehicle and industrial pollution indicator."},
    "temperature":      {"label": "Temperature",             "desc": "Air temperature at the monitoring station."},
    "relativehumidity": {"label": "Humidity",                "desc": "Moisture level in the air."},
    "wind_speed":       {"label": "Wind Speed",              "desc": "How fast the air is moving — affects pollution dispersal."},
    "wind_direction":   {"label": "Wind Direction",          "desc": "Direction the wind is blowing from."},
}

POLLUTANT_THRESHOLDS = {
    "pm25": [(30, "Good", "🟢"), (60, "Moderate", "🟡"), (90,  "Unhealthy for Sensitive Groups", "🟠"), (120, "Unhealthy", "🔴"), (float("inf"), "Very Unhealthy", "🟣")],
    "pm10": [(50, "Good", "🟢"), (100,"Moderate", "🟡"), (150, "Unhealthy for Sensitive Groups", "🟠"), (250, "Unhealthy", "🔴"), (float("inf"), "Very Unhealthy", "🟣")],
    "no2":  [(40, "Good", "🟢"), (80, "Moderate", "🟡"), (180, "Unhealthy for Sensitive Groups", "🟠"), (280, "Unhealthy", "🔴"), (float("inf"), "Very Unhealthy", "🟣")],
    "so2":  [(40, "Good", "🟢"), (80, "Moderate", "🟡"), (380, "Unhealthy for Sensitive Groups", "🟠"), (800, "Unhealthy", "🔴"), (float("inf"), "Very Unhealthy", "🟣")],
    "co":   [(1000,"Good","🟢"),(2000,"Moderate","🟡"),(10000,"Unhealthy for Sensitive Groups","🟠"),(17000,"Unhealthy","🔴"),(float("inf"),"Very Unhealthy","🟣")],
    "o3":   [(50, "Good", "🟢"), (100,"Moderate", "🟡"), (168, "Unhealthy for Sensitive Groups", "🟠"), (208, "Unhealthy", "🔴"), (float("inf"), "Very Unhealthy", "🟣")],
}

WEATHER_PARAMS = {"temperature", "relativehumidity", "wind_speed", "wind_direction", "humidity"}


def get_category(parameter, value):
    if parameter not in POLLUTANT_THRESHOLDS or value is None:
        return None, None
    for limit, label, emoji in POLLUTANT_THRESHOLDS[parameter]:
        if value <= limit:
            return label, emoji
    return "Very Unhealthy", "🟣"


def friendly_name(p):
    return PARAMETER_INFO.get(p, {}).get("label", p)


def get_engine():
    """SQLAlchemy engine — works correctly with pandas and Streamlit Cloud Python 3.12."""
    # return create_engine(
    #     f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    #     connect_args={"sslmode": "require" if PG_HOST != "localhost" else "prefer"}
    # )
    return create_engine(
        f"postgresql+pg8000://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        connect_args={"ssl_context": True} if PG_HOST != "localhost" else {}
    )


@st.cache_data(ttl=300)
def load_all_data():
    """Load everything from the single mart table — used for both trend and latest breakdown."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM public_marts.mart_air_quality_summary ORDER BY measured_at DESC;"),
            conn
        )
    df["measured_at"] = pd.to_datetime(df["measured_at"])
    return df


df = load_all_data()

st.title("🌤️ Is the Air Safe to Breathe?")
st.caption(
    "This is an interactive air quality dashboard that provides a city-level view of air pollution across selected Indian cities."
    " It displays the latest pollutant levels using real-time data from OpenAQ and visualizes 7-day pollution trends through easy-to-understand charts."
    " The dashboard is designed to make air quality information accessible to everyone by presenting pollutant levels, health categories, and historical trends in a simple, user-friendly format."
)

if df.empty:
    st.warning("No data yet — trigger the Airflow DAG and run dbt first.")
    st.stop()

# Split weather vs pollutants
df["is_weather"] = df["parameter"].isin(WEATHER_PARAMS)
pollutant_df = df[~df["is_weather"]]
cities = sorted(df["city"].unique())

# Latest ingestion date per city (for the pollutant breakdown cards)
latest_date_per_city = (
    df.groupby("city")["measured_at"].max().dt.normalize()
)

with st.expander("ℹ️ What do these colors mean?"):
    st.markdown("""
| Color | Meaning | What to do |
|---|---|---|
| 🟢 Good | Air quality is satisfactory | Enjoy outdoor activities normally |
| 🟡 Moderate | Acceptable, minor concern for very sensitive people | Limit prolonged outdoor exertion if sensitive |
| 🟠 Unhealthy for Sensitive Groups | Children, elderly, asthma conditions may feel effects | Reduce prolonged outdoor exertion |
| 🔴 Unhealthy | Everyone may feel health effects | Limit outdoor exertion |
| 🟣 Very Unhealthy | Health alert — serious effects possible | Avoid outdoor exertion |
    """)

st.divider()

tabs = st.tabs([city.title() for city in cities])

for tab, city in zip(tabs, cities):
    with tab:
        city_latest_date = latest_date_per_city.get(city)

        # ── Latest snapshot rows (for pollutant breakdown cards) ──────────────
        city_latest = pollutant_df[
            (pollutant_df["city"] == city) &
            (pollutant_df["measured_at"].dt.normalize() == city_latest_date)
        ].sort_values("parameter")

        # ── All accumulated rows (for trend chart) ───────────────────────────
        city_trend = pollutant_df[pollutant_df["city"] == city].copy()

        # ── Headline status ───────────────────────────────────────────────────
        if city_latest.empty:
            st.info(f"No data available for {city.title()} yet.")
        else:
            headline = city_latest[city_latest["parameter"] == "pm25"]
            if headline.empty:
                headline = city_latest.iloc[[0]]
            row = headline.iloc[0]
            label, emoji = get_category(row["parameter"], row["avg_value"])
            st.markdown(f"## {emoji or '⚪'} {label or 'Data available'}")
            st.caption(
                f"Based on {friendly_name(row['parameter'])} — "
                f"last pipeline run: {city_latest_date.strftime('%d %b %Y') if city_latest_date else 'unknown'}"
            )

            # ── Pollutant breakdown cards (latest only) ───────────────────────
            st.subheader("Pollutant Breakdown — Today's Reading")
            cols = st.columns(3)
            for idx, (_, prow) in enumerate(city_latest.iterrows()):
                p_label, p_emoji = get_category(prow["parameter"], prow["avg_value"])
                with cols[idx % 3]:
                    st.markdown(f"**{p_emoji or '⚪'} {friendly_name(prow['parameter'])}**")
                    st.markdown(f"Level: **{prow['avg_value']:.1f}**" + (f" — {p_label}" if p_label else ""))
                    st.caption(PARAMETER_INFO.get(prow["parameter"], {}).get("desc", ""))

        st.divider()

        # ── Trend chart (all accumulated days) ───────────────────────────────
        st.subheader(f"Trend Over Time — {city.title()}")

        num_days = city_trend["measured_at"].dt.normalize().nunique()

        if num_days == 0:
            st.info("No trend data yet — will build up as the pipeline runs daily.")
        elif num_days == 1:
            st.info(
                "Only one day of data so far — the trend chart will show a line "
                "as more days accumulate. Come back tomorrow!"
            )
        else:
            st.caption(f"Showing {num_days} days of accumulated pipeline data.")

        if num_days > 0:
            available_params = sorted(city_trend["parameter"].unique())
            param_labels = {p: friendly_name(p) for p in available_params}
            default = list(param_labels.values())
            selected_labels = st.multiselect(
                "Choose pollutant(s)",
                options=list(param_labels.values()),
                default=default,
                key=f"trend_{city}",
            )
            selected_params = [p for p, lbl in param_labels.items() if lbl in selected_labels]

            plot_df = city_trend[city_trend["parameter"].isin(selected_params)].copy()
            plot_df["Pollutant"] = plot_df["parameter"].map(friendly_name)

            if not plot_df.empty:
                fig = px.line(
                    plot_df.sort_values("measured_at"),
                    x="measured_at",
                    y="avg_value",
                    color="Pollutant",
                    markers=True,
                    labels={"avg_value": "Average Level", "measured_at": "Date"},
                )
                fig.update_layout(legend_title="", xaxis_tickformat="%d %b")
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{city}")

        # ── Raw table (collapsed) ─────────────────────────────────────────────
        with st.expander("📋 See full readings for today"):
            if not city_latest.empty:
                disp = city_latest[["parameter", "avg_value", "min_value", "max_value", "num_readings"]].copy()
                disp["parameter"] = disp["parameter"].map(friendly_name)
                disp.columns = ["Pollutant", "Average", "Minimum", "Maximum", "Readings"]
                st.dataframe(disp, use_container_width=True, hide_index=True)

st.divider()
st.caption("Based on OpenAQ data | Educational use only")
st.caption("Built by Shraddha G Pai — shraddhagpaii@gmail.com")