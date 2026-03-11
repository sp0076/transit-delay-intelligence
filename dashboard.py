#!/usr/bin/env python3
"""
BART Transit Delay Intelligence — Exploration Dashboard

Powered by: marts.route_performance, marts.stop_performance,
            marts.route_hour_performance, marts.weather_impact,
            marts.route_weather_impact

Data source: GTFS-Realtime matched to GTFS static via best-offset alignment.
"""

import duckdb
import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./data/transit_delay.duckdb")

CAVEAT = (
    "**Exploratory MVP** — based on matched GTFS-RT trips using best-offset alignment. "
    "Route-level KPIs exclude unmatched RT trip IDs. "
    "See `docs/METHODOLOGY_LIMITATIONS.md` for alignment and coverage details. "
    "Do not treat these numbers as audited operational performance."
)


@st.cache_resource
def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str) -> pd.DataFrame:
    return get_conn().execute(sql).fetchdf()


def coverage_snapshot() -> dict:
    stats = q(
        """
        WITH base AS (
            SELECT
                (SELECT COUNT(*) FROM raw.gtfs_rt_trip_updates) AS total_raw_rt_rows,
                (SELECT COUNT(*) FROM staging.delay_preview_best_offset) AS matched_rt_rows,
                (SELECT COUNT(DISTINCT trip_id) FROM raw.gtfs_rt_trip_updates WHERE trip_id IS NOT NULL) AS raw_trip_ids,
                (SELECT COUNT(DISTINCT trip_id) FROM staging.delay_preview_best_offset) AS matched_trip_ids
        )
        SELECT
            total_raw_rt_rows,
            matched_rt_rows,
            (total_raw_rt_rows - matched_rt_rows) AS unmatched_rt_rows,
            CASE WHEN total_raw_rt_rows = 0 THEN 0 ELSE ROUND(100.0 * matched_rt_rows / total_raw_rt_rows, 1) END AS pct_rt_rows_matched,
            raw_trip_ids,
            matched_trip_ids,
            (raw_trip_ids - matched_trip_ids) AS unmatched_trip_ids,
            CASE WHEN raw_trip_ids = 0 THEN 0 ELSE ROUND(100.0 * matched_trip_ids / raw_trip_ids, 1) END AS pct_trip_ids_matched
        FROM base
        """
    )
    if stats.empty:
        return {
            "pct_trip_ids_matched": 0.0,
            "pct_rt_rows_matched": 0.0,
            "unmatched_trip_ids": 0,
            "matched_trip_ids": 0,
            "dry_only": True,
        }

    weather = q(
        """
        SELECT COALESCE(SUM(CASE WHEN is_raining THEN stop_event_count ELSE 0 END), 0) AS rainy_rows
        FROM marts.weather_impact
        """
    )
    rainy_rows = int(weather["rainy_rows"].iloc[0]) if not weather.empty else 0
    row = stats.iloc[0]
    return {
        "pct_trip_ids_matched": float(row["pct_trip_ids_matched"]),
        "pct_rt_rows_matched": float(row["pct_rt_rows_matched"]),
        "unmatched_trip_ids": int(row["unmatched_trip_ids"]),
        "matched_trip_ids": int(row["matched_trip_ids"]),
        "dry_only": rainy_rows == 0,
    }


def apply_theme() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background: #f8fafc;
            }
            [data-testid="stSidebar"] {
                background: #ffffff;
                border-right: 1px solid #e5e7eb;
            }
            .header-block {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 16px;
                padding: 18px 20px;
                box-shadow: 0 4px 16px rgba(15, 23, 42, 0.05);
                margin-bottom: 14px;
            }
            .header-title {
                font-size: 1.8rem;
                font-weight: 700;
                color: #0f172a;
                margin: 0;
            }
            .header-subtitle {
                color: #475569;
                margin: 4px 0 0 0;
                font-size: 0.95rem;
            }
            .chip-row {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 8px;
            }
            .chip {
                background: #eff6ff;
                border: 1px solid #dbeafe;
                color: #1e3a8a;
                border-radius: 999px;
                padding: 4px 10px;
                font-size: 0.78rem;
                font-weight: 600;
                display: inline-block;
            }
            .chip.warn {
                background: #fffbeb;
                border-color: #fde68a;
                color: #92400e;
            }
            .kpi-label {
                font-size: 0.8rem;
                color: #64748b;
                margin-bottom: 4px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: .04em;
            }
            .kpi-value {
                font-size: 1.5rem;
                font-weight: 700;
                color: #0f172a;
                line-height: 1.2;
            }
            .kpi-note {
                font-size: 0.78rem;
                color: #64748b;
                margin-top: 2px;
            }
            div[data-testid="stMetric"] {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                padding: 10px 14px;
                box-shadow: 0 2px 12px rgba(15, 23, 42, 0.04);
            }
            div[data-testid="stDataFrame"] {
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 12px;
                padding: 8px;
            }
            h2, h3 {
                color: #0f172a;
            }
            .stAlert {
                border-radius: 12px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header(page_label: str, stats: dict) -> None:
    dry_text = "Dry-only weather slice" if stats["dry_only"] else "Mixed weather slice"
    st.markdown(
        f"""
        <div class="header-block">
            <p class="header-title">BART Delay Intelligence</p>
            <p class="header-subtitle">{page_label} · Matched-trip analytics from GTFS-RT best-offset alignment</p>
            <div class="chip-row">
                <span class="chip warn">Exploratory MVP</span>
                <span class="chip">Trip match: {stats['pct_trip_ids_matched']:.1f}%</span>
                <span class="chip">RT row match: {stats['pct_rt_rows_matched']:.1f}%</span>
                <span class="chip">{dry_text}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def clean_fig(fig, height: int = 380):
    fig.update_layout(
        height=height,
        margin=dict(l=0, r=12, t=24, b=0),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#0f172a"),
    )
    fig.update_xaxes(gridcolor="#e5e7eb", zerolinecolor="#e5e7eb")
    fig.update_yaxes(gridcolor="#e5e7eb", zerolinecolor="#e5e7eb")
    return fig


st.set_page_config(
    page_title="BART Delay Intelligence",
    page_icon="🚇",
    layout="wide",
)

apply_theme()
stats = coverage_snapshot()

pages = ["Network / Route Overview", "Stop & Hour Drilldown", "Weather Impact"]
query_page = st.query_params.get("page", pages[0])
if isinstance(query_page, list):
    query_page = query_page[0] if query_page else pages[0]
default_index = pages.index(query_page) if query_page in pages else 0

page = st.sidebar.radio(
    "Page",
    pages,
    index=default_index,
)
st.query_params["page"] = page

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Coverage**  \n"
    f"Trip ID match rate: **{stats['pct_trip_ids_matched']:.1f}%**  \n"
    f"RT row match rate: **{stats['pct_rt_rows_matched']:.1f}%**  \n"
    f"Unmatched trips: **{stats['unmatched_trip_ids']}**"
)
st.sidebar.markdown("---")
st.sidebar.caption(
    "Sources: `marts.route_performance`, `marts.stop_performance`, "
    "`marts.route_hour_performance`, `marts.weather_impact`, "
    "`marts.route_weather_impact`"
)


# ─────────────────────────────────────────────────────────────
# PAGE 1 — NETWORK / ROUTE OVERVIEW
# ─────────────────────────────────────────────────────────────
if page == "Network / Route Overview":
    render_header("Network / Route Overview", stats)
    st.caption(CAVEAT)

    routes = q(
        """
        SELECT
            route_id,
            trip_count,
            stop_event_count,
            avg_delay_minutes,
            median_delay_minutes,
            p95_delay_minutes,
            on_time_rate,
            coverage_rows,
            coverage_trips,
            pct_rt_rows_matched
        FROM marts.route_performance
        ORDER BY avg_delay_minutes DESC
        """
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Delay", f"{routes['avg_delay_minutes'].mean():.1f} min", "Across routes")
    col2.metric("Median Delay", f"{routes['median_delay_minutes'].median():.1f} min", "Route median")
    col3.metric("On-Time Rate", f"{routes['on_time_rate'].mean():.1f}%", "Delay ≤ 5 min")
    col4.metric("Matched RT Coverage", f"{stats['pct_rt_rows_matched']:.1f}%", "Global RT rows")

    st.markdown("---")
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Average delay by route")
        fig_avg = px.bar(
            routes.sort_values("avg_delay_minutes", ascending=True),
            x="avg_delay_minutes",
            y="route_id",
            orientation="h",
            color="avg_delay_minutes",
            color_continuous_scale="Blues",
            labels={"avg_delay_minutes": "Avg delay (min)", "route_id": "Route"},
            text="avg_delay_minutes",
        )
        fig_avg.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig_avg.update_layout(coloraxis_showscale=False)
        st.plotly_chart(clean_fig(fig_avg), use_container_width=True)

    with col_r:
        st.subheader("On-time rate by route")
        fig_ot = px.bar(
            routes.sort_values("on_time_rate", ascending=True),
            x="on_time_rate",
            y="route_id",
            orientation="h",
            color="on_time_rate",
            color_continuous_scale="Greens",
            labels={"on_time_rate": "On-time rate (%)", "route_id": "Route"},
            text="on_time_rate",
        )
        fig_ot.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_ot.update_layout(coloraxis_showscale=False)
        st.plotly_chart(clean_fig(fig_ot), use_container_width=True)

    col_l2, col_r2 = st.columns(2)
    with col_l2:
        st.subheader("P95 delay by route")
        fig_p95 = px.bar(
            routes.sort_values("p95_delay_minutes", ascending=True),
            x="p95_delay_minutes",
            y="route_id",
            orientation="h",
            color="p95_delay_minutes",
            color_continuous_scale="OrRd",
            labels={"p95_delay_minutes": "P95 delay (min)", "route_id": "Route"},
            text="p95_delay_minutes",
        )
        fig_p95.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig_p95.update_layout(coloraxis_showscale=False)
        st.plotly_chart(clean_fig(fig_p95), use_container_width=True)

    with col_r2:
        st.subheader("Route coverage")
        cov = routes[["route_id", "coverage_rows", "coverage_trips", "pct_rt_rows_matched"]].copy()
        fig_cov = px.bar(
            cov.sort_values("coverage_rows", ascending=True),
            x="coverage_rows",
            y="route_id",
            orientation="h",
            color="pct_rt_rows_matched",
            color_continuous_scale="Blues",
            labels={"coverage_rows": "Matched RT rows", "route_id": "Route", "pct_rt_rows_matched": "Coverage %"},
            text="coverage_rows",
        )
        fig_cov.update_traces(texttemplate="%{text:.0f}", textposition="outside")
        fig_cov.update_layout(coloraxis_showscale=False)
        st.plotly_chart(clean_fig(fig_cov), use_container_width=True)

    st.markdown("---")
    st.subheader("Route coverage table")
    st.caption(
        "pct_rt_rows_matched = 100% for all rows here because unmatched trip IDs "
        "have no route assignment and are excluded before aggregation, not counted as low-coverage."
    )
    display_cols = [
        "route_id", "trip_count", "stop_event_count",
        "avg_delay_minutes", "median_delay_minutes", "p95_delay_minutes",
        "on_time_rate", "coverage_rows", "coverage_trips", "pct_rt_rows_matched",
    ]
    st.dataframe(
        routes[display_cols].style.format({
            "avg_delay_minutes": "{:.2f}",
            "median_delay_minutes": "{:.2f}",
            "p95_delay_minutes": "{:.2f}",
            "on_time_rate": "{:.1f}%",
            "pct_rt_rows_matched": "{:.1f}%",
        }),
        use_container_width=True,
    )


# ─────────────────────────────────────────────────────────────
# PAGE 2 — STOP & HOUR DRILLDOWN
# ─────────────────────────────────────────────────────────────
elif page == "Stop & Hour Drilldown":
    render_header("Stop & Hour Drilldown", stats)
    st.caption(CAVEAT)

    stops = q(
        """
        SELECT static_stop_id, stop_name, route_id,
               stop_event_count, avg_delay_minutes, median_delay_minutes, on_time_rate
        FROM marts.stop_performance
        ORDER BY avg_delay_minutes DESC
        """
    )

    hours = q(
        """
        SELECT route_id, CAST(hour_of_day AS INTEGER) AS hour_of_day,
               stop_event_count, trip_count, avg_delay_minutes,
               median_delay_minutes, on_time_rate
        FROM marts.route_hour_performance
        ORDER BY route_id, hour_of_day
        """
    )

    all_routes = sorted(stops["route_id"].unique().tolist())
    selected_route = st.selectbox("Route filter", ["All"] + all_routes)

    if selected_route != "All":
        stops_view = stops[stops["route_id"] == selected_route]
        hours_view = hours[hours["route_id"] == selected_route]
    else:
        stops_view = stops
        hours_view = hours

    st.markdown("---")
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Worst stops by average delay")
        top_stops = stops_view.nlargest(15, "avg_delay_minutes")
        fig_stops = px.bar(
            top_stops.sort_values("avg_delay_minutes", ascending=True),
            x="avg_delay_minutes",
            y="stop_name",
            orientation="h",
            color="avg_delay_minutes",
            color_continuous_scale="RdYlGn_r",
            hover_data=["route_id", "stop_event_count", "on_time_rate"],
            labels={"avg_delay_minutes": "Avg delay (min)", "stop_name": "Stop"},
        )
        fig_stops.update_layout(coloraxis_showscale=False)
        st.plotly_chart(clean_fig(fig_stops, height=500), use_container_width=True)

    with col_r:
        st.subheader("Average delay by hour of day")
        if hours_view.empty:
            st.info("No hourly data for this selection.")
        else:
            fig_hour = px.line(
                hours_view,
                x="hour_of_day",
                y="avg_delay_minutes",
                color="route_id" if selected_route == "All" else None,
                markers=True,
                labels={"hour_of_day": "Hour (local)", "avg_delay_minutes": "Avg delay (min)"},
            )
            fig_hour.update_xaxes(dtick=1)
            st.plotly_chart(clean_fig(fig_hour, height=500), use_container_width=True)

    st.markdown("---")
    st.subheader("Stop performance table")
    st.dataframe(
        stops_view[
            ["stop_name", "route_id", "stop_event_count", "avg_delay_minutes", "median_delay_minutes", "on_time_rate"]
        ].style.format({
            "avg_delay_minutes": "{:.2f}",
            "median_delay_minutes": "{:.2f}",
            "on_time_rate": "{:.1f}%",
        }),
        use_container_width=True,
    )


# ─────────────────────────────────────────────────────────────
# PAGE 3 — WEATHER IMPACT
# ─────────────────────────────────────────────────────────────
elif page == "Weather Impact":
    render_header("Weather Impact", stats)
    st.caption(CAVEAT)

    weather = q(
        """
        SELECT precipitation_bucket, is_raining, stop_event_count,
               trip_count, avg_delay_minutes, median_delay_minutes,
               p95_delay_minutes, on_time_rate
        FROM marts.weather_impact
        ORDER BY precipitation_bucket
        """
    )

    route_weather = q(
        """
        SELECT route_id, precipitation_bucket, is_raining,
               stop_event_count, trip_count, avg_delay_minutes,
               median_delay_minutes, on_time_rate,
               avg_temperature_c, avg_windspeed_kmh
        FROM marts.route_weather_impact
        ORDER BY route_id, precipitation_bucket
        """
    )

    has_rain = bool((weather["is_raining"] == True).any())  # noqa: E712

    if not has_rain:
        st.info(
            "**Current data slice is dry-only.** "
            "All matched delay rows currently fall in the `dry` precipitation bucket (precipitation_mm = 0). "
            "Wet/dry delay comparison is not possible until rain-period data is ingested. "
            "The tables are built and ready — they just have nothing to compare yet."
        )
        st.markdown("---")
        st.subheader("Current weather summary")
        st.dataframe(
            weather[
                ["precipitation_bucket", "stop_event_count", "trip_count",
                 "avg_delay_minutes", "median_delay_minutes", "p95_delay_minutes", "on_time_rate"]
            ].style.format({
                "avg_delay_minutes": "{:.2f}",
                "median_delay_minutes": "{:.2f}",
                "p95_delay_minutes": "{:.2f}",
                "on_time_rate": "{:.1f}%",
            }),
            use_container_width=True,
        )

        st.markdown("---")
        st.subheader("What will appear here once rain data exists")
        st.markdown(
            "- Avg/median delay by precipitation bucket (dry / light_rain / moderate_rain / heavy_rain)\n"
            "- On-time rate per weather bucket\n"
            "- Per-route rain sensitivity (rainy avg delay − dry avg delay)\n"
            "- P95 delay in wet conditions vs dry baseline"
        )
    else:
        col1, col2, col3 = st.columns(3)
        rainy_rows = int(weather.loc[weather["is_raining"] == True, "stop_event_count"].sum())  # noqa: E712
        dry_rows = int(weather.loc[weather["is_raining"] == False, "stop_event_count"].sum())  # noqa: E712
        col1.metric("Dry observation rows", f"{dry_rows:,}")
        col2.metric("Rainy observation rows", f"{rainy_rows:,}")
        col3.metric("Precipitation buckets", len(weather))

        st.markdown("---")
        wx_l, wx_r = st.columns(2)
        with wx_l:
            st.subheader("Average delay by precipitation bucket")
            fig_bucket = px.bar(
                weather,
                x="precipitation_bucket",
                y="avg_delay_minutes",
                color="avg_delay_minutes",
                color_continuous_scale="Blues",
                text="avg_delay_minutes",
                labels={"avg_delay_minutes": "Avg delay (min)", "precipitation_bucket": "Precipitation"},
            )
            fig_bucket.update_traces(texttemplate="%{text:.2f}", textposition="outside")
            fig_bucket.update_layout(coloraxis_showscale=False)
            st.plotly_chart(clean_fig(fig_bucket), use_container_width=True)

        with wx_r:
            st.subheader("On-time rate by precipitation bucket")
            fig_ot = px.bar(
                weather,
                x="precipitation_bucket",
                y="on_time_rate",
                color="on_time_rate",
                color_continuous_scale="Greens",
                text="on_time_rate",
                labels={"on_time_rate": "On-time rate (%)", "precipitation_bucket": "Precipitation"},
            )
            fig_ot.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_ot.update_layout(coloraxis_showscale=False)
            st.plotly_chart(clean_fig(fig_ot), use_container_width=True)

        st.subheader("Per-route weather sensitivity")
        pivot = (
            route_weather.groupby(["route_id", "is_raining"])["avg_delay_minutes"]
            .mean()
            .unstack()
            .rename(columns={False: "dry_avg", True: "rainy_avg"})
            .dropna()
        )
        pivot["delta"] = pivot["rainy_avg"] - pivot["dry_avg"]
        pivot = pivot.reset_index().sort_values("delta", ascending=False)
        if pivot.empty:
            st.info("Not enough mixed wet/dry rows per route to compute sensitivity.")
        else:
            fig_sens = px.bar(
                pivot,
                x="route_id",
                y="delta",
                color="delta",
                color_continuous_scale="RdYlGn_r",
                labels={"delta": "Extra delay in rain (min)", "route_id": "Route"},
                text="delta",
            )
            fig_sens.update_traces(texttemplate="%{text:+.2f}", textposition="outside")
            fig_sens.update_layout(coloraxis_showscale=False)
            st.plotly_chart(clean_fig(fig_sens), use_container_width=True)
