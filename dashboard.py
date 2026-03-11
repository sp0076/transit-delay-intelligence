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
    "Route-level KPIs exclude unmatched RT trip IDs (88.1% trip ID match rate, 98.7% RT row match rate). "
    "See `docs/METHODOLOGY_LIMITATIONS.md` for alignment and coverage details. "
    "Do not treat these numbers as audited operational performance."
)


@st.cache_resource
def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str) -> pd.DataFrame:
    return get_conn().execute(sql).fetchdf()


st.set_page_config(
    page_title="BART Delay Intelligence",
    page_icon="🚇",
    layout="wide",
)

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
    "Trip ID match rate: **88.1%**  \n"
    "RT row match rate: **98.7%**  \n"
    "Unmatched trips: **10**"
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
    st.title("🚇 Network / Route Overview")
    st.warning(CAVEAT)

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
    col1.metric("Routes measured", len(routes))
    col2.metric("Total stop events", f"{routes['stop_event_count'].sum():,.0f}")
    col3.metric("Mean on-time rate", f"{routes['on_time_rate'].mean():.1f}%")
    col4.metric("RT row match rate", "98.7%")

    st.markdown("---")
    st.subheader("Average delay by route")
    fig_avg = px.bar(
        routes.sort_values("avg_delay_minutes", ascending=True),
        x="avg_delay_minutes",
        y="route_id",
        orientation="h",
        color="avg_delay_minutes",
        color_continuous_scale="RdYlGn_r",
        labels={"avg_delay_minutes": "Avg delay (min)", "route_id": "Route"},
        text="avg_delay_minutes",
    )
    fig_avg.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig_avg.update_layout(coloraxis_showscale=False, height=400, margin=dict(l=0, r=40))
    st.plotly_chart(fig_avg, use_container_width=True)

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("On-time rate by route (on-time = delay ≤ 5 min)")
        fig_ot = px.bar(
            routes.sort_values("on_time_rate", ascending=True),
            x="on_time_rate",
            y="route_id",
            orientation="h",
            color="on_time_rate",
            color_continuous_scale="RdYlGn",
            range_color=[95, 100],
            labels={"on_time_rate": "On-time rate (%)", "route_id": "Route"},
            text="on_time_rate",
        )
        fig_ot.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_ot.update_layout(coloraxis_showscale=False, height=380, margin=dict(l=0, r=50))
        st.plotly_chart(fig_ot, use_container_width=True)

    with col_r:
        st.subheader("P95 delay by route")
        fig_p95 = px.bar(
            routes.sort_values("p95_delay_minutes", ascending=True),
            x="p95_delay_minutes",
            y="route_id",
            orientation="h",
            color="p95_delay_minutes",
            color_continuous_scale="RdYlGn_r",
            labels={"p95_delay_minutes": "P95 delay (min)", "route_id": "Route"},
            text="p95_delay_minutes",
        )
        fig_p95.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig_p95.update_layout(coloraxis_showscale=False, height=380, margin=dict(l=0, r=50))
        st.plotly_chart(fig_p95, use_container_width=True)

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
    st.title("📍 Stop & Hour Drilldown")
    st.warning(CAVEAT)

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
    selected_route = st.selectbox("Filter by route (applies to both panels)", ["All"] + all_routes)

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
        fig_stops.update_layout(coloraxis_showscale=False, height=500, margin=dict(l=0, r=40))
        st.plotly_chart(fig_stops, use_container_width=True)

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
            fig_hour.update_layout(height=500, margin=dict(l=0, r=0))
            st.plotly_chart(fig_hour, use_container_width=True)

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
    st.title("🌧 Weather Impact")
    st.warning(CAVEAT)

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
            "All 1,056 matched delay rows fall in the `dry` precipitation bucket (precipitation_mm = 0). "
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
        st.subheader("Average delay by precipitation bucket")
        fig_bucket = px.bar(
            weather,
            x="precipitation_bucket",
            y="avg_delay_minutes",
            color="avg_delay_minutes",
            color_continuous_scale="RdYlGn_r",
            text="avg_delay_minutes",
            labels={"avg_delay_minutes": "Avg delay (min)", "precipitation_bucket": "Precipitation"},
        )
        fig_bucket.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig_bucket.update_layout(coloraxis_showscale=False, height=380)
        st.plotly_chart(fig_bucket, use_container_width=True)

        st.subheader("On-time rate by precipitation bucket")
        fig_ot = px.bar(
            weather,
            x="precipitation_bucket",
            y="on_time_rate",
            color="on_time_rate",
            color_continuous_scale="RdYlGn",
            range_color=[90, 100],
            text="on_time_rate",
            labels={"on_time_rate": "On-time rate (%)", "precipitation_bucket": "Precipitation"},
        )
        fig_ot.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_ot.update_layout(coloraxis_showscale=False, height=380)
        st.plotly_chart(fig_ot, use_container_width=True)

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
            fig_sens.update_layout(coloraxis_showscale=False, height=380)
            st.plotly_chart(fig_sens, use_container_width=True)
