"""
Module: streamlit_app.py
Description: Displays real-time alerts and latest completed Spark batch analytics.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from psycopg2.extras import RealDictCursor

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CONFIG_PATH = "/app/config/config.yaml"


def connection_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    """
    Build PostgreSQL connection parameters.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    dict[str, Any]
        psycopg2 connection keyword arguments.
    """
    postgres = config["postgres"]
    return {
        "host": postgres["host"],
        "port": int(postgres["port"]),
        "dbname": postgres["database"],
        "user": postgres["user"],
        "password": postgres["password"],
    }


def query_df(config: dict[str, Any], query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    """
    Execute a read-only PostgreSQL query and return a DataFrame.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    query : str
        SQL query.
    params : tuple[Any, ...]
        SQL parameters.

    Returns
    -------
    pd.DataFrame
        Query results. Empty when the database is unavailable or has no rows.
    """
    try:
        with psycopg2.connect(**connection_kwargs(config)) as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return pd.DataFrame(cursor.fetchall())
    except Exception as exc:
        logger.warning("Dashboard query failed: %s", exc)
        return pd.DataFrame()


def latest_completed_run_id(config: dict[str, Any]) -> str | None:
    """
    Fetch the latest completed Spark run_id.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    str | None
        Latest completed run_id, if present.
    """
    df = query_df(
        config,
        """
        SELECT run_id
        FROM batch_job_status
        WHERE status = 'completed'
        ORDER BY finished_at DESC
        LIMIT 1;
        """,
    )
    if df.empty:
        return None
    return str(df.iloc[0]["run_id"])


def render_dataframe_section(title: str, df: pd.DataFrame) -> None:
    """
    Render a table section only when rows exist.

    Parameters
    ----------
    title : str
        Section title.
    df : pd.DataFrame
        DataFrame to display.
    """
    st.subheader(title)
    if df.empty:
        st.info("No rows available yet.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_crime_trends(config: dict[str, Any], run_id: str) -> None:
    """
    Render crime trend charts for the latest completed run.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Latest completed run identifier.
    """
    trends = query_df(
        config,
        """
        SELECT trend_type, trend_key, crime_count
        FROM crime_trends
        WHERE run_id = %s
        ORDER BY trend_type, trend_key;
        """,
        (run_id,),
    )
    st.subheader("Historical Crime Trends")
    if trends.empty:
        st.info("No crime trend rows are available for the latest completed run.")
        return

    for trend_type in ["year", "month", "day_of_week", "hour"]:
        subset = trends[trends["trend_type"] == trend_type].copy()
        if subset.empty:
            continue
        figure = px.bar(
            subset,
            x="trend_key",
            y="crime_count",
            title=trend_type.replace("_", " ").title(),
        )
        st.plotly_chart(figure, use_container_width=True)


def render_arrest_rates(config: dict[str, Any], run_id: str) -> None:
    """
    Render arrest rate analysis, including the required top 10 crime types.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Latest completed run identifier.
    """
    st.subheader("Arrest Rate Analysis")

    top_crime_types = query_df(
        config,
        """
        SELECT primary_type, total_crimes, total_arrests, arrest_rate
        FROM arrest_rates
        WHERE run_id = %s
          AND district = 'ALL'
          AND race = 'ALL'
        ORDER BY arrest_rate DESC NULLS LAST, total_crimes DESC, primary_type ASC
        LIMIT 10;
        """,
        (run_id,),
    )

    if top_crime_types.empty:
        st.info("No top crime-type arrest-rate rows are available for the latest completed run.")
    else:
        figure = px.bar(
            top_crime_types,
            x="primary_type",
            y="arrest_rate",
            hover_data=["total_crimes", "total_arrests"],
            title="Top 10 Crime Types by Arrest Rate",
        )
        st.plotly_chart(figure, use_container_width=True)
        st.dataframe(top_crime_types, use_container_width=True, hide_index=True)

    detailed = query_df(
        config,
        """
        SELECT primary_type, district, race, total_crimes, total_arrests, arrest_rate
        FROM arrest_rates
        WHERE run_id = %s
          AND NOT (district = 'ALL' AND race = 'ALL')
        ORDER BY primary_type ASC, district ASC, race ASC
        LIMIT 500;
        """,
        (run_id,),
    )
    render_dataframe_section("Arrest Rates by Crime Type, District, and Race", detailed)


def render_sex_offender_density(config: dict[str, Any], run_id: str) -> None:
    """
    Render ranked sex offender density without treating unmatched blocks as a district.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Latest completed run identifier.
    """
    density = query_df(
        config,
        """
        SELECT district, district_name, offender_count, victim_minor_count, density_rank
        FROM sex_offender_density
        WHERE run_id = %s
        ORDER BY density_rank ASC NULLS LAST, offender_count DESC;
        """,
        (run_id,),
    )

    st.subheader("Sex Offender Density")
    if density.empty:
        st.info("No sex offender density rows are available for the latest completed run.")
        return

    unmatched = density[density["district"] == "UNMATCHED"].copy()
    ranked = density[density["district"] != "UNMATCHED"].copy()

    if not unmatched.empty:
        row = unmatched.iloc[0]
        st.info(
            "Unmatched block-level records are excluded from district ranking: "
            f"{int(row['offender_count'])} offenders, "
            f"{int(row['victim_minor_count'])} victim-minor flags. "
            "The sex offender dataset does not provide district, and these blocks could not be matched to crime blocks."
        )

    if ranked.empty:
        st.info("No district-matched sex offender rows are available for the latest completed run.")
        return

    district_names = ranked["district_name"].fillna("").astype(str)
    if district_names.str.contains("unavailable", case=False).all():
        ranked = ranked.drop(columns=["district_name"])

    st.dataframe(ranked, use_container_width=True, hide_index=True)


def render_violence_analysis(config: dict[str, Any], run_id: str) -> None:
    """
    Render violence analysis with full historical coverage.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Latest completed run identifier.
    """
    monthly = query_df(
        config,
        """
        SELECT
            month,
            SUM(total_homicides) AS total_homicides,
            SUM(total_nonfatal_shootings) AS total_nonfatal_shootings,
            SUM(gunshot_incidents) AS gunshot_incidents,
            SUM(total_incidents) AS total_incidents,
            CASE
                WHEN SUM(total_incidents) > 0
                THEN SUM(gunshot_incidents)::DOUBLE PRECISION / SUM(total_incidents)
                ELSE 0.0
            END AS gunshot_proportion
        FROM violence_stats
        WHERE run_id = %s
          AND month <> 'ALL'
        GROUP BY month
        ORDER BY month ASC;
        """,
        (run_id,),
    )

    st.subheader("Violence Analysis")
    if monthly.empty:
        st.info("No violence rows are available for the latest completed run.")
        return

    numeric_columns = [
        "total_homicides",
        "total_nonfatal_shootings",
        "gunshot_incidents",
        "total_incidents",
        "gunshot_proportion",
    ]
    for column in numeric_columns:
        monthly[column] = pd.to_numeric(monthly[column], errors="coerce").fillna(0)

    metric_columns = st.columns(4)
    metric_columns[0].metric("Earliest Month", str(monthly.iloc[0]["month"]))
    metric_columns[1].metric("Latest Month", str(monthly.iloc[-1]["month"]))
    metric_columns[2].metric("Victimizations", f"{int(monthly['total_incidents'].sum()):,}")
    total_incidents = monthly["total_incidents"].sum()
    gunshot_incidents = monthly["gunshot_incidents"].sum()
    gunshot_rate = 0.0 if total_incidents == 0 else gunshot_incidents / total_incidents
    metric_columns[3].metric("Gunshot Proportion", f"{gunshot_rate:.1%}")

    timeline = monthly.melt(
        id_vars=["month"],
        value_vars=["total_homicides", "total_nonfatal_shootings", "gunshot_incidents", "total_incidents"],
        var_name="metric",
        value_name="count",
    )
    figure = px.line(
        timeline,
        x="month",
        y="count",
        color="metric",
        title="Violence Victimizations by Month",
    )
    st.plotly_chart(figure, use_container_width=True)
    st.dataframe(monthly, use_container_width=True, hide_index=True)

    district_month = query_df(
        config,
        """
        SELECT
            month,
            district,
            SUM(total_homicides) AS total_homicides,
            SUM(total_nonfatal_shootings) AS total_nonfatal_shootings,
            SUM(gunshot_incidents) AS gunshot_incidents,
            SUM(total_incidents) AS total_incidents,
            CASE
                WHEN SUM(total_incidents) > 0
                THEN SUM(gunshot_incidents)::DOUBLE PRECISION / SUM(total_incidents)
                ELSE 0.0
            END AS gunshot_proportion
        FROM violence_stats
        WHERE run_id = %s
          AND month <> 'ALL'
        GROUP BY month, district
        ORDER BY month ASC, district ASC;
        """,
        (run_id,),
    )
    if not district_month.empty:
        for column in numeric_columns:
            district_month[column] = pd.to_numeric(district_month[column], errors="coerce").fillna(0)

        available_districts = sorted(
            district
            for district in district_month["district"].dropna().astype(str).unique().tolist()
            if district != "ALL"
        )
        selected_district = st.selectbox(
            "Violence District",
            ["ALL"] + available_districts,
            index=0,
        )
        selected_rows = (
            district_month
            if selected_district == "ALL"
            else district_month[district_month["district"].astype(str) == selected_district]
        )
        if selected_district != "ALL" and not selected_rows.empty:
            district_timeline = selected_rows.melt(
                id_vars=["month"],
                value_vars=[
                    "total_homicides",
                    "total_nonfatal_shootings",
                    "gunshot_incidents",
                    "total_incidents",
                ],
                var_name="metric",
                value_name="count",
            )
            district_figure = px.line(
                district_timeline,
                x="month",
                y="count",
                color="metric",
                title=f"Violence Victimizations in District {selected_district}",
            )
            st.plotly_chart(district_figure, use_container_width=True)
        render_dataframe_section("Violence by Month and District", selected_rows)

    top_communities = query_df(
        config,
        """
        SELECT
            community_area,
            total_homicides,
            total_nonfatal_shootings,
            gunshot_incidents,
            total_incidents,
            gunshot_proportion
        FROM violence_stats
        WHERE run_id = %s
          AND month = 'ALL'
          AND district = 'ALL'
        ORDER BY total_incidents DESC NULLS LAST, community_area ASC
        LIMIT 20;
        """,
        (run_id,),
    )
    render_dataframe_section("Top Violence Community Areas", top_communities)


def render_dashboard() -> None:
    """
    Render the Streamlit dashboard.
    """
    config = load_config(CONFIG_PATH)
    dashboard_config = config["dashboard"]
    max_live_alerts = int(dashboard_config["max_live_alerts"])

    st.set_page_config(
        page_title="Crime Analytics Dashboard",
        layout="wide",
    )
    st.title(config["app"]["project_name"])

    status_df = query_df(
        config,
        """
        SELECT run_id, job_name, status, started_at, finished_at, message
        FROM batch_job_status
        ORDER BY created_at DESC
        LIMIT 10;
        """,
    )

    alerts_df = query_df(
        config,
        """
        SELECT alert_id, district, alert_timestamp, event_count, threshold_value, severity, message
        FROM alerts
        ORDER BY alert_timestamp DESC
        LIMIT %s;
        """,
        (max_live_alerts,),
    )

    counts_df = query_df(
        config,
        """
        SELECT district, window_start, window_end, event_count, updated_at
        FROM realtime_district_counts
        ORDER BY window_end DESC
        LIMIT 100;
        """,
    )

    st.subheader("System Status")
    status_columns = st.columns(3)
    latest_run = latest_completed_run_id(config)
    latest_status = "none" if status_df.empty else str(status_df.iloc[0]["status"])
    status_columns[0].metric("Latest Batch Status", latest_status)
    status_columns[1].metric("Latest Completed Run", latest_run or "none")
    status_columns[2].metric("Live Alerts Loaded", 0 if alerts_df.empty else len(alerts_df))

    render_dataframe_section("Batch Job Status", status_df)
    render_dataframe_section("Real-Time Alerts", alerts_df)
    render_dataframe_section("Real-Time District Counts", counts_df)

    if latest_run is None:
        st.info("Historical analytics are still being generated. Live alerts are active.")
    else:
        render_crime_trends(config, latest_run)
        render_arrest_rates(config, latest_run)
        render_violence_analysis(config, latest_run)
        render_sex_offender_density(config, latest_run)

        hotspots = query_df(
            config,
            """
            SELECT cluster_id, centroid_latitude, centroid_longitude, crime_count
            FROM hotspots
            WHERE run_id = %s
            ORDER BY cluster_id;
            """,
            (latest_run,),
        )
        st.subheader("Hotspot Map")
        if hotspots.empty:
            st.info("No hotspot rows are available for the latest completed run.")
        else:
            map_df = hotspots.rename(
                columns={
                    "centroid_latitude": "lat",
                    "centroid_longitude": "lon",
                }
            )[["lat", "lon", "crime_count"]]
            st.map(map_df)
            st.dataframe(hotspots, use_container_width=True, hide_index=True)

        render_dataframe_section(
            "Correlations",
            query_df(
                config,
                """
                SELECT correlation_name, grouping_key, x_metric, y_metric,
                       x_value, y_value, correlation_value
                FROM correlations
                WHERE run_id = %s
                LIMIT 100;
                """,
                (latest_run,),
            ),
        )

    if st.sidebar.toggle("Auto refresh", value=True):
        time.sleep(int(dashboard_config["refresh_seconds"]))
        st.rerun()


if __name__ == "__main__":
    render_dashboard()
