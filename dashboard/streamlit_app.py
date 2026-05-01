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
        render_dataframe_section(
            "Arrest Rates",
            query_df(
                config,
                """
                SELECT primary_type, district, race, total_crimes, total_arrests, arrest_rate
                FROM arrest_rates
                WHERE run_id = %s
                ORDER BY arrest_rate DESC NULLS LAST
                LIMIT 100;
                """,
                (latest_run,),
            ),
        )
        render_dataframe_section(
            "Violence Analysis",
            query_df(
                config,
                """
                SELECT month, district, community_area, total_homicides,
                       total_nonfatal_shootings, gunshot_incidents,
                       total_incidents, gunshot_proportion
                FROM violence_stats
                WHERE run_id = %s
                LIMIT 100;
                """,
                (latest_run,),
            ),
        )
        render_dataframe_section(
            "Sex Offender Density",
            query_df(
                config,
                """
                SELECT district, district_name, offender_count, victim_minor_count, density_rank
                FROM sex_offender_density
                WHERE run_id = %s
                ORDER BY density_rank ASC NULLS LAST
                LIMIT 100;
                """,
                (latest_run,),
            ),
        )

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
