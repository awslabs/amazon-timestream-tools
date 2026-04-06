# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Live Migration Dashboard
========================
Streamlit application to monitor the migration of data from Amazon Timestream
for LiveAnalytics to InfluxDB. The UI surfaces key configuration values, overall
progress metrics, detailed batch statistics, and raw log output—all sourced
from logs written by the migration job.
"""

from __future__ import annotations

import operator
import pathlib
from collections import defaultdict
from functools import reduce
from typing import Any, Iterable

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

EXPECTED_COLS: list[str] = [
    "batch_id",
    "batch_name",
    "executed_at",
    "duration",
    "batch_start_time",
    "batch_end_time",
    "total_lines_ingested",
    "validation",
]

EMPTY_VALUE = "—"

# ---------------------------------------------------------------------------
#  Config & Data Loading Helpers
# ---------------------------------------------------------------------------


def load_config(path: str | pathlib.Path = "config.yaml") -> dict[str, Any]:
    """Load YAML configuration once every minute (cached)."""

    @st.cache_data(ttl=60, show_spinner=False)
    def _inner(file_path: str | pathlib.Path) -> dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as fp:
                return yaml.safe_load(fp) or {}
        except Exception as exc:
            st.error(f"Failed to read {file_path}: {exc}")
            return {}

    return _inner(path)


def load_log_csv(path: pathlib.Path) -> pd.DataFrame | None:
    """Read a *live_replication_*.log* CSV file (cached, 5 s)."""

    @st.cache_data(ttl=5, show_spinner=False)
    def _inner(csv_path: pathlib.Path) -> pd.DataFrame | None:  # type: ignore[override]
        try:
            return pd.read_csv(
                csv_path,
                parse_dates=["executed_at", "batch_start_time", "batch_end_time"],
            )
        except Exception as exc:
            st.error(f"Failed to read {csv_path.name}: {exc}")
            return None

    return _inner(path)


# ---------------------------------------------------------------------------
#  Misc. Utility Functions
# ---------------------------------------------------------------------------


def fmt(value: Any) -> str:
    """Human‑friendly representation suitable for *st.metric()* values."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"

    if isinstance(value, (int, float)):
        return f"{value:,}"  # thousands‑separator for numbers

    try:
        return pd.to_datetime(value).strftime("%b %d, %Y %H:%M")
    except Exception:
        return str(value)


def get_conf(cfg: dict[str, Any], path: Iterable[str], default: Any = None) -> Any:
    """Safely traverse *cfg* via *path* (sequence of keys)."""
    try:
        return reduce(operator.getitem, path, cfg)
    except (KeyError, TypeError):
        return default


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee the presence/order of *EXPECTED_COLS*; fill missing with <NA>."""
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[EXPECTED_COLS]


def parse_duration(duration: str | float | None) -> int | None:
    """Convert duration strings like "1h 5m 30s" or "51s" to seconds."""
    if pd.isna(duration):
        return None

    duration = str(duration).strip().lower()
    seconds = 0

    if "h" in duration:
        hours, duration = duration.split("h", maxsplit=1)
        seconds += int(hours) * 3600
        duration = duration.strip()

    if "m" in duration:
        minutes, duration = duration.split("m", maxsplit=1)
        seconds += int(minutes) * 60
        duration = duration.strip()

    if "s" in duration:
        seconds += int(duration.replace("s", ""))

    return seconds


# ---------------------------------------------------------------------------
#  Charting Helpers
# ---------------------------------------------------------------------------


def _plot_empty(msg: str) -> None:
    """Display *msg* inside the chart area when no data is available."""
    st.info(msg)


def chart_duration(df: pd.DataFrame) -> None:
    """Render batch‑duration scatter/line chart."""
    subset = df.loc[
        pd.notna(df["executed_at"]) & pd.notna(df["duration"]),
        ["executed_at", "duration", "batch_id"],
    ].copy()

    if subset.empty:
        return _plot_empty("No duration data available.")

    subset["duration_seconds"] = subset["duration"].apply(parse_duration)
    subset = subset.dropna(subset=["duration_seconds"]).sort_values("executed_at")

    fig = (px.scatter if len(subset) == 1 else px.line)(
        subset,
        x="executed_at",
        y="duration_seconds",
        hover_data=["batch_id", "duration"],
        title="Batch Duration Over Time",
        labels={
            "executed_at": "Execution Time",
            "duration_seconds": "Duration (s)",
        },
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


def chart_batch_lines(df: pd.DataFrame) -> None:
    """Render bar chart of *total_lines_ingested* by batch."""
    subset = df.loc[
        pd.notna(df["executed_at"]) & pd.notna(df["total_lines_ingested"]),
        ["executed_at", "total_lines_ingested", "batch_id"],
    ].sort_values("executed_at")

    if subset.empty:
        return _plot_empty("No timeline data available.")

    fig = px.bar(
        subset,
        x="executed_at",
        y="total_lines_ingested",
        hover_data=["batch_id"],
        title="Lines Migrated per Batch",
        labels={
            "executed_at": "Execution Time",
            "total_lines_ingested": "Lines Migrated",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def chart_cumulative(df: pd.DataFrame) -> None:
    """Render area + markers chart of cumulative lines migrated."""
    subset = df.loc[
        pd.notna(df["executed_at"]) & pd.notna(df["total_lines_ingested"]),
        ["executed_at", "total_lines_ingested", "batch_id"],
    ].sort_values("executed_at")

    if subset.empty:
        return _plot_empty("No cumulative data available.")

    subset["cumulative_lines"] = subset["total_lines_ingested"].cumsum()

    fig = (px.scatter if len(subset) == 1 else px.area)(
        subset,
        x="executed_at",
        y="cumulative_lines",
        hover_data=["batch_id", "total_lines_ingested"],
        title="Cumulative Lines Migrated Over Time",
        labels={
            "executed_at": "Execution Time",
            "cumulative_lines": "Cumulative Lines Migrated",
        },
    )

    # Sprinkle markers for readability if using area chart
    if len(subset) > 1:
        fig.add_scatter(
            x=subset["executed_at"],
            y=subset["cumulative_lines"],
            mode="markers",
            marker={"size": 8, "color": "white", "line": {"width": 2}},
            showlegend=False,
        )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
#  Streamlit App
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry‑point for *streamlit run*."""

    # ── Page Header ─────────────────────────────────────────────────────────
    st.title("Live Migration Dashboard")
    st.subheader("Timestream for LiveAnalytics  ➡️  InfluxDB")
    st.divider()

    # ── Configuration ─────────────
    cfg = load_config()

    log_dir_from_yaml = get_conf(cfg, ["global", "logs_dir"])
    logdir = pathlib.Path(log_dir_from_yaml or "./migration-logs")

    batch_sleep_min = get_conf(cfg, ["global", "live_replication", "batch_sleep_min"])
    backfill_start = get_conf(
        cfg, ["global", "live_replication", "backfill_start_time"]
    )
    cutoff_time = get_conf(cfg, ["global", "live_replication", "cutoff_time"])
    overlap_min = get_conf(cfg, ["global", "live_replication", "backfill_min_overlap"])
    all_dbs = get_conf(cfg, ["source", "all_databases"], False)
    source_dbs = get_conf(cfg, ["source", "databases"], {})

    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 5rem;
            padding-bottom: 1rem;
            padding-left: 2rem;
            padding-right: 2rem;
        }
        .main .block-container {
            max-width: 100%;
        }
        div[data-testid=\"metric-container\"] > div:nth-child(2) > div {
            justify-content: center;
            font-size: 12px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Top Banner ─────────────────
    top = st.container()
    with top:
        col1, col2 = st.columns([2, 1])

        # Source databases list
        with col1:
            st.markdown("**Source Databases:**")
            if all_dbs:
                st.markdown("- **All databases**")
            else:
                for db_name, tables in source_dbs.items():
                    if tables and isinstance(tables, list):
                        st.markdown(f"- **{db_name}**: {', '.join(tables)}")
                    else:
                        st.markdown(f"- **{db_name}**: *all tables*")

        # Refresh control
        with col2:
            st.markdown("Refresh Interval")
            refresh_sec = st.slider(
                "Refresh interval (seconds)",
                min_value=5,
                max_value=300,
                value=10,
                label_visibility="collapsed",
            )
            st.caption(f"(auto‑refreshes every {refresh_sec} s)")
            st_autorefresh(interval=refresh_sec * 1000, key="migration_refresh")

        metric_cols = st.columns(3)
        metric_cols[0].metric("**Backfill Start Date**", fmt(backfill_start))
        metric_cols[1].metric("**Cutoff Date**", fmt(cutoff_time))
        metric_cols[2].metric("Logs Directory", str(logdir))

        metric_cols2 = st.columns(3)
        metric_cols2[0].metric("Batch Sleep (min)", fmt(batch_sleep_min))
        metric_cols2[1].metric("Backfill Overlap (min)", fmt(overlap_min))

    # ── Data Ingestion ─────────────
    st.divider()

    live_logs = sorted(logdir.glob("live_replication_*.log"), reverse=True)

    if not live_logs:
        st.warning("No *live_replication* log files found.")
        df = pd.DataFrame(columns=EXPECTED_COLS)
    else:
        selected = st.selectbox(
            "Select a live_replication log:", [p.name for p in live_logs], index=0
        )
        df_raw = load_log_csv(logdir / selected)
        df = (
            ensure_columns(df_raw)
            if df_raw is not None
            else pd.DataFrame(columns=EXPECTED_COLS)
        )

    empty = df.empty

    # ── Metrics ───────────────
    total_rows = len(df)
    succ_rows = df[df["validation"] == "succeeded"].shape[0] if not empty else 0
    success_rate = succ_rows / total_rows * 100 if total_rows else 0

    # Display headline metrics
    headline = st.columns(3)
    headline[0].metric(
        "Batches Processed", df["batch_id"].nunique() if not empty else 0
    )
    headline[1].metric(
        "Total Lines Migrated", df["total_lines_ingested"].sum() if not empty else 0
    )
    headline[2].metric(
        "Success Rate", f"{success_rate:.1f} %" if total_rows else EMPTY_VALUE
    )

    # ── Charts ─────────────────
    st.subheader("Batch Metrics Over Time")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        chart_duration(df)
    with col_b:
        chart_batch_lines(df)
    with col_c:
        chart_cumulative(df)

    # ── Raw Data ───────────────
    st.subheader("Completed Batches")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Batch Log Files ───────
    st.divider()
    st.title("Batch Logs")

    batch_names = (
        df["batch_name"].dropna().unique().tolist()
        if not empty and "batch_name" in df.columns
        else []
    )
    batch_names.sort(reverse=True)

    if not batch_names:
        st.info("No batch‑* folders found.")
        return

    selected_batch = st.selectbox("Select a batch", batch_names, index=0)
    batch_folder = pathlib.Path(selected_batch).name
    batch_path = logdir / batch_folder

    st.subheader(batch_folder)

    files_by_category: dict[str, list[pathlib.Path]] = defaultdict(list)
    for log_file in batch_path.rglob("*.log"):
        files_by_category[log_file.parent.name].append(log_file)

    for category in sorted(files_by_category):
        with st.expander(category):
            for file in sorted(files_by_category[category]):
                rel = file.relative_to(batch_path).as_posix()
                if st.checkbox(rel, key=rel):
                    st.code(file.read_text(errors="replace"), language="bash")


if __name__ == "__main__":
    main()
