"""Page 3: Exception report viewer with filtering."""
import os
import sys

import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app_helpers.state import init_state
from app_helpers.theme import CUSTOM_CSS
from app_helpers.file_manager import df_to_excel_bytes
from app_helpers import chart_builder as charts

st.set_page_config(page_title="Exception Queue", page_icon=":warning:", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
init_state()

st.title(":warning: Exception Queue")

results = st.session_state.pipeline_results
exception_df = results.get("exception_df", pd.DataFrame()) if results else pd.DataFrame()

if exception_df.empty:
    st.info("No exceptions to display. Run the pipeline first.")
    st.stop()

# ── Summary Badges ──
st.markdown("---")
if "category" in exception_df.columns:
    cats = exception_df["category"].value_counts()
    badge_cols = st.columns(min(len(cats), 6))
    color_map = {
        "Unmatched Employee": ":red_circle:",
        "Unmapped Service": ":large_orange_circle:",
        "Validation": ":large_blue_circle:",
        "Multi-Location": ":purple_circle:",
        "Duplicate Record": ":white_circle:",
        "Revenue Outlier": ":green_circle:",
    }
    for i, (cat, count) in enumerate(cats.items()):
        if i < len(badge_cols):
            icon = color_map.get(cat, ":small_blue_diamond:")
            badge_cols[i].metric(f"{icon} {cat}", count)

# ── Filters ──
st.markdown("---")
f1, f2, f3, f4 = st.columns(4)

with f1:
    all_categories = exception_df["category"].unique().tolist() if "category" in exception_df.columns else []
    sel_categories = st.multiselect("Category", all_categories, default=all_categories)

with f2:
    all_severities = exception_df["severity"].unique().tolist() if "severity" in exception_df.columns else []
    sel_severities = st.multiselect("Severity", all_severities, default=all_severities)

with f3:
    emp_search = st.text_input("Search Employee", "")

with f4:
    all_locations = exception_df["location"].dropna().unique().tolist() if "location" in exception_df.columns else []
    all_locations = [l for l in all_locations if l and str(l).strip()]
    sel_locations = st.multiselect("Location", all_locations) if all_locations else []

# Apply filters
filtered = exception_df.copy()
if sel_categories and "category" in filtered.columns:
    filtered = filtered[filtered["category"].isin(sel_categories)]
if sel_severities and "severity" in filtered.columns:
    filtered = filtered[filtered["severity"].isin(sel_severities)]
if emp_search:
    filtered = filtered[
        filtered["employee"].astype(str).str.contains(emp_search, case=False, na=False)
        | filtered["id"].astype(str).str.contains(emp_search, case=False, na=False)
    ]
if sel_locations and "location" in filtered.columns:
    filtered = filtered[filtered["location"].isin(sel_locations)]

# ── Exception Table ──
st.markdown(f"**Showing {len(filtered)} of {len(exception_df)} exceptions**")


def _severity_color(severity: str) -> str:
    if severity == "error":
        return "background-color: #FFEAEA"
    if severity == "warning":
        return "background-color: #FFF8E1"
    if severity == "info":
        return "background-color: #E8F4FD"
    return ""


if not filtered.empty:
    display_cols = [c for c in ["category", "severity", "employee", "id", "service", "location", "reason", "detail"]
                    if c in filtered.columns]
    st.dataframe(
        filtered[display_cols].style.apply(
            lambda row: [_severity_color(row.get("severity", ""))] * len(row), axis=1
        ),
        use_container_width=True,
        height=400,
    )

    # Category breakdown chart
    st.plotly_chart(charts.exception_category_chart(filtered), use_container_width=True)

    # Download
    st.download_button(
        ":inbox_tray: Download Filtered Exceptions",
        df_to_excel_bytes(filtered),
        "filtered_exceptions.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.success("No exceptions match the current filters.")
