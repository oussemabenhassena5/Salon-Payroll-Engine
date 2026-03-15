"""Page 1: Run the payroll pipeline — upload files, execute, download results."""
import streamlit as st
from datetime import date

from app_helpers.state import init_state
from app_helpers.file_manager import save_uploads, df_to_excel_bytes, df_to_csv_bytes, json_to_bytes
from app_helpers.pipeline_runner import run_pipeline
from app_helpers.theme import CUSTOM_CSS

st.set_page_config(page_title="Run Pipeline", page_icon=":arrow_forward:", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
init_state()

st.title(":arrow_forward: Run Payroll Pipeline")
st.markdown("Upload your files and run the full payroll calculation in one click.")
st.markdown("---")

# ── File Upload Zone ──
st.subheader("1. Upload Input Files")

col1, col2 = st.columns(2)

with col1:
    branch_csvs = st.file_uploader(
        "Branch Sales CSVs (Phorest exports)",
        type=["csv"],
        accept_multiple_files=True,
        help="Upload one CSV per location (e.g. Bay Ridge.csv, Dumbo.csv)",
    )
    bridge = st.file_uploader(
        "Bridge Service Categories",
        type=["xlsx"],
        help="Maps services to categories (Hair, Treatment, Makeup, Retail)",
    )
    master = st.file_uploader(
        "Master Employee",
        type=["xlsx"],
        help="Employee IDs, commission rules, house deductions, salary hour",
    )

with col2:
    hours = st.file_uploader(
        "Hours Worked",
        type=["xlsx"],
        help="Employee hours for the pay period (auto-detects column names)",
    )
    dates = st.file_uploader(
        "Date Table (optional)",
        type=["xlsx"],
        help="Excel with first_day and last_day columns. If not uploaded, set dates manually below.",
    )
    if not dates:
        st.markdown("**Or set pay period manually:**")
        d1, d2 = st.columns(2)
        with d1:
            manual_start = st.date_input("Period Start", value=date(2024, 11, 10))
        with d2:
            manual_end = st.date_input("Period End", value=date(2024, 11, 16))
    else:
        manual_start = manual_end = None

# ── Quick Settings ──
with st.expander("Quick Settings", expanded=False):
    sc1, sc2 = st.columns(2)
    with sc1:
        provider = st.radio(
            "Payroll Provider",
            ["adp", "trinet", "paylocity"],
            index=["adp", "trinet", "paylocity"].index(
                st.session_state.config.get("payroll_provider", "adp")
            ),
            horizontal=True,
        )
        st.session_state.config["payroll_provider"] = provider
    with sc2:
        db_mode = st.radio(
            "Double-Booking Mode",
            ["flag_only", "primary_location"],
            index=0 if st.session_state.config.get("double_booking_allocation") == "flag_only" else 1,
            horizontal=True,
        )
        st.session_state.config["double_booking_allocation"] = db_mode

# ── Validation ──
st.markdown("---")
required_ok = branch_csvs and bridge and master and hours

if not required_ok:
    missing = []
    if not branch_csvs:
        missing.append("Branch CSVs")
    if not bridge:
        missing.append("Bridge Service Categories")
    if not master:
        missing.append("Master Employee")
    if not hours:
        missing.append("Hours Worked")
    st.warning(f"Missing required files: **{', '.join(missing)}**")

# ── Run Button ──
st.subheader("2. Run Pipeline")
run_btn = st.button(
    "Run Payroll Pipeline",
    type="primary",
    disabled=not required_ok,
    use_container_width=True,
)

if run_btn and required_ok:
    # Save uploaded files
    uploaded = {
        "branch_csvs": branch_csvs,
        "bridge": bridge,
        "master": master,
        "hours": hours,
        "dates": dates,
    }
    config = save_uploads(uploaded, st.session_state.temp_dir, st.session_state.config)
    st.session_state.config = config

    # Manual dates
    manual = None
    if not dates and manual_start and manual_end:
        manual = (manual_start, manual_end)

    # Run with progress
    progress_bar = st.progress(0)
    status = st.status("Running payroll pipeline...", expanded=True)

    def on_progress(step, total, msg):
        progress_bar.progress(step / total)
        status.write(f"**Step {step}/{total}:** {msg}")

    try:
        results = run_pipeline(config, manual_dates=manual, on_progress=on_progress)
        st.session_state.pipeline_results = results
        st.session_state.pipeline_status = "complete"
        st.session_state.warehouse_path = results.get("warehouse_path", "")
        progress_bar.progress(1.0)
        status.update(label="Pipeline complete!", state="complete")
    except Exception as e:
        st.session_state.pipeline_status = "error"
        status.update(label=f"Pipeline failed: {e}", state="error")
        st.error(f"Error: {e}")

# ── Results & Downloads ──
if st.session_state.pipeline_status == "complete" and st.session_state.pipeline_results:
    results = st.session_state.pipeline_results
    st.markdown("---")
    st.subheader("3. Results")

    # KPI metrics
    final = results.get("final")
    exception_df = results.get("exception_df")
    location_summary = results.get("location_summary")
    validation_issues = results.get("validation_issues", [])

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Employees", len(final) if final is not None else 0)
    m2.metric("Locations", len(location_summary) if location_summary is not None else 0)
    if final is not None and "total_service" in final.columns:
        total_rev = final["total_service"].sum() + final.get("Retail", 0).sum()
        total_comm = final.get("service_comission", 0).sum() + final.get("total_retail_commission", 0).sum()
        # Use K format for large numbers to avoid truncation
        def _fmt(v):
            if v >= 1_000_000:
                return f"${v / 1_000_000:.1f}M"
            if v >= 1_000:
                return f"${v / 1_000:.1f}K"
            return f"${v:,.0f}"
        m3.metric("Total Revenue", _fmt(total_rev))
        m4.metric("Total Commission", _fmt(total_comm))
    m5.metric("Exceptions", len(exception_df) if exception_df is not None else 0)
    m6.metric("Validation Issues", len(validation_issues))

    # Branch names
    st.info(f"**Branches processed:** {', '.join(results.get('branch_names', []))}")

    # Download section
    st.subheader("4. Download Outputs")
    d1, d2, d3, d4 = st.columns(4)

    with d1:
        if final is not None and not final.empty:
            st.download_button(
                ":page_facing_up: Final Payroll", df_to_excel_bytes(final),
                "final_database.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        loc = results.get("location_summary")
        if loc is not None and not loc.empty:
            st.download_button(
                ":round_pushpin: Location Summary", df_to_excel_bytes(loc),
                "location_summary.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with d2:
        plc = results.get("payroll_cost_loc")
        if plc is not None and not plc.empty:
            st.download_button(
                ":money_with_wings: Payroll Cost by Location", df_to_excel_bytes(plc),
                "payroll_cost_by_location.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        exc = results.get("exception_df")
        if exc is not None and not exc.empty:
            st.download_button(
                ":warning: Exception Report", df_to_excel_bytes(exc),
                "exception_report.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with d3:
        hrs = results.get("hours_summ")
        if hrs is not None and not hrs.empty:
            st.download_button(
                ":clock3: Hours Summary", df_to_excel_bytes(hrs),
                "hours_worked_summ.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        ms = results.get("missing_services")
        if ms is not None and not ms.empty:
            st.download_button(
                ":mag: Missing Services", df_to_excel_bytes(ms),
                "missing_services.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with d4:
        me = results.get("missing_employees")
        if me is not None and not me.empty:
            st.download_button(
                ":bust_in_silhouette: Missing Employees", df_to_excel_bytes(me),
                "missing_employees.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        # Payroll import CSV
        import os
        pp = results.get("payroll_export_path", "")
        if pp and os.path.isfile(pp):
            with open(pp, "rb") as f:
                st.download_button(
                    f":outbox_tray: {results.get('payroll_provider', 'ADP')} Import",
                    f.read(), os.path.basename(pp), "text/csv",
                )
