"""Page 2: Interactive analytics dashboard."""
import os
import sys

import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app_helpers.state import init_state, get_warehouse_path
from app_helpers.theme import CUSTOM_CSS
from app_helpers import chart_builder as charts

st.set_page_config(page_title="Dashboard", page_icon=":bar_chart:", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
init_state()

st.title(":bar_chart: Dashboard")

tab_current, tab_trends, tab_employee = st.tabs(["Current Run", "Trend Analysis", "Employee Deep Dive"])

# ── Tab 1: Current Run ──
with tab_current:
    results = st.session_state.pipeline_results
    if not results:
        st.info("No pipeline results yet. Go to **Run Pipeline** to process payroll first.")
    else:
        final = results.get("final", pd.DataFrame())
        location_summary = results.get("location_summary", pd.DataFrame())
        payroll_cost_loc = results.get("payroll_cost_loc", pd.DataFrame())
        exception_df = results.get("exception_df", pd.DataFrame())

        # KPI row
        if not final.empty:
            total_svc = final["total_service"].sum() if "total_service" in final.columns else 0
            total_ret = final["Retail"].sum() if "Retail" in final.columns else 0
            total_tips = final["Tips"].sum() if "Tips" in final.columns else 0
            total_comm = (
                (final["service_comission"].sum() if "service_comission" in final.columns else 0)
                + (final["total_retail_commission"].sum() if "total_retail_commission" in final.columns else 0)
            )
            total_rev = total_svc + total_ret
            pct = round(total_comm / total_rev * 100, 1) if total_rev > 0 else 0

            k1, k2, k3, k4, k5, k6 = st.columns(6)
            k1.metric("Total Service", f"${total_svc:,.0f}")
            k2.metric("Total Retail", f"${total_ret:,.0f}")
            k3.metric("Total Tips", f"${total_tips:,.0f}")
            k4.metric("Total Commission", f"${total_comm:,.0f}")
            k5.metric("Payroll % Revenue", f"{pct}%")
            k6.metric("Employees", len(final))

        st.markdown("---")

        # Charts row 1
        c1, c2 = st.columns(2)
        with c1:
            if not location_summary.empty:
                st.plotly_chart(charts.revenue_by_location(location_summary), use_container_width=True)
        with c2:
            if not final.empty:
                st.plotly_chart(charts.commission_donut(final), use_container_width=True)

        # Charts row 2
        c3, c4 = st.columns(2)
        with c3:
            if not final.empty:
                st.plotly_chart(charts.top_employees(final), use_container_width=True)
        with c4:
            if not final.empty:
                st.plotly_chart(charts.commission_distribution(final), use_container_width=True)

        # Payroll cost by location
        if not payroll_cost_loc.empty:
            st.markdown("---")
            st.plotly_chart(charts.payroll_cost_by_location_chart(payroll_cost_loc), use_container_width=True)
            with st.expander("View Payroll Cost Data"):
                st.dataframe(payroll_cost_loc, use_container_width=True)

# ── Tab 2: Trend Analysis ──
with tab_trends:
    wh = get_warehouse_path()
    if not wh or not os.path.isfile(wh):
        st.info("No historical data yet. Run the pipeline at least once to populate the warehouse.")
    else:
        from payroll_warehouse import (
            get_payroll_by_period,
            get_commission_summary_by_period,
            get_payroll_pct_sales,
            get_exception_double_booking_counts,
            get_period_over_period,
        )

        runs = get_payroll_by_period(wh)
        if runs.empty:
            st.info("No payroll runs in the warehouse yet.")
        else:
            # Period-over-period
            pop = get_period_over_period(wh)
            if not pop.empty:
                st.plotly_chart(charts.period_trend(runs), use_container_width=True)

            t1, t2 = st.columns(2)
            with t1:
                comm = get_commission_summary_by_period(wh)
                if not comm.empty:
                    st.plotly_chart(charts.commission_trend(comm), use_container_width=True)
            with t2:
                pct = get_payroll_pct_sales(wh)
                if not pct.empty:
                    st.plotly_chart(charts.payroll_pct_trend(pct), use_container_width=True)

            exc = get_exception_double_booking_counts(wh)
            if not exc.empty:
                st.plotly_chart(charts.exception_trend(exc), use_container_width=True)

            with st.expander("All Payroll Runs"):
                st.dataframe(runs, use_container_width=True)

# ── Tab 3: Employee Deep Dive ──
with tab_employee:
    wh = get_warehouse_path()
    if not wh or not os.path.isfile(wh):
        st.info("No historical data. Run the pipeline first.")
    else:
        from payroll_warehouse import get_payroll_by_employee

        all_emp = get_payroll_by_employee(wh)
        if all_emp.empty:
            st.info("No employee data in the warehouse.")
        else:
            emp_ids = sorted(all_emp["employee_id"].unique().tolist())
            # Build a display name map
            name_map = {}
            for _, r in all_emp.drop_duplicates(subset=["employee_id"]).iterrows():
                eid = r["employee_id"]
                name = r.get("employee_name", eid)
                name_map[eid] = f"{name} ({eid})"

            selected = st.selectbox(
                "Select Employee",
                emp_ids,
                format_func=lambda x: name_map.get(x, x),
            )

            if selected:
                emp_data = get_payroll_by_employee(wh, selected)
                if not emp_data.empty:
                    # Summary
                    latest = emp_data.iloc[-1]
                    e1, e2, e3, e4 = st.columns(4)
                    e1.metric("Employee", latest.get("employee_name", selected))
                    e2.metric("Total Service", f"${latest.get('total_service', 0):,.0f}")
                    e3.metric("Service Commission", f"${latest.get('service_commission', 0):,.0f}")
                    e4.metric("Hours Worked", f"{latest.get('hours_worked', 0):,.0f}")

                    st.plotly_chart(charts.employee_history(emp_data), use_container_width=True)

                    with st.expander("Full History"):
                        st.dataframe(emp_data, use_container_width=True)
