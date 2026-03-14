"""Page 4: Historical payroll run management — view, compare, delete, edit."""
import os
import sys

import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app_helpers.state import init_state, get_warehouse_path
from app_helpers.theme import CUSTOM_CSS
from app_helpers import chart_builder as charts

st.set_page_config(page_title="Historical Runs", page_icon=":file_folder:", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
init_state()

st.title(":file_folder: Historical Runs")

wh = get_warehouse_path()
if not wh or not os.path.isfile(wh):
    st.info("No warehouse data found. Run the pipeline at least once.")
    st.stop()

from payroll_warehouse import (
    list_payroll_runs,
    get_payroll_by_employee,
    get_payroll_by_period,
    delete_payroll_run,
    delete_payroll_run_by_period,
    update_payroll_employee,
    get_period_over_period,
)
from payroll_engine import append_override_audit

tab_history, tab_compare, tab_edit, tab_delete, tab_lookup = st.tabs([
    "Run History", "Compare Periods", "Edit Employee", "Delete by Period", "Employee Lookup"
])

# ── Tab 1: Run History ──
with tab_history:
    runs = list_payroll_runs(wh)
    if runs.empty:
        st.info("No runs recorded yet.")
    else:
        st.dataframe(runs, use_container_width=True)
        st.markdown("---")

        # Delete a specific run
        run_ids = runs["run_id"].tolist()
        sel_run = st.selectbox("Select run to delete", run_ids, key="del_run_select")
        if sel_run:
            run_info = runs[runs["run_id"] == sel_run].iloc[0]
            st.caption(
                f"Period: {run_info['period_first']} to {run_info['period_last']} | "
                f"Employees: {run_info['employee_count']} | "
                f"Commission: ${run_info.get('total_commission', 0):,.0f}"
            )

            if "confirm_delete_run" not in st.session_state:
                st.session_state.confirm_delete_run = False

            if st.button(":wastebasket: Delete This Run", key="del_run_btn"):
                st.session_state.confirm_delete_run = True

            if st.session_state.confirm_delete_run:
                st.warning(f"Are you sure you want to delete run #{sel_run}? This cannot be undone.")
                c1, c2 = st.columns(2)
                if c1.button("Yes, delete", key="confirm_yes"):
                    ok = delete_payroll_run(wh, sel_run)
                    if ok:
                        st.success(f"Run #{sel_run} deleted.")
                        st.session_state.confirm_delete_run = False
                        st.rerun()
                    else:
                        st.error("Run not found.")
                if c2.button("Cancel", key="confirm_no"):
                    st.session_state.confirm_delete_run = False
                    st.rerun()

# ── Tab 2: Compare Periods ──
with tab_compare:
    runs = list_payroll_runs(wh)
    if len(runs) < 2:
        st.info("Need at least 2 payroll runs to compare.")
    else:
        periods = [f"{r['period_first']} to {r['period_last']} (run #{r['run_id']})" for _, r in runs.iterrows()]
        c1, c2 = st.columns(2)
        with c1:
            sel_a = st.selectbox("Period A", range(len(periods)), format_func=lambda i: periods[i], key="cmp_a")
        with c2:
            sel_b = st.selectbox("Period B", range(len(periods)),
                                 index=min(1, len(periods) - 1),
                                 format_func=lambda i: periods[i], key="cmp_b")

        if sel_a is not None and sel_b is not None:
            ra = runs.iloc[sel_a]
            rb = runs.iloc[sel_b]

            m1, m2, m3, m4 = st.columns(4)

            def _delta(a, b):
                if b == 0:
                    return None
                return f"{(a - b) / b * 100:+.1f}%"

            svc_a = ra.get("total_service", 0) or 0
            svc_b = rb.get("total_service", 0) or 0
            m1.metric("Total Service (A)", f"${svc_a:,.0f}", _delta(svc_a, svc_b))

            comm_a = ra.get("total_commission", 0) or 0
            comm_b = rb.get("total_commission", 0) or 0
            m2.metric("Commission (A)", f"${comm_a:,.0f}", _delta(comm_a, comm_b))

            emp_a = int(ra.get("employee_count", 0) or 0)
            emp_b = int(rb.get("employee_count", 0) or 0)
            m3.metric("Employees (A)", emp_a, f"{emp_a - emp_b:+d}" if emp_b else None)

            exc_a = int(ra.get("exception_count", 0) or 0)
            exc_b = int(rb.get("exception_count", 0) or 0)
            m4.metric("Exceptions (A)", exc_a, f"{exc_a - exc_b:+d}" if exc_b else None, delta_color="inverse")

            st.caption("Deltas show Period A relative to Period B.")

# ── Tab 3: Edit Employee ──
with tab_edit:
    runs = list_payroll_runs(wh)
    if runs.empty:
        st.info("No runs to edit.")
    else:
        sel_run_id = st.selectbox(
            "Select Payroll Run",
            runs["run_id"].tolist(),
            format_func=lambda rid: f"Run #{rid} ({runs[runs['run_id'] == rid].iloc[0]['period_first']})",
            key="edit_run",
        )

        if sel_run_id:
            all_emp = get_payroll_by_employee(wh)
            run_emp = all_emp[all_emp["run_id"] == sel_run_id] if not all_emp.empty else pd.DataFrame()

            if run_emp.empty:
                st.info("No employee records in this run.")
            else:
                emp_ids = run_emp["employee_id"].tolist()
                name_map = {r["employee_id"]: f"{r.get('employee_name', '')} ({r['employee_id']})"
                            for _, r in run_emp.iterrows()}
                sel_emp = st.selectbox("Select Employee", emp_ids,
                                       format_func=lambda x: name_map.get(x, x), key="edit_emp")

                if sel_emp:
                    emp_row = run_emp[run_emp["employee_id"] == sel_emp].iloc[0]
                    st.markdown(f"**Editing: {emp_row.get('employee_name', sel_emp)}**")

                    with st.form("edit_form"):
                        e1, e2 = st.columns(2)
                        with e1:
                            new_svc = st.number_input("Total Service", value=float(emp_row.get("total_service", 0) or 0), step=10.0)
                            new_ret = st.number_input("Total Retail", value=float(emp_row.get("total_retail", 0) or 0), step=10.0)
                            new_scomm = st.number_input("Service Commission", value=float(emp_row.get("service_commission", 0) or 0), step=5.0)
                        with e2:
                            new_rcomm = st.number_input("Retail Commission", value=float(emp_row.get("retail_commission", 0) or 0), step=5.0)
                            new_hours = st.number_input("Hours Worked", value=float(emp_row.get("hours_worked", 0) or 0), step=1.0)
                            new_flag = st.text_input("Double Booking Flag", value=str(emp_row.get("double_booking_flag", "") or ""))

                        submitted = st.form_submit_button("Save Changes", type="primary")
                        if submitted:
                            updates = {
                                "total_service": new_svc,
                                "total_retail": new_ret,
                                "service_commission": new_scomm,
                                "retail_commission": new_rcomm,
                                "hours_worked": new_hours,
                                "double_booking_flag": new_flag,
                            }
                            ok = update_payroll_employee(wh, sel_run_id, sel_emp, updates)
                            if ok:
                                # Audit log
                                cfg = st.session_state.config
                                audit_path = cfg.get("path_override_audit_log", "")
                                if audit_path:
                                    append_override_audit(
                                        audit_path, "", "", "manual_edit",
                                        f"Edited employee {sel_emp} in run #{sel_run_id}: {updates}",
                                        affected_ids=[sel_emp], user="streamlit_ui",
                                    )
                                st.success(f"Employee {sel_emp} updated successfully.")
                            else:
                                st.error("Update failed. Record not found.")

# ── Tab 4: Delete by Period ──
with tab_delete:
    st.markdown("Delete all payroll runs for a specific pay period.")
    dc1, dc2 = st.columns(2)
    with dc1:
        del_first = st.text_input("Period First (e.g. 2024-11-10)", key="del_period_first")
    with dc2:
        del_last = st.text_input("Period Last (e.g. 2024-11-16)", key="del_period_last")

    if del_first and del_last:
        runs = list_payroll_runs(wh)
        matching = runs[(runs["period_first"] == del_first) & (runs["period_last"] == del_last)]
        if matching.empty:
            st.info("No matching runs found.")
        else:
            st.warning(f"Found **{len(matching)}** run(s) matching this period:")
            st.dataframe(matching, use_container_width=True)
            if st.button(":wastebasket: Delete All Matching Runs", key="del_period_btn"):
                n = delete_payroll_run_by_period(wh, del_first, del_last)
                st.success(f"Deleted {n} run(s).")
                st.rerun()

# ── Tab 5: Employee Lookup ──
with tab_lookup:
    emp_search = st.text_input("Enter Employee ID", key="lookup_emp_id")
    if emp_search:
        emp_data = get_payroll_by_employee(wh, emp_search)
        if emp_data.empty:
            st.info(f"No records found for employee '{emp_search}'.")
        else:
            latest = emp_data.iloc[-1]
            l1, l2, l3, l4 = st.columns(4)
            l1.metric("Employee", latest.get("employee_name", emp_search))
            l2.metric("Periods", len(emp_data))
            l3.metric("Latest Commission", f"${(latest.get('service_commission', 0) or 0) + (latest.get('retail_commission', 0) or 0):,.0f}")
            l4.metric("Latest Hours", f"{latest.get('hours_worked', 0):,.0f}")

            st.plotly_chart(charts.employee_history(emp_data), use_container_width=True)
            with st.expander("Full History"):
                st.dataframe(emp_data, use_container_width=True)
