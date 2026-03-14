"""Page 5: Configuration settings."""
import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app_helpers.state import init_state, reset_config
from app_helpers.theme import CUSTOM_CSS

st.set_page_config(page_title="Settings", page_icon=":gear:", layout="wide")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
init_state()

st.title(":gear: Settings")
st.markdown("Configure commission rules, provider settings, and exception detection. Changes apply to the next pipeline run.")
st.markdown("---")

cfg = st.session_state.config

# ── Provider Selection ──
with st.expander("Provider Selection", expanded=True):
    c1, c2 = st.columns(2)
    with c1:
        pos_options = ["phorest", "vagaro", "fresha", "zenoti", "mindbody"]
        pos_idx = pos_options.index(cfg.get("pos_source", "phorest")) if cfg.get("pos_source", "phorest") in pos_options else 0
        pos = st.selectbox("POS Source", pos_options, index=pos_idx,
                           help="Only Phorest is currently implemented. Others are planned for future.")
        cfg["pos_source"] = pos
        if pos != "phorest":
            st.caption(":construction: This POS adapter is not yet implemented.")

    with c2:
        prov_options = ["adp", "trinet", "paylocity"]
        prov_idx = prov_options.index(cfg.get("payroll_provider", "adp")) if cfg.get("payroll_provider", "adp") in prov_options else 0
        prov = st.selectbox("Payroll Provider", prov_options, index=prov_idx)
        cfg["payroll_provider"] = prov

# ── Commission Thresholds ──
with st.expander("Commission Rules"):
    st.markdown("**Service Categories**")
    s1, s2, s3 = st.columns(3)
    with s1:
        cfg["service_category_4_treatment_threshold"] = st.number_input(
            "Cat 4: Treatment Threshold ($)",
            value=float(cfg.get("service_category_4_treatment_threshold", 800)),
            step=50.0, help="Minimum treatment revenue for 10% specialty commission",
        )
    with s2:
        cfg["service_category_4_rate"] = st.number_input(
            "Cat 4: Commission Rate",
            value=float(cfg.get("service_category_4_rate", 0.10)),
            step=0.01, format="%.2f",
        )
    with s3:
        cfg["service_category_5_apprentice_rate"] = st.number_input(
            "Cat 5: Apprentice Rate",
            value=float(cfg.get("service_category_5_apprentice_rate", 0.20)),
            step=0.01, format="%.2f",
        )

    st.markdown("**Retail Categories**")
    r1, r2, r3, r4 = st.columns(4)
    with r1:
        cfg["retail_threshold_499"] = st.number_input(
            "Retail Threshold ($)",
            value=float(cfg.get("retail_threshold_499", 499)),
            step=10.0,
        )
    with r2:
        cfg["retail_rate_10pct"] = st.number_input(
            "Retail Rate (10%)",
            value=float(cfg.get("retail_rate_10pct", 0.10)),
            step=0.01, format="%.2f",
        )
    with r3:
        cfg["retail_rate_5pct"] = st.number_input(
            "Retail Rate (5%)",
            value=float(cfg.get("retail_rate_5pct", 0.05)),
            step=0.01, format="%.2f",
        )
    with r4:
        cfg["product_sales_pct_of_service_for_commission"] = st.number_input(
            "Product % of Service",
            value=float(cfg.get("product_sales_pct_of_service_for_commission", 0.10)),
            step=0.01, format="%.2f", help="Retail Cat 4: product must exceed this % of service revenue",
        )

# ── Pay Code Mapping ──
with st.expander("Pay Code Mapping"):
    provider = cfg.get("payroll_provider", "adp")
    st.markdown(f"**Pay codes for {provider.upper()}**")

    if provider == "adp":
        codes = cfg.get("adp_pay_codes", {}) or {}
    elif provider == "trinet":
        codes = cfg.get("trinet_earnings_codes", {}) or {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}
    else:
        codes = cfg.get("paylocity_pay_codes", {}) or {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}

    p1, p2, p3 = st.columns(3)
    with p1:
        reg = st.text_input("Regular Hours Code", value=codes.get("regular_hours", "REG"), key="pc_reg")
    with p2:
        comm = st.text_input("Commission Code", value=codes.get("commission", "COMM"), key="pc_comm")
    with p3:
        tips = st.text_input("Tips Code", value=codes.get("tips", "TIPS"), key="pc_tips")

    new_codes = {"regular_hours": reg, "commission": comm, "tips": tips}
    if provider == "adp":
        cfg["adp_pay_codes"] = new_codes
    elif provider == "trinet":
        cfg["trinet_earnings_codes"] = new_codes
    else:
        cfg["paylocity_pay_codes"] = new_codes

# ── Column Mapping ──
with st.expander("Column Mapping"):
    st.markdown("**ADP Export Column Names**")
    st.caption("Leave blank to use defaults (employee_id, pay_code, amount, hours)")
    ac1, ac2, ac3, ac4 = st.columns(4)
    existing_adp_map = cfg.get("adp_column_mapping") or {}
    with ac1:
        adp_eid = st.text_input("Employee ID Column", value=existing_adp_map.get("employee_id", ""), key="adp_eid")
    with ac2:
        adp_code = st.text_input("Pay Code Column", value=existing_adp_map.get("pay_code", ""), key="adp_code")
    with ac3:
        adp_amt = st.text_input("Amount Column", value=existing_adp_map.get("amount", ""), key="adp_amt")
    with ac4:
        adp_hrs = st.text_input("Hours Column", value=existing_adp_map.get("hours", ""), key="adp_hrs")

    if any([adp_eid, adp_code, adp_amt, adp_hrs]):
        cfg["adp_column_mapping"] = {
            "employee_id": adp_eid or "employee_id",
            "pay_code": adp_code or "pay_code",
            "amount": adp_amt or "amount",
            "hours": adp_hrs or "hours",
        }
    else:
        cfg["adp_column_mapping"] = None

    st.markdown("---")
    st.markdown("**Hours Worked Column Mapping**")
    hours_mode = st.radio("Column Detection", ["Auto-detect", "Custom"], horizontal=True, key="hours_mode")
    if hours_mode == "Custom":
        hc1, hc2, hc3 = st.columns(3)
        existing_hours_map = cfg.get("hours_column_mapping") or {}
        with hc1:
            h_fn = st.text_input("First Name Column", value=existing_hours_map.get("first_name", ""), key="h_fn")
        with hc2:
            h_ln = st.text_input("Last Name Column", value=existing_hours_map.get("last_name", ""), key="h_ln")
        with hc3:
            h_hrs = st.text_input("Hours Column", value=existing_hours_map.get("hours", ""), key="h_hrs")
        mapping = {}
        if h_fn:
            mapping["first_name"] = h_fn
        if h_ln:
            mapping["last_name"] = h_ln
        if h_hrs:
            mapping["hours"] = h_hrs
        cfg["hours_column_mapping"] = mapping if mapping else None
    else:
        cfg["hours_column_mapping"] = None

# ── Exception Detection ──
with st.expander("Exception Detection"):
    e1, e2 = st.columns(2)
    with e1:
        db_mode = st.radio(
            "Double-Booking Allocation",
            ["flag_only", "primary_location"],
            index=0 if cfg.get("double_booking_allocation") == "flag_only" else 1,
            help="flag_only: report only. primary_location: keep highest-revenue location, remove rest.",
        )
        cfg["double_booking_allocation"] = db_mode
    with e2:
        outlier_mult = st.slider(
            "Revenue Outlier Sensitivity (std multiplier)",
            min_value=1.0, max_value=5.0,
            value=float(cfg.get("revenue_outlier_std_multiplier", 2.0)),
            step=0.5, help="Lower = more sensitive (flags more outliers). Default: 2.0",
        )
        cfg["revenue_outlier_std_multiplier"] = outlier_mult

# ── Advanced ──
with st.expander("Advanced"):
    cfg["fail_fast_on_missing_file"] = st.toggle(
        "Fail Fast on Missing File",
        value=bool(cfg.get("fail_fast_on_missing_file", False)),
    )
    st.text_input("Warehouse Path", value=cfg.get("path_warehouse", ""), disabled=True)
    st.text_input("Audit Log Path", value=cfg.get("path_override_audit_log", ""), disabled=True)

# ── Apply / Reset ──
st.markdown("---")
c1, c2 = st.columns([1, 5])
with c1:
    if st.button("Reset to Defaults", key="reset_btn"):
        reset_config()
        st.rerun()

st.session_state.config = cfg
st.success("Settings are saved automatically and will apply to the next pipeline run.")
