"""File upload handling, temp file management, and download helpers."""
import io
import json
import os

import pandas as pd


def save_uploads(uploaded_files: dict, temp_dir: str, config: dict) -> dict:
    """Save uploaded Streamlit files to temp directory and update config paths.

    uploaded_files keys: branch_csvs, bridge, master, hours, dates
    Returns updated config dict.
    """
    sales_dir = os.path.join(temp_dir, "Sales")
    os.makedirs(sales_dir, exist_ok=True)
    out_dir = os.path.join(temp_dir, "output")
    os.makedirs(out_dir, exist_ok=True)

    # Branch CSVs
    branch_csvs = uploaded_files.get("branch_csvs", [])
    csv_paths = []
    for f in branch_csvs:
        path = os.path.join(sales_dir, f.name)
        with open(path, "wb") as fh:
            fh.write(f.getbuffer())
        csv_paths.append(path)
    if csv_paths:
        config["list_of_places"] = csv_paths

    # Bridge service categories
    bridge = uploaded_files.get("bridge")
    if bridge:
        path = os.path.join(temp_dir, "bridge_service_categories.xlsx")
        with open(path, "wb") as fh:
            fh.write(bridge.getbuffer())
        config["path_bridge_service_categories"] = path

    # Master employee
    master = uploaded_files.get("master")
    if master:
        path = os.path.join(temp_dir, "master_employee.xlsx")
        with open(path, "wb") as fh:
            fh.write(master.getbuffer())
        config["path_master_employee"] = path

    # Hours worked
    hours = uploaded_files.get("hours")
    if hours:
        path = os.path.join(temp_dir, "hours_worked.xlsx")
        with open(path, "wb") as fh:
            fh.write(hours.getbuffer())
        config["path_hours_worked"] = path

    # Date table
    dates = uploaded_files.get("dates")
    if dates:
        path = os.path.join(temp_dir, "Tabela_Datas.xlsx")
        with open(path, "wb") as fh:
            fh.write(dates.getbuffer())
        config["path_dates"] = path

    # Update output paths to temp dir
    config["path_database"] = os.path.join(out_dir, "Database.xlsx")
    config["path_missing_services"] = os.path.join(out_dir, "missing_services.xlsx")
    config["path_missing_employees"] = os.path.join(out_dir, "missing_employees.xlsx")
    config["path_hours_summ"] = os.path.join(out_dir, "hours_worked_summ.xlsx")
    config["path_final_database"] = os.path.join(out_dir, "final_database.xlsx")
    config["path_exception_report"] = os.path.join(out_dir, "exception_report.xlsx")
    config["path_adp_import"] = os.path.join(out_dir, "ADP_payroll_import.csv")
    config["path_trinet_import"] = os.path.join(out_dir, "Trinet_payroll_import.csv")
    config["path_paylocity_import"] = os.path.join(out_dir, "Paylocity_payroll_import.csv")
    config["path_location_summary"] = os.path.join(out_dir, "location_summary.xlsx")
    config["path_run_log"] = os.path.join(out_dir, "run_log.json")
    config["path_warehouse"] = os.path.join(out_dir, "payroll_warehouse.db")
    config["path_trend_report"] = os.path.join(out_dir, "payroll_trend_report.xlsx")
    config["path_payroll_cost_by_location"] = os.path.join(out_dir, "payroll_cost_by_location.xlsx")
    config["path_override_audit_log"] = os.path.join(out_dir, "override_audit.jsonl")

    return config


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to Excel bytes for st.download_button."""
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode("utf-8")


def json_to_bytes(data: dict) -> bytes:
    """Serialize dict to JSON bytes."""
    return json.dumps(data, indent=2).encode("utf-8")
