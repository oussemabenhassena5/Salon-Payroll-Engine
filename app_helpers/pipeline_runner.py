"""12-step pipeline orchestrator — mirrors main.py exactly, returns all results."""
import os
import sys
from datetime import datetime
from typing import Any, Callable

import pandas as pd

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from payroll_engine import (
    add_service_category,
    add_id,
    hour_worked,
    validate_inputs,
    apply_double_booking_allocation,
    apply_double_booking_flag_to_payroll,
    calculation,
    build_exception_report,
    build_location_summary,
    build_payroll_cost_by_location,
    write_run_log,
)
from integrations import get_pos_adapter, get_payroll_adapter, get_payroll_export_path
from payroll_warehouse import save_payroll_run, build_trend_report


def run_pipeline(
    config: dict,
    manual_dates: tuple[Any, Any] | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict:
    """Execute the full 12-step payroll pipeline.

    Args:
        config: CONFIG dict with all paths and settings.
        manual_dates: Optional (first_day, last_day) if no date table uploaded.
        on_progress: Callback(step, total_steps, message) for progress updates.

    Returns:
        Dict with all intermediate DataFrames and metadata.
    """
    results = {}
    total = 12

    def progress(step: int, msg: str):
        if on_progress:
            on_progress(step, total, msg)

    # Step 1: Resolve branch CSVs
    progress(1, "Resolving branch files...")
    places = [p for p in config["list_of_places"] if os.path.isfile(p)]
    if not places:
        raise ValueError("No branch CSV files found. Please upload at least one branch CSV.")
    results["places"] = places
    results["branch_names"] = [os.path.splitext(os.path.basename(p))[0] for p in places]

    # Step 2: Load date range
    progress(2, "Loading date range...")
    dates_path = config.get("path_dates", "")
    if dates_path and os.path.isfile(dates_path):
        df_dates = pd.read_excel(dates_path)
    elif manual_dates:
        df_dates = pd.DataFrame(
            [[manual_dates[0], manual_dates[1]]],
            columns=["first_day", "last_day"],
        )
    else:
        df_dates = pd.DataFrame(
            [[datetime(2024, 11, 10), datetime(2024, 11, 16)]],
            columns=["first_day", "last_day"],
        )
    first_day, last_day = df_dates.iloc[0, 0], df_dates.iloc[0, -1]
    results["first_day"] = first_day
    results["last_day"] = last_day

    # Step 3: Ingest sales data
    progress(3, "Ingesting sales data...")
    pos = get_pos_adapter(config)
    database_01 = pos.ingest(places, df_dates, config)
    if database_01.empty:
        raise ValueError("No data after ingestion. Check branch CSV format.")
    database_01.to_excel(config["path_database"], index=False)
    results["database_raw"] = database_01
    results["pos_name"] = pos.name

    # Step 4: Location summary
    progress(4, "Building location summary...")
    location_summary = build_location_summary(database_01)
    location_summary.to_excel(config["path_location_summary"], index=False)
    results["location_summary"] = location_summary

    # Step 5: Service classification
    progress(5, "Classifying services...")
    database_01 = pd.read_excel(config["path_database"])
    database_01, missing_services = add_service_category(
        database_01, config["path_bridge_service_categories"]
    )
    database_01.to_excel(config["path_database"], index=False)
    if not missing_services.empty:
        missing_services.to_excel(config["path_missing_services"], index=False)
    results["missing_services"] = missing_services

    # Step 6: Employee ID mapping + double-booking
    progress(6, "Mapping employees and detecting exceptions...")
    database_01, missing_employees = add_id(database_01, config["path_master_employee"])
    database_01, flagged_ids = apply_double_booking_allocation(
        database_01, config, first_day, last_day, config.get("path_override_audit_log")
    )
    database_01.to_excel(config["path_database"], index=False)
    if not missing_employees.empty:
        missing_employees.to_excel(config.get("path_missing_employees", ""), index=False)
    results["database"] = database_01
    results["missing_employees"] = missing_employees
    results["flagged_ids"] = flagged_ids

    # Step 7: Hours worked
    progress(7, "Summarizing hours worked...")
    hours_summ = hour_worked(
        config["path_hours_worked"],
        config["path_master_employee"],
        config["path_hours_summ"],
        config,
    )
    results["hours_summ"] = hours_summ

    # Step 8: Validation
    progress(8, "Running validation checks...")
    validation_issues = validate_inputs(
        database_01,
        config["path_master_employee"],
        config["path_bridge_service_categories"],
        config["path_hours_summ"],
    )
    results["validation_issues"] = validation_issues

    # Step 9: Commission calculation
    progress(9, "Calculating commissions...")
    final = calculation(
        config["path_database"],
        config["path_master_employee"],
        config["path_hours_summ"],
        config,
    )
    final = apply_double_booking_flag_to_payroll(final, flagged_ids)
    final.to_excel(config["path_final_database"], index=False)
    results["final"] = final

    # Step 10: Payroll cost by location
    progress(10, "Computing payroll cost by location...")
    payroll_cost_loc = build_payroll_cost_by_location(database_01, final)
    if not payroll_cost_loc.empty:
        payroll_cost_loc.to_excel(
            config.get("path_payroll_cost_by_location", ""), index=False
        )
    results["payroll_cost_loc"] = payroll_cost_loc

    # Step 11: Exception report
    progress(11, "Building exception report...")
    exception_df = build_exception_report(
        missing_employees, missing_services, validation_issues, final,
        first_day, last_day, database=database_01,
    )
    exception_df.to_excel(config["path_exception_report"], index=False)
    results["exception_df"] = exception_df

    # Step 12: Export + warehouse + trend report
    progress(12, "Exporting payroll and saving to warehouse...")
    path_payroll = get_payroll_export_path(config)
    adapter = get_payroll_adapter(config)
    adapter.export(final, path_payroll, config)
    results["payroll_export_path"] = path_payroll
    results["payroll_provider"] = adapter.name

    write_run_log(
        config["path_run_log"],
        {k: v for k, v in config.items() if k.startswith("path_") and isinstance(v, str)},
        (first_day, last_day),
        validation_issues,
        {"database": len(database_01), "payroll": len(final), "exceptions": len(exception_df)},
    )

    wh_path = config.get("path_warehouse")
    if wh_path:
        save_payroll_run(wh_path, final, first_day, last_day, location_summary, len(exception_df))
        trend_path = config.get("path_trend_report", "")
        if trend_path:
            build_trend_report(wh_path, trend_path)
        results["warehouse_path"] = wh_path

    return results
