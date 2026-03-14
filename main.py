"""
Salon Payroll & Sales Analysis – Main entry point.
Run: python main.py
"""
import os
import sys
from datetime import datetime

import pandas as pd

from config import CONFIG
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
    export_adp_csv,
    write_run_log,
)
from integrations import get_pos_adapter, get_payroll_adapter, get_payroll_export_path
from payroll_warehouse import save_payroll_run, build_trend_report


def main():
    print("=" * 60)
    print("  Salon Payroll & Sales Analysis")
    print("=" * 60)

    # 1) Resolve branch CSVs
    places = [p for p in CONFIG["list_of_places"] if os.path.isfile(p)]
    if not places:
        print("ERROR: No branch CSV files found. Check CONFIG['list_of_places'] in config.py")
        return False
    print(f"\n[1/12] Found {len(places)} branch(es): {', '.join(os.path.basename(p) for p in places)}")

    # 2) Date range
    dates_path = CONFIG["path_dates"]
    if os.path.isfile(dates_path):
        df_dates = pd.read_excel(dates_path)
    else:
        print(f"  WARNING: Date table not found at {dates_path}, using default period.")
        df_dates = pd.DataFrame(
            [[datetime(2024, 11, 10), datetime(2024, 11, 16)]],
            columns=["first_day", "last_day"],
        )
    first_day, last_day = df_dates.iloc[0, 0], df_dates.iloc[0, -1]
    print(f"[2/12] Pay period: {first_day} to {last_day}")

    # 3) Ingest (POS adapter)
    pos = get_pos_adapter(CONFIG)
    print(f"[3/12] Ingesting sales data via {pos.name}...")
    database_01 = pos.ingest(places, df_dates, CONFIG)
    if database_01.empty:
        print("ERROR: No data after ingestion. Check branch CSV format.")
        return False
    database_01.to_excel(CONFIG["path_database"], index=False)
    print(f"  -> {len(database_01)} rows ingested")

    # 4) Location summary
    location_summary = build_location_summary(database_01)
    location_summary.to_excel(CONFIG["path_location_summary"], index=False)
    print(f"[4/12] Location summary: {len(location_summary)} locations")

    # 5) Service classification
    database_01 = pd.read_excel(CONFIG["path_database"])
    database_01, missing_services = add_service_category(
        database_01, CONFIG["path_bridge_service_categories"]
    )
    database_01.to_excel(CONFIG["path_database"], index=False)
    if not missing_services.empty:
        missing_services.to_excel(CONFIG["path_missing_services"], index=False)
        print(f"[5/12] Service classification done. WARNING: {len(missing_services)} unmapped service(s) -> missing_services.xlsx")
    else:
        print("[5/12] Service classification done. All services mapped.")

    # 6) Employee ID mapping + double-booking
    database_01, missing_employees = add_id(database_01, CONFIG["path_master_employee"])
    database_01, flagged_ids = apply_double_booking_allocation(
        database_01, CONFIG, first_day, last_day, CONFIG.get("path_override_audit_log")
    )
    database_01.to_excel(CONFIG["path_database"], index=False)
    if not missing_employees.empty:
        missing_employees.to_excel(
            CONFIG.get("path_missing_employees", os.path.join("output", "missing_employees.xlsx")),
            index=False,
        )
        print(f"[6/12] Employee mapping done. WARNING: {len(missing_employees)} unmatched employee(s) -> missing_employees.xlsx")
    else:
        print("[6/12] Employee mapping done. All employees matched.")
    if flagged_ids:
        print(f"  -> {len(flagged_ids)} employee(s) flagged for double-booking")

    # 7) Hours worked
    hours_summ = hour_worked(
        CONFIG["path_hours_worked"],
        CONFIG["path_master_employee"],
        CONFIG["path_hours_summ"],
        CONFIG,
    )
    print(f"[7/12] Hours summarized: {len(hours_summ)} employees")

    # 8) Validation
    validation_issues = validate_inputs(
        database_01,
        CONFIG["path_master_employee"],
        CONFIG["path_bridge_service_categories"],
        CONFIG["path_hours_summ"],
    )
    if validation_issues:
        print(f"[8/12] Validation: {len(validation_issues)} issue(s) found")
        for vi in validation_issues:
            print(f"  [{vi['severity'].upper()}] {vi['message']}")
    else:
        print("[8/12] Validation: No issues found")

    # 9) Commission calculation
    final = calculation(
        CONFIG["path_database"],
        CONFIG["path_master_employee"],
        CONFIG["path_hours_summ"],
        CONFIG,
    )
    final = apply_double_booking_flag_to_payroll(final, flagged_ids)
    final.to_excel(CONFIG["path_final_database"], index=False)
    print(f"[9/12] Payroll calculated: {len(final)} employees")

    # 10) Payroll cost by location
    payroll_cost_loc = build_payroll_cost_by_location(database_01, final)
    if not payroll_cost_loc.empty:
        payroll_cost_loc.to_excel(CONFIG.get("path_payroll_cost_by_location", os.path.join("output", "payroll_cost_by_location.xlsx")), index=False)
        print(f"[10/12] Payroll cost by location: {len(payroll_cost_loc)} locations")
    else:
        print("[10/12] Payroll cost by location: no data")

    # 11) Exception report
    exception_df = build_exception_report(
        missing_employees, missing_services, validation_issues, final, first_day, last_day,
        database=database_01,
    )
    exception_df.to_excel(CONFIG["path_exception_report"], index=False)
    print(f"[11/12] Exception report: {len(exception_df)} item(s)")

    # 12) Payroll export + run log + warehouse
    path_payroll = get_payroll_export_path(CONFIG)
    adapter = get_payroll_adapter(CONFIG)
    adapter.export(final, path_payroll, CONFIG)
    print(f"[12/12] Payroll export ({adapter.name}): {path_payroll}")

    write_run_log(
        CONFIG["path_run_log"],
        {k: v for k, v in CONFIG.items() if k.startswith("path_") and isinstance(v, str)},
        (first_day, last_day),
        validation_issues,
        {"database": len(database_01), "payroll": len(final), "exceptions": len(exception_df)},
    )

    wh_path = CONFIG.get("path_warehouse")
    if wh_path:
        save_payroll_run(wh_path, final, first_day, last_day, location_summary, len(exception_df))
        build_trend_report(wh_path, CONFIG.get("path_trend_report", ""))
        print(f"  -> Historical warehouse updated: {wh_path}")

    # Summary
    print("\n" + "=" * 60)
    print("  DONE - All outputs in:", os.path.dirname(CONFIG["path_database"]))
    print("=" * 60)
    print(f"  Employees processed:  {len(final)}")
    print(f"  Locations:            {len(location_summary)}")
    print(f"  Exceptions:           {len(exception_df)}")
    print(f"  Validation issues:    {len(validation_issues)}")
    if flagged_ids:
        print(f"  Double-booking flags: {len(flagged_ids)}")
    print()
    return True


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
