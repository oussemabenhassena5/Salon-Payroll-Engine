"""Run the full pipeline for integration testing. Uses TestBranch.csv if no other branch CSVs exist."""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    write_run_log,
)
from integrations import get_pos_adapter, get_payroll_adapter, get_payroll_export_path

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Ensure test data exists and is in the list
    test_csv = os.path.join(base, "branch_data", "TestBranch.csv")
    if os.path.isfile(test_csv):
        places = [p for p in CONFIG["list_of_places"] if os.path.isfile(p)]
        if not places:
            CONFIG["list_of_places"] = [test_csv]
    places = [p for p in CONFIG["list_of_places"] if os.path.isfile(p)]
    if not places:
        print("No branch CSVs found. Run tests/make_test_data.py first.")
        return False

    # 1) Dates
    dates_path = CONFIG["path_dates"]
    if os.path.isfile(dates_path):
        df_dates = pd.read_excel(dates_path)
    else:
        df_dates = pd.DataFrame(
            [[datetime(2024, 11, 10), datetime(2024, 11, 16)]],
            columns=["first_day", "last_day"],
        )
    first_day, last_day = df_dates.iloc[0, 0], df_dates.iloc[0, -1]

    # 2) Ingest (POS adapter: Phorest by default)
    database_01 = get_pos_adapter(CONFIG).ingest(places, df_dates, CONFIG)
    assert not database_01.empty, "Empty after process_sheets"
    database_01.to_excel(CONFIG["path_database"], index=False)

    # 2b) Location summary
    location_summary = build_location_summary(database_01)
    location_summary.to_excel(CONFIG["path_location_summary"], index=False)

    # 3) Service category
    database_01 = pd.read_excel(CONFIG["path_database"])
    database_01, missing_services = add_service_category(
        database_01, CONFIG["path_bridge_service_categories"]
    )
    database_01.to_excel(CONFIG["path_database"], index=False)
    if not missing_services.empty:
        missing_services.to_excel(CONFIG["path_missing_services"], index=False)

    # 4) Add ID
    database_01, missing_employees = add_id(database_01, CONFIG["path_master_employee"])
    # 4b) Double-booking allocation and audit
    database_01, flagged_ids = apply_double_booking_allocation(
        database_01, CONFIG, first_day, last_day, CONFIG.get("path_override_audit_log")
    )
    database_01.to_excel(CONFIG["path_database"], index=False)
    if not missing_employees.empty:
        missing_employees.to_excel(CONFIG["path_missing_employees"], index=False)

    # 5) Hours
    hour_worked(
        CONFIG["path_hours_worked"],
        CONFIG["path_master_employee"],
        CONFIG["path_hours_summ"],
        CONFIG,
    )

    # 6) Validate
    validation_issues = validate_inputs(
        database_01,
        CONFIG["path_master_employee"],
        CONFIG["path_bridge_service_categories"],
        CONFIG["path_hours_summ"],
    )

    # 7) Calculation
    final = calculation(
        CONFIG["path_database"],
        CONFIG["path_master_employee"],
        CONFIG["path_hours_summ"],
        CONFIG,
    )
    final = apply_double_booking_flag_to_payroll(final, flagged_ids)
    final.to_excel(CONFIG["path_final_database"], index=False)

    # 8) Exception report
    exception_df = build_exception_report(
        missing_employees, missing_services, validation_issues, final, first_day, last_day,
        database=database_01,
    )
    exception_df.to_excel(CONFIG["path_exception_report"], index=False)

    # 9) Payroll export (ADP / TriNet / Paylocity per config)
    path_payroll = get_payroll_export_path(CONFIG)
    get_payroll_adapter(CONFIG).export(final, path_payroll, CONFIG)

    # 10) Run log
    write_run_log(
        CONFIG["path_run_log"],
        {k: v for k, v in CONFIG.items() if k.startswith("path_") and isinstance(v, str)},
        (first_day, last_day),
        validation_issues,
        {"database": len(database_01), "payroll": len(final), "exceptions": len(exception_df)},
    )

    # 11) Historical warehouse and trend report
    from payroll_warehouse import save_payroll_run, build_trend_report
    save_payroll_run(
        CONFIG.get("path_warehouse", os.path.join(os.path.dirname(CONFIG["path_database"]), "payroll_warehouse.db")),
        final, first_day, last_day,
        location_summary_df=location_summary,
        exception_count=len(exception_df),
    )
    build_trend_report(
        CONFIG.get("path_warehouse", os.path.join(os.path.dirname(CONFIG["path_database"]), "payroll_warehouse.db")),
        CONFIG.get("path_trend_report", os.path.join(os.path.dirname(CONFIG["path_database"]), "payroll_trend_report.xlsx")),
    )

    # Verify outputs
    assert os.path.isfile(CONFIG["path_database"]), "Database.xlsx missing"
    assert os.path.isfile(CONFIG["path_final_database"]), "final_database.xlsx missing"
    assert os.path.isfile(path_payroll), "Payroll export CSV missing"
    assert os.path.isfile(CONFIG["path_exception_report"]), "exception_report.xlsx missing"
    assert os.path.isfile(CONFIG["path_run_log"]), "run_log.json missing"
    assert os.path.isfile(CONFIG["path_location_summary"]), "location_summary.xlsx missing"
    if CONFIG.get("path_warehouse"):
        assert os.path.isfile(CONFIG["path_warehouse"]), "payroll_warehouse.db missing"
    if CONFIG.get("path_trend_report"):
        assert os.path.isfile(CONFIG["path_trend_report"]), "payroll_trend_report.xlsx missing"

    fd = pd.read_excel(CONFIG["path_final_database"])
    assert "id" in fd.columns and "employee" in fd.columns
    assert "service_comission" in fd.columns or "service_commission" in fd.columns
    assert "total_retail_commission" in fd.columns
    assert len(fd) >= 1

    payroll_csv = pd.read_csv(path_payroll)
    # ADP: employee_id, pay_code; TriNet: Employee ID, Earnings Code; Paylocity: EmployeeId, PayCode
    assert any(c in payroll_csv.columns for c in ["employee_id", "Employee ID", "EmployeeId"])
    assert any(c in payroll_csv.columns for c in ["pay_code", "Earnings Code", "PayCode"])

    print("Full pipeline test OK. Outputs in", os.path.dirname(CONFIG["path_database"]))
    return True

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
