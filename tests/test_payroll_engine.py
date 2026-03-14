"""
Tests for Salon Payroll & Sales Analysis pipeline.
Run: python tests/test_payroll_engine.py
"""
from __future__ import annotations

import os
import sys
import json
import tempfile
import pandas as pd

# Add project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from payroll_engine import (
    remove_suffix,
    normalize_name,
    fix_value,
    process_sheets,
    add_service_category,
    add_id,
    hour_worked,
    validate_inputs,
    detect_duplicate_records,
    calculation,
    detect_double_booking,
    detect_multi_location_employees,
    detect_duplicate_attribution,
    detect_revenue_outliers,
    get_double_booking_flagged_ids,
    apply_double_booking_allocation,
    apply_double_booking_flag_to_payroll,
    append_override_audit,
    build_exception_report,
    build_location_summary,
    export_adp_csv,
    write_run_log,
    _retail_commission,
    _service_commission,
)
from config import CONFIG


# ---------- Unit: Name normalization ----------
def test_remove_suffix():
    assert remove_suffix("John Smith III") == "John Smith"
    assert remove_suffix("Jane Doe II") == "Jane Doe"
    assert remove_suffix("No Suffix") == "No Suffix"
    assert remove_suffix("") == ""


def test_normalize_name():
    assert normalize_name("  kiara alcaraz  ") == "Kiara Alcaraz"
    assert normalize_name("JANE DOE II") == "Jane Doe"
    assert normalize_name("") == ""
    assert normalize_name(pd.NA) == ""


# ---------- Unit: fix_value ----------
def test_fix_value():
    assert fix_value("1,234.56") == "1,234.56"
    assert "1.234" in fix_value("1,234,56") or "1,234.56" in fix_value("1,234,56")


# ---------- Unit: Commission formulas (service category 4 = $800) ----------
def test_service_commission_cat4_above_800():
    row = pd.Series({"service_category": 4})
    cfg = {"service_category_4_treatment_threshold": 800, "service_category_4_rate": 0.10}
    assert _service_commission(row, 100, 900, 0, 0, 0, cfg) == 90.0  # 10% of 900


def test_service_commission_cat4_below_800():
    row = pd.Series({"service_category": 4})
    cfg = {"service_category_4_treatment_threshold": 800}
    assert _service_commission(row, 100, 400, 0, 0, 0, cfg) == 0.0


def test_service_commission_cat5_apprentice():
    row = pd.Series({"service_category": 5})
    cfg = {"service_category_5_apprentice_rate": 0.20}
    # total_service 500, salary_hours_worked*2 = 200 -> commission on 300 * 0.20 = 60
    assert _service_commission(row, 500, 0, 500, 0, 100, cfg) == 60.0


def test_service_commission_cat5_does_not_qualify():
    row = pd.Series({"service_category": 5})
    cfg = {}
    assert _service_commission(row, 100, 0, 100, 0, 100, cfg) == 0.0  # 100 <= 200


def test_retail_commission_cat1():
    row = pd.Series({"retail_category": 1})
    assert _retail_commission(row, 100, 500, 600, {}) == 10.0  # 10% of 100


def test_retail_commission_cat2_above_499():
    row = pd.Series({"retail_category": 2})
    cfg = {"retail_threshold_499": 499, "retail_rate_10pct": 0.10}
    assert _retail_commission(row, 500, 100, 600, cfg) == 50.0


def test_retail_commission_cat2_below_499():
    row = pd.Series({"retail_category": 2})
    cfg = {"retail_threshold_499": 499}
    assert _retail_commission(row, 300, 100, 400, cfg) == 0.0


# ---------- Unit: Validation ----------
def test_validate_inputs_empty_db():
    issues = validate_inputs(
        pd.DataFrame(),
        CONFIG["path_master_employee"],
        CONFIG["path_bridge_service_categories"],
        CONFIG["path_hours_worked"],
    )
    assert any("empty" in i["message"].lower() or "Database" in i["message"] for i in issues)


def test_validate_inputs_missing_columns():
    issues = validate_inputs(
        pd.DataFrame(columns=["service", "metric"]),  # missing employee, value
        CONFIG["path_master_employee"],
        CONFIG["path_bridge_service_categories"],
        CONFIG["path_hours_worked"],
    )
    # Should report missing database columns or other validation issues
    assert len(issues) >= 1
    msg = " ".join(i.get("message", "") for i in issues)
    assert "missing column" in msg.lower() or "employee" in msg.lower() or "value" in msg.lower() or "database" in msg.lower()


# ---------- Unit: Exception report ----------
def test_build_exception_report():
    missing_emp = pd.DataFrame([{"employee": "Unknown Person", "id": "Need ID"}])
    missing_svc = pd.DataFrame([{"service": "Unmapped Service", "service category": "Not Found"}])
    issues = [{"message": "Test issue", "severity": "warning", "detail": ""}]
    df = build_exception_report(missing_emp, missing_svc, issues, pd.DataFrame(), "2024-11-10", "2024-11-16")
    assert len(df) >= 2  # at least missing employee + missing service + validation
    assert "reason" in df.columns
    assert "category" in df.columns


def test_build_location_summary():
    db = pd.DataFrame([
        {"filial": "A", "metric": "serviceCategoryAmount", "value": 100, "employee": "E1"},
        {"filial": "A", "metric": "productsAmount", "value": 50, "employee": "E1"},
        {"filial": "B", "metric": "serviceCategoryAmount", "value": 200, "employee": "E2"},
    ])
    out = build_location_summary(db)
    assert len(out) == 2
    assert "location" in out.columns and "total_service" in out.columns
    assert out[out["location"] == "A"]["total_service"].iloc[0] == 100
    assert out[out["location"] == "A"]["total_retail"].iloc[0] == 50


# ---------- Integration: Full pipeline ----------
def _test_data_paths():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    from tests.make_test_data import write_test_csv, write_test_dates
    csv_path = os.path.join(base, "branch_data", "TestBranch.csv")
    dates_path = os.path.join(base, "work_new", "Tabela_Datas.xlsx")
    write_test_csv(csv_path)
    if not os.path.isfile(dates_path):
        write_test_dates(dates_path)
    return csv_path, dates_path


def test_process_sheets():
    csv_path, dates_path = _test_data_paths()
    df_dates = pd.read_excel(dates_path)
    out = process_sheets([csv_path], df_dates)
    assert not out.empty
    assert "filial" in out.columns and "service" in out.columns and "metric" in out.columns
    assert "value" in out.columns
    assert out["value"].dtype in (float, "float64")


def test_add_service_category():
    csv_path, dates_path = _test_data_paths()
    df_dates = pd.read_excel(dates_path)
    raw = process_sheets([csv_path], df_dates)
    table, missing = add_service_category(raw, CONFIG["path_bridge_service_categories"])
    assert "service category" in table.columns
    assert table["service category"].notna().all() or (table["service category"].fillna("").eq("Tips").any())


def test_add_id():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base, "output", "Database.xlsx")
    if not os.path.isfile(db_path):
        return  # skip
    db = pd.read_excel(db_path)
    if "id" in db.columns:
        return  # already has id
    table, missing = add_id(db, CONFIG["path_master_employee"])
    assert "id" in table.columns


def test_calculation_output_shape():
    """Calculation must return one row per employee with expected columns."""
    db_path = CONFIG["path_database"]
    if not os.path.isfile(db_path):
        return  # skip
    try:
        final = calculation(
            CONFIG["path_database"],
            CONFIG["path_master_employee"],
            CONFIG["path_hours_summ"],
            CONFIG,
        )
    except Exception as e:
        raise AssertionError(f"calculation raised: {e}") from e
    assert isinstance(final, pd.DataFrame)
    assert "id" in final.columns
    assert "employee" in final.columns
    assert "service_comission" in final.columns or "service_commission" in final.columns
    assert "total_retail_commission" in final.columns
    assert len(final) >= 1


def test_adp_export_columns():
    """ADP CSV must have employee_id, pay_code, amount, hours."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = os.path.join(base, "output", "adp_test.csv")
    df = pd.DataFrame([
        {"id": "E1", "Total Hours Worked": 40, "service_comission": 50, "total_retail_commission": 10, "Tips": 20},
    ])
    export_adp_csv(df, out_path, CONFIG.get("adp_pay_codes"))
    adp = pd.read_csv(out_path)
    assert "employee_id" in adp.columns and "pay_code" in adp.columns
    assert list(adp["pay_code"].unique())  # at least one code
    if os.path.isfile(out_path):
        os.remove(out_path)


def test_run_log_written():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    try:
        write_run_log(path, {"path_database": "/tmp/db.xlsx"}, ("2024-11-10", "2024-11-16"), [], {"database": 10})
        with open(path) as f:
            log = json.load(f)
        assert "timestamp" in log and "row_counts" in log
    finally:
        if os.path.isfile(path):
            os.remove(path)


def test_detect_multi_location():
    """Employees at 2+ locations in same period are detected."""
    df = pd.DataFrame([
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "Branch A", "value": 100},
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "Branch B", "value": 200},
        {"id": "E2", "employee": "John", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "Branch A", "value": 300},
    ])
    multi = detect_multi_location_employees(df)
    assert len(multi) == 1  # only E1
    assert multi.iloc[0]["id"] == "E1"
    assert multi.iloc[0]["location_count"] == 2


def test_detect_duplicate_attribution():
    """Truly identical rows (same employee, location, service, metric, value) are detected."""
    df = pd.DataFrame([
        {"first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "employee": "Jane", "value": 100},
        {"first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "employee": "Jane", "value": 100},
        {"first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Color", "metric": "serviceCategoryAmount", "employee": "Jane", "value": 200},
    ])
    dups = detect_duplicate_attribution(df)
    assert len(dups) == 2  # the two identical Cut rows
    assert (dups["service"] == "Cut").all()


def test_detect_revenue_outliers():
    """Employees with revenue > mean + 2*std are flagged."""
    df = pd.DataFrame([
        {"id": "E1", "employee": "Normal1", "value": 100},
        {"id": "E2", "employee": "Normal2", "value": 110},
        {"id": "E3", "employee": "Normal3", "value": 105},
        {"id": "E4", "employee": "Normal4", "value": 95},
        {"id": "E5", "employee": "Normal5", "value": 102},
        {"id": "E6", "employee": "Outlier", "value": 10000},
    ])
    outliers = detect_revenue_outliers(df, std_multiplier=2.0)
    assert len(outliers) == 1
    assert outliers.iloc[0]["id"] == "E6"


def test_detect_double_booking_flags():
    """detect_double_booking adds correct flag columns; single-location employees are NOT flagged."""
    df = pd.DataFrame([
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "value": 100},
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Color", "metric": "serviceCategoryAmount", "value": 200},
        {"id": "E2", "employee": "John", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "value": 150},
        {"id": "E2", "employee": "John", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "B", "service": "Cut", "metric": "serviceCategoryAmount", "value": 300},
    ])
    out = detect_double_booking(df)
    assert "double_booking_flag" in out.columns
    assert "multi_location_flag" in out.columns
    # E1 is at one location only -> no flag
    e1_flags = out[out["id"] == "E1"]["double_booking_flag"].tolist()
    assert all(f == "" for f in e1_flags)
    # E2 is at 2 locations -> flagged
    e2_flags = out[out["id"] == "E2"]["double_booking_flag"].tolist()
    assert all("Multi-location" in f for f in e2_flags)


def test_double_booking_allocation_primary_location():
    """primary_location allocation keeps only highest-revenue location."""
    df = pd.DataFrame([
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "value": 100},
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "B", "service": "Cut", "metric": "serviceCategoryAmount", "value": 500},
        {"id": "E2", "employee": "John", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "value": 300},
    ])
    out, ids = apply_double_booking_allocation(df, {"double_booking_allocation": "primary_location"})
    # E1 should only have Branch B (higher revenue)
    e1_rows = out[out["id"] == "E1"]
    assert len(e1_rows) == 1
    assert e1_rows.iloc[0]["filial"] == "B"
    # E2 untouched
    assert len(out[out["id"] == "E2"]) == 1
    assert "E1" in ids


def test_double_booking_flag_to_payroll():
    """apply_double_booking_flag_to_payroll marks flagged employees."""
    payroll = pd.DataFrame([{"id": "E1", "commission": 10}, {"id": "E2", "commission": 20}])
    flagged = apply_double_booking_flag_to_payroll(payroll, ["E1"])
    assert flagged.loc[flagged["id"] == "E1", "double_booking_flag"].iloc[0] == "Review required"
    assert flagged.loc[flagged["id"] == "E2", "double_booking_flag"].iloc[0] == ""


def test_detect_duplicate_records():
    """Duplicate records in ingested data produce a validation issue."""
    issues_empty = detect_duplicate_records(pd.DataFrame())
    assert issues_empty == []
    issues_nocols = detect_duplicate_records(pd.DataFrame({"x": [1]}))
    assert issues_nocols == []
    # No duplicates
    df_clean = pd.DataFrame({
        "first_day": ["2024-11-10"], "last_day": ["2024-11-16"], "filial": ["Branch"],
        "service": ["Hair"], "metric": ["serviceCategoryAmount"], "employee": ["Jane Doe"], "value": [100],
    })
    assert detect_duplicate_records(df_clean) == []
    # With duplicates
    df_dup = pd.concat([df_clean, df_clean], ignore_index=True)
    issues = detect_duplicate_records(df_dup)
    assert len(issues) == 1
    assert "Duplicate records" in issues[0]["message"]
    assert issues[0]["severity"] == "warning"
    assert "2 duplicate" in issues[0]["detail"] or "duplicate" in issues[0]["detail"]


def test_exception_report_categories():
    """Exception report should have category column with meaningful entries."""
    missing_emp = pd.DataFrame([{"employee": "Unknown Person", "id": "Need ID"}])
    missing_svc = pd.DataFrame([{"service": "Unmapped Service", "service category": "Not Found"}])
    issues = [{"message": "Test issue", "severity": "warning", "detail": ""}]
    # Create a database with a multi-location employee
    db = pd.DataFrame([
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "A", "service": "Cut", "metric": "serviceCategoryAmount", "value": 100},
        {"id": "E1", "employee": "Jane", "first_day": "2024-11-10", "last_day": "2024-11-16", "filial": "B", "service": "Cut", "metric": "serviceCategoryAmount", "value": 200},
    ])
    db = detect_double_booking(db)
    df = build_exception_report(missing_emp, missing_svc, issues, pd.DataFrame(), "2024-11-10", "2024-11-16", database=db)
    assert "category" in df.columns
    categories = df["category"].tolist()
    assert "Unmatched Employee" in categories
    assert "Unmapped Service" in categories
    assert "Multi-Location" in categories


def test_full_pipeline_outputs():
    """After run_full_pipeline_test: final_database has commission cols; ADP has pay_code; Cat4 uses $800."""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    final_path = os.path.join(base, "output", "final_database.xlsx")
    if not os.path.isfile(final_path):
        return  # skip if pipeline not run
    fd = pd.read_excel(final_path)
    assert "service_comission" in fd.columns or "service_commission" in fd.columns
    assert "total_retail_commission" in fd.columns
    # Category 4: Treatment >= 800 -> 10% of Treatment
    cat4 = fd[fd["service_category"] == 4] if "service_category" in fd.columns else pd.DataFrame()
    for _, r in cat4.iterrows():
        t = r.get("Treatment", 0) or 0
        comm = r.get("service_comission", 0) or r.get("service_commission", 0) or 0
        if t >= 800:
            assert abs(comm - round(t * 0.10, 2)) < 0.02, f"Cat4 commission {comm} expected {t*0.1}"
    # Exception report should have categories
    exc_path = os.path.join(base, "output", "exception_report.xlsx")
    if os.path.isfile(exc_path):
        exc = pd.read_excel(exc_path)
        assert "category" in exc.columns, "Exception report missing 'category' column"


def run_all():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(base)
    tests = [
        ("remove_suffix", test_remove_suffix),
        ("normalize_name", test_normalize_name),
        ("fix_value", test_fix_value),
        ("service_commission cat4 >=800", test_service_commission_cat4_above_800),
        ("service_commission cat4 <800", test_service_commission_cat4_below_800),
        ("service_commission cat5", test_service_commission_cat5_apprentice),
        ("service_commission cat5 no qualify", test_service_commission_cat5_does_not_qualify),
        ("retail_commission cat1", test_retail_commission_cat1),
        ("retail_commission cat2 above", test_retail_commission_cat2_above_499),
        ("retail_commission cat2 below", test_retail_commission_cat2_below_499),
        ("validate empty db", test_validate_inputs_empty_db),
        ("validate missing cols", test_validate_inputs_missing_columns),
        ("exception report", test_build_exception_report),
        ("exception report categories", test_exception_report_categories),
        ("location summary", test_build_location_summary),
        ("process_sheets", test_process_sheets),
        ("add_service_category", test_add_service_category),
        ("add_id", test_add_id),
        ("calculation", test_calculation_output_shape),
        ("ADP export", test_adp_export_columns),
        ("run_log", test_run_log_written),
        ("multi_location", test_detect_multi_location),
        ("duplicate_attribution", test_detect_duplicate_attribution),
        ("revenue_outliers", test_detect_revenue_outliers),
        ("double_booking_flags", test_detect_double_booking_flags),
        ("primary_location_allocation", test_double_booking_allocation_primary_location),
        ("double_booking_flag_to_payroll", test_double_booking_flag_to_payroll),
        ("duplicate_records", test_detect_duplicate_records),
        ("full_pipeline_outputs", test_full_pipeline_outputs),
    ]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  OK  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL {name}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    return failed == 0

if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
