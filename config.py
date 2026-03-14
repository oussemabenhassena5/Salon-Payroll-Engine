"""
Configuration for Salon Payroll & Sales Analysis pipeline.
Update paths for your environment (local or Colab).
Support for up to 8 locations.
"""
import os

# Base directory: use work_new for inputs; project root for outputs when running locally
_BASE = os.path.dirname(os.path.abspath(__file__))
_WORK_NEW = os.path.join(_BASE, "work_new")
_SALES = os.path.join(_WORK_NEW, "Sales")
_OUT = os.path.join(_BASE, "output")

# Ensure output directory exists
os.makedirs(_OUT, exist_ok=True)

CONFIG = {
    # Branch CSV paths (from work_new/Sales when extracted)
    "list_of_places": [
        os.path.join(_SALES, "Bay Ridge.csv"),
        os.path.join(_SALES, "Carroll Gardens.csv"),
        os.path.join(_SALES, "Dekalb.csv"),
        os.path.join(_SALES, "Dumbo.csv"),
        os.path.join(_SALES, "Fulton.csv"),
        os.path.join(_SALES, "Myrtle.csv"),
        os.path.join(_SALES, "Vanderbilt.csv"),
        # 8th location: add path when branch CSV is available (e.g. os.path.join(_SALES, "Eighth Branch.csv"))
        os.path.join(_SALES, "Eighth Branch.csv"),
    ],
    # Date range table (Excel) with first_day and last_day columns
    "path_dates": os.path.join(_WORK_NEW, "Tabela_Datas.xlsx"),
    # Master and bridge files
    "path_bridge_service_categories": os.path.join(_WORK_NEW, "bridge_service_categories.xlsx"),
    "path_master_employee": os.path.join(_WORK_NEW, "master_employee.xlsx"),
    "path_hours_worked": os.path.join(_WORK_NEW, "hours_worked.xlsx"),
    # Output paths
    "path_database": os.path.join(_OUT, "Database.xlsx"),
    "path_missing_services": os.path.join(_OUT, "missing_services.xlsx"),
    "path_missing_employees": os.path.join(_OUT, "missing_employees.xlsx"),
    "path_hours_summ": os.path.join(_OUT, "hours_worked_summ.xlsx"),
    "path_final_database": os.path.join(_OUT, "final_database.xlsx"),
    "path_exception_report": os.path.join(_OUT, "exception_report.xlsx"),
    "path_adp_import": os.path.join(_OUT, "ADP_payroll_import.csv"),
    # Phase 2: POS and payroll provider selection
    "pos_source": "phorest",  # phorest | vagaro | fresha | zenoti | mindbody
    "payroll_provider": "adp",  # adp | trinet | paylocity
    # Provider-specific export paths (path_payroll_import overrides; else auto by payroll_provider)
    "path_payroll_import": None,
    "path_trinet_import": os.path.join(_OUT, "Trinet_payroll_import.csv"),
    "path_paylocity_import": os.path.join(_OUT, "Paylocity_payroll_import.csv"),
    # TriNet / Paylocity pay code mapping (optional; defaults in adapters)
    "trinet_earnings_codes": None,  # e.g. {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}
    "paylocity_pay_codes": None,
    "path_location_summary": os.path.join(_OUT, "location_summary.xlsx"),
    "path_run_log": os.path.join(_OUT, "run_log.json"),
    # Historical payroll warehouse (SQLite)
    "path_warehouse": os.path.join(_OUT, "payroll_warehouse.db"),
    # Trend report output (Excel)
    "path_trend_report": os.path.join(_OUT, "payroll_trend_report.xlsx"),
    # Commission rules
    "service_category_4_treatment_threshold": 800,  # $800 in treatments for specialty 10%
    "service_category_4_rate": 0.10,
    "service_category_5_apprentice_rate": 0.20,
    "retail_threshold_499": 499,
    "retail_rate_10pct": 0.10,
    "retail_rate_5pct": 0.05,
    "product_sales_pct_of_service_for_commission": 0.10,  # Product Sales > 10% of service revenue
    # ADP pay code mapping (internal label -> ADP pay code)
    "adp_pay_codes": {
        "regular_hours": "REG",
        "commission": "COMM",
        "tips": "TIPS",
    },
    # ADP column name mapping (override if your payroll provider expects different column headers)
    # Example: {"employee_id": "Co Code", "pay_code": "Earnings Code", "amount": "Amount", "hours": "Hours"}
    "adp_column_mapping": None,
    # Hours worked file column mapping (auto-detected by default; set if your template differs)
    # Example: {"first_name": "FName", "last_name": "LName", "hours": "Hrs"}
    # Or for single-name column: {"employee": "Staff Name", "hours": "Total Hours"}
    "hours_column_mapping": None,
    # Payroll cost by location output
    "path_payroll_cost_by_location": os.path.join(_OUT, "payroll_cost_by_location.xlsx"),
    # Validation: fail fast if critical file missing
    "fail_fast_on_missing_file": False,
    # Double-booking allocation: "flag_only" (default) or "primary_location" (keep highest-revenue location)
    "double_booking_allocation": "flag_only",
    # Revenue outlier detection: flag employees with revenue > mean + N * std deviation
    "revenue_outlier_std_multiplier": 2.0,
    # Audit trail for overrides and allocation decisions (append-only JSONL)
    "path_override_audit_log": os.path.join(_OUT, "override_audit.jsonl"),
}
