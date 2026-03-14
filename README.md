# Salon Payroll & Sales Analysis

Python pipeline for Phorest sales data: ingest branch CSVs, map employees and service categories, calculate commissions (service 1–5, retail 1–4), and produce payroll-ready outputs plus an ADP import file.

## Run order

1. **Config** – Open [config.py](config.py) and set paths (branch CSVs, date table, master employee, bridge, hours, outputs). Support for up to 8 locations.
2. **Notebook** – Open [Old_Python_Script.ipynb](Old_Python_Script.ipynb):
   - Run the first code cell (imports and config).
   - Run the **Run full pipeline** cell. It will:
     - Ingest branch CSVs and build the database
     - Add service category (bridge), employee ID (master), and summarized hours
     - Validate inputs and run commission calculation
     - Write final payroll, exception report, ADP CSV, and run log

## Input files

| File | Purpose |
|------|--------|
| **Branch CSVs** | Phorest export per location (columns: metric, service, employee names, values). Paths in `CONFIG["list_of_places"]`. |
| **Date table** | Excel with `first_day` and `last_day` for the pay period. Path: `CONFIG["path_dates"]`. If missing, a default period is used. |
| **work_new/bridge_service_categories.xlsx** | Maps `service` → `service category` (Hair, Treatment, Makeup, Retail, etc.). |
| **work_new/master_employee.xlsx** | Columns: `employee`, `id`, `salary_hour`, `service_category` (1–5), `retail_category` (1–4), house_*, commission rates. |
| **work_new/hours_worked.xlsx** | Columns: First Name, Last Name, Total Hours Worked (or similar). |

## Output files (in `output/` by default)

| File | Description |
|------|-------------|
| **Database.xlsx** | Normalized sales DB with service category and employee ID. |
| **missing_services.xlsx** | Services in data not found in bridge (if any). |
| **missing_employees.xlsx** | Employees in data not mapped to master (if any). |
| **hours_worked_summ.xlsx** | Hours per employee (with id). |
| **final_database.xlsx** | One row per employee: totals, service/retail commission, tips, flags. |
| **exception_report.xlsx** | Unmatched employees, unmapped services, validation issues, double-booking flags. |
| **ADP_payroll_import.csv** | ADP-ready lines: employee_id, pay_code, amount, hours (default when `payroll_provider` is `adp`). |
| **location_summary.xlsx** | Per-location totals: service, retail, tips, employee count. |
| **run_log.json** | Run metadata (paths, period, counts) for reproducibility. |
| **payroll_warehouse.db** | SQLite store of each run for historical analysis. |
| **payroll_trend_report.xlsx** | Trend report: payroll by period, commission summary, payroll % of sales, exception/double-booking counts, period-over-period. |
| **override_audit.jsonl** | Audit log of double-booking allocation and overrides (when used). |

## Historical warehouse and trend reports

Each pipeline run appends to **payroll_warehouse.db** (SQLite) and regenerates **payroll_trend_report.xlsx**. The trend report has sheets: payroll by period, commission summary, payroll % of sales, exception/double-booking counts, period-over-period comparison. To build only the trend report from existing warehouse data:

```python
from payroll_warehouse import build_trend_report
from config import CONFIG
build_trend_report(CONFIG["path_warehouse"], CONFIG["path_trend_report"])
```

## Phase 2: Multiple POS and payroll providers

The pipeline uses **adapters** so you can plug in different data sources and payroll exports:

- **POS source** (`CONFIG["pos_source"]`): `phorest` (default), `vagaro`, `fresha`, `zenoti`, `mindbody`. Only Phorest is implemented; others raise a clear error until their export format is supported.
- **Payroll provider** (`CONFIG["payroll_provider"]`): `adp` (default), `trinet`, `paylocity`. Each writes a provider-specific CSV (e.g. TriNet: Employee ID, Earnings Code, Hours, Amount).

Set `CONFIG["path_payroll_import"]` to override the export path; otherwise the path is chosen from `path_adp_import`, `path_trinet_import`, or `path_paylocity_import` by provider.

```python
from integrations import get_pos_adapter, get_payroll_adapter, get_payroll_export_path
# Ingest using configured POS (e.g. Phorest)
database_01 = get_pos_adapter(CONFIG).ingest(places, df_dates, CONFIG)
# Export using configured provider (e.g. ADP, TriNet, Paylocity)
path_payroll = get_payroll_export_path(CONFIG)
get_payroll_adapter(CONFIG).export(final, path_payroll, CONFIG)
```

## Double-booking and exception handling

The engine runs 3 automatic checks on every payroll run:

- **Multi-Location Detection**: Flags employees with sales at 2+ locations in the same pay period. The exception report shows which locations.
- **Duplicate Record Detection**: Flags truly identical rows in the source data (same employee, location, service, metric, value) that could inflate revenue.
- **Revenue Outlier Detection**: Flags employees with unusually high revenue (> mean + 2× std deviation). Configurable via `CONFIG["revenue_outlier_std_multiplier"]`.

Allocation modes in [config.py](config.py) (`CONFIG["double_booking_allocation"]`):
- `"flag_only"` (default): only flag in exception report for manual review.
- `"primary_location"`: for multi-location employees, keep only their highest-revenue location and remove the rest (logged to audit trail).

All allocation decisions are logged to `override_audit.jsonl` with timestamps and affected employee IDs.

## Commission rules

**Service categories (1–5)**  
- 1: Simple (single formula with house deduction).  
- 2: Multiple (per-category rates for Hair, Treatment, Makeup).  
- 3: Commission or salary, whichever is higher.  
- 4: **$800** in treatments → 10% specialty commission.  
- 5: Apprentice: 20% on (service total − 2× hourly rate); qualify only if service total > 2× hourly rate.  

**Retail categories (1–4)**  
- 1: 10% of retail.  
- 2: If retail sale > $499 then 10%.  
- 3: If retail > $499 and retail ≥ 10% of sales then 10%.  
- 4: If product sales > 10% of service revenue then 5% of product sales; else 0.  

## Testing

To run the test suite and a full-pipeline integration test:

```bash
# Generate test data (Phorest-style CSV + date table)
python tests/make_test_data.py

# Run unit and integration tests (name normalization, commission rules, validation, pipeline)
python tests/test_payroll_engine.py

# Run full pipeline with test branch and verify all outputs
python tests/run_full_pipeline_test.py
```

Tests verify: service Category 4 uses a **$800** treatment threshold for 10% commission; retail categories 1–4; validation and exception report; ADP export columns; run log.

## ADP pay code mapping

Edit [config.py](config.py) `CONFIG["adp_pay_codes"]` to match your ADP pay codes:

- `regular_hours` → e.g. `"REG"`
- `commission` → e.g. `"COMM"`
- `tips` → e.g. `"TIPS"`

The CSV has columns: `employee_id`, `pay_code`, `amount`, `hours`.

## Colab

To run in Google Colab, in the first notebook cell set paths to your uploaded files, e.g.:

```python
CONFIG["list_of_places"] = ["/content/Bay Ridge.csv", "/content/Dekalb.csv", ...]
CONFIG["path_dates"] = "/content/Tabela_Datas.xlsx"
CONFIG["path_database"] = "/content/Database.xlsx"
CONFIG["path_missing_services"] = "/content/missing_services.xlsx"
CONFIG["path_missing_employees"] = "/content/missing_employees.xlsx"
CONFIG["path_hours_summ"] = "/content/hours_worked_summ.xlsx"
CONFIG["path_final_database"] = "/content/final_database.xlsx"
CONFIG["path_exception_report"] = "/content/exception_report.xlsx"
CONFIG["path_adp_import"] = "/content/ADP_payroll_import.csv"
CONFIG["path_location_summary"] = "/content/location_summary.xlsx"
CONFIG["path_run_log"] = "/content/run_log.json"
```

Then run the pipeline cell.
