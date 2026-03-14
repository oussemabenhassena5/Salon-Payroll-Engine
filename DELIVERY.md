# Salon Payroll & Sales Analysis – Setup Guide

## What's Included

- **Modular pipeline**: `config.py` (paths, rules), `payroll_engine.py` (logic), `main.py` (entry point)
- **Commission rules**: Service categories 1–5, retail categories 1–4 ($800 threshold for Cat 4)
- **Outputs**: `final_database.xlsx`, `exception_report.xlsx`, `ADP_payroll_import.csv`, `location_summary.xlsx`, `run_log.json`
- **Tests**: Unit and integration tests in `tests/`

## Setup Checklist

- [x] **Extract Sales.rar** – Extracted to `work_new/Sales/` (7 branch CSVs: Bay Ridge, Carroll Gardens, Dekalb, Dumbo, Fulton, Myrtle, Vanderbilt).
- [x] **Config** – `list_of_places` points to `work_new/Sales/*.csv`. Add 8th location in config if needed.
- [ ] **Verify master files** – Ensure `work_new/master_employee.xlsx`, `bridge_service_categories.xlsx`, `hours_worked.xlsx`, `Tabela_Datas.xlsx` exist and match expected columns.
- [ ] **Run pipeline** – Run `python main.py` or open `Old_Python_Script.ipynb` and run the cells.
- [ ] **Check outputs** – Review `output/final_database.xlsx`, `output/exception_report.xlsx`, `output/ADP_payroll_import.csv`, `output/location_summary.xlsx`.
- [ ] **ADP pay codes** – Confirm `CONFIG["adp_pay_codes"]` matches your ADP system (REG, COMM, TIPS or your codes).

## Open Questions

1. **ADP pay codes** – What are the exact ADP pay codes for regular hours, commission, and tips? (Current defaults: REG, COMM, TIPS.)

2. **8th location** – When will it be added? A placeholder path is already in config.

3. **Commission rule confirmation** – Service Category 4: is the treatment threshold $800 or a different amount? Retail Category 4: product sales > 10% of service revenue for 5% commission – correct?

## Quick Start

```bash
pip install -r requirements.txt
python main.py
```

Or run with tests:

```bash
python tests/test_payroll_engine.py
python tests/run_full_pipeline_test.py
```

## Support

- See [README.md](README.md) for full documentation.
- Commission rules and ADP mapping are in [config.py](config.py).
