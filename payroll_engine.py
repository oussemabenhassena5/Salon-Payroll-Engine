"""
Salon Payroll & Sales Analysis – Commission and payroll calculation engine.
Supports service categories 1–5, retail categories 1–4, validation, exception reporting, and ADP export.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any

import pandas as pd


# ---------- Name normalization (employee matching) ----------
def remove_suffix(nome: str) -> str:
    """Remove suffixes like 'I', 'II', 'III' from names."""
    if pd.isna(nome) or not isinstance(nome, str):
        return str(nome).strip()
    return re.sub(r"\s+[IVXLCDM]+\s*$", "", str(nome).strip()).strip()


def normalize_name(name: str) -> str:
    """Standardize name: strip, remove suffix, title case."""
    if pd.isna(name) or not isinstance(name, str):
        return ""
    s = str(name).strip()
    s = remove_suffix(s)
    return s.title() if s else ""


def fix_value(valor: Any) -> str:
    """Handle numeric strings with two commas (e.g. 1,234,56 -> 1.234,56)."""
    s = str(valor).strip()
    if s.count(",") == 2:
        s = s.replace(",", ".", 1)
    return s


# ---------- Data ingestion ----------
def process_sheets(lista_caminhos: list[str], df_dates: pd.DataFrame) -> pd.DataFrame:
    """Read branch CSVs and date range; return long-format database."""
    dataframes_processados = []
    for caminho in lista_caminhos:
        if not os.path.isfile(caminho):
            continue
        nome_filial = os.path.splitext(os.path.basename(caminho))[0]
        db = pd.read_csv(caminho, delimiter=",", encoding="ISO-8859-1")
        db.columns.values[0] = "service"
        db.drop(db.columns[-1], axis=1, inplace=True)
        db = db.fillna(0)
        db = db.drop(0, errors="ignore")
        # Normalize Phorest metric names (productTotal→productsAmount, coursesTotal→courseServiceAmount)
        if "metric" in db.columns:
            db["metric"] = db["metric"].replace({
                "productTotal": "productsAmount",
                "coursesTotal": "courseServiceAmount",
            })
        first_col = df_dates.iloc[0, 0]
        last_col = df_dates.iloc[0, -1]
        db["first_day"] = first_col
        db["last_day"] = last_col
        df1 = db[db["metric"] == "serviceCategoryAmount"]
        df2 = db[db["metric"] == "productsAmount"]
        df3 = db[db["metric"] == "courseServiceAmount"]
        df4 = db[db["metric"] == "tips"]
        df_concatenado = pd.concat([df1, df2, df3, df4], ignore_index=True)
        df_melted = df_concatenado.melt(
            id_vars=["first_day", "last_day", "service", "metric"],
            var_name="employee",
            value_name="value",
        )
        df_melted = df_melted[["first_day", "last_day", "service", "metric", "employee", "value"]]
        df_melted["employee"] = df_melted["employee"].apply(normalize_name)
        df_melted["value"] = df_melted["value"].astype(str).str.replace(".", ",", regex=False)
        df_melted["value"] = df_melted["value"].apply(fix_value)
        df_melted["value"] = df_melted["value"].str.replace(".", "", regex=False)
        df_melted["value"] = df_melted["value"].str.replace(",", ".", regex=False)
        df_melted["value"] = pd.to_numeric(df_melted["value"], errors="coerce").fillna(0).astype(float)
        df_melted["filial"] = nome_filial
        df_melted = df_melted[["first_day", "last_day", "filial", "service", "metric", "employee", "value"]]
        dataframes_processados.append(df_melted)
    if not dataframes_processados:
        return pd.DataFrame()
    return pd.concat(dataframes_processados, ignore_index=True)


# Phorest aggregate rows (productTotal, coursesTotal, tips) - map before bridge
_AGGREGATE_SERVICE_CATEGORY = {
    "Product total": "Retail",
    "Series completed total": "Hair",
    "Tips": "Tips",
}


def add_service_category(tabela: pd.DataFrame, bridge_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map service -> service category; return (enriched table, missing_services_df)."""
    bridge = pd.read_excel(bridge_path)
    service_col = "service"
    cat_col = "service category"
    if cat_col not in bridge.columns:
        cat_col = [c for c in bridge.columns if "category" in c.lower()][0] if bridge.columns.any() else None
    if cat_col is None:
        tabela[cat_col] = "Unmapped"
        return tabela, pd.DataFrame()
    # Pre-fill known Phorest aggregate rows
    tabela[cat_col] = tabela[service_col].map(_AGGREGATE_SERVICE_CATEGORY)
    tabela[cat_col] = tabela[cat_col].fillna(
        tabela[service_col].map(bridge.set_index(service_col)[cat_col])
    )
    missing = tabela.loc[tabela[cat_col].isnull(), service_col].unique()
    missing_df = pd.DataFrame({service_col: missing, cat_col: "Not Found"}) if len(missing) else pd.DataFrame()
    tabela[cat_col] = tabela[cat_col].fillna("Tips")
    col_order = ["first_day", "last_day", "filial", cat_col, service_col, "metric", "employee", "value"]
    tabela = tabela[[c for c in col_order if c in tabela.columns]]
    return tabela, missing_df


def _ensure_employee_column(master: pd.DataFrame) -> pd.DataFrame:
    """Build 'employee' from First Name + Last Name if missing."""
    if "employee" in master.columns and master["employee"].notna().any():
        return master
    for fn, ln in [("First Name", "Last Name"), ("First name", "Last name"), ("first_name", "last_name")]:
        if fn in master.columns and ln in master.columns:
            master = master.copy()
            master["employee"] = (master[fn].astype(str).str.strip() + " " + master[ln].astype(str).str.strip()).str.strip()
            master["employee"] = master["employee"].apply(normalize_name)
            return master
    return master


def add_id(tabela: pd.DataFrame, master_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map employee name -> id using master; return (enriched table, missing_employees_df)."""
    master = pd.read_excel(master_path)
    master = _ensure_employee_column(master)
    if "employee" not in master.columns or "id" not in master.columns:
        tabela["id"] = "Need ID"
        missing = tabela[["employee"]].drop_duplicates()
        missing["id"] = "Need ID"
        return tabela, missing
    master_norm = master.copy()
    master_norm["employee_norm"] = master_norm["employee"].apply(normalize_name)
    name_to_id = master_norm.set_index("employee_norm")["id"].to_dict()
    tabela["id"] = tabela["employee"].apply(normalize_name).map(name_to_id)
    tabela["id"] = tabela["id"].fillna("Need ID")
    missing = tabela[tabela["id"] == "Need ID"][["employee", "id"]].drop_duplicates(subset=["employee"])
    col_order = ["id", "employee", "first_day", "last_day", "filial", "service category", "service", "metric", "value"]
    tabela = tabela[[c for c in col_order if c in tabela.columns]]
    return tabela, missing


def _detect_hours_columns(database: pd.DataFrame, config: dict | None = None) -> tuple[str | None, str | None, str]:
    """Auto-detect first name, last name, and hours columns from hours_worked file.
    Supports configurable column mapping via config['hours_column_mapping'].
    Returns (first_name_col, last_name_col, hours_col)."""
    cfg = config or {}
    mapping = cfg.get("hours_column_mapping", {})

    # If explicit mapping is provided, use it
    if mapping:
        fn = mapping.get("first_name")
        ln = mapping.get("last_name")
        emp = mapping.get("employee")
        hrs = mapping.get("hours", "Total Hours Worked")
        if emp and emp in database.columns:
            return None, None, hrs  # full name column provided
        if fn and ln and fn in database.columns and ln in database.columns:
            return fn, ln, hrs
        # Fall through to auto-detect if mapped columns not found

    # Auto-detect: try common patterns for name columns
    cols = list(database.columns)
    first_name_col = None
    last_name_col = None

    # Check for "employee" or "Employee Name" or "Full Name" (single name column)
    for c in cols:
        cl = c.lower().strip()
        if cl in ("employee", "employee name", "full name", "name", "employee_name"):
            first_name_col = c
            break

    if first_name_col and last_name_col is None:
        # Single name column — will be used directly
        pass
    else:
        # Look for first/last name pair
        for c in cols:
            cl = c.lower().strip()
            if cl in ("first name", "first_name", "firstname", "fname"):
                first_name_col = c
            elif cl in ("last name", "last_name", "lastname", "lname"):
                last_name_col = c

    # Auto-detect hours column
    hours_col = None
    for c in cols:
        cl = c.lower().strip()
        if cl in ("total hours worked", "total_hours_worked", "hours worked", "hours", "total hours", "total_hours", "hrs"):
            hours_col = c
            break
    if hours_col is None:
        # Fallback: any column with "hour" in the name
        hour_candidates = [c for c in cols if "hour" in c.lower()]
        if hour_candidates:
            hours_col = hour_candidates[0]
    if hours_col is None:
        hours_col = "Total Hours Worked"

    return first_name_col, last_name_col, hours_col


def hour_worked(path_hour: str, path_master: str, out_path: str, config: dict | None = None) -> pd.DataFrame:
    """Summarize hours by employee and merge with master id.
    Auto-detects column names from the hours_worked file, or uses config['hours_column_mapping']
    for custom templates. Supported mappings:
      {"first_name": "FName", "last_name": "LName", "hours": "Hrs"}
      {"employee": "Employee Name", "hours": "Total Hours"}
    """
    database = pd.read_excel(path_hour)
    master = pd.read_excel(path_master)
    master = _ensure_employee_column(master)

    fn_col, ln_col, hrs_col = _detect_hours_columns(database, config)

    # Build employee name
    if fn_col and ln_col and fn_col in database.columns and ln_col in database.columns:
        database["employee"] = (database[fn_col].astype(str) + " " + database[ln_col].astype(str)).str.strip()
    elif fn_col and fn_col in database.columns:
        # Single name column (employee, full name, etc.)
        database["employee"] = database[fn_col].astype(str).str.strip()
    else:
        # Last resort: first column
        database["employee"] = database.iloc[:, 0].astype(str)
    database["employee"] = database["employee"].apply(normalize_name)

    if hrs_col not in database.columns:
        raise ValueError(
            f"Hours column '{hrs_col}' not found in {path_hour}. "
            f"Available columns: {list(database.columns)}. "
            f"Set config['hours_column_mapping'] = {{'hours': 'YourColumnName'}} to fix."
        )

    summarized = database.groupby("employee")[hrs_col].sum().reset_index()
    summarized.columns = ["employee", "Total Hours Worked"]
    if "employee" in master.columns and "id" in master.columns:
        master_norm = master.copy()
        master_norm["employee_norm"] = master_norm["employee"].apply(normalize_name)
        summarized = summarized.merge(
            master_norm[["employee_norm", "id"]].drop_duplicates(),
            left_on="employee",
            right_on="employee_norm",
            how="left",
        )
        summarized = summarized.drop(columns=["employee_norm"], errors="ignore")
    summarized.to_excel(out_path, index=False)
    return summarized


# ---------- Validation ----------
def validate_inputs(
    database: pd.DataFrame,
    master_path: str,
    bridge_path: str,
    hours_path: str,
) -> list[dict[str, Any]]:
    """Run file/structure checks; return list of issues {message, severity, detail}."""
    issues = []
    if database is None or (isinstance(database, pd.DataFrame) and database.empty):
        issues.append({"message": "Database is empty after ingestion", "severity": "error", "detail": ""})
    required_db_cols = ["service", "metric", "employee", "value"]
    if isinstance(database, pd.DataFrame) and not database.empty:
        for c in required_db_cols:
            if c not in database.columns:
                issues.append({"message": f"Database missing column: {c}", "severity": "error", "detail": ""})
    if os.path.isfile(master_path):
        try:
            m = pd.read_excel(master_path)
            if "id" not in m.columns:
                issues.append({"message": "master_employee missing column: id", "severity": "error", "detail": ""})
            if "employee" not in m.columns and not ("First Name" in m.columns and "Last Name" in m.columns):
                issues.append({"message": "master_employee missing employee or First/Last Name", "severity": "warning", "detail": ""})
        except Exception as e:
            issues.append({"message": "Could not read master_employee", "severity": "error", "detail": str(e)})
    else:
        issues.append({"message": f"master_employee file not found: {master_path}", "severity": "error", "detail": ""})
    if os.path.isfile(bridge_path):
        try:
            b = pd.read_excel(bridge_path)
            if "service" not in b.columns:
                issues.append({"message": "bridge missing column: service", "severity": "error", "detail": ""})
        except Exception as e:
            issues.append({"message": "Could not read bridge_service_categories", "severity": "error", "detail": str(e)})
    else:
        issues.append({"message": f"bridge file not found: {bridge_path}", "severity": "error", "detail": ""})
    if os.path.isfile(hours_path):
        try:
            h = pd.read_excel(hours_path)
            if "Total Hours Worked" not in h.columns and not any("hour" in str(c).lower() for c in h.columns):
                issues.append({"message": "hours_worked missing Total Hours Worked (or similar)", "severity": "warning", "detail": ""})
        except Exception as e:
            issues.append({"message": "Could not read hours_worked", "severity": "warning", "detail": str(e)})
    # Duplicate records in ingested data (same period, location, service, metric, employee)
    dup_issues = detect_duplicate_records(database)
    issues.extend(dup_issues)
    return issues


def detect_duplicate_records(database: pd.DataFrame) -> list[dict[str, Any]]:
    """Detect duplicate rows in the ingested database (same period, filial, service, metric, employee).
    Returns a list of validation issues for inclusion in the exception report."""
    issues: list[dict[str, Any]] = []
    if database is None or not isinstance(database, pd.DataFrame) or database.empty:
        return issues
    key_cols = ["first_day", "last_day", "filial", "service", "metric", "employee"]
    missing = [c for c in key_cols if c not in database.columns]
    if missing:
        return issues
    dup_mask = database.duplicated(subset=key_cols, keep=False)
    n_dup = dup_mask.sum()
    if n_dup > 0:
        n_groups = database.loc[dup_mask].drop_duplicates(subset=key_cols).shape[0]
        issues.append({
            "message": "Duplicate records in ingested data (same period, location, service, metric, employee)",
            "severity": "warning",
            "detail": f"{int(n_dup)} duplicate rows in {int(n_groups)} duplicate group(s). Review before payroll.",
        })
    return issues


# ---------- Commission rules (service 1–5, retail 1–4) ----------
# Service: 1=Simple, 2=Multiple, 3=Commission or Salary whichever higher, 4=$800 treatments 10%, 5=Apprentice 20%
# Retail: 1=10% always, 2=if >499 then 10%, 3=if >499 and 10% of sales then 10%, 4=if product >10% service rev then 5% else 0

def _retail_commission(
    row: pd.Series,
    retail: float,
    total_service: float,
    total_sales: float,
    cfg: dict,
) -> float:
    """Compute retail commission by retail_category (1–4)."""
    rc = row.get("retail_category")
    if pd.isna(rc):
        rc = getattr(row, "retail_category", None)
    try:
        rc = int(float(rc))
    except (TypeError, ValueError):
        rc = 1
    th = cfg.get("retail_threshold_499", 499)
    r10 = cfg.get("retail_rate_10pct", 0.10)
    r5 = cfg.get("retail_rate_5pct", 0.05)
    pct_svc = cfg.get("product_sales_pct_of_service_for_commission", 0.10)
    if rc == 1:
        return round(retail * r10, 2)
    if rc == 2:
        return round(retail * r10, 2) if retail > th else 0.0
    if rc == 3:
        if retail <= th:
            return 0.0
        # "10% of sales" = retail >= 10% of total sales (or service)
        total = total_sales if total_sales > 0 else (total_service + retail)
        if total <= 0:
            return 0.0
        if retail >= 0.10 * total:
            return round(retail * r10, 2)
        return 0.0
    if rc == 4:
        if total_service <= 0:
            return 0.0
        if retail > pct_svc * total_service:
            return round(retail * r5, 2)
        return 0.0
    return round(retail * r10, 2)


def _service_commission(
    row: pd.Series,
    total_service: float,
    treatment: float,
    hair: float,
    makeup: float,
    salary_hours_worked: float,
    cfg: dict,
) -> float:
    """Compute service commission by service_category (1–5)."""
    sc = row.get("service_category")
    if pd.isna(sc):
        sc = getattr(row, "service_category", None)
    try:
        sc = int(float(sc))
    except (TypeError, ValueError):
        sc = 1
    th800 = cfg.get("service_category_4_treatment_threshold", 800)
    rate4 = cfg.get("service_category_4_rate", 0.10)
    rate5 = cfg.get("service_category_5_apprentice_rate", 0.20)
    # Category 1: Simple (single formula from master: treatment-style with house deduction)
    if sc == 1:
        house = row.get("house_service_treatment", 0) or 0
        house_add = row.get("house_add", 0) or 0
        comm_rate = row.get("service_treatment_comission", 0) or 0
        base = total_service - (total_service * house + house_add)
        return round(max(0, base) * comm_rate, 2)
    # Category 2: Multiple (per-category rates)
    if sc == 2:
        h_house = row.get("house_service_hair", 0) or 0
        t_house = row.get("house_service_treatment", 0) or 0
        m_house = row.get("house_service_makeup", 0) or 0
        h_rate = row.get("service_hair_comission", 0) or 0
        t_rate = row.get("service_treatment_comission", 0) or 0
        m_rate = row.get("service_makeup_comission", 0) or 0
        v = (hair * (1 - h_house) * h_rate + treatment * (1 - t_house) * t_rate + makeup * (1 - m_house) * m_rate)
        return round(v, 2)
    # Category 3: Commission or salary whichever higher
    if sc == 3:
        salary_comm = (salary_hours_worked * (1 - 0.15)) * 0.4
        service_comm = (total_service * (1 - 0.15)) * 0.4
        return round(max(salary_comm, service_comm), 2)
    # Category 4: $800 in treatments -> 10% specialty
    if sc == 4:
        if treatment >= th800:
            return round(treatment * rate4, 2)
        return 0.0
    # Category 5: Apprentice 20% on (service total - 2*hourly rate); qualify only if double hourly in services
    if sc == 5:
        if total_service <= salary_hours_worked * 2:
            return 0.0
        return round((total_service - salary_hours_worked * 2) * rate5, 2)
    return 0.0


def _get_service_categories(database: pd.DataFrame, cat_col: str) -> list[str]:
    """Discover all service categories from the data (dynamic, not hardcoded).
    Returns list of category names excluding Retail and Tips."""
    all_cats = database[cat_col].dropna().unique().tolist()
    # Retail and Tips are handled separately
    service_cats = [c for c in all_cats if c not in ("Retail", "Tips")]
    return sorted(service_cats)


def _service_commission_dynamic(
    row: pd.Series,
    total_service: float,
    category_values: dict[str, float],
    salary_hours_worked: float,
    cfg: dict,
) -> float:
    """Compute service commission by service_category (1–5).
    category_values is a dict of {category_name: revenue} for this employee."""
    sc = row.get("service_category")
    if pd.isna(sc):
        sc = getattr(row, "service_category", None)
    try:
        sc = int(float(sc))
    except (TypeError, ValueError):
        sc = 1
    th800 = cfg.get("service_category_4_treatment_threshold", 800)
    rate4 = cfg.get("service_category_4_rate", 0.10)
    rate5 = cfg.get("service_category_5_apprentice_rate", 0.20)
    # Category 1: Simple (single formula with house deduction on total service)
    if sc == 1:
        house = row.get("house_service_treatment", 0) or 0
        house_add = row.get("house_add", 0) or 0
        comm_rate = row.get("service_treatment_comission", 0) or 0
        base = total_service - (total_service * house + house_add)
        return round(max(0, base) * comm_rate, 2)
    # Category 2: Multiple (per-category rates — reads dynamically from master columns)
    if sc == 2:
        total_comm = 0.0
        for cat_name, cat_revenue in category_values.items():
            # Look for house_service_{cat} and service_{cat}_comission in master
            cat_key = cat_name.lower()
            house_col = f"house_service_{cat_key}"
            rate_col = f"service_{cat_key}_comission"
            house_pct = row.get(house_col, 0) or 0
            rate = row.get(rate_col, 0) or 0
            total_comm += cat_revenue * (1 - house_pct) * rate
        return round(total_comm, 2)
    # Category 3: Commission or salary whichever higher
    if sc == 3:
        salary_comm = (salary_hours_worked * (1 - 0.15)) * 0.4
        service_comm = (total_service * (1 - 0.15)) * 0.4
        return round(max(salary_comm, service_comm), 2)
    # Category 4: $800 in treatments -> 10% specialty
    if sc == 4:
        treatment = category_values.get("Treatment", 0)
        if treatment >= th800:
            return round(treatment * rate4, 2)
        return 0.0
    # Category 5: Apprentice 20% on (service total - 2*hourly rate)
    if sc == 5:
        if total_service <= salary_hours_worked * 2:
            return 0.0
        return round((total_service - salary_hours_worked * 2) * rate5, 2)
    return 0.0


def calculation(
    path_database: str,
    path_master_employee: str,
    path_hours_summ: str,
    config: dict | None = None,
) -> pd.DataFrame:
    """Compute payroll: pivot by service category, merge master + hours, apply service and retail rules.
    Service categories are discovered dynamically from the data — not hardcoded to Hair/Treatment/Makeup.
    Adding a new category (e.g. Color) only requires:
    1. Adding it to bridge_service_categories.xlsx
    2. Adding house_service_color and service_color_comission columns to master_employee.xlsx
    """
    cfg = config or {}
    database = pd.read_excel(path_database)
    master_employee = pd.read_excel(path_master_employee)
    master_employee = _ensure_employee_column(master_employee)
    hours = pd.read_excel(path_hours_summ)

    database_temp = database.copy()
    cat_col = "service category"
    if cat_col not in database_temp.columns:
        cat_col = [c for c in database_temp.columns if "category" in c.lower()][0]

    # Discover all service categories dynamically from the data
    service_cats = _get_service_categories(database_temp, cat_col)

    database_temp["ordem"] = database_temp.groupby(["id", "employee", cat_col]).cumcount()
    database_temp = database_temp.pivot_table(
        index=["id", "employee", "ordem"],
        columns=cat_col,
        values="value",
        aggfunc="sum",
    ).reset_index()
    database_temp = database_temp.drop(columns=["ordem", "employee"], errors="ignore")
    database_temp.columns.name = None
    database_temp = database_temp.groupby("id").sum().reset_index()

    # Ensure all discovered category columns exist (fill 0 for missing)
    for col in service_cats + ["Retail", "Tips"]:
        if col not in database_temp.columns:
            database_temp[col] = 0.0

    database_temp = database_temp.merge(master_employee, on="id", how="left", suffixes=("", "_master"))
    # Drop duplicate columns from master (keep left/original if both exist)
    dup = [c for c in database_temp.columns if c.endswith("_master")]
    database_temp = database_temp.drop(columns=dup, errors="ignore")

    hours_col = "Total Hours Worked" if "Total Hours Worked" in hours.columns else ([c for c in hours.columns if "hour" in str(c).lower()] or ["Total Hours Worked"])[0]
    hours_sub = hours[["id", hours_col]].copy()
    hours_sub.columns = ["id", "Total Hours Worked"]
    database_temp = database_temp.merge(hours_sub, on="id", how="left")
    if "Total Hours Worked" not in database_temp.columns:
        database_temp["Total Hours Worked"] = 0.0

    # Total service = sum of ALL service categories (dynamic, not just Hair+Treatment+Makeup)
    database_temp["total_service"] = sum(database_temp[c].fillna(0) for c in service_cats)
    database_temp["salary_hours_worked"] = database_temp["salary_hour"].fillna(0) * database_temp["Total Hours Worked"].fillna(0)
    database_temp["Retail"] = database_temp["Retail"].fillna(0)
    database_temp["Tips"] = database_temp["Tips"].fillna(0)

    # Ensure employee column
    if "employee" not in database_temp.columns and "employee_master" in database_temp.columns:
        database_temp["employee"] = database_temp["employee_master"]
    if "employee" not in database_temp.columns:
        database_temp["employee"] = "Unknown"

    # Retail commission (support retail_category 1–4; fallback to legacy q_amt / q_pct)
    def retail_comm_row(r):
        tot_svc = r["total_service"]
        retail = r["Retail"]
        tot_sales = tot_svc + retail
        if "retail_category" in r.index:
            return _retail_commission(r, retail, tot_svc, tot_sales, cfg)
        # Legacy: condition from original script
        q_amt = r.get("retail_comission_q_amt") or 0
        q_pct = r.get("retail_comission_q_percentage") or 0
        comm = r.get("retail_comission") or 0.10
        if tot_svc < q_amt or retail < (tot_svc * q_pct):
            return 0.0
        return round(retail * comm, 2)

    database_temp["total_retail_commission"] = database_temp.apply(retail_comm_row, axis=1)

    # Service commission by category (dynamic — reads per-category columns from master)
    def service_comm_row(r):
        cat_vals = {c: (r.get(c, 0) or 0) for c in service_cats}
        return _service_commission_dynamic(
            r,
            r["total_service"],
            cat_vals,
            r["salary_hours_worked"],
            cfg,
        )

    database_temp["service_comission"] = database_temp.apply(service_comm_row, axis=1)
    database_temp = database_temp.fillna(0)
    database_temp = database_temp.drop_duplicates(subset=["id"])
    database_temp["service_comission"] = database_temp["service_comission"].round(2)
    return database_temp


# ---------- Location summary ----------
def build_location_summary(database: pd.DataFrame) -> pd.DataFrame:
    """Build per-location summary: total service, retail, tips, employee count."""
    if database.empty or "filial" not in database.columns:
        return pd.DataFrame(columns=["location", "total_service", "total_retail", "total_tips", "employee_count"])
    loc_col = "filial"
    has_metric = "metric" in database.columns
    rows = []
    for loc, grp in database.groupby(loc_col):
        if has_metric:
            service = grp[(grp["metric"] == "serviceCategoryAmount") | (grp["metric"] == "courseServiceAmount")]["value"].sum()
            retail = grp[grp["metric"] == "productsAmount"]["value"].sum()
            tips = grp[grp["metric"] == "tips"]["value"].sum()
        else:
            service = retail = tips = grp["value"].sum() if "value" in grp.columns else 0
        emp_count = grp["employee"].nunique() if "employee" in grp.columns else 0
        rows.append({
            "location": loc,
            "total_service": round(float(service), 2),
            "total_retail": round(float(retail), 2),
            "total_tips": round(float(tips), 2),
            "employee_count": int(emp_count),
        })
    return pd.DataFrame(rows)


# ---------- Payroll cost by location ----------
def build_payroll_cost_by_location(
    database: pd.DataFrame,
    payroll_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build payroll cost breakdown by location.
    Uses the ingested database (with filial) to find each employee's primary location,
    then aggregates payroll costs (commission, retail commission, tips, hours) per location."""
    if database.empty or payroll_df.empty:
        return pd.DataFrame()
    if "filial" not in database.columns or "id" not in database.columns:
        return pd.DataFrame()

    # Find primary location per employee (location with highest revenue)
    emp_loc = database.groupby(["id", "filial"])["value"].sum().reset_index()
    emp_loc = emp_loc.sort_values("value", ascending=False).drop_duplicates(subset=["id"], keep="first")
    emp_loc = emp_loc[["id", "filial"]].rename(columns={"filial": "location"})

    # Merge location into payroll
    merged = payroll_df.merge(emp_loc, on="id", how="left")
    merged["location"] = merged["location"].fillna("Unknown")

    # Aggregate by location
    agg = merged.groupby("location").agg(
        employee_count=("id", "nunique"),
        total_service=("total_service", "sum"),
        total_retail=("Retail", "sum"),
        total_tips=("Tips", "sum"),
        total_service_commission=("service_comission", "sum"),
        total_retail_commission=("total_retail_commission", "sum"),
        total_hours=("Total Hours Worked", "sum"),
    ).reset_index()

    # Compute totals
    agg["total_commission"] = agg["total_service_commission"] + agg["total_retail_commission"]
    agg["total_payroll_cost"] = agg["total_commission"] + agg["total_tips"]
    agg["total_revenue"] = agg["total_service"] + agg["total_retail"]
    agg["payroll_pct_revenue"] = (
        (agg["total_payroll_cost"] / agg["total_revenue"].replace(0, float("nan"))) * 100
    ).round(2)

    # Round money columns
    for c in ["total_service", "total_retail", "total_tips", "total_service_commission",
              "total_retail_commission", "total_commission", "total_payroll_cost", "total_revenue"]:
        agg[c] = agg[c].round(2)

    return agg.sort_values("total_revenue", ascending=False).reset_index(drop=True)


# ---------- Exception / double-booking ----------

def detect_multi_location_employees(database: pd.DataFrame) -> pd.DataFrame:
    """Detect employees working at multiple locations in the same pay period.
    Returns DataFrame with columns: id, employee, location_count, locations."""
    if database.empty or "id" not in database.columns or "filial" not in database.columns:
        return pd.DataFrame(columns=["id", "employee", "location_count", "locations"])
    period_cols = [c for c in ["first_day", "last_day"] if c in database.columns]
    group_cols = ["id"] + period_cols
    if "employee" in database.columns:
        # Get one employee name per id
        emp_map = database.drop_duplicates(subset=["id"])[["id", "employee"]]
    loc_info = database.groupby(group_cols)["filial"].agg(
        location_count="nunique",
        locations=lambda x: ", ".join(sorted(x.unique())),
    ).reset_index()
    multi = loc_info[loc_info["location_count"] > 1].copy()
    if "employee" in database.columns and not multi.empty:
        multi = multi.merge(emp_map, on="id", how="left")
    return multi


def detect_duplicate_attribution(database: pd.DataFrame) -> pd.DataFrame:
    """Detect truly duplicate rows: same employee, location, service, metric, period, and value.
    These are suspicious because the same sale may have been counted twice.
    Returns DataFrame of duplicate rows."""
    if database.empty:
        return pd.DataFrame()
    key_cols = ["first_day", "last_day", "filial", "service", "metric", "employee", "value"]
    existing = [c for c in key_cols if c in database.columns]
    if len(existing) < 4:
        return pd.DataFrame()
    dup_mask = database.duplicated(subset=existing, keep=False)
    return database[dup_mask].copy()


def detect_revenue_outliers(database: pd.DataFrame, std_multiplier: float = 2.0) -> pd.DataFrame:
    """Detect employees with suspiciously high total revenue (> mean + std_multiplier * std).
    Returns DataFrame with columns: id, employee, total_value, mean, std, threshold."""
    if database.empty or "id" not in database.columns or "value" not in database.columns:
        return pd.DataFrame(columns=["id", "employee", "total_value", "mean", "std", "threshold"])
    emp_totals = database.groupby(["id", "employee"])["value"].sum().reset_index(name="total_value") \
        if "employee" in database.columns else database.groupby("id")["value"].sum().reset_index(name="total_value")
    mean_val = emp_totals["total_value"].mean()
    std_val = emp_totals["total_value"].std()
    threshold = mean_val + std_multiplier * std_val
    outliers = emp_totals[emp_totals["total_value"] > threshold].copy()
    if not outliers.empty:
        outliers["mean"] = round(mean_val, 2)
        outliers["std"] = round(std_val, 2)
        outliers["threshold"] = round(threshold, 2)
        outliers["total_value"] = outliers["total_value"].round(2)
    return outliers


def detect_double_booking(database: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
    """Run all exception detections and add flag columns to the database.
    Flags: multi_location_flag, duplicate_attribution_flag, revenue_outlier_flag, double_booking_flag (summary)."""
    if database.empty or "id" not in database.columns:
        return database
    cfg = config or {}
    db = database.copy()
    db["multi_location_flag"] = ""
    db["duplicate_attribution_flag"] = ""
    db["revenue_outlier_flag"] = ""

    # 1) Multi-location employees
    multi = detect_multi_location_employees(db)
    if not multi.empty:
        multi_ids = set(multi["id"].tolist())
        for eid in multi_ids:
            mask = db["id"] == eid
            locs = multi.loc[multi["id"] == eid, "locations"].iloc[0]
            count = int(multi.loc[multi["id"] == eid, "location_count"].iloc[0])
            db.loc[mask, "multi_location_flag"] = f"Works at {count} locations: {locs}"

    # 2) Duplicate attribution (truly identical rows)
    dup_df = detect_duplicate_attribution(db)
    if not dup_df.empty and dup_df.index.isin(db.index).any():
        db.loc[dup_df.index.intersection(db.index), "duplicate_attribution_flag"] = "Duplicate record"

    # 3) Revenue outliers
    std_mult = cfg.get("revenue_outlier_std_multiplier", 2.0)
    outliers = detect_revenue_outliers(db, std_mult)
    if not outliers.empty:
        outlier_ids = set(outliers["id"].tolist())
        for eid in outlier_ids:
            mask = db["id"] == eid
            val = outliers.loc[outliers["id"] == eid, "total_value"].iloc[0]
            thresh = outliers.loc[outliers["id"] == eid, "threshold"].iloc[0]
            db.loc[mask, "revenue_outlier_flag"] = f"Revenue ${val:,.0f} exceeds threshold ${thresh:,.0f}"

    # Summary flag: combine all flags into one column for backward compatibility
    def _summary_flag(row):
        flags = []
        if row.get("multi_location_flag", ""):
            flags.append("Multi-location")
        if row.get("duplicate_attribution_flag", ""):
            flags.append("Duplicate record")
        if row.get("revenue_outlier_flag", ""):
            flags.append("Revenue outlier")
        return "; ".join(flags)

    db["double_booking_flag"] = db.apply(_summary_flag, axis=1)
    return db


def get_double_booking_flagged_ids(database: pd.DataFrame) -> list[Any]:
    """Return list of employee ids that have any exception flag set."""
    if database.empty or "id" not in database.columns or "double_booking_flag" not in database.columns:
        return []
    flagged = database[database["double_booking_flag"].astype(str).str.len() > 0]
    return flagged["id"].unique().tolist()


def append_override_audit(
    out_path: str,
    period_first: Any,
    period_last: Any,
    action: str,
    detail: str,
    affected_ids: list[Any] | None = None,
    user: str = "system",
) -> None:
    """Append one record to the override/audit log (JSONL)."""
    record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "period_first": str(period_first) if period_first is not None else "",
        "period_last": str(period_last) if period_last is not None else "",
        "action": action,
        "detail": detail,
        "affected_ids": affected_ids or [],
        "user": user,
    }
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "a") as f:
        f.write(json.dumps(record) + "\n")


def apply_double_booking_allocation(
    database: pd.DataFrame,
    config: dict[str, Any] | None = None,
    period_first: Any = None,
    period_last: Any = None,
    audit_path: str | None = None,
) -> tuple[pd.DataFrame, list[Any]]:
    """Run exception detection and optionally apply allocation rules.
    Returns (database_with_flags, flagged_ids).
    Allocation modes (config['double_booking_allocation']):
      - 'flag_only' (default): only flag, no row removal
      - 'primary_location': keep rows from the employee's primary location (most revenue), remove others
      - 'split_even': split multi-location revenue evenly (future)
    """
    cfg = config or {}
    allocation = cfg.get("double_booking_allocation", "flag_only")
    path_audit = audit_path or cfg.get("path_override_audit_log", "")

    if database.empty or "id" not in database.columns:
        return database, []

    db = detect_double_booking(database, cfg)
    flagged_ids = get_double_booking_flagged_ids(db)

    if allocation == "primary_location" and "filial" in db.columns and len(flagged_ids) > 0:
        # For multi-location employees, keep only their primary location (highest revenue)
        multi = detect_multi_location_employees(db)
        if not multi.empty:
            n_before = len(db)
            ids_to_filter = set(multi["id"].tolist())
            rows_keep = []
            rows_other = []
            for eid in ids_to_filter:
                emp_rows = db[db["id"] == eid]
                # Find primary location (highest total value)
                loc_totals = emp_rows.groupby("filial")["value"].sum()
                primary_loc = loc_totals.idxmax()
                rows_keep.append(emp_rows[emp_rows["filial"] == primary_loc])
                rows_other.append(emp_rows[emp_rows["filial"] != primary_loc])
            # Rebuild: non-multi employees + primary-location rows of multi employees
            non_multi = db[~db["id"].isin(ids_to_filter)]
            db = pd.concat([non_multi] + rows_keep, ignore_index=True)
            n_after = len(db)
            if path_audit and n_before > n_after:
                append_override_audit(
                    path_audit,
                    period_first,
                    period_last,
                    "double_booking_allocation",
                    f"primary_location: removed {n_before - n_after} rows from secondary locations for {len(ids_to_filter)} employee(s)",
                    affected_ids=list(ids_to_filter),
                )
    return db, flagged_ids


def apply_double_booking_flag_to_payroll(payroll_df: pd.DataFrame, flagged_ids: list[Any]) -> pd.DataFrame:
    """Set double_booking_flag on payroll (one row per id) from list of ids that had exceptions."""
    out = payroll_df.copy()
    if "id" not in out.columns:
        return out
    out["double_booking_flag"] = ""
    for eid in flagged_ids or []:
        mask = out["id"] == eid
        out.loc[mask, "double_booking_flag"] = "Review required"
    return out


# ---------- Exception report ----------
def build_exception_report(
    missing_employees: pd.DataFrame,
    missing_services: pd.DataFrame,
    validation_issues: list[dict],
    payroll_df: pd.DataFrame,
    first_day: Any = None,
    last_day: Any = None,
    database: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Comprehensive exception report with categories:
    - Unmatched employees (no ID in master)
    - Unmapped services (not in bridge)
    - Validation issues
    - Multi-location employees (working at 2+ locations same period)
    - Duplicate records (identical rows in source data)
    - Revenue outliers (suspiciously high revenue)
    """
    rows = []

    # 1) Unmatched employees
    if not missing_employees.empty:
        for _, r in missing_employees.iterrows():
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Unmatched Employee",
                "employee": r.get("employee", ""),
                "id": r.get("id", ""),
                "service": "",
                "location": "",
                "reason": "Employee not found in master_employee file",
                "detail": "Add this employee to master_employee.xlsx with their ID and commission rules",
                "severity": "error",
            })

    # 2) Unmapped services
    if not missing_services.empty:
        for _, r in missing_services.iterrows():
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Unmapped Service",
                "employee": "",
                "id": "",
                "service": r.get("service", ""),
                "location": "",
                "reason": "Service not found in bridge_service_categories",
                "detail": "Add this service to bridge_service_categories.xlsx with the correct category (Hair/Treatment/Makeup/Retail)",
                "severity": "warning",
            })

    # 3) Validation issues
    for i in validation_issues:
        rows.append({
            "period_first": first_day,
            "period_last": last_day,
            "category": "Validation",
            "employee": "",
            "id": "",
            "service": "",
            "location": "",
            "reason": i.get("message", "Validation issue"),
            "detail": i.get("detail", ""),
            "severity": i.get("severity", "warning"),
        })

    # 4) Multi-location employees (from database flags)
    db = database if database is not None else pd.DataFrame()
    if not db.empty and "multi_location_flag" in db.columns:
        multi_rows = db[db["multi_location_flag"].astype(str).str.len() > 0]
        # One row per employee (not per service row)
        seen_ids = set()
        for _, r in multi_rows.iterrows():
            eid = r.get("id", "")
            if eid in seen_ids:
                continue
            seen_ids.add(eid)
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Multi-Location",
                "employee": r.get("employee", ""),
                "id": eid,
                "service": "",
                "location": r.get("multi_location_flag", ""),
                "reason": "Employee has sales at multiple locations in the same period",
                "detail": "Verify if revenue should be split or attributed to primary location",
                "severity": "warning",
            })

    # 5) Duplicate attribution (from database flags)
    if not db.empty and "duplicate_attribution_flag" in db.columns:
        dup_rows = db[db["duplicate_attribution_flag"].astype(str).str.len() > 0]
        if not dup_rows.empty:
            n_dup = len(dup_rows)
            n_groups = dup_rows.drop_duplicates(
                subset=[c for c in ["filial", "service", "metric", "employee"] if c in dup_rows.columns]
            ).shape[0]
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Duplicate Record",
                "employee": "",
                "id": "",
                "service": "",
                "location": "",
                "reason": f"{n_dup} duplicate rows found in {n_groups} group(s)",
                "detail": "Identical records (same employee, location, service, metric, value). May inflate revenue.",
                "severity": "warning",
            })

    # 6) Revenue outliers (from database flags)
    if not db.empty and "revenue_outlier_flag" in db.columns:
        outlier_rows = db[db["revenue_outlier_flag"].astype(str).str.len() > 0]
        seen_ids = set()
        for _, r in outlier_rows.iterrows():
            eid = r.get("id", "")
            if eid in seen_ids:
                continue
            seen_ids.add(eid)
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Revenue Outlier",
                "employee": r.get("employee", ""),
                "id": eid,
                "service": "",
                "location": "",
                "reason": "Unusually high revenue compared to other employees",
                "detail": r.get("revenue_outlier_flag", ""),
                "severity": "info",
            })

    # 7) Payroll-level double_booking_flag (backward compat for flagged payroll rows)
    if not payroll_df.empty and "double_booking_flag" in payroll_df.columns and db.empty:
        flagged = payroll_df[payroll_df["double_booking_flag"].astype(str).str.len() > 0]
        for _, r in flagged.iterrows():
            rows.append({
                "period_first": first_day,
                "period_last": last_day,
                "category": "Exception",
                "employee": r.get("employee", ""),
                "id": r.get("id", ""),
                "service": "",
                "location": "",
                "reason": r.get("double_booking_flag", "Review required"),
                "detail": "",
                "severity": "warning",
            })

    return pd.DataFrame(rows)


# ---------- ADP export ----------
def export_adp_csv(
    payroll_df: pd.DataFrame,
    out_path: str,
    pay_codes: dict[str, str] | None = None,
    column_mapping: dict[str, str] | None = None,
) -> None:
    """Write ADP-ready CSV: employee id and earnings lines (hours/amounts) by pay code.
    column_mapping overrides the default output column names. Example:
      {"employee_id": "Co Code", "pay_code": "Earnings Code", "amount": "Amount", "hours": "Hours"}
    This allows the CSV to match any payroll provider's expected column headers.
    """
    pay_codes = pay_codes or {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}
    col_map = column_mapping or {}
    col_eid = col_map.get("employee_id", "employee_id")
    col_code = col_map.get("pay_code", "pay_code")
    col_amount = col_map.get("amount", "amount")
    col_hours = col_map.get("hours", "hours")

    if payroll_df.empty:
        pd.DataFrame(columns=[col_eid, col_code, col_amount, col_hours]).to_csv(out_path, index=False)
        return
    rows = []
    id_col = "id"
    for _, r in payroll_df.iterrows():
        eid = r.get(id_col, "")
        # Regular hours
        h = r.get("Total Hours Worked", 0) or 0
        if h != 0:
            rows.append({col_eid: eid, col_code: pay_codes.get("regular_hours", "REG"), col_amount: "", col_hours: h})
        # Commission (service + retail)
        comm = (r.get("service_comission", 0) or 0) + (r.get("total_retail_commission", 0) or 0)
        if comm != 0:
            rows.append({col_eid: eid, col_code: pay_codes.get("commission", "COMM"), col_amount: round(comm, 2), col_hours: ""})
        # Tips
        tips = r.get("Tips", 0) or 0
        if tips != 0:
            rows.append({col_eid: eid, col_code: pay_codes.get("tips", "TIPS"), col_amount: round(tips, 2), col_hours: ""})
    pd.DataFrame(rows).to_csv(out_path, index=False)


# ---------- Run log ----------
def write_run_log(
    out_path: str,
    paths_used: dict[str, str],
    period: tuple[Any, Any],
    validation_issues: list[dict],
    row_counts: dict[str, int],
) -> None:
    """Write run metadata for reproducibility."""
    log = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "paths": paths_used,
        "period_first": str(period[0]) if period and len(period) > 0 else None,
        "period_last": str(period[1]) if period and len(period) > 1 else None,
        "validation_issues_count": len(validation_issues),
        "row_counts": row_counts,
    }
    with open(out_path, "w") as f:
        json.dump(log, f, indent=2)
