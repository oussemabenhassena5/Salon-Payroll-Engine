"""
Historical payroll data warehouse and trend reporting.
Stores each run in SQLite; provides payroll by period/employee, commission summary,
payroll % of sales, exception/double-booking counts, and period-over-period comparison.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any

import pandas as pd


def _connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    return sqlite3.connect(path)


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payroll_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_first TEXT NOT NULL,
            period_last TEXT NOT NULL,
            run_timestamp TEXT NOT NULL,
            total_service REAL,
            total_retail REAL,
            total_tips REAL,
            total_commission REAL,
            total_hours REAL,
            employee_count INTEGER,
            exception_count INTEGER,
            double_booking_count INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payroll_detail (
            run_id INTEGER NOT NULL,
            employee_id TEXT,
            employee_name TEXT,
            total_service REAL,
            total_retail REAL,
            total_tips REAL,
            service_commission REAL,
            retail_commission REAL,
            hours_worked REAL,
            double_booking_flag TEXT,
            PRIMARY KEY (run_id, employee_id),
            FOREIGN KEY (run_id) REFERENCES payroll_runs(run_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS location_summary (
            run_id INTEGER NOT NULL,
            location TEXT NOT NULL,
            total_service REAL,
            total_retail REAL,
            total_tips REAL,
            employee_count INTEGER,
            PRIMARY KEY (run_id, location),
            FOREIGN KEY (run_id) REFERENCES payroll_runs(run_id)
        )
    """)
    conn.commit()


def save_payroll_run(
    warehouse_path: str,
    final_df: pd.DataFrame,
    period_first: Any,
    period_last: Any,
    location_summary_df: pd.DataFrame | None = None,
    exception_count: int = 0,
) -> int:
    """Append one payroll run to the warehouse. Returns run_id."""
    if final_df is None or (isinstance(final_df, pd.DataFrame) and final_df.empty):
        return 0
    conn = _connect(warehouse_path)
    _init_schema(conn)
    run_ts = datetime.utcnow().isoformat() + "Z"
    period_first_s = str(period_first) if period_first is not None else ""
    period_last_s = str(period_last) if period_last is not None else ""

    total_service = final_df["total_service"].sum() if "total_service" in final_df.columns else 0
    total_retail = final_df["Retail"].sum() if "Retail" in final_df.columns else 0
    total_tips = final_df["Tips"].sum() if "Tips" in final_df.columns else 0
    sc = final_df["service_comission"].sum() if "service_comission" in final_df.columns else 0
    rc = final_df["total_retail_commission"].sum() if "total_retail_commission" in final_df.columns else 0
    total_commission = float(sc) + float(rc)
    total_hours = final_df["Total Hours Worked"].sum() if "Total Hours Worked" in final_df.columns else 0
    employee_count = len(final_df)
    double_booking_count = 0
    if "double_booking_flag" in final_df.columns:
        double_booking_count = int((final_df["double_booking_flag"].astype(str).str.len() > 0).sum())

    conn.execute(
        """INSERT INTO payroll_runs (
            period_first, period_last, run_timestamp,
            total_service, total_retail, total_tips, total_commission, total_hours,
            employee_count, exception_count, double_booking_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            period_first_s, period_last_s, run_ts,
            total_service, total_retail, total_tips, total_commission, total_hours,
            employee_count, exception_count, double_booking_count,
        ),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    for _, row in final_df.iterrows():
        eid = row.get("id", "")
        ename = row.get("employee", "")
        ts = row.get("total_service", 0) or 0
        tr = row.get("Retail", 0) or 0
        tips = row.get("Tips", 0) or 0
        scomm = row.get("service_comission", 0) or 0
        rcomm = row.get("total_retail_commission", 0) or 0
        hw = row.get("Total Hours Worked", 0) or 0
        flag = str(row.get("double_booking_flag", "") or "")
        conn.execute(
            """INSERT OR REPLACE INTO payroll_detail (
                run_id, employee_id, employee_name, total_service, total_retail, total_tips,
                service_commission, retail_commission, hours_worked, double_booking_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, eid, ename, ts, tr, tips, scomm, rcomm, hw, flag),
        )

    if location_summary_df is not None and not location_summary_df.empty:
        loc_map = {"location": "location", "total_service": "total_service", "total_retail": "total_retail",
                   "total_tips": "total_tips", "employee_count": "employee_count"}
        for _, row in location_summary_df.iterrows():
            loc = row.get("location", "")
            ls = row.get("total_service", 0) or 0
            lr = row.get("total_retail", 0) or 0
            lt = row.get("total_tips", 0) or 0
            ec = int(row.get("employee_count", 0) or 0)
            conn.execute(
                """INSERT OR REPLACE INTO location_summary (run_id, location, total_service, total_retail, total_tips, employee_count)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (run_id, loc, ls, lr, lt, ec),
            )
    conn.commit()
    conn.close()
    return run_id


def get_payroll_by_period(warehouse_path: str) -> pd.DataFrame:
    """Return payroll runs summary: period_first, period_last, run_timestamp, totals, counts."""
    if not os.path.isfile(warehouse_path):
        return pd.DataFrame()
    conn = _connect(warehouse_path)
    df = pd.read_sql_query(
        "SELECT run_id, period_first, period_last, run_timestamp, total_service, total_retail, total_tips, "
        "total_commission, total_hours, employee_count, exception_count, double_booking_count FROM payroll_runs ORDER BY period_first",
        conn,
    )
    conn.close()
    return df


def get_payroll_by_employee(warehouse_path: str, employee_id: str | None = None) -> pd.DataFrame:
    """Return payroll detail by employee (optionally filter by employee_id) with period from runs."""
    if not os.path.isfile(warehouse_path):
        return pd.DataFrame()
    conn = _connect(warehouse_path)
    q = """
        SELECT d.run_id, r.period_first, r.period_last, d.employee_id, d.employee_name,
               d.total_service, d.total_retail, d.service_commission, d.retail_commission, d.hours_worked, d.double_booking_flag
        FROM payroll_detail d JOIN payroll_runs r ON d.run_id = r.run_id
    """
    params: list[Any] = []
    if employee_id is not None:
        q += " WHERE d.employee_id = ?"
        params.append(employee_id)
    q += " ORDER BY r.period_first, d.employee_id"
    df = pd.read_sql_query(q, conn, params=params if params else None)
    conn.close()
    return df


def get_commission_summary_by_period(warehouse_path: str) -> pd.DataFrame:
    """Return per-period totals for service commission, retail commission, and total commission."""
    runs = get_payroll_by_period(warehouse_path)
    if runs.empty:
        return pd.DataFrame()
    return runs[["period_first", "period_last", "run_timestamp", "total_commission"]].copy()


def get_payroll_pct_sales(warehouse_path: str) -> pd.DataFrame:
    """Return effective payroll % of sales (commission / (service + retail)) per period."""
    runs = get_payroll_by_period(warehouse_path)
    if runs.empty:
        return pd.DataFrame()
    out = runs[["run_id", "period_first", "period_last", "total_service", "total_retail", "total_commission"]].copy()
    out["total_sales"] = out["total_service"].fillna(0) + out["total_retail"].fillna(0)
    out["payroll_pct_sales"] = (out["total_commission"] / out["total_sales"].replace(0, float("nan")) * 100).round(2)
    return out


def get_exception_double_booking_counts(warehouse_path: str) -> pd.DataFrame:
    """Return exception_count and double_booking_count per period."""
    runs = get_payroll_by_period(warehouse_path)
    if runs.empty:
        return pd.DataFrame()
    return runs[["period_first", "period_last", "exception_count", "double_booking_count"]].copy()


def get_period_over_period(warehouse_path: str) -> pd.DataFrame:
    """Return period-over-period comparison: current vs prior period totals and % change."""
    runs = get_payroll_by_period(warehouse_path)
    if runs is None or len(runs) < 2:
        return pd.DataFrame()
    runs = runs.sort_values("period_first").reset_index(drop=True)
    out = []
    for i in range(1, len(runs)):
        curr = runs.iloc[i]
        prev = runs.iloc[i - 1]
        total_curr = (curr.get("total_service", 0) or 0) + (curr.get("total_retail", 0) or 0)
        total_prev = (prev.get("total_service", 0) or 0) + (prev.get("total_retail", 0) or 0)
        comm_curr = curr.get("total_commission", 0) or 0
        comm_prev = prev.get("total_commission", 0) or 0
        pct_change_sales = ((total_curr - total_prev) / total_prev * 100) if total_prev else None
        pct_change_comm = ((comm_curr - comm_prev) / comm_prev * 100) if comm_prev else None
        out.append({
            "period_current": str(curr["period_first"]),
            "period_prior": str(prev["period_first"]),
            "total_sales_current": total_curr,
            "total_sales_prior": total_prev,
            "pct_change_sales": round(pct_change_sales, 2) if pct_change_sales is not None else None,
            "total_commission_current": comm_curr,
            "total_commission_prior": comm_prev,
            "pct_change_commission": round(pct_change_comm, 2) if pct_change_comm is not None else None,
        })
    return pd.DataFrame(out)


def delete_payroll_run(warehouse_path: str, run_id: int) -> bool:
    """Delete a payroll run and all its detail/location records by run_id.
    Returns True if the run was found and deleted, False if not found."""
    if not os.path.isfile(warehouse_path):
        return False
    conn = _connect(warehouse_path)
    _init_schema(conn)
    existing = conn.execute("SELECT run_id FROM payroll_runs WHERE run_id = ?", (run_id,)).fetchone()
    if not existing:
        conn.close()
        return False
    conn.execute("DELETE FROM payroll_detail WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM location_summary WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM payroll_runs WHERE run_id = ?", (run_id,))
    conn.commit()
    conn.close()
    return True


def delete_payroll_run_by_period(warehouse_path: str, period_first: str, period_last: str) -> int:
    """Delete all payroll runs matching a specific period.
    Returns the number of runs deleted."""
    if not os.path.isfile(warehouse_path):
        return 0
    conn = _connect(warehouse_path)
    _init_schema(conn)
    runs = conn.execute(
        "SELECT run_id FROM payroll_runs WHERE period_first = ? AND period_last = ?",
        (period_first, period_last),
    ).fetchall()
    if not runs:
        conn.close()
        return 0
    for (rid,) in runs:
        conn.execute("DELETE FROM payroll_detail WHERE run_id = ?", (rid,))
        conn.execute("DELETE FROM location_summary WHERE run_id = ?", (rid,))
        conn.execute("DELETE FROM payroll_runs WHERE run_id = ?", (rid,))
    conn.commit()
    conn.close()
    return len(runs)


def update_payroll_employee(
    warehouse_path: str,
    run_id: int,
    employee_id: str,
    updates: dict[str, Any],
) -> bool:
    """Update specific fields for an employee in a payroll run.
    updates can include: total_service, total_retail, total_tips, service_commission,
    retail_commission, hours_worked, double_booking_flag.
    Returns True if the record was found and updated."""
    if not os.path.isfile(warehouse_path):
        return False
    allowed = {"total_service", "total_retail", "total_tips", "service_commission",
               "retail_commission", "hours_worked", "double_booking_flag", "employee_name"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return False
    conn = _connect(warehouse_path)
    _init_schema(conn)
    existing = conn.execute(
        "SELECT run_id FROM payroll_detail WHERE run_id = ? AND employee_id = ?",
        (run_id, employee_id),
    ).fetchone()
    if not existing:
        conn.close()
        return False
    set_clause = ", ".join(f"{k} = ?" for k in filtered)
    values = list(filtered.values()) + [run_id, employee_id]
    conn.execute(
        f"UPDATE payroll_detail SET {set_clause} WHERE run_id = ? AND employee_id = ?",
        values,
    )
    conn.commit()
    conn.close()
    return True


def list_payroll_runs(warehouse_path: str) -> pd.DataFrame:
    """List all payroll runs with run_id, period, timestamp, and summary totals.
    Useful for selecting which run to delete or edit."""
    return get_payroll_by_period(warehouse_path)


def build_trend_report(warehouse_path: str, out_path: str) -> None:
    """Write a single Excel file with sheets: payroll_by_period, commission_summary, payroll_pct_sales,
    exception_double_booking, period_over_period."""
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        get_payroll_by_period(warehouse_path).to_excel(w, sheet_name="payroll_by_period", index=False)
        get_commission_summary_by_period(warehouse_path).to_excel(w, sheet_name="commission_summary", index=False)
        get_payroll_pct_sales(warehouse_path).to_excel(w, sheet_name="payroll_pct_sales", index=False)
        get_exception_double_booking_counts(warehouse_path).to_excel(w, sheet_name="exception_double_booking", index=False)
        get_period_over_period(warehouse_path).to_excel(w, sheet_name="period_over_period", index=False)
    return None
