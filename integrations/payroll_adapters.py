"""
Payroll provider adapters for Phase 2: export to multiple payroll systems.
Each adapter writes the calculated payroll (same internal DataFrame) to a provider-specific format.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


def _export_adp_csv(payroll_df: pd.DataFrame, out_path: str, pay_codes: dict[str, str] | None, column_mapping: dict[str, str] | None = None) -> None:
    from payroll_engine import export_adp_csv
    export_adp_csv(payroll_df, out_path, pay_codes, column_mapping)


class PayrollAdapter(ABC):
    """Base class for payroll provider export adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Display name of the provider (e.g. 'ADP', 'TriNet')."""
        pass

    @abstractmethod
    def export(
        self,
        payroll_df: pd.DataFrame,
        out_path: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Write payroll_df to out_path in the provider's required format."""
        pass


class ADPAdapter(PayrollAdapter):
    """ADP: CSV with employee_id, pay_code, amount, hours."""

    @property
    def name(self) -> str:
        return "ADP"

    def export(
        self,
        payroll_df: pd.DataFrame,
        out_path: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        config = config or {}
        pay_codes = config.get("adp_pay_codes") or {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}
        col_mapping = config.get("adp_column_mapping")
        _export_adp_csv(payroll_df, out_path, pay_codes, col_mapping)


class TriNetAdapter(PayrollAdapter):
    """TriNet: CSV format with Employee ID, Earnings Code, Hours, Amount (TriNet-style column names)."""

    @property
    def name(self) -> str:
        return "TriNet"

    def export(
        self,
        payroll_df: pd.DataFrame,
        out_path: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        config = config or {}
        # TriNet often uses Earnings Code; map from internal keys
        pay_codes = config.get("trinet_earnings_codes") or {
            "regular_hours": "REG",
            "commission": "COMM",
            "tips": "TIPS",
        }
        if payroll_df.empty:
            pd.DataFrame(columns=["Employee ID", "Earnings Code", "Hours", "Amount"]).to_csv(out_path, index=False)
            return
        rows = []
        for _, r in payroll_df.iterrows():
            eid = r.get("id", "")
            h = r.get("Total Hours Worked", 0) or 0
            if h != 0:
                rows.append({"Employee ID": eid, "Earnings Code": pay_codes.get("regular_hours", "REG"), "Hours": h, "Amount": ""})
            comm = (r.get("service_comission", 0) or 0) + (r.get("total_retail_commission", 0) or 0)
            if comm != 0:
                rows.append({"Employee ID": eid, "Earnings Code": pay_codes.get("commission", "COMM"), "Hours": "", "Amount": round(comm, 2)})
            tips = r.get("Tips", 0) or 0
            if tips != 0:
                rows.append({"Employee ID": eid, "Earnings Code": pay_codes.get("tips", "TIPS"), "Hours": "", "Amount": round(tips, 2)})
        pd.DataFrame(rows).to_csv(out_path, index=False)


class PaylocityAdapter(PayrollAdapter):
    """Paylocity: CSV with EmployeeId, PayCode, Hours, Amount (Paylocity-style)."""

    @property
    def name(self) -> str:
        return "Paylocity"

    def export(
        self,
        payroll_df: pd.DataFrame,
        out_path: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        config = config or {}
        pay_codes = config.get("paylocity_pay_codes") or {
            "regular_hours": "REG",
            "commission": "COMM",
            "tips": "TIPS",
        }
        if payroll_df.empty:
            pd.DataFrame(columns=["EmployeeId", "PayCode", "Hours", "Amount"]).to_csv(out_path, index=False)
            return
        rows = []
        for _, r in payroll_df.iterrows():
            eid = r.get("id", "")
            h = r.get("Total Hours Worked", 0) or 0
            if h != 0:
                rows.append({"EmployeeId": eid, "PayCode": pay_codes.get("regular_hours", "REG"), "Hours": h, "Amount": ""})
            comm = (r.get("service_comission", 0) or 0) + (r.get("total_retail_commission", 0) or 0)
            if comm != 0:
                rows.append({"EmployeeId": eid, "PayCode": pay_codes.get("commission", "COMM"), "Hours": "", "Amount": round(comm, 2)})
            tips = r.get("Tips", 0) or 0
            if tips != 0:
                rows.append({"EmployeeId": eid, "PayCode": pay_codes.get("tips", "TIPS"), "Hours": "", "Amount": round(tips, 2)})
        pd.DataFrame(rows).to_csv(out_path, index=False)


_REGISTRY: dict[str, type[PayrollAdapter]] = {
    "adp": ADPAdapter,
    "trinet": TriNetAdapter,
    "paylocity": PaylocityAdapter,
}


def get_payroll_adapter(config: dict[str, Any]) -> PayrollAdapter:
    """Return the payroll export adapter for config['payroll_provider'] (default 'adp')."""
    key = (config.get("payroll_provider") or "adp").lower().strip()
    if key not in _REGISTRY:
        raise ValueError(f"Unknown payroll_provider: {config.get('payroll_provider')}. Valid: {list(_REGISTRY.keys())}")
    return _REGISTRY[key]()


def list_payroll_providers() -> list[str]:
    """Return list of registered payroll provider identifiers."""
    return list(_REGISTRY.keys())


def get_payroll_export_path(config: dict[str, Any]) -> str:
    """Resolve output path for payroll export from config (path_payroll_import or provider-specific)."""
    path = config.get("path_payroll_import")
    if path:
        return path
    provider = (config.get("payroll_provider") or "adp").lower().strip()
    if provider == "adp":
        return config.get("path_adp_import", "")
    if provider == "trinet":
        return config.get("path_trinet_import", "")
    if provider == "paylocity":
        return config.get("path_paylocity_import", "")
    return config.get("path_adp_import", "")
