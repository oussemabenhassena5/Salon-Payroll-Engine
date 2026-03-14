"""
Tests for Phase 2 integration adapters (POS and payroll providers).
Run: python tests/test_integrations.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations import get_pos_adapter, get_payroll_adapter, get_payroll_export_path
from integrations.pos_adapters import list_pos_sources
from integrations.payroll_adapters import list_payroll_providers


def test_get_pos_adapter_phorest():
    config = {"pos_source": "phorest"}
    adapter = get_pos_adapter(config)
    assert adapter.name == "Phorest"


def test_get_pos_adapter_unknown():
    try:
        get_pos_adapter({"pos_source": "unknown"})
    except ValueError as e:
        assert "Unknown pos_source" in str(e)


def test_list_pos_sources():
    sources = list_pos_sources()
    assert "phorest" in sources
    assert "vagaro" in sources
    assert "fresha" in sources
    assert "zenoti" in sources
    assert "mindbody" in sources


def test_get_payroll_adapter_adp():
    config = {"payroll_provider": "adp"}
    adapter = get_payroll_adapter(config)
    assert adapter.name == "ADP"


def test_get_payroll_adapter_trinet_paylocity():
    for key, name in [("trinet", "TriNet"), ("paylocity", "Paylocity")]:
        adapter = get_payroll_adapter({"payroll_provider": key})
        assert adapter.name == name


def test_get_payroll_export_path():
    base = {"path_adp_import": "/out/adp.csv", "path_trinet_import": "/out/trinet.csv", "path_paylocity_import": "/out/paylocity.csv"}
    assert get_payroll_export_path({**base, "path_payroll_import": "/custom.csv"}) == "/custom.csv"
    assert get_payroll_export_path({**base, "payroll_provider": "adp"}) == "/out/adp.csv"
    assert get_payroll_export_path({**base, "payroll_provider": "trinet"}) == "/out/trinet.csv"
    assert get_payroll_export_path({**base, "payroll_provider": "paylocity"}) == "/out/paylocity.csv"


def test_adp_export_via_adapter():
    adapter = get_payroll_adapter({"payroll_provider": "adp", "adp_pay_codes": {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}})
    df = pd.DataFrame([
        {"id": "E001", "Total Hours Worked": 40, "service_comission": 100, "total_retail_commission": 50, "Tips": 20},
    ])
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        adapter.export(df, path, {"adp_pay_codes": {"regular_hours": "REG", "commission": "COMM", "tips": "TIPS"}})
        out = pd.read_csv(path)
        assert "employee_id" in out.columns and "pay_code" in out.columns
        assert len(out) >= 3  # hours, commission, tips
    finally:
        os.unlink(path)


def test_trinet_export():
    adapter = get_payroll_adapter({"payroll_provider": "trinet"})
    df = pd.DataFrame([{"id": "E001", "Total Hours Worked": 40, "service_comission": 100, "total_retail_commission": 0, "Tips": 0}])
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        adapter.export(df, path, {})
        out = pd.read_csv(path)
        assert "Employee ID" in out.columns and "Earnings Code" in out.columns
    finally:
        os.unlink(path)


def test_paylocity_export():
    adapter = get_payroll_adapter({"payroll_provider": "paylocity"})
    df = pd.DataFrame([{"id": "E001", "Total Hours Worked": 40, "service_comission": 0, "total_retail_commission": 0, "Tips": 0}])
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        path = f.name
    try:
        adapter.export(df, path, {})
        out = pd.read_csv(path)
        assert "EmployeeId" in out.columns and "PayCode" in out.columns
    finally:
        os.unlink(path)


def run_all():
    test_get_pos_adapter_phorest()
    test_get_pos_adapter_unknown()
    test_list_pos_sources()
    test_get_payroll_adapter_adp()
    test_get_payroll_adapter_trinet_paylocity()
    test_get_payroll_export_path()
    test_adp_export_via_adapter()
    test_trinet_export()
    test_paylocity_export()
    print("All integration tests passed.")


if __name__ == "__main__":
    run_all()
