"""
Phase 2 integration framework: multiple POS systems and payroll providers.
Use get_pos_adapter() and get_payroll_adapter() with config to obtain the right adapter.
"""
from integrations.pos_adapters import get_pos_adapter, POSAdapter
from integrations.payroll_adapters import get_payroll_adapter, PayrollAdapter, get_payroll_export_path

__all__ = [
    "get_pos_adapter",
    "POSAdapter",
    "get_payroll_adapter",
    "PayrollAdapter",
    "get_payroll_export_path",
]
