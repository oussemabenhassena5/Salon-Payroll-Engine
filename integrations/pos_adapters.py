"""
POS (point-of-sale) adapters for Phase 2: multiple upstream systems.
Each adapter ingests source files and returns a normalized DataFrame with columns:
  first_day, last_day, filial, service, metric, employee, value
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

# Lazy import to avoid circular dependency; engine used by Phorest adapter
def _process_sheets(paths: list[str], df_dates: pd.DataFrame) -> pd.DataFrame:
    from payroll_engine import process_sheets
    return process_sheets(paths, df_dates)


class POSAdapter(ABC):
    """Base class for POS data source adapters. Output is always normalized long format."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Display name of the POS system (e.g. 'Phorest', 'Vagaro')."""
        pass

    @abstractmethod
    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        """
        Ingest sales data from one or more files. Return DataFrame with columns:
        first_day, last_day, filial, service, metric, employee, value
        """
        pass


class PhorestAdapter(POSAdapter):
    """Phorest export: branch CSVs with metric, service, employee columns, values."""

    @property
    def name(self) -> str:
        return "Phorest"

    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        return _process_sheets(source_paths, df_dates)


class VagaroAdapter(POSAdapter):
    """Vagaro: placeholder for future implementation. Expects similar export structure."""

    @property
    def name(self) -> str:
        return "Vagaro"

    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        raise NotImplementedError(
            "Vagaro adapter not yet implemented. "
            "Export from Vagaro into CSV with columns compatible with normalized format (service, metric, employee columns, values) "
            "or use Phorest as POS source."
        )


class FreshaAdapter(POSAdapter):
    """Fresha: placeholder for future implementation."""

    @property
    def name(self) -> str:
        return "Fresha"

    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        raise NotImplementedError(
            "Fresha adapter not yet implemented. "
            "Export from Fresha into normalized CSV or use Phorest as POS source."
        )


class ZenotiAdapter(POSAdapter):
    """Zenoti: placeholder for future implementation."""

    @property
    def name(self) -> str:
        return "Zenoti"

    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        raise NotImplementedError(
            "Zenoti adapter not yet implemented. "
            "Export from Zenoti into normalized CSV or use Phorest as POS source."
        )


class MindbodyAdapter(POSAdapter):
    """Mindbody: placeholder for future implementation."""

    @property
    def name(self) -> str:
        return "Mindbody"

    def ingest(self, source_paths: list[str], df_dates: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
        raise NotImplementedError(
            "Mindbody adapter not yet implemented. "
            "Export from Mindbody into normalized CSV or use Phorest as POS source."
        )


_REGISTRY: dict[str, type[POSAdapter]] = {
    "phorest": PhorestAdapter,
    "vagaro": VagaroAdapter,
    "fresha": FreshaAdapter,
    "zenoti": ZenotiAdapter,
    "mindbody": MindbodyAdapter,
}


def get_pos_adapter(config: dict[str, Any]) -> POSAdapter:
    """Return the POS adapter for config['pos_source'] (default 'phorest')."""
    key = (config.get("pos_source") or "phorest").lower().strip()
    if key not in _REGISTRY:
        raise ValueError(f"Unknown pos_source: {config.get('pos_source')}. Valid: {list(_REGISTRY.keys())}")
    return _REGISTRY[key]()


def list_pos_sources() -> list[str]:
    """Return list of registered POS source identifiers."""
    return list(_REGISTRY.keys())
