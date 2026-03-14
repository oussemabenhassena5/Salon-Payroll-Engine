"""Session state initialization and management."""
import copy
import os
import sys
import tempfile

import streamlit as st

# Add project root to path so engine imports work
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import CONFIG


def init_state():
    """Initialize session state with defaults. Only sets keys that don't already exist."""
    if "config" not in st.session_state:
        st.session_state.config = copy.deepcopy(CONFIG)

    if "pipeline_results" not in st.session_state:
        st.session_state.pipeline_results = {}

    if "pipeline_status" not in st.session_state:
        st.session_state.pipeline_status = None

    if "pipeline_log" not in st.session_state:
        st.session_state.pipeline_log = []

    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    if "temp_dir" not in st.session_state:
        st.session_state.temp_dir = tempfile.mkdtemp(prefix="salon_payroll_")

    if "warehouse_path" not in st.session_state:
        st.session_state.warehouse_path = CONFIG.get("path_warehouse", "")


def get_config() -> dict:
    """Return the current config from session state."""
    return st.session_state.config


def get_warehouse_path() -> str:
    """Return resolved warehouse path."""
    return st.session_state.warehouse_path


def reset_config():
    """Reset config to defaults."""
    st.session_state.config = copy.deepcopy(CONFIG)
