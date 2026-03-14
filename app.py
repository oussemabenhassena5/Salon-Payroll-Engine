"""
Salon Payroll & Sales Analysis — Streamlit App
Run: streamlit run app.py
"""
import streamlit as st

from app_helpers.state import init_state
from app_helpers.theme import CUSTOM_CSS

st.set_page_config(
    page_title="Salon Payroll",
    page_icon=":scissors:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject custom CSS
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# Initialize session state
init_state()

# Sidebar branding
with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding: 1rem 0 0.5rem 0;">
            <h2 style="color:#6C5CE7; margin-bottom:0;">Salon Payroll</h2>
            <p style="color:#636E72; font-size:0.85rem; margin-top:4px;">Sales Analysis Engine</p>
        </div>
        <hr style="border:none; border-top:1px solid #E8E4F0; margin:0.5rem 0 1rem 0;">
        """,
        unsafe_allow_html=True,
    )

# Home page content
st.title("Salon Payroll & Sales Analysis")
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
        ### :arrow_forward: Run Pipeline
        Upload your Phorest exports, master employee file, and hours worked.
        Click Run to process payroll in one click.
        """
    )

with col2:
    st.markdown(
        """
        ### :bar_chart: Dashboard
        View revenue by location, commission breakdowns,
        top employees, and historical trends.
        """
    )

with col3:
    st.markdown(
        """
        ### :warning: Exceptions
        Review unmatched employees, unmapped services,
        multi-location flags, and revenue outliers.
        """
    )

st.markdown("")

col4, col5 = st.columns(2)

with col4:
    st.markdown(
        """
        ### :file_folder: Historical Runs
        View past payroll runs, compare periods,
        edit employee records, and manage your data.
        """
    )

with col5:
    st.markdown(
        """
        ### :gear: Settings
        Configure commission thresholds, payroll provider,
        pay codes, and exception detection rules.
        """
    )

st.markdown("---")
st.caption("Use the sidebar to navigate between pages.")
