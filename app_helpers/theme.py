"""Color palette and CSS overrides for the Streamlit app."""

# Color palette
PRIMARY = "#6C5CE7"
PRIMARY_LIGHT = "#A29BFE"
SUCCESS = "#00B894"
WARNING = "#FDCB6E"
ERROR = "#E17055"
INFO = "#74B9FF"
TEXT = "#2D3436"
TEXT_LIGHT = "#636E72"
BG = "#FAFAFA"
SIDEBAR_BG = "#F0EDFA"
CARD_BG = "#FFFFFF"

# Location chart colors (8 distinguishable colors for up to 8 branches)
LOCATION_COLORS = [
    "#6C5CE7", "#00B894", "#E17055", "#74B9FF",
    "#FDCB6E", "#A29BFE", "#FF7675", "#55EFC4",
]

# Plotly chart template overrides
PLOTLY_LAYOUT = dict(
    font=dict(family="Inter, sans-serif", color=TEXT),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=20, r=20, t=40, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)

# CSS injection for custom styling
CUSTOM_CSS = """
<style>
    /* Metric cards */
    div[data-testid="stMetric"] {
        background: white;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(108, 92, 231, 0.08);
        border-left: 4px solid #6C5CE7;
    }
    div[data-testid="stMetric"] label {
        color: #636E72;
        font-size: 0.85rem;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #2D3436;
        font-weight: 600;
    }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #6C5CE7 0%, #A29BFE 100%);
        border: none;
        border-radius: 8px;
        padding: 0.6rem 2rem;
        font-weight: 600;
        transition: all 0.2s;
    }
    .stButton > button[kind="primary"]:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(108, 92, 231, 0.3);
    }

    /* File uploader */
    [data-testid="stFileUploader"] {
        border-radius: 12px;
    }
    [data-testid="stFileUploader"] section {
        border: 2px dashed #A29BFE;
        border-radius: 12px;
        padding: 1rem;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab"] {
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        color: #6C5CE7;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #F0EDFA 0%, #FAFAFA 100%);
    }

    /* Download buttons */
    .stDownloadButton > button {
        border-radius: 8px;
        border: 1px solid #E0E0E0;
        font-size: 0.85rem;
    }

    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #6C5CE7, #A29BFE);
    }

    /* Expander */
    [data-testid="stExpander"] {
        border-radius: 12px;
        border: 1px solid #E8E4F0;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
"""
