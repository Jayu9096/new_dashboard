import streamlit as st

from nifty_option_chain_page import render_app as render_nifty_app
from sensex_option_chain_page import render_app as render_sensex_app
from option_analysis_live_multi import render_app as render_analysis_app


st.set_page_config(
    page_title="AI Trading Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <div style='font-size:18px; font-weight:700; margin-bottom:2px;'>
        📊 AI Option Chain Dashboard
    </div>
    <div style='font-size:12px; color:gray; margin-bottom:8px;'>
        NIFTY | SENSEX | Live Memory Analysis
    </div>
    """,
    unsafe_allow_html=True,
)

tab1, tab2, tab3 = st.tabs(["NIFTY", "SENSEX", "AI Analysis"])

with tab1:
    render_nifty_app()

with tab2:
    render_sensex_app()

with tab3:
    render_analysis_app()
