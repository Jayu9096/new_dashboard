from option_chain_core import render_option_chain_page

TITLE = "SENSEX Live Option Chain"
SYMBOL = "SENSEX"
INSTRUMENT = "BSE_INDEX|SENSEX"
EXPIRY = "2026-04-30"


def render_app() -> None:
    render_option_chain_page(title=TITLE, symbol=SYMBOL, instrument=INSTRUMENT, expiry=EXPIRY)


if __name__ == "__main__":
    import streamlit as st
    st.set_page_config(page_title=TITLE, layout="wide")
    render_app()
