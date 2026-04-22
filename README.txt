Files included

1. master_dashboard.py
2. nifty_option_chain_page.py
3. sensex_option_chain_page.py
4. option_analysis_live_multi.py
5. option_chain_core.py
6. shared_market_state.py

How this architecture fixes your issue

- Both NIFTY and SENSEX publish into dedicated index-specific session keys.
- The AI analysis page reads shared in-memory state, not generic latest_df only.
- Generic latest_df is used only as guarded compatibility fallback.
- Same core fetch/render logic is reused for both indices.
- Each index has its own last keys, snapshot buffer, latest spot, pcr, and OHLC.

Required existing local files

- login.py
- upstox_ws.py

Run

streamlit run master_dashboard.py
