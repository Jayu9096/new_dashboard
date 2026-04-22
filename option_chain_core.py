from __future__ import annotations

import html
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import streamlit as st

from login import get_access_token
from upstox_ws import start_ws, subscribe

from shared_market_state import ensure_index_state, publish_snapshot


REQUEST_TIMEOUT = 12
TABLE_HEIGHT_PX = 560
REFRESH_SECONDS = 2


@st.cache_resource
def get_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_num(value: Any, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return str(value)


def fmt_volume(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
            # Indian style shorthand used in your existing UI
        v = float(value)
        if abs(v) >= 100000:
            return f"{v / 100000:.2f}L"
        if abs(v) >= 1000:
            return f"{v / 1000:.2f}K"
        return f"{int(v)}"
    except (TypeError, ValueError):
        return str(value)


def esc(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    return html.escape(str(value))


def inject_page_style() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0.12rem;
            padding-bottom: 0.12rem;
            padding-left: 0.35rem;
            padding-right: 0.35rem;
            max-width: 100% !important;
        }
        div[data-testid="stMetric"] {
            border: 1px solid rgba(128,128,128,0.10);
            border-radius: 8px;
            padding: 2px 6px;
            background: transparent;
            box-shadow: none;
        }
        div[data-testid="stMetricLabel"] p {
            font-size: 11px !important;
            margin-bottom: 0px !important;
        }
        div[data-testid="stMetricValue"] {
            font-size: 14px !important;
            line-height: 1.0 !important;
        }
        .main-title {
            font-size: 14px;
            font-weight: 700;
            margin-bottom: 0px;
        }
        .sub-title {
            color: #6b7280;
            font-size: 12px;
            margin-bottom: 4px;
        }
        .oc-shell {
            width: 100%;
            overflow: hidden;
            background: transparent;
        }
        .oc-wrap {
            width: 100%;
            overflow: auto;
            background: transparent;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
        }
        .oc-table {
            border-collapse: separate;
            border-spacing: 0;
            min-width: 100%;
            width: max-content;
            font-size: 12px;
            background: transparent;
        }
        .oc-table th, .oc-table td {
            border-right: 1px solid #e5e7eb;
            border-bottom: 1px solid #e5e7eb;
            padding: 6px 10px;
            text-align: center;
            white-space: nowrap;
            background: transparent;
            min-width: 105px;
        }
        .oc-table th {
            position: sticky;
            top: 0;
            z-index: 10;
            background: #ffffff;
            color: #5a5a5a;
            font-weight: 700;
            font-size: 12px;
        }
        .oc-strike-col { min-width: 120px; max-width: 120px; }
        .oc-main { font-size: 12px; line-height: 1.1; color: #2a2a2a; }
        .oc-sub { font-size: 12px; line-height: 1.1; margin-top: 4px; color: #666; }
        .oc-green { color: #0f7b5f; }
        .oc-orange { color: #f05a28; }
        .oc-nearest td { font-weight: 700; }
        .oc-wrap::-webkit-scrollbar { height: 10px; width: 10px; }
        .oc-wrap::-webkit-scrollbar-thumb { background: #c9c0ae; border-radius: 10px; }
        .oc-wrap::-webkit-scrollbar-track { background: transparent; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_response_json(url: str, *, headers: dict[str, str] | None = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    session = get_http_session()
    response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def extract_ohlc(data: dict[str, Any], instrument: str) -> dict[str, Any] | None:
    payload = data.get("data", {})
    if not isinstance(payload, dict) or not payload:
        return None

    aliases = [instrument, instrument.replace("|", ":"), instrument.replace(":", "|")]
    for key in aliases:
        candidate = payload.get(key)
        if isinstance(candidate, dict) and "ohlc" in candidate:
            return candidate["ohlc"]

    first_value = next(iter(payload.values()), None)
    if isinstance(first_value, dict) and "ohlc" in first_value:
        return first_value["ohlc"]
    return None


def get_index_ohlc(token: str, instrument: str) -> tuple[float | None, float | None, float | None, float | None]:
    try:
        data = get_response_json(
            "https://api.upstox.com/v2/market-quote/ohlc",
            headers={"Authorization": f"Bearer {token}"},
            params={"instrument_key": instrument, "interval": "1d"},
        )
        ohlc = extract_ohlc(data, instrument)
        if not ohlc:
            return None, None, None, None
        return (
            safe_float(ohlc.get("open")),
            safe_float(ohlc.get("high")),
            safe_float(ohlc.get("low")),
            safe_float(ohlc.get("close")),
        )
    except Exception:
        return None, None, None, None


def fetch_chain(token: str, instrument: str, expiry: str) -> tuple[pd.DataFrame, list[str], float | None]:
    data = get_response_json(
        "https://api.upstox.com/v2/option/chain",
        headers={"Authorization": f"Bearer {token}"},
        params={"instrument_key": instrument, "expiry_date": expiry},
    )

    raw_rows = data.get("data", [])
    rows: list[dict[str, Any]] = []
    keys: list[str] = []
    spot: float | None = None

    for item in raw_rows:
        strike = safe_float(item.get("strike_price"))
        row_spot = safe_float(item.get("underlying_spot_price"))
        if row_spot is not None:
            spot = row_spot

        pcr = safe_float(item.get("pcr"))
        call = item.get("call_options") or {}
        put = item.get("put_options") or {}
        ce_md = call.get("market_data") or {}
        pe_md = put.get("market_data") or {}
        ce_gk = call.get("option_greeks") or {}
        pe_gk = put.get("option_greeks") or {}

        ce_key = call.get("instrument_key")
        pe_key = put.get("instrument_key")
        if ce_key:
            keys.append(ce_key)
        if pe_key:
            keys.append(pe_key)

        ce_oi = safe_float(ce_md.get("oi"))
        pe_oi = safe_float(pe_md.get("oi"))
        ce_prev_oi = safe_float(ce_md.get("prev_oi")) or 0.0
        pe_prev_oi = safe_float(pe_md.get("prev_oi")) or 0.0

        rows.append(
            {
                "STRIKE": strike,
                "SPOT": row_spot,
                "PCR": pcr,
                "CE_KEY": ce_key,
                "CE_OI": ce_oi,
                "CE_CHG_OI": None if ce_oi is None else ce_oi - ce_prev_oi,
                "CE_VOLUME": safe_float(ce_md.get("volume")),
                "CE_IV": safe_float(ce_gk.get("iv")),
                "CE_DELTA": safe_float(ce_gk.get("delta")),
                "CE_GAMMA": safe_float(ce_gk.get("gamma")),
                "CE_THETA": safe_float(ce_gk.get("theta")),
                "CE_VEGA": safe_float(ce_gk.get("vega")),
                "CE_LTP": safe_float(ce_md.get("ltp")),
                "CE_BID": safe_float(ce_md.get("bid_price")),
                "CE_ASK": safe_float(ce_md.get("ask_price")),
                "CE_BID_QTY": safe_float(ce_md.get("bid_qty")),
                "CE_ASK_QTY": safe_float(ce_md.get("ask_qty")),
                "PE_KEY": pe_key,
                "PE_BID_QTY": safe_float(pe_md.get("bid_qty")),
                "PE_ASK_QTY": safe_float(pe_md.get("ask_qty")),
                "PE_BID": safe_float(pe_md.get("bid_price")),
                "PE_ASK": safe_float(pe_md.get("ask_price")),
                "PE_LTP": safe_float(pe_md.get("ltp")),
                "PE_VEGA": safe_float(pe_gk.get("vega")),
                "PE_THETA": safe_float(pe_gk.get("theta")),
                "PE_GAMMA": safe_float(pe_gk.get("gamma")),
                "PE_DELTA": safe_float(pe_gk.get("delta")),
                "PE_IV": safe_float(pe_gk.get("iv")),
                "PE_VOLUME": safe_float(pe_md.get("volume")),
                "PE_CHG_OI": None if pe_oi is None else pe_oi - pe_prev_oi,
                "PE_OI": pe_oi,
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("STRIKE").reset_index(drop=True)
    return df, sorted(set(keys)), spot


def init_ws_for_index(token: str, symbol: str, instrument: str, expiry: str, keys: list[str]) -> None:
    if not keys:
        return
    state = ensure_index_state(symbol, instrument, expiry)
    try:
        if not getattr(st.session_state, "ws_started_global", False):
            start_ws(token)
            st.session_state.ws_started_global = True

        prev = set(state.last_keys)
        curr = set(keys)
        new_keys = sorted(curr - prev)
        if new_keys or not prev:
            subscribe(sorted(curr))
        state.last_keys = sorted(curr)
    except Exception:
        pass


def build_display_df(df: pd.DataFrame, spot: float | None) -> tuple[pd.DataFrame, int | None]:
    if df.empty:
        return pd.DataFrame(), None

    nearest_idx = None
    view_df = df.copy()
    if spot is not None:
        nearest_idx = int((view_df["STRIKE"] - spot).abs().idxmin())

    def lakh_value(x: Any) -> float | None:
        if x is None or pd.isna(x):
            return None
        return float(x) / 100000.0

    view_df["CE_OI_LAKHS"] = view_df["CE_OI"].apply(lakh_value)
    view_df["PE_OI_LAKHS"] = view_df["PE_OI"].apply(lakh_value)
    view_df["CE_VOL_FMT"] = view_df["CE_VOLUME"].apply(fmt_volume)
    view_df["PE_VOL_FMT"] = view_df["PE_VOLUME"].apply(fmt_volume)

    view_df["CE_OI_PCT"] = ((view_df["CE_CHG_OI"] / view_df["CE_OI"]) * 100).replace([float("inf"), -float("inf")], pd.NA)
    view_df["PE_OI_PCT"] = ((view_df["PE_CHG_OI"] / view_df["PE_OI"]) * 100).replace([float("inf"), -float("inf")], pd.NA)

    ce_mid = (view_df["CE_BID"].fillna(0) + view_df["CE_ASK"].fillna(0)) / 2
    pe_mid = (view_df["PE_BID"].fillna(0) + view_df["PE_ASK"].fillna(0)) / 2

    view_df["CE_LTP_PCT"] = (((view_df["CE_LTP"] - ce_mid) / ce_mid.replace(0, pd.NA)) * 100).replace([float("inf"), -float("inf")], pd.NA)
    view_df["PE_LTP_PCT"] = (((view_df["PE_LTP"] - pe_mid) / pe_mid.replace(0, pd.NA)) * 100).replace([float("inf"), -float("inf")], pd.NA)

    ordered_cols = [
        "CE_IV", "CE_GAMMA", "CE_THETA", "CE_DELTA", "CE_CHG_OI", "CE_OI_LAKHS", "CE_VOL_FMT", "CE_LTP",
        "STRIKE", "PCR", "PE_LTP", "PE_VOL_FMT", "PE_OI_LAKHS", "PE_CHG_OI", "PE_DELTA", "PE_THETA", "PE_GAMMA", "PE_IV",
        "CE_OI_PCT", "PE_OI_PCT", "CE_LTP_PCT", "PE_LTP_PCT",
    ]
    return view_df[ordered_cols].copy(), nearest_idx


def render_option_chain_html(display_df: pd.DataFrame, nearest_idx: int | None) -> str:
    if display_df.empty:
        return "<div style='padding:8px;'>No option chain data available.</div>"

    def fmt_main(col_name: str, val: Any) -> str:
        if val is None or pd.isna(val):
            return "-"
        if col_name in ["CE_CHG_OI", "PE_CHG_OI", "STRIKE"]:
            return fmt_int(val)
        if col_name in ["CE_OI_LAKHS", "PE_OI_LAKHS"]:
            return fmt_num(val, 1)
        if col_name in ["CE_VOL_FMT", "PE_VOL_FMT"]:
            return str(val)
        if "GAMMA" in col_name:
            return fmt_num(val, 4)
        if "DELTA" in col_name or "THETA" in col_name:
            return fmt_num(val, 4)
        if "IV" in col_name:
            return fmt_num(val, 2)
        return fmt_num(val, 2)

    def fmt_sub_pct(val: Any) -> str:
        if val is None or pd.isna(val):
            return ""
        sign = "+" if float(val) > 0 else ""
        return f"{sign}{float(val):,.2f} %"

    headers = [
        ("CE_IV", "IV"), ("CE_GAMMA", "Gamma"), ("CE_THETA", "Theta"), ("CE_DELTA", "Delta"),
        ("CE_CHG_OI", "OI (chg)"), ("CE_OI_LAKHS", "OI (lakhs)"), ("CE_VOL_FMT", "Volume"), ("CE_LTP", "LTP"),
        ("STRIKE", "Strike"), ("PE_LTP", "LTP"), ("PE_VOL_FMT", "Volume"), ("PE_OI_LAKHS", "OI (lakhs)"),
        ("PE_CHG_OI", "OI (chg)"), ("PE_DELTA", "Delta"), ("PE_THETA", "Theta"), ("PE_GAMMA", "Gamma"), ("PE_IV", "IV"),
    ]

    header_html = "<tr>" + "".join(
        f"<th>{esc(label)}</th>" for _, label in headers
    ) + "</tr>"

    rows_html = ""
    for row_idx, (_, row) in enumerate(display_df.iterrows()):
        is_nearest = nearest_idx is not None and row_idx == nearest_idx
        tr_class = "oc-nearest" if is_nearest else ""
        ce_oi_pct = fmt_sub_pct(row["CE_OI_PCT"])
        pe_oi_pct = fmt_sub_pct(row["PE_OI_PCT"])
        ce_ltp_pct = fmt_sub_pct(row["CE_LTP_PCT"])
        pe_ltp_pct = fmt_sub_pct(row["PE_LTP_PCT"])
        pcr_text = f"PCR: {float(row['PCR']):.2f}" if row["PCR"] is not None and not pd.isna(row["PCR"]) else ""

        def dual_cell(main: str, sub: str = "", cls: str = "") -> str:
            sub_html = f"<div class='oc-sub {cls}'>{esc(sub)}</div>" if sub else "<div class='oc-sub'>&nbsp;</div>"
            return f"<td><div class='oc-main {cls}'>{esc(main)}</div>{sub_html}</td>"

        rows_html += f"<tr class='{tr_class}'>"
        rows_html += dual_cell(fmt_main("CE_IV", row["CE_IV"]))
        rows_html += dual_cell(fmt_main("CE_GAMMA", row["CE_GAMMA"]))
        rows_html += dual_cell(fmt_main("CE_THETA", row["CE_THETA"]))
        rows_html += dual_cell(fmt_main("CE_DELTA", row["CE_DELTA"]))
        rows_html += dual_cell(fmt_main("CE_CHG_OI", row["CE_CHG_OI"]))
        rows_html += dual_cell(fmt_main("CE_OI_LAKHS", row["CE_OI_LAKHS"]), ce_oi_pct, "oc-green")
        rows_html += dual_cell(fmt_main("CE_VOL_FMT", row["CE_VOL_FMT"]))
        rows_html += dual_cell(fmt_main("CE_LTP", row["CE_LTP"]), ce_ltp_pct, "oc-orange")
        rows_html += f"<td class='oc-strike-col'><div class='oc-main'>{esc(fmt_main('STRIKE', row['STRIKE']))}</div><div class='oc-sub'>{esc(pcr_text) if pcr_text else '&nbsp;'}</div></td>"
        rows_html += dual_cell(fmt_main("PE_LTP", row["PE_LTP"]), pe_ltp_pct, "oc-orange")
        rows_html += dual_cell(fmt_main("PE_VOL_FMT", row["PE_VOL_FMT"]))
        rows_html += dual_cell(fmt_main("PE_OI_LAKHS", row["PE_OI_LAKHS"]), pe_oi_pct, "oc-green")
        rows_html += dual_cell(fmt_main("PE_CHG_OI", row["PE_CHG_OI"]))
        rows_html += dual_cell(fmt_main("PE_DELTA", row["PE_DELTA"]))
        rows_html += dual_cell(fmt_main("PE_THETA", row["PE_THETA"]))
        rows_html += dual_cell(fmt_main("PE_GAMMA", row["PE_GAMMA"]))
        rows_html += dual_cell(fmt_main("PE_IV", row["PE_IV"]))
        rows_html += "</tr>"

    return f"""
    <div class="oc-shell">
        <div class="oc-wrap" style="height:{TABLE_HEIGHT_PX}px; max-height:{TABLE_HEIGHT_PX}px;">
            <table class="oc-table">
                <thead>{header_html}</thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>
    """


def refresh_index_snapshot(*, symbol: str, instrument: str, expiry: str) -> None:
    token = get_access_token()
    if not token:
        publish_snapshot(
            symbol=symbol, instrument=instrument, expiry=expiry,
            df=pd.DataFrame(), spot=None, pcr=None, fetch_error="Access token not found. Please run login.py first."
        )
        return

    df, keys, spot = fetch_chain(token, instrument, expiry)
    if df.empty:
        publish_snapshot(
            symbol=symbol, instrument=instrument, expiry=expiry,
            df=pd.DataFrame(), spot=None, pcr=None, fetch_error=f"No {symbol} option chain data received."
        )
        return

    init_ws_for_index(token, symbol, instrument, expiry, keys)

    open_, high, low, prev_close = get_index_ohlc(token, instrument)
    total_call = df["CE_OI"].fillna(0).sum()
    total_put = df["PE_OI"].fillna(0).sum()
    pcr = (total_put / total_call) if total_call else None

    publish_snapshot(
        symbol=symbol,
        instrument=instrument,
        expiry=expiry,
        df=df,
        spot=spot,
        pcr=pcr,
        open_=open_,
        high=high,
        low=low,
        prev_close=prev_close,
        subscribed_keys=keys,
    )


def render_option_chain_page(*, title: str, symbol: str, instrument: str, expiry: str) -> None:
    inject_page_style()
    ensure_index_state(symbol, instrument, expiry)

    st.markdown(f'<div class="main-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="sub-title">Expiry: {expiry} | Instrument: {instrument}</div>',
        unsafe_allow_html=True,
    )

    data = st.session_state.get("live_option_data", {}).get(symbol.upper())
    if not data or data.get("df") is None or data["df"].empty:
        try:
            refresh_index_snapshot(symbol=symbol, instrument=instrument, expiry=expiry)
        except Exception as exc:
            publish_snapshot(
                symbol=symbol, instrument=instrument, expiry=expiry,
                df=pd.DataFrame(), spot=None, pcr=None, fetch_error=f"Initial load error: {exc}"
            )

    top_cols = st.columns(6)
    spot_ph, pcr_ph, open_ph, high_ph, low_ph, prev_close_ph = [c.empty() for c in top_cols]
    st.divider()
    table_ph = st.empty()
    st.divider()
    info_cols = st.columns(4)
    upd_ph, rows_ph, instr_ph, err_ph = [c.empty() for c in info_cols]

    def paint() -> None:
        snap = st.session_state.get("live_option_data", {}).get(symbol.upper(), {})
        df = snap.get("df", pd.DataFrame())
        spot = snap.get("spot")
        pcr = snap.get("pcr")
        open_ = snap.get("open")
        high = snap.get("high")
        low = snap.get("low")
        prev_close = snap.get("prev_close")
        last_update = snap.get("last_update")
        fetch_error = snap.get("fetch_error")

        spot_ph.metric("Spot", fmt_num(spot, 2))
        pcr_ph.metric("PCR", fmt_num(pcr, 2))
        open_ph.metric("Open", fmt_num(open_, 2))
        high_ph.metric("High", fmt_num(high, 2))
        low_ph.metric("Low", fmt_num(low, 2))
        prev_close_ph.metric("Prev Close", fmt_num(prev_close, 2))

        if df is not None and not df.empty:
            display_df, nearest_idx = build_display_df(df, spot)
            table_ph.markdown(render_option_chain_html(display_df, nearest_idx), unsafe_allow_html=True)
        else:
            table_ph.warning(f"No {symbol} option chain data available.")

        upd_ph.metric("Last Update", last_update.strftime("%H:%M:%S") if last_update else "-")
        rows_ph.metric("Strikes", fmt_int(len(df) if df is not None else 0))
        instr_ph.metric("Symbol", symbol.upper())
        err_ph.metric("WS Keys", fmt_int(len(snap.get("subscribed_keys", []))))

        if fetch_error:
            st.error(fetch_error)

    paint()

    @st.fragment(run_every=f"{REFRESH_SECONDS}s")
    def live_updater() -> None:
        try:
            refresh_index_snapshot(symbol=symbol, instrument=instrument, expiry=expiry)
        except Exception as exc:
            publish_snapshot(
                symbol=symbol, instrument=instrument, expiry=expiry,
                df=pd.DataFrame(), spot=None, pcr=None, fetch_error=f"Live update error: {exc}"
            )
        paint()

    live_updater()
