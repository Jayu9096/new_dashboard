from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st


BUFFER_SIZE = 300


@dataclass
class IndexState:
    symbol: str
    instrument: str
    expiry: str
    latest_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    latest_spot: float | None = None
    latest_pcr: float | None = None
    latest_open: float | None = None
    latest_high: float | None = None
    latest_low: float | None = None
    latest_prev_close: float | None = None
    fetch_error: str | None = None
    last_update: datetime | None = None
    last_keys: list[str] = field(default_factory=list)
    buffer: deque = field(default_factory=lambda: deque(maxlen=BUFFER_SIZE))


def _state_key(symbol: str) -> str:
    return f"index_state_{symbol.upper()}"


def ensure_index_state(symbol: str, instrument: str, expiry: str) -> IndexState:
    key = _state_key(symbol)
    if key not in st.session_state:
        st.session_state[key] = IndexState(
            symbol=symbol.upper(),
            instrument=instrument,
            expiry=expiry,
        )
    return st.session_state[key]


def get_index_state(symbol: str) -> IndexState | None:
    return st.session_state.get(_state_key(symbol.upper()))


def publish_snapshot(
    *,
    symbol: str,
    instrument: str,
    expiry: str,
    df: pd.DataFrame,
    spot: float | None,
    pcr: float | None,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
    prev_close: float | None = None,
    fetch_error: str | None = None,
    subscribed_keys: list[str] | None = None,
) -> None:
    state = ensure_index_state(symbol, instrument, expiry)

    state.latest_df = df.copy() if df is not None else pd.DataFrame()
    state.latest_spot = spot
    state.latest_pcr = pcr
    state.latest_open = open_
    state.latest_high = high
    state.latest_low = low
    state.latest_prev_close = prev_close
    state.fetch_error = fetch_error
    state.last_update = datetime.now()
    state.last_keys = list(subscribed_keys or [])

    snapshot = {
        "ts": state.last_update,
        "df": state.latest_df.copy(),
        "spot": spot,
        "pcr": pcr,
        "open": open_,
        "high": high,
        "low": low,
        "prev_close": prev_close,
    }
    state.buffer.append(snapshot)

    prefix = symbol.lower()

    # explicit symbol-specific keys
    st.session_state[f"{prefix}_latest_df"] = state.latest_df
    st.session_state[f"{prefix}_latest_spot"] = spot
    st.session_state[f"{prefix}_latest_pcr"] = pcr
    st.session_state[f"{prefix}_latest_open"] = open_
    st.session_state[f"{prefix}_latest_high"] = high
    st.session_state[f"{prefix}_latest_low"] = low
    st.session_state[f"{prefix}_latest_prev_close"] = prev_close
    st.session_state[f"{prefix}_fetch_error"] = fetch_error
    st.session_state[f"{prefix}_last_update"] = state.last_update

    # compatibility aliases
    st.session_state[f"latest_df_{prefix}"] = state.latest_df
    st.session_state[f"latest_spot_{prefix}"] = spot
    st.session_state[f"latest_pcr_{prefix}"] = pcr

    # shared indexed container
    live_option_data = st.session_state.setdefault("live_option_data", {})
    live_option_data[symbol.upper()] = {
        "df": state.latest_df,
        "spot": spot,
        "pcr": pcr,
        "open": open_,
        "high": high,
        "low": low,
        "prev_close": prev_close,
        "fetch_error": fetch_error,
        "last_update": state.last_update,
        "instrument": instrument,
        "expiry": expiry,
        "buffer_size": len(state.buffer),
        "subscribed_keys": list(subscribed_keys or []),
    }

    # generic keys only if single-index mode; never overwrite if another symbol already active
    owner_key = "generic_latest_owner"
    owner = st.session_state.get(owner_key)
    if owner in (None, symbol.upper()):
        st.session_state[owner_key] = symbol.upper()
        st.session_state["latest_df"] = state.latest_df
        st.session_state["latest_spot"] = spot
        st.session_state["latest_pcr"] = pcr


def buffer_as_list(symbol: str) -> list[dict[str, Any]]:
    state = get_index_state(symbol)
    if not state:
        return []
    return list(state.buffer)
