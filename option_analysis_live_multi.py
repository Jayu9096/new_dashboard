# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import streamlit as st

from shared_market_state import buffer_as_list


FAST_REFRESH_SECONDS = 1
SLOW_REFRESH_SECONDS = 5
MAX_ALERT_HISTORY = 50


APP_CSS = """
<style>
.block-container { padding-top: 0.35rem; padding-bottom: 0.50rem; padding-left: 0.70rem; padding-right: 0.70rem; max-width: 100% !important; }
.page-title { font-size: 22px; font-weight: 700; color: #111827; margin-bottom: 2px; }
.page-subtitle { font-size: 12px; color: #6b7280; margin-bottom: 10px; }
.section-title { font-size: 15px; font-weight: 700; color: #111827; margin-bottom: 8px; margin-top: 2px; }
.panel-box { border: 1px solid #e5e7eb; border-radius: 14px; padding: 12px; background: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.04); margin-bottom: 10px; }
.symbol-head { font-size: 16px; font-weight: 700; color: #111827; margin-bottom: 2px; }
.symbol-sub { font-size: 11px; color: #6b7280; margin-bottom: 10px; }
.metric-card { border: 1px solid rgba(128,128,128,0.12); border-radius: 12px; padding: 10px 10px; min-height: 84px; background: #fafafa; }
.metric-label { font-size: 11px; color: #6b7280; margin-bottom: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.metric-value { font-size: 18px; font-weight: 700; line-height: 1.15; color: #111827; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.metric-delta { font-size: 11px; margin-top: 5px; color: #6b7280; min-height: 16px; }
.decision-box { border: 1px solid #dbeafe; border-radius: 12px; padding: 12px; background: #f8fbff; min-height: 240px; color: #111827; }
.analysis-head { font-size: 14px; font-weight: 700; margin-bottom: 8px; color: #111827; }
.small-note { color: #6b7280; font-size: 11px; margin-top: 8px; line-height: 1.5; }
.status-chip-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
.status-chip { border: 1px solid #e5e7eb; background: #f9fafb; color: #374151; border-radius: 999px; padding: 4px 10px; font-size: 11px; font-weight: 600; }
.signal-pill { display: inline-block; border-radius: 999px; padding: 4px 10px; font-size: 11px; font-weight: 700; margin-right: 6px; margin-bottom: 6px; border: 1px solid #d1d5db; background: #fff; }
div[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
</style>
"""


def fmt_num(value: Any, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return str(value)


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{int(round(float(value))):,}"
    except Exception:
        return str(value)


def fmt_pct(value: Any, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        val = float(value)
        sign = "+" if val > 0 else ""
        return f"{sign}{val:.{decimals}f}%"
    except Exception:
        return str(value)


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def fmt_ts(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)


def get_col(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(default)
    return pd.Series([default] * len(df), index=df.index, dtype="float64")


def find_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def classify_change(value: Optional[float], positive_text: str, negative_text: str) -> str:
    if value is None:
        return "No comparison data"
    if value > 0:
        return positive_text
    if value < 0:
        return negative_text
    return "Flat"


def render_levels(levels: list[float]) -> str:
    if not levels:
        return "-"
    return ", ".join(fmt_num(x, 0) for x in levels[:3])


def normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).upper() for c in out.columns]
    for col in out.columns:
        if col != "TIMESTAMP":
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def get_support_resistance(df: pd.DataFrame, spot: Optional[float]):
    if df.empty or "STRIKE" not in df.columns:
        return None, None, [], []
    tmp = df.copy()
    tmp["STRIKE"] = pd.to_numeric(tmp["STRIKE"], errors="coerce")
    tmp["CE_OI"] = get_col(tmp, "CE_OI")
    tmp["PE_OI"] = get_col(tmp, "PE_OI")
    tmp = tmp.dropna(subset=["STRIKE"])
    if tmp.empty:
        return None, None, [], []
    supports_df = tmp[tmp["STRIKE"] <= (spot if spot is not None else float("inf"))].sort_values(["PE_OI", "STRIKE"], ascending=[False, False])
    resist_df = tmp[tmp["STRIKE"] >= (spot if spot is not None else float("-inf"))].sort_values(["CE_OI", "STRIKE"], ascending=[False, True])
    support_levels = supports_df["STRIKE"].head(3).astype(float).tolist()
    resistance_levels = resist_df["STRIKE"].head(3).astype(float).tolist()
    nearest_support = min(support_levels, key=lambda x: abs((spot or x) - x)) if support_levels else None
    nearest_resistance = min(resistance_levels, key=lambda x: abs((spot or x) - x)) if resistance_levels else None
    return nearest_support, nearest_resistance, support_levels, resistance_levels


@dataclass
class SnapshotAnalysis:
    symbol: str
    latest_ts: Optional[str]
    oldest_ts: Optional[str]
    prev_ts: Optional[str]
    snapshot_count: int
    snapshot_id: int
    latest_df: pd.DataFrame
    oldest_df: pd.DataFrame
    latest_merged_df: pd.DataFrame
    spot: Optional[float]
    spot_change: Optional[float]
    spot_change_pct: Optional[float]
    oi_pcr: Optional[float]
    vol_pcr: Optional[float]
    ce_oi_total: float
    pe_oi_total: float
    ce_vol_total: float
    pe_vol_total: float
    ce_oi_change: Optional[float]
    pe_oi_change: Optional[float]
    ce_vol_change: Optional[float]
    pe_vol_change: Optional[float]
    net_oi_change: Optional[float]
    call_oi_bias: str
    put_oi_bias: str
    volume_bias: str
    oi_direction: str
    oi_direction_detail: str
    trend: str
    trend_score: int
    trend_reason: str
    trap_signal: str
    trap_reason: str
    near_atm_call_oi_change: Optional[float]
    near_atm_put_oi_change: Optional[float]
    nearest_support: Optional[float]
    nearest_resistance: Optional[float]
    support_levels: list[float]
    resistance_levels: list[float]
    call_side_signal: str
    put_side_signal: str
    call_writing_pressure: Optional[float]
    put_writing_pressure: Optional[float]
    writing_bias: str
    ce_iv_avg: Optional[float]
    pe_iv_avg: Optional[float]
    ce_iv_change: Optional[float]
    pe_iv_change: Optional[float]
    iv_bias: str
    iv_signal: str
    ce_money_flow: Optional[float]
    pe_money_flow: Optional[float]
    ce_near_money_flow: Optional[float]
    pe_near_money_flow: Optional[float]
    money_flow_bias: str
    opportunity_tag: str
    buyer_signal: str
    buyer_confidence: str
    entry_zone: str
    stop_reference: str
    target_zone: str
    agent_reason: str
    agent_warning: str
    top_call_oi_added: pd.DataFrame
    top_put_oi_added: pd.DataFrame
    top_call_oi_unwound: pd.DataFrame
    top_put_oi_unwound: pd.DataFrame


def detect_call_side_signal(spot_change, ce_oi_change, ce_vol_change) -> str:
    if ce_oi_change is None or ce_vol_change is None:
        return "No comparison data"
    if ce_vol_change <= 0 and ce_oi_change <= 0:
        return "Call activity weak"
    if ce_vol_change > 0 and ce_oi_change > 0 and (spot_change or 0) > 0:
        return "Call Buying"
    if ce_vol_change > 0 and ce_oi_change > 0 and (spot_change or 0) <= 0:
        return "Call Writing"
    if ce_vol_change > 0 and ce_oi_change < 0 and (spot_change or 0) > 0:
        return "Call Short Covering"
    if ce_vol_change > 0 and ce_oi_change < 0 and (spot_change or 0) <= 0:
        return "Call Unwinding"
    return "Mixed Call Activity"


def detect_put_side_signal(spot_change, pe_oi_change, pe_vol_change) -> str:
    if pe_oi_change is None or pe_vol_change is None:
        return "No comparison data"
    if pe_vol_change <= 0 and pe_oi_change <= 0:
        return "Put activity weak"
    if pe_vol_change > 0 and pe_oi_change > 0 and (spot_change or 0) < 0:
        return "Put Buying"
    if pe_vol_change > 0 and pe_oi_change > 0 and (spot_change or 0) >= 0:
        return "Put Writing"
    if pe_vol_change > 0 and pe_oi_change < 0 and (spot_change or 0) < 0:
        return "Put Short Covering"
    if pe_vol_change > 0 and pe_oi_change < 0 and (spot_change or 0) >= 0:
        return "Put Unwinding"
    return "Mixed Put Activity"


def derive_oi_direction(ce_oi_change, pe_oi_change):
    if ce_oi_change is None or pe_oi_change is None:
        return "No comparison data", "Total OI comparison not available", None
    net_oi_change = pe_oi_change - ce_oi_change
    if ce_oi_change > 0 and pe_oi_change > 0:
        if pe_oi_change > ce_oi_change:
            return "Bullish Bias", "Put OI increasing faster than Call OI", net_oi_change
        if ce_oi_change > pe_oi_change:
            return "Bearish Bias", "Call OI increasing faster than Put OI", net_oi_change
        return "Balanced Build-up", "Call OI and Put OI increasing equally", net_oi_change
    if ce_oi_change < 0 and pe_oi_change < 0:
        if abs(pe_oi_change) > abs(ce_oi_change):
            return "Bullish Unwinding", "Put side unwinding faster than Call side", net_oi_change
        if abs(ce_oi_change) > abs(pe_oi_change):
            return "Bearish Unwinding", "Call side unwinding faster than Put side", net_oi_change
        return "Balanced Unwinding", "Call OI and Put OI decreasing together", net_oi_change
    if ce_oi_change > 0 and pe_oi_change < 0:
        return "Strong Bearish", "Call OI increasing while Put OI decreasing", net_oi_change
    if ce_oi_change < 0 and pe_oi_change > 0:
        return "Strong Bullish", "Put OI increasing while Call OI decreasing", net_oi_change
    return "Neutral", "No strong OI direction", net_oi_change


def detect_trap_signal(spot_change_pct, ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change, near_atm_call_oi_change, near_atm_put_oi_change, oi_pcr, vol_pcr):
    if spot_change_pct is None or ce_oi_change is None or pe_oi_change is None or ce_vol_change is None or pe_vol_change is None:
        return "No clear trap", "Insufficient comparison data"
    if spot_change_pct > 0.20 and ce_oi_change > pe_oi_change and (near_atm_call_oi_change or 0) > (near_atm_put_oi_change or 0):
        return "Bull Trap", "Price is rising but call-side build-up is stronger"
    if spot_change_pct < -0.20 and pe_oi_change > ce_oi_change and (near_atm_put_oi_change or 0) > (near_atm_call_oi_change or 0):
        return "Bear Trap", "Price is falling but put-side build-up is stronger"
    return "No clear trap", "No strong trap pattern in the live memory window"


def build_trend(spot_change_pct, oi_pcr, vol_pcr, ce_oi_change, pe_oi_change, near_atm_call_oi_change, near_atm_put_oi_change):
    score = 0
    reasons = []
    if spot_change_pct is not None:
        if spot_change_pct >= 0.50:
            score += 3; reasons.append("spot strongly up")
        elif spot_change_pct >= 0.20:
            score += 2; reasons.append("spot up")
        elif spot_change_pct > 0:
            score += 1; reasons.append("spot mildly up")
        elif spot_change_pct <= -0.50:
            score -= 3; reasons.append("spot strongly down")
        elif spot_change_pct <= -0.20:
            score -= 2; reasons.append("spot down")
        elif spot_change_pct < 0:
            score -= 1; reasons.append("spot mildly down")
    if oi_pcr is not None:
        if oi_pcr >= 1.20:
            score += 2; reasons.append("OI PCR bullish")
        elif oi_pcr >= 1.00:
            score += 1; reasons.append("OI PCR supportive")
        elif oi_pcr <= 0.80:
            score -= 2; reasons.append("OI PCR bearish")
        elif oi_pcr < 1.00:
            score -= 1; reasons.append("OI PCR weak")
    if vol_pcr is not None:
        if vol_pcr >= 1.10:
            score += 1; reasons.append("put volume stronger")
        elif vol_pcr <= 0.90:
            score -= 1; reasons.append("call volume stronger")
    if ce_oi_change is not None and pe_oi_change is not None:
        net_oi_bias = pe_oi_change - ce_oi_change
        if net_oi_bias > 0:
            score += 2 if abs(net_oi_bias) > max(abs(ce_oi_change), abs(pe_oi_change), 1) * 0.25 else 1
            reasons.append("put OI built faster")
        elif net_oi_bias < 0:
            score -= 2 if abs(net_oi_bias) > max(abs(ce_oi_change), abs(pe_oi_change), 1) * 0.25 else 1
            reasons.append("call OI built faster")
    if near_atm_call_oi_change is not None and near_atm_put_oi_change is not None:
        if near_atm_put_oi_change > near_atm_call_oi_change:
            score += 2; reasons.append("near ATM put side stronger")
        elif near_atm_call_oi_change > near_atm_put_oi_change:
            score -= 2; reasons.append("near ATM call side stronger")
    if score >= 7: trend = "Strong Uptrend"
    elif score >= 3: trend = "Uptrend"
    elif score <= -7: trend = "Strong Downtrend"
    elif score <= -3: trend = "Downtrend"
    else: trend = "Neutral"
    return trend, score, ", ".join(reasons) if reasons else "insufficient comparison data"


def detect_iv_context(latest_df, oldest_df):
    ce_iv_avg = safe_float(get_col(latest_df, "CE_IV").mean()) if "CE_IV" in latest_df.columns else None
    pe_iv_avg = safe_float(get_col(latest_df, "PE_IV").mean()) if "PE_IV" in latest_df.columns else None
    old_ce = safe_float(get_col(oldest_df, "CE_IV").mean()) if "CE_IV" in oldest_df.columns else None
    old_pe = safe_float(get_col(oldest_df, "PE_IV").mean()) if "PE_IV" in oldest_df.columns else None
    ce_iv_change = None if ce_iv_avg is None or old_ce is None else ce_iv_avg - old_ce
    pe_iv_change = None if pe_iv_avg is None or old_pe is None else pe_iv_avg - old_pe
    if ce_iv_avg is None or pe_iv_avg is None:
        iv_bias = "IV unavailable"
    elif pe_iv_avg > ce_iv_avg + 1:
        iv_bias = "PE IV higher"
    elif ce_iv_avg > pe_iv_avg + 1:
        iv_bias = "CE IV higher"
    else:
        iv_bias = "IV balanced"
    net_iv = (pe_iv_change or 0) - (ce_iv_change or 0)
    if abs(ce_iv_change or 0) < 0.25 and abs(pe_iv_change or 0) < 0.25:
        iv_signal = "IV stable"
    elif net_iv > 0.5:
        iv_signal = "Put IV expanding"
    elif net_iv < -0.5:
        iv_signal = "Call IV expanding"
    elif (ce_iv_change or 0) > 0 and (pe_iv_change or 0) > 0:
        iv_signal = "Broad IV expansion"
    elif (ce_iv_change or 0) < 0 and (pe_iv_change or 0) < 0:
        iv_signal = "Broad IV crush"
    else:
        iv_signal = "Mixed IV shift"
    return ce_iv_avg, pe_iv_avg, ce_iv_change, pe_iv_change, iv_bias, iv_signal


def detect_money_flow(latest_df, oldest_df, spot):
    latest = pd.DataFrame({
        "STRIKE": get_col(latest_df, "STRIKE"),
        "CE_LTP_NEW": get_col(latest_df, "CE_LTP"),
        "PE_LTP_NEW": get_col(latest_df, "PE_LTP"),
        "CE_VOLUME_NEW": get_col(latest_df, "CE_VOLUME"),
        "PE_VOLUME_NEW": get_col(latest_df, "PE_VOLUME"),
    })
    oldest = pd.DataFrame({
        "STRIKE": get_col(oldest_df, "STRIKE"),
        "CE_LTP_OLD": get_col(oldest_df, "CE_LTP"),
        "PE_LTP_OLD": get_col(oldest_df, "PE_LTP"),
        "CE_VOLUME_OLD": get_col(oldest_df, "CE_VOLUME"),
        "PE_VOLUME_OLD": get_col(oldest_df, "PE_VOLUME"),
    })
    merged = pd.merge(latest, oldest, on="STRIKE", how="outer").fillna(0)
    merged["CE_PREMIUM_FLOW"] = (merged["CE_LTP_NEW"] * merged["CE_VOLUME_NEW"]) - (merged["CE_LTP_OLD"] * merged["CE_VOLUME_OLD"])
    merged["PE_PREMIUM_FLOW"] = (merged["PE_LTP_NEW"] * merged["PE_VOLUME_NEW"]) - (merged["PE_LTP_OLD"] * merged["PE_VOLUME_OLD"])
    ce_money_flow = safe_float(merged["CE_PREMIUM_FLOW"].sum())
    pe_money_flow = safe_float(merged["PE_PREMIUM_FLOW"].sum())
    ce_near_money_flow = None
    pe_near_money_flow = None
    if spot is not None and not merged.empty:
        tmp = merged.copy()
        tmp["DIST"] = (pd.to_numeric(tmp["STRIKE"], errors="coerce") - spot).abs()
        tmp = tmp.sort_values("DIST", ascending=True).head(5)
        ce_near_money_flow = safe_float(tmp["CE_PREMIUM_FLOW"].sum())
        pe_near_money_flow = safe_float(tmp["PE_PREMIUM_FLOW"].sum())
    if (pe_money_flow or 0) > (ce_money_flow or 0):
        bias = "Put premium inflow stronger"
    elif (ce_money_flow or 0) > (pe_money_flow or 0):
        bias = "Call premium inflow stronger"
    else:
        bias = "Premium flow balanced"
    return ce_money_flow, pe_money_flow, ce_near_money_flow, pe_near_money_flow, bias


def derive_opportunity_tag(trend, trap_signal, iv_signal, money_flow_bias, writing_bias):
    if trap_signal != "No clear trap":
        return "Trap Risk"
    bullish = trend in {"Strong Uptrend", "Uptrend"} and money_flow_bias == "Call premium inflow stronger" and writing_bias == "PUT SIDE"
    bearish = trend in {"Strong Downtrend", "Downtrend"} and money_flow_bias == "Put premium inflow stronger" and writing_bias == "CALL SIDE"
    if bullish and iv_signal in {"Call IV expanding", "Broad IV expansion"}:
        return "Aggressive CE Opportunity"
    if bearish and iv_signal in {"Put IV expanding", "Broad IV expansion"}:
        return "Aggressive PE Opportunity"
    if bullish:
        return "CE Watchlist Setup"
    if bearish:
        return "PE Watchlist Setup"
    if iv_signal == "Broad IV crush":
        return "Avoid option buying"
    return "No strong opportunity"


def derive_agent_signal(trend, trap_signal, nearest_support, nearest_resistance, spot, call_side_signal, put_side_signal, call_writing_pressure, put_writing_pressure, oi_pcr, vol_pcr, iv_signal, money_flow_bias, ce_near_money_flow, pe_near_money_flow):
    call_wp = call_writing_pressure or 0.0
    put_wp = put_writing_pressure or 0.0
    ce_nf = ce_near_money_flow or 0.0
    pe_nf = pe_near_money_flow or 0.0
    support_gap = ((spot - nearest_support) / spot) * 100 if spot and nearest_support is not None else None
    resistance_gap = ((nearest_resistance - spot) / spot) * 100 if spot and nearest_resistance is not None else None
    score_ce = 0
    score_pe = 0
    reasons = []
    warning = "Normal risk."
    if trap_signal != "No clear trap":
        score_ce -= 3; score_pe -= 3; warning = f"Trap warning: {trap_signal}"
    if trend == "Strong Uptrend":
        score_ce += 4; reasons.append("strong uptrend")
    elif trend == "Uptrend":
        score_ce += 2; reasons.append("uptrend")
    elif trend == "Strong Downtrend":
        score_pe += 4; reasons.append("strong downtrend")
    elif trend == "Downtrend":
        score_pe += 2; reasons.append("downtrend")
    if put_wp > call_wp:
        score_ce += 2; reasons.append("put writing stronger")
    elif call_wp > put_wp:
        score_pe += 2; reasons.append("call writing stronger")
    if call_side_signal in {"Call Buying", "Call Short Covering"}:
        score_ce += 2; reasons.append(call_side_signal.lower())
    if put_side_signal in {"Put Buying", "Put Short Covering"}:
        score_pe += 2; reasons.append(put_side_signal.lower())
    if money_flow_bias == "Call premium inflow stronger":
        score_ce += 2; reasons.append("call premium inflow stronger")
    elif money_flow_bias == "Put premium inflow stronger":
        score_pe += 2; reasons.append("put premium inflow stronger")
    if ce_nf > pe_nf:
        score_ce += 2; reasons.append("near ATM CE flow stronger")
    elif pe_nf > ce_nf:
        score_pe += 2; reasons.append("near ATM PE flow stronger")
    if iv_signal in {"Call IV expanding", "Broad IV expansion"}:
        score_ce += 1; reasons.append(iv_signal.lower())
    if iv_signal in {"Put IV expanding", "Broad IV expansion"}:
        score_pe += 1
    if oi_pcr is not None:
        if oi_pcr >= 1.10: score_ce += 1
        elif oi_pcr <= 0.90: score_pe += 1
    if vol_pcr is not None:
        if vol_pcr >= 1.05 and trend in {"Strong Uptrend", "Uptrend"}: score_ce += 1
        elif vol_pcr <= 0.95 and trend in {"Strong Downtrend", "Downtrend"}: score_pe += 1
    if support_gap is not None and support_gap <= 0.70: score_ce += 1
    if resistance_gap is not None and resistance_gap <= 0.70: score_pe += 1
    if iv_signal == "Broad IV crush":
        score_ce -= 3; score_pe -= 3; warning = "IV crush risk: option buying may underperform."
    if score_ce >= 7 and score_ce > score_pe + 1:
        signal = "BUY CE"; entry = f"ATM to slight ITM near {fmt_num(spot, 0)} / support {fmt_num(nearest_support, 0)}"; stop = f"Below support {fmt_num(nearest_support, 0)}"; target = f"Towards resistance {fmt_num(nearest_resistance, 0)}"; confidence_raw = score_ce
    elif score_pe >= 7 and score_pe > score_ce + 1:
        signal = "BUY PE"; entry = f"ATM to slight ITM near {fmt_num(spot, 0)} / resistance {fmt_num(nearest_resistance, 0)}"; stop = f"Above resistance {fmt_num(nearest_resistance, 0)}"; target = f"Towards support {fmt_num(nearest_support, 0)}"; confidence_raw = score_pe
    elif score_ce >= 5 and score_ce > score_pe:
        signal = "CE WATCH"; entry = f"Watch breakout continuation above {fmt_num(spot, 0)}"; stop = "Enter only on confirmation"; target = f"Resistance {fmt_num(nearest_resistance, 0)}"; confidence_raw = score_ce
    elif score_pe >= 5 and score_pe > score_ce:
        signal = "PE WATCH"; entry = f"Watch breakdown continuation below {fmt_num(spot, 0)}"; stop = "Enter only on confirmation"; target = f"Support {fmt_num(nearest_support, 0)}"; confidence_raw = score_pe
    else:
        signal = "WAIT"; entry = "No clean buyer edge"; stop = "Stay light"; target = "Wait for alignment"; confidence_raw = max(score_ce, score_pe)
    confidence = "High" if confidence_raw >= 8 else "Medium" if confidence_raw >= 6 else "Low"
    return signal, confidence, entry, stop, target, ", ".join(reasons[:8]) if reasons else "mixed structure", warning


def build_memory_snapshot_analysis(symbol: str) -> Optional[SnapshotAnalysis]:
    snapshots = buffer_as_list(symbol)
    if not snapshots:
        return None
    snapshots = [s for s in snapshots if isinstance(s, dict) and isinstance(s.get("df"), pd.DataFrame)]
    if not snapshots:
        return None
    snapshots = sorted(snapshots, key=lambda x: str(x.get("ts") or ""))
    latest_snap = snapshots[-1]
    oldest_snap = snapshots[0]
    prev_snap = snapshots[-2] if len(snapshots) > 1 else None
    latest_ts = str(latest_snap.get("ts")) if latest_snap.get("ts") else None
    oldest_ts = str(oldest_snap.get("ts")) if oldest_snap.get("ts") else None
    prev_ts = str(prev_snap.get("ts")) if prev_snap and prev_snap.get("ts") else None
    snapshot_id = len(snapshots)

    latest_df = normalize_snapshot_df(latest_snap.get("df", pd.DataFrame()))
    oldest_df = normalize_snapshot_df(oldest_snap.get("df", pd.DataFrame()))
    if latest_df.empty or oldest_df.empty:
        return None

    spot = safe_float(latest_snap.get("spot"))
    oldest_spot = safe_float(oldest_snap.get("spot"))
    if spot is None:
        c = find_first_existing_col(latest_df, ["SPOT", "UNDERLYING_SPOT_PRICE"])
        if c and latest_df[c].dropna().size:
            spot = safe_float(latest_df[c].dropna().iloc[0])
    if oldest_spot is None:
        c = find_first_existing_col(oldest_df, ["SPOT", "UNDERLYING_SPOT_PRICE"])
        if c and oldest_df[c].dropna().size:
            oldest_spot = safe_float(oldest_df[c].dropna().iloc[0])

    spot_change = None
    spot_change_pct = None
    if spot is not None and oldest_spot is not None and oldest_spot != 0:
        spot_change = spot - oldest_spot
        spot_change_pct = (spot_change / oldest_spot) * 100

    ce_oi_total = float(get_col(latest_df, "CE_OI").sum())
    pe_oi_total = float(get_col(latest_df, "PE_OI").sum())
    ce_vol_total = float(get_col(latest_df, "CE_VOLUME").sum())
    pe_vol_total = float(get_col(latest_df, "PE_VOLUME").sum())
    old_ce_oi_total = float(get_col(oldest_df, "CE_OI").sum())
    old_pe_oi_total = float(get_col(oldest_df, "PE_OI").sum())
    old_ce_vol_total = float(get_col(oldest_df, "CE_VOLUME").sum())
    old_pe_vol_total = float(get_col(oldest_df, "PE_VOLUME").sum())

    oi_pcr = (pe_oi_total / ce_oi_total) if ce_oi_total else None
    vol_pcr = (pe_vol_total / ce_vol_total) if ce_vol_total else None
    ce_oi_change = ce_oi_total - old_ce_oi_total
    pe_oi_change = pe_oi_total - old_pe_oi_total
    ce_vol_change = ce_vol_total - old_ce_vol_total
    pe_vol_change = pe_vol_total - old_pe_vol_total

    latest_merge = pd.DataFrame({"STRIKE": get_col(latest_df, "STRIKE"), "CE_OI_NEW": get_col(latest_df, "CE_OI"), "PE_OI_NEW": get_col(latest_df, "PE_OI"), "CE_VOLUME_NEW": get_col(latest_df, "CE_VOLUME"), "PE_VOLUME_NEW": get_col(latest_df, "PE_VOLUME")})
    oldest_merge = pd.DataFrame({"STRIKE": get_col(oldest_df, "STRIKE"), "CE_OI_OLD": get_col(oldest_df, "CE_OI"), "PE_OI_OLD": get_col(oldest_df, "PE_OI"), "CE_VOLUME_OLD": get_col(oldest_df, "CE_VOLUME"), "PE_VOLUME_OLD": get_col(oldest_df, "PE_VOLUME")})
    merged = pd.merge(latest_merge, oldest_merge, on="STRIKE", how="outer").fillna(0)
    merged["CE_OI_DELTA"] = merged["CE_OI_NEW"] - merged["CE_OI_OLD"]
    merged["PE_OI_DELTA"] = merged["PE_OI_NEW"] - merged["PE_OI_OLD"]
    merged["CE_VOL_DELTA"] = merged["CE_VOLUME_NEW"] - merged["CE_VOLUME_OLD"]
    merged["PE_VOL_DELTA"] = merged["PE_VOLUME_NEW"] - merged["PE_VOLUME_OLD"]

    top_call_oi_added = merged.sort_values("CE_OI_DELTA", ascending=False)[["STRIKE", "CE_OI_DELTA"]].head(5)
    top_put_oi_added = merged.sort_values("PE_OI_DELTA", ascending=False)[["STRIKE", "PE_OI_DELTA"]].head(5)
    top_call_oi_unwound = merged.sort_values("CE_OI_DELTA", ascending=True)[["STRIKE", "CE_OI_DELTA"]].head(5)
    top_put_oi_unwound = merged.sort_values("PE_OI_DELTA", ascending=True)[["STRIKE", "PE_OI_DELTA"]].head(5)

    call_oi_bias = classify_change(ce_oi_change, "Call OI increasing", "Call OI decreasing")
    put_oi_bias = classify_change(pe_oi_change, "Put OI increasing", "Put OI decreasing")
    oi_direction, oi_direction_detail, net_oi_change = derive_oi_direction(ce_oi_change, pe_oi_change)
    volume_score = (pe_vol_change or 0) - (ce_vol_change or 0)
    volume_bias = "Put volume stronger" if volume_score > 0 else "Call volume stronger" if volume_score < 0 else "Volume balanced"

    nearest_support, nearest_resistance, support_levels, resistance_levels = get_support_resistance(latest_df, spot)
    near_atm_call_oi_change = None
    near_atm_put_oi_change = None
    if spot is not None and not merged.empty:
        tmp = merged.copy()
        tmp["DIST"] = (pd.to_numeric(tmp["STRIKE"], errors="coerce") - spot).abs()
        tmp = tmp.sort_values("DIST", ascending=True).head(5)
        near_atm_call_oi_change = safe_float(tmp["CE_OI_DELTA"].sum())
        near_atm_put_oi_change = safe_float(tmp["PE_OI_DELTA"].sum())

    trend, trend_score, trend_reason = build_trend(spot_change_pct, oi_pcr, vol_pcr, ce_oi_change, pe_oi_change, near_atm_call_oi_change, near_atm_put_oi_change)
    trap_signal, trap_reason = detect_trap_signal(spot_change_pct, ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change, near_atm_call_oi_change, near_atm_put_oi_change, oi_pcr, vol_pcr)
    call_side_signal = detect_call_side_signal(spot_change, ce_oi_change, ce_vol_change)
    put_side_signal = detect_put_side_signal(spot_change, pe_oi_change, pe_vol_change)
    call_writing_pressure = safe_float(merged[merged["CE_OI_DELTA"] > 0]["CE_OI_DELTA"].sum())
    put_writing_pressure = safe_float(merged[merged["PE_OI_DELTA"] > 0]["PE_OI_DELTA"].sum())
    writing_bias = "PUT SIDE" if (put_writing_pressure or 0) > (call_writing_pressure or 0) else "CALL SIDE" if (call_writing_pressure or 0) > (put_writing_pressure or 0) else "BALANCED"
    ce_iv_avg, pe_iv_avg, ce_iv_change, pe_iv_change, iv_bias, iv_signal = detect_iv_context(latest_df, oldest_df)
    ce_money_flow, pe_money_flow, ce_near_money_flow, pe_near_money_flow, money_flow_bias = detect_money_flow(latest_df, oldest_df, spot)
    opportunity_tag = derive_opportunity_tag(trend, trap_signal, iv_signal, money_flow_bias, writing_bias)
    buyer_signal, buyer_confidence, entry_zone, stop_reference, target_zone, agent_reason, agent_warning = derive_agent_signal(
        trend, trap_signal, nearest_support, nearest_resistance, spot, call_side_signal, put_side_signal, call_writing_pressure, put_writing_pressure, oi_pcr, vol_pcr, iv_signal, money_flow_bias, ce_near_money_flow, pe_near_money_flow
    )

    return SnapshotAnalysis(
        symbol=symbol, latest_ts=latest_ts, oldest_ts=oldest_ts, prev_ts=prev_ts, snapshot_count=len(snapshots), snapshot_id=snapshot_id,
        latest_df=latest_df, oldest_df=oldest_df, latest_merged_df=merged, spot=spot, spot_change=spot_change, spot_change_pct=spot_change_pct,
        oi_pcr=oi_pcr, vol_pcr=vol_pcr, ce_oi_total=ce_oi_total, pe_oi_total=pe_oi_total, ce_vol_total=ce_vol_total, pe_vol_total=pe_vol_total,
        ce_oi_change=ce_oi_change, pe_oi_change=pe_oi_change, ce_vol_change=ce_vol_change, pe_vol_change=pe_vol_change, net_oi_change=net_oi_change,
        call_oi_bias=call_oi_bias, put_oi_bias=put_oi_bias, volume_bias=volume_bias, oi_direction=oi_direction, oi_direction_detail=oi_direction_detail,
        trend=trend, trend_score=trend_score, trend_reason=trend_reason, trap_signal=trap_signal, trap_reason=trap_reason,
        near_atm_call_oi_change=near_atm_call_oi_change, near_atm_put_oi_change=near_atm_put_oi_change, nearest_support=nearest_support, nearest_resistance=nearest_resistance,
        support_levels=support_levels, resistance_levels=resistance_levels, call_side_signal=call_side_signal, put_side_signal=put_side_signal,
        call_writing_pressure=call_writing_pressure, put_writing_pressure=put_writing_pressure, writing_bias=writing_bias,
        ce_iv_avg=ce_iv_avg, pe_iv_avg=pe_iv_avg, ce_iv_change=ce_iv_change, pe_iv_change=pe_iv_change, iv_bias=iv_bias, iv_signal=iv_signal,
        ce_money_flow=ce_money_flow, pe_money_flow=pe_money_flow, ce_near_money_flow=ce_near_money_flow, pe_near_money_flow=pe_near_money_flow, money_flow_bias=money_flow_bias,
        opportunity_tag=opportunity_tag, buyer_signal=buyer_signal, buyer_confidence=buyer_confidence, entry_zone=entry_zone, stop_reference=stop_reference,
        target_zone=target_zone, agent_reason=agent_reason, agent_warning=agent_warning,
        top_call_oi_added=top_call_oi_added, top_put_oi_added=top_put_oi_added, top_call_oi_unwound=top_call_oi_unwound, top_put_oi_unwound=top_put_oi_unwound
    )


def init_state() -> None:
    defaults = {"NIFTY_analysis": None, "SENSEX_analysis": None, "alert_history": [], "NIFTY_last_alert_state": None, "SENSEX_last_alert_state": None, "ai_last_paint_ts": None}
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def build_alert_message(old: dict[str, Any], new: dict[str, Any], analysis: SnapshotAnalysis) -> Optional[str]:
    parts = []
    if old.get("trend") != new.get("trend"): parts.append(f"Trend changed from {old.get('trend', '-')} to {new.get('trend', '-')}")
    if old.get("buyer_signal") != new.get("buyer_signal"): parts.append(f"Agent signal changed from {old.get('buyer_signal', '-')} to {new.get('buyer_signal', '-')}")
    if old.get("opportunity_tag") != new.get("opportunity_tag"): parts.append(f"Opportunity changed from {old.get('opportunity_tag', '-')} to {new.get('opportunity_tag', '-')}")
    if parts:
        return f"{analysis.symbol}: " + " | ".join(parts) + f" | Spot: {fmt_num(analysis.spot)} | OI PCR: {fmt_num(analysis.oi_pcr)} | IV: {analysis.iv_signal}"
    return None


def push_alert(symbol: str, message: str, alert_type: str = "Trend Change") -> None:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history = st.session_state.get("alert_history", [])
    history.insert(0, {"Time": now_text, "Symbol": symbol, "Type": alert_type, "Message": message})
    st.session_state["alert_history"] = history[:MAX_ALERT_HISTORY]
    try:
        st.toast(f"{symbol}: {alert_type}", icon="🚨")
    except Exception:
        pass


def process_realtime_alerts(symbol: str, analysis: SnapshotAnalysis) -> None:
    state_key = f"{symbol}_last_alert_state"
    new_state = {"trend": analysis.trend, "buyer_signal": analysis.buyer_signal, "opportunity_tag": analysis.opportunity_tag, "latest_ts": analysis.latest_ts, "snapshot_id": analysis.snapshot_id}
    old_state = st.session_state.get(state_key)
    if old_state is None:
        st.session_state[state_key] = new_state
        return
    if old_state.get("latest_ts") == new_state.get("latest_ts") and old_state.get("snapshot_id") == new_state.get("snapshot_id"):
        return
    msg = build_alert_message(old_state, new_state, analysis)
    st.session_state[state_key] = new_state
    if msg: push_alert(symbol, msg, "AI Agent Alert")


def refresh_memory_analysis_cache(symbol: str) -> None:
    analysis_key = f"{symbol}_analysis"
    analysis = build_memory_snapshot_analysis(symbol)
    if analysis is None: return
    prev_analysis = st.session_state.get(analysis_key)
    st.session_state[analysis_key] = analysis
    if prev_analysis is None:
        process_realtime_alerts(symbol, analysis); return
    changed = prev_analysis.latest_ts != analysis.latest_ts or prev_analysis.snapshot_id != analysis.snapshot_id
    if changed: process_realtime_alerts(symbol, analysis)


def update_analysis_cache() -> None:
    refresh_memory_analysis_cache("NIFTY")
    refresh_memory_analysis_cache("SENSEX")


def metric_card_html(label: str, value: str, delta: str = "") -> str:
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div><div class="metric-delta">{delta or "&nbsp;"}</div></div>'


def draw_metric(ph, label: str, value: str, delta: str = "") -> None:
    ph.markdown(metric_card_html(label, value, delta), unsafe_allow_html=True)


def section_title(text: str) -> None:
    st.markdown(f'<div class="section-title">{text}</div>', unsafe_allow_html=True)


def show_table(df: pd.DataFrame, value_col: str, title: str) -> None:
    st.markdown(f"**{title}**")
    if df is None or df.empty:
        st.info("No data"); return
    show_df = df.copy()
    if "STRIKE" in show_df.columns: show_df["STRIKE"] = show_df["STRIKE"].apply(fmt_int)
    if value_col in show_df.columns: show_df[value_col] = show_df[value_col].apply(fmt_int)
    st.dataframe(show_df, use_container_width=True, hide_index=True)


def render_agent_box(analysis: SnapshotAnalysis) -> None:
    st.markdown(f'''
        <div class="decision-box">
            <div class="analysis-head">AI Agent Decision</div>
            <div>
                <span class="signal-pill">Signal: {analysis.buyer_signal}</span>
                <span class="signal-pill">Confidence: {analysis.buyer_confidence}</span>
                <span class="signal-pill">Opportunity: {analysis.opportunity_tag}</span>
            </div>
            <div style="margin-top:8px;"><b>Entry Zone:</b> {analysis.entry_zone}</div>
            <div style="margin-top:8px;"><b>Target Zone:</b> {analysis.target_zone}</div>
            <div style="margin-top:8px;"><b>Stop Reference:</b> {analysis.stop_reference}</div>
            <div style="margin-top:8px;"><b>Why:</b> {analysis.agent_reason}</div>
            <div class="small-note">
                Warning: {analysis.agent_warning}<br/>
                Trend reason: {analysis.trend_reason}<br/>
                Trap reason: {analysis.trap_reason}<br/>
                IV signal: {analysis.iv_signal}<br/>
                Money flow: {analysis.money_flow_bias}
            </div>
        </div>
    ''', unsafe_allow_html=True)


def render_analysis_cards(symbol: str, analysis: SnapshotAnalysis) -> None:
    st.markdown('<div class="panel-box">', unsafe_allow_html=True)
    st.markdown(f'<div class="symbol-head">{symbol}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="symbol-sub">Live memory window: {analysis.oldest_ts} → {analysis.latest_ts} | Snapshots: {analysis.snapshot_count} | Snapshot ID: {analysis.snapshot_id}</div>', unsafe_allow_html=True)
    st.markdown(f'''
        <div class="status-chip-row">
            <div class="status-chip">Trend: {analysis.trend}</div>
            <div class="status-chip">Score: {analysis.trend_score}</div>
            <div class="status-chip">Trap: {analysis.trap_signal}</div>
            <div class="status-chip">OI Direction: {analysis.oi_direction}</div>
            <div class="status-chip">Call Signal: {analysis.call_side_signal}</div>
            <div class="status-chip">Put Signal: {analysis.put_side_signal}</div>
            <div class="status-chip">Agent: {analysis.buyer_signal}</div>
        </div>
    ''', unsafe_allow_html=True)

    row1 = st.columns(4); row2 = st.columns(4); row3 = st.columns(4); row4 = st.columns([1.2, 1.2])
    draw_metric(row1[0].empty(), "Spot", fmt_num(analysis.spot), fmt_pct(analysis.spot_change_pct))
    draw_metric(row1[1].empty(), "OI PCR", fmt_num(analysis.oi_pcr), analysis.oi_direction)
    draw_metric(row1[2].empty(), "Vol PCR", fmt_num(analysis.vol_pcr), analysis.volume_bias)
    draw_metric(row1[3].empty(), "Trend", analysis.trend, f"Score: {analysis.trend_score}")
    draw_metric(row2[0].empty(), "CE ΔOI", fmt_int(analysis.ce_oi_change), analysis.call_oi_bias)
    draw_metric(row2[1].empty(), "PE ΔOI", fmt_int(analysis.pe_oi_change), analysis.put_oi_bias)
    draw_metric(row2[2].empty(), "Support", fmt_num(analysis.nearest_support, 0), render_levels(analysis.support_levels))
    draw_metric(row2[3].empty(), "Resistance", fmt_num(analysis.nearest_resistance, 0), render_levels(analysis.resistance_levels))
    draw_metric(row3[0].empty(), "CE IV Avg", fmt_num(analysis.ce_iv_avg), fmt_num(analysis.ce_iv_change))
    draw_metric(row3[1].empty(), "PE IV Avg", fmt_num(analysis.pe_iv_avg), fmt_num(analysis.pe_iv_change))
    draw_metric(row3[2].empty(), "IV Context", analysis.iv_signal, analysis.iv_bias)
    draw_metric(row3[3].empty(), "Opportunity", analysis.opportunity_tag, analysis.money_flow_bias)

    with row4[0]:
        render_agent_box(analysis)
    with row4[1]:
        st.markdown(f'''
            <div class="decision-box">
                <div class="analysis-head">Writing, Money Flow & Buyer Edge</div>
                <div><b>Call Writing Pressure:</b> {fmt_int(analysis.call_writing_pressure)}</div>
                <div style="margin-top:8px;"><b>Put Writing Pressure:</b> {fmt_int(analysis.put_writing_pressure)}</div>
                <div style="margin-top:8px;"><b>CE Money Flow:</b> {fmt_int(analysis.ce_money_flow)}</div>
                <div style="margin-top:8px;"><b>PE Money Flow:</b> {fmt_int(analysis.pe_money_flow)}</div>
                <div style="margin-top:8px;"><b>Near ATM CE Flow:</b> {fmt_int(analysis.ce_near_money_flow)}</div>
                <div style="margin-top:8px;"><b>Near ATM PE Flow:</b> {fmt_int(analysis.pe_near_money_flow)}</div>
                <div class="small-note">
                    Writing Bias: {analysis.writing_bias}<br/>
                    Call Side Signal: {analysis.call_side_signal}<br/>
                    Put Side Signal: {analysis.put_side_signal}<br/>
                    Total CE OI: {fmt_int(analysis.ce_oi_total)}<br/>
                    Total PE OI: {fmt_int(analysis.pe_oi_total)}<br/>
                    Total CE Vol: {fmt_int(analysis.ce_vol_total)}<br/>
                    Total PE Vol: {fmt_int(analysis.pe_vol_total)}
                </div>
            </div>
        ''', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_slow_block(analysis: SnapshotAnalysis) -> None:
    c1, c2 = st.columns(2)
    with c1:
        show_table(analysis.top_call_oi_added, "CE_OI_DELTA", "Top Call OI Added")
        show_table(analysis.top_call_oi_unwound, "CE_OI_DELTA", "Top Call OI Unwound")
    with c2:
        show_table(analysis.top_put_oi_added, "PE_OI_DELTA", "Top Put OI Added")
        show_table(analysis.top_put_oi_unwound, "PE_OI_DELTA", "Top Put OI Unwound")


def show_header() -> None:
    st.markdown('<div class="page-title">Option Chain AI Agent Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-subtitle">Aggressive live option-buyer agent using RAM snapshots, IV context, money flow, writing pressure and opportunity detection</div>', unsafe_allow_html=True)


@st.fragment(run_every=f"{FAST_REFRESH_SECONDS}s")
def fast_fragment() -> None:
    update_analysis_cache()
    st.session_state["ai_last_paint_ts"] = datetime.now()
    nifty = st.session_state.get("NIFTY_analysis")
    sensex = st.session_state.get("SENSEX_analysis")
    if nifty is None and sensex is None:
        st.info("Waiting for live in-memory snapshots from collectors...")
        return

    section_title("Executive Monitor")
    top = st.columns(8)
    total_symbols = int(nifty is not None) + int(sensex is not None)
    trap_count = sum(1 for a in [nifty, sensex] if a is not None and a.trap_signal != "No clear trap")
    snapshot_total = sum(a.snapshot_count for a in [nifty, sensex] if a is not None)
    avg_oi_pcr = [a.oi_pcr for a in [nifty, sensex] if a is not None and a.oi_pcr is not None]
    avg_vol_pcr = [a.vol_pcr for a in [nifty, sensex] if a is not None and a.vol_pcr is not None]

    draw_metric(top[0].empty(), "Indexes Live", str(total_symbols), "NIFTY + SENSEX")
    draw_metric(top[1].empty(), "Total RAM Snapshots", str(snapshot_total), "Across all collectors")
    draw_metric(top[2].empty(), "Average OI PCR", fmt_num(sum(avg_oi_pcr) / len(avg_oi_pcr) if avg_oi_pcr else None), "")
    draw_metric(top[3].empty(), "Average Vol PCR", fmt_num(sum(avg_vol_pcr) / len(avg_vol_pcr) if avg_vol_pcr else None), "")
    draw_metric(top[4].empty(), "Trap Alerts", str(trap_count), "Live detected")
    draw_metric(top[5].empty(), "NIFTY Snapshot", str(nifty.snapshot_id) if nifty else "-", fmt_ts(nifty.latest_ts if nifty else None))
    draw_metric(top[6].empty(), "SENSEX Snapshot", str(sensex.snapshot_id) if sensex else "-", fmt_ts(sensex.latest_ts if sensex else None))
    draw_metric(top[7].empty(), "AI Last Paint", st.session_state["ai_last_paint_ts"].strftime("%H:%M:%S") if st.session_state.get("ai_last_paint_ts") else "-", "Live refresh")

    st.divider()
    section_title("Index Analysis Panels")
    col_left, col_right = st.columns(2)
    with col_left:
        if nifty is not None: render_analysis_cards("NIFTY", nifty)
        else: st.info("NIFTY analysis not available.")
    with col_right:
        if sensex is not None: render_analysis_cards("SENSEX", sensex)
        else: st.info("SENSEX analysis not available.")

    st.divider()
    section_title("Combined Live AI Monitor")
    rows = []
    for a, name in [(nifty, "NIFTY"), (sensex, "SENSEX")]:
        if a is not None:
            rows.append({
                "Index": name, "Spot": fmt_num(a.spot), "Snapshots": a.snapshot_count, "Spot Change %": fmt_pct(a.spot_change_pct),
                "OI PCR": fmt_num(a.oi_pcr), "Vol PCR": fmt_num(a.vol_pcr), "Trend": a.trend, "IV": a.iv_signal,
                "Money Flow": a.money_flow_bias, "Opportunity": a.opportunity_tag, "Agent Signal": a.buyer_signal,
                "Confidence": a.buyer_confidence, "Support": fmt_num(a.nearest_support, 0), "Resistance": fmt_num(a.nearest_resistance, 0),
            })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


@st.fragment(run_every=f"{SLOW_REFRESH_SECONDS}s")
def slow_fragment() -> None:
    nifty = st.session_state.get("NIFTY_analysis")
    sensex = st.session_state.get("SENSEX_analysis")
    st.divider()
    section_title("Detailed Analysis Workspace")
    tab1, tab2, tab3 = st.tabs(["OI Build-up Tables", "Delta Matrix", "Alert History"])
    with tab1:
        sub1, sub2 = st.columns(2)
        with sub1:
            st.markdown("#### NIFTY")
            if nifty is None: st.info("NIFTY tables not available.")
            else: render_slow_block(nifty)
        with sub2:
            st.markdown("#### SENSEX")
            if sensex is None: st.info("SENSEX tables not available.")
            else: render_slow_block(sensex)
    with tab2:
        if nifty is not None:
            st.markdown("#### NIFTY Merged OI / Volume Delta")
            merged_show = nifty.latest_merged_df.copy()
            for col in merged_show.columns: merged_show[col] = merged_show[col].apply(fmt_int)
            st.dataframe(merged_show, use_container_width=True, hide_index=True)
        if sensex is not None:
            st.markdown("#### SENSEX Merged OI / Volume Delta")
            merged_show = sensex.latest_merged_df.copy()
            for col in merged_show.columns: merged_show[col] = merged_show[col].apply(fmt_int)
            st.dataframe(merged_show, use_container_width=True, hide_index=True)
    with tab3:
        alerts = st.session_state.get("alert_history", [])
        if not alerts: st.info("No alerts generated yet.")
        else: st.dataframe(pd.DataFrame(alerts), use_container_width=True, hide_index=True)


def render_app() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)
    init_state()
    show_header()
    fast_fragment()
    slow_fragment()


def main() -> None:
    st.set_page_config(page_title="Option Chain AI Agent Dashboard", layout="wide")
    render_app()


if __name__ == "__main__":
    main()
