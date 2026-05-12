"""Institutional investor flow data loading and observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


INSTITUTIONAL_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
    "institutional_total_net_buy",
    "institutional_3d_sum",
    "institutional_5d_sum",
]

NEUTRAL_REASON = "籌碼資料不足，採中性分數"


def score_institutional_for_symbols(
    symbols: list[str],
    institutional_path: str | Path = "data/institutional.csv",
) -> pd.DataFrame:
    institutional = load_institutional(institutional_path)
    return pd.DataFrame([score_institutional(symbol, institutional) for symbol in symbols])


def score_institutional(stock_id: str, institutional: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if institutional.empty:
        return _neutral(symbol)
    frame = institutional[institutional["stock_id"].astype(str).str.strip() == symbol].copy()
    if frame.empty:
        return _neutral(symbol)

    frame = frame.sort_values("trade_date")
    latest = frame.iloc[-1]
    foreign = _to_float(latest.get("foreign_net_buy"))
    trust = _to_float(latest.get("investment_trust_net_buy"))
    dealer = _to_float(latest.get("dealer_net_buy"))
    total = _to_float(latest.get("institutional_total_net_buy"))
    if total is None:
        total = sum(value or 0.0 for value in [foreign, trust, dealer])
    three_day = _to_float(latest.get("institutional_3d_sum"))
    if three_day is None:
        three_day = float(pd.to_numeric(frame["institutional_total_net_buy"], errors="coerce").fillna(0).tail(3).sum())
    five_day = _to_float(latest.get("institutional_5d_sum"))
    if five_day is None:
        five_day = float(pd.to_numeric(frame["institutional_total_net_buy"], errors="coerce").fillna(0).tail(5).sum())

    trust_tail = pd.to_numeric(frame["investment_trust_net_buy"], errors="coerce").dropna().tail(3)
    foreign_tail = pd.to_numeric(frame["foreign_net_buy"], errors="coerce").dropna().tail(3)

    score = 50.0
    reasons: list[str] = []
    if len(trust_tail) == 3 and (trust_tail > 0).all():
        score += 15
        reasons.append("投信連買")
    if len(foreign_tail) == 3 and (foreign_tail > 0).all():
        score += 10
        reasons.append("外資連買")
    if total is not None and total > 0:
        score += 8
        reasons.append("三大法人合計買超")
    if five_day is not None and five_day < 0:
        score -= 12
        reasons.append("法人連賣扣分")

    if not reasons:
        reasons.append("籌碼資料中性")

    return {
        "stock_id": symbol,
        "institutional_score": _bounded(score),
        "foreign_net_buy": foreign,
        "investment_trust_net_buy": trust,
        "dealer_net_buy": dealer,
        "institutional_total_net_buy": total,
        "institutional_3d_sum": three_day,
        "institutional_5d_sum": five_day,
        "institutional_reason": "；".join(reasons),
    }


def load_institutional(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame(columns=INSTITUTIONAL_COLUMNS)
    frame = pd.read_csv(csv_path, dtype={"stock_id": str})
    for column in INSTITUTIONAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in [
        "foreign_net_buy",
        "investment_trust_net_buy",
        "dealer_net_buy",
        "institutional_total_net_buy",
        "institutional_3d_sum",
        "institutional_5d_sum",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[INSTITUTIONAL_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "institutional_score": 50.0,
        "foreign_net_buy": None,
        "investment_trust_net_buy": None,
        "dealer_net_buy": None,
        "institutional_total_net_buy": None,
        "institutional_3d_sum": None,
        "institutional_5d_sum": None,
        "institutional_reason": NEUTRAL_REASON,
    }


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, round(float(value), 2)))
