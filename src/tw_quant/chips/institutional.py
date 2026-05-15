"""Institutional investor flow data loading and observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


INSTITUTIONAL_COLUMNS = [
    "trade_date",
    "date",
    "stock_id",
    "stock_name",
    "foreign_net_buy",
    "investment_trust_net_buy",
    "dealer_net_buy",
    "institutional_total_net_buy",
    "total_institutional_net_buy",
    "institutional_3d_sum",
    "institutional_5d_sum",
    "volume",
]

NEUTRAL_REASON = "三大法人資料不足，採中性分數"


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
    total = _to_float(latest.get("total_institutional_net_buy"))
    if total is None:
        total = _to_float(latest.get("institutional_total_net_buy"))
    if total is None:
        total = sum(value or 0.0 for value in [foreign, trust, dealer])
    three_day = _to_float(latest.get("institutional_3d_sum"))
    if three_day is None:
        total_series = _total_series(frame)
        three_day = float(total_series.tail(3).sum())
    five_day = _to_float(latest.get("institutional_5d_sum"))
    if five_day is None:
        total_series = _total_series(frame)
        five_day = float(total_series.tail(5).sum())

    trust_tail = pd.to_numeric(frame["investment_trust_net_buy"], errors="coerce").dropna().tail(3)
    foreign_tail = pd.to_numeric(frame["foreign_net_buy"], errors="coerce").dropna().tail(3)
    total_tail = _total_series(frame).dropna().tail(3)
    volume = _to_float(latest.get("volume"))
    institutional_buy_ratio = (total / volume) if volume and total is not None else None

    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []
    if len(foreign_tail) == 3 and (foreign_tail > 0).all():
        score += 10
        reasons.append("外資連買 3 天")
    if len(trust_tail) == 3 and (trust_tail > 0).all():
        score += 18
        reasons.append("投信連買 3 天")
    if (foreign or 0) > 0 and (trust or 0) > 0:
        score += 8
        reasons.append("外資與投信同買")
    if institutional_buy_ratio is not None and institutional_buy_ratio > 0.10:
        score += 8
        reasons.append("法人買超占成交量大於 10%")
    if len(total_tail) == 3 and (total_tail < 0).all():
        score -= 16
        reasons.append("法人連賣 3 天")
        warnings.append("法人連賣")
    if len(trust_tail) == 3 and (trust_tail < 0).all():
        score -= 14
        reasons.append("投信連賣")
        warnings.append("投信連賣")
    if not reasons:
        reasons.append("三大法人籌碼中性")

    return {
        "stock_id": symbol,
        "institutional_score": _bounded(score),
        "chip_score": _bounded(score),
        "foreign_net_buy": foreign,
        "investment_trust_net_buy": trust,
        "dealer_net_buy": dealer,
        "institutional_total_net_buy": total,
        "total_institutional_net_buy": total,
        "institutional_3d_sum": three_day,
        "institutional_5d_sum": five_day,
        "foreign_buy_days": _positive_tail_days(foreign_tail),
        "investment_trust_buy_days": _positive_tail_days(trust_tail),
        "institutional_buy_ratio": institutional_buy_ratio,
        "institutional_warning": "；".join(warnings),
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
    if frame["trade_date"].isna().all() and "date" in frame.columns:
        frame["trade_date"] = frame["date"]
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in [
        "foreign_net_buy",
        "investment_trust_net_buy",
        "dealer_net_buy",
        "institutional_total_net_buy",
        "total_institutional_net_buy",
        "institutional_3d_sum",
        "institutional_5d_sum",
        "volume",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[INSTITUTIONAL_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "institutional_score": 50.0,
        "chip_score": 50.0,
        "foreign_net_buy": None,
        "investment_trust_net_buy": None,
        "dealer_net_buy": None,
        "institutional_total_net_buy": None,
        "total_institutional_net_buy": None,
        "institutional_3d_sum": None,
        "institutional_5d_sum": None,
        "foreign_buy_days": 0,
        "investment_trust_buy_days": 0,
        "institutional_buy_ratio": None,
        "institutional_warning": "",
        "institutional_reason": NEUTRAL_REASON,
    }


def _total_series(frame: pd.DataFrame) -> pd.Series:
    total = pd.to_numeric(frame.get("total_institutional_net_buy"), errors="coerce")
    if total.isna().all():
        total = pd.to_numeric(frame.get("institutional_total_net_buy"), errors="coerce")
    if total.isna().all():
        total = (
            pd.to_numeric(frame.get("foreign_net_buy"), errors="coerce").fillna(0)
            + pd.to_numeric(frame.get("investment_trust_net_buy"), errors="coerce").fillna(0)
            + pd.to_numeric(frame.get("dealer_net_buy"), errors="coerce").fillna(0)
        )
    return total


def _positive_tail_days(values: pd.Series) -> int:
    count = 0
    for value in reversed(values.tolist()):
        if value > 0:
            count += 1
        else:
            break
    return count


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
