"""Monthly revenue observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


REVENUE_COLUMNS = [
    "stock_id",
    "stock_name",
    "year_month",
    "revenue",
    "revenue_yoy",
    "revenue_mom",
    "accumulated_revenue",
    "accumulated_revenue_yoy",
]

NEUTRAL_REASON = "基本面資料不足，採中性分數"


def score_revenue_for_symbols(
    symbols: list[str],
    revenue_path: str | Path = "data/monthly_revenue.csv",
) -> pd.DataFrame:
    revenue = _load_monthly_revenue(revenue_path)
    rows = [score_monthly_revenue(symbol, revenue) for symbol in symbols]
    return pd.DataFrame(rows)


def score_monthly_revenue(stock_id: str, revenue: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if revenue.empty:
        return _neutral(symbol)
    frame = revenue[revenue["stock_id"].astype(str).str.strip() == symbol].copy()
    if frame.empty:
        return _neutral(symbol)

    frame = frame.sort_values("year_month")
    latest = frame.iloc[-1]
    latest_yoy = _to_float(latest.get("revenue_yoy"))
    latest_mom = _to_float(latest.get("revenue_mom"))
    accumulated_yoy = _to_float(latest.get("accumulated_revenue_yoy"))
    last3 = pd.to_numeric(frame["revenue_yoy"], errors="coerce").dropna().tail(3)

    score = 50.0
    reasons: list[str] = []
    if latest_yoy is not None and latest_yoy > 20:
        score += 18
        reasons.append("月營收年增率大於 20%")
    if len(last3) == 3 and (last3 > 0).all():
        score += 12
        reasons.append("月營收年增率連續 3 個月為正")
    if accumulated_yoy is not None and accumulated_yoy > 10:
        score += 12
        reasons.append("累計營收年增率大於 10%")
    if latest_yoy is not None and latest_yoy < -20:
        score -= 18
        reasons.append("月營收年增率低於 -20%")
    if len(last3) == 3 and (last3 < 0).all():
        score -= 12
        reasons.append("月營收年增率連續 3 個月為負")

    if not reasons:
        reasons.append("基本面資料中性")

    return {
        "stock_id": symbol,
        "revenue_yoy": latest_yoy,
        "revenue_mom": latest_mom,
        "accumulated_revenue_yoy": accumulated_yoy,
        "revenue_score": max(0.0, min(100.0, round(score, 2))),
        "revenue_reason": "；".join(reasons),
        "fundamental_score": max(0.0, min(100.0, round(score, 2))),
        "fundamental_reason": "；".join(reasons),
    }


def _load_monthly_revenue(path: str | Path) -> pd.DataFrame:
    revenue_path = Path(path)
    if not revenue_path.exists():
        return pd.DataFrame(columns=REVENUE_COLUMNS)
    frame = pd.read_csv(revenue_path, dtype={"stock_id": str})
    for column in REVENUE_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["year_month"] = frame["year_month"].astype(str)
    for column in ["revenue", "revenue_yoy", "revenue_mom", "accumulated_revenue", "accumulated_revenue_yoy"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[REVENUE_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "revenue_yoy": None,
        "revenue_mom": None,
        "accumulated_revenue_yoy": None,
        "revenue_score": 50.0,
        "revenue_reason": NEUTRAL_REASON,
        "fundamental_score": 50.0,
        "fundamental_reason": NEUTRAL_REASON,
    }


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number
