"""Monthly revenue observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


REVENUE_COLUMNS = [
    "stock_id",
    "stock_name",
    "year_month",
    "revenue",
    "monthly_revenue",
    "revenue_yoy",
    "revenue_mom",
    "accumulated_revenue",
    "accumulated_revenue_yoy",
    "revenue_3m_trend",
    "revenue_12m_high",
    "revenue_warning",
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
    revenue_value = _to_float(latest.get("monthly_revenue"))
    latest_yoy = _to_float(latest.get("revenue_yoy"))
    latest_mom = _to_float(latest.get("revenue_mom"))
    accumulated_yoy = _to_float(latest.get("accumulated_revenue_yoy"))
    last3 = pd.to_numeric(frame["revenue_yoy"], errors="coerce").dropna().tail(3)
    trailing_revenue = pd.to_numeric(frame["monthly_revenue"], errors="coerce").dropna().tail(12)
    is_12m_high = bool(revenue_value is not None and len(trailing_revenue) >= 2 and revenue_value >= trailing_revenue.max())

    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []
    if latest_yoy is not None and latest_yoy > 20:
        score += 18
        reasons.append("月營收年增率大於 20%")
    elif latest_yoy is not None and latest_yoy > 0:
        score += 8
        reasons.append("月營收 YoY 正成長")
    if len(last3) == 3 and (last3 > 0).all():
        score += 12
        reasons.append("月營收 YoY 連續 3 個月為正")
    if is_12m_high:
        score += 15
        reasons.append("月營收創 12 個月新高")
    if accumulated_yoy is not None and accumulated_yoy > 10:
        score += 12
        reasons.append("累計營收 YoY 大於 10%")
    if latest_yoy is not None and latest_yoy < -20:
        score -= 18
        reasons.append("月營收年增率低於 -20%")
        warnings.append("月營收 YoY 明顯衰退")
    if len(last3) == 3 and (last3 < 0).all():
        score -= 12
        reasons.append("月營收 YoY 連續 3 個月衰退")
        warnings.append("月營收 YoY 連續 3 個月衰退")

    if not reasons:
        reasons.append("基本面資料中性")

    return {
        "stock_id": symbol,
        "monthly_revenue": revenue_value,
        "revenue_yoy": latest_yoy,
        "revenue_mom": latest_mom,
        "accumulated_revenue_yoy": accumulated_yoy,
        "revenue_3m_trend": _three_month_trend(last3),
        "revenue_12m_high": is_12m_high,
        "revenue_warning": "；".join(warnings),
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
    if frame["monthly_revenue"].isna().all():
        frame["monthly_revenue"] = frame["revenue"]
    for column in [
        "revenue",
        "monthly_revenue",
        "revenue_yoy",
        "revenue_mom",
        "accumulated_revenue",
        "accumulated_revenue_yoy",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["revenue_12m_high"] = frame["revenue_12m_high"].apply(_to_bool)
    return frame[REVENUE_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "monthly_revenue": None,
        "revenue_yoy": None,
        "revenue_mom": None,
        "accumulated_revenue_yoy": None,
        "revenue_3m_trend": "neutral",
        "revenue_12m_high": False,
        "revenue_warning": "",
        "revenue_score": 50.0,
        "revenue_reason": NEUTRAL_REASON,
        "fundamental_score": 50.0,
        "fundamental_reason": NEUTRAL_REASON,
    }


def _three_month_trend(values: pd.Series) -> str:
    if len(values) == 3 and (values > 0).all():
        return "positive"
    if len(values) == 3 and (values < 0).all():
        return "negative"
    return "neutral"


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
