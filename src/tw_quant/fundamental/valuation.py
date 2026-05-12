"""Valuation data loading and observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


VALUATION_COLUMNS = [
    "stock_id",
    "stock_name",
    "financial_quarter",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
]

NEUTRAL_REASON = "估值資料不足，採中性分數"


def score_valuation_for_symbols(
    symbols: list[str],
    valuation_path: str | Path = "data/valuation.csv",
    revenue_scores: pd.DataFrame | None = None,
) -> pd.DataFrame:
    valuation = load_valuation(valuation_path)
    revenue_lookup = _revenue_lookup(revenue_scores)
    return pd.DataFrame([score_valuation(symbol, valuation, revenue_lookup) for symbol in symbols])


def score_valuation(
    stock_id: str,
    valuation: pd.DataFrame,
    revenue_lookup: dict[str, float] | None = None,
) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if valuation.empty:
        return _neutral(symbol)

    frame = valuation[valuation["stock_id"].astype(str).str.strip() == symbol].copy()
    if frame.empty:
        return _neutral(symbol)

    frame = frame.sort_values("financial_quarter")
    latest = frame.iloc[-1]
    pe_ratio = _to_float(latest.get("pe_ratio"))
    pb_ratio = _to_float(latest.get("pb_ratio"))
    dividend_yield = _to_float(latest.get("dividend_yield"))
    revenue_yoy = (revenue_lookup or {}).get(symbol)

    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []

    if pe_ratio is None or pe_ratio <= 0:
        warnings.append("PE 為空或負值")
    elif pe_ratio <= 20 and revenue_yoy is not None and revenue_yoy > 0:
        score += 12
        reasons.append("PE 合理且營收成長")
    elif pe_ratio > 40:
        score -= 14
        warnings.append("PE 偏高")
        reasons.append("PE 過高扣分")

    if pb_ratio is not None and pb_ratio > 5:
        score -= 10
        warnings.append("PB 偏高")
        reasons.append("PB 過高扣分")
    elif pb_ratio is not None and 0 < pb_ratio <= 2:
        score += 5
        reasons.append("PB 相對合理")

    if dividend_yield is not None and dividend_yield >= 3:
        score += 5
        reasons.append("殖利率穩定")

    if not reasons and not warnings:
        reasons.append("估值資料中性")

    return {
        "stock_id": symbol,
        "valuation_score": _bounded(score),
        "pe_ratio": pe_ratio,
        "pb_ratio": pb_ratio,
        "dividend_yield": dividend_yield,
        "valuation_reason": "；".join(reasons),
        "valuation_warning": "；".join(warnings),
    }


def load_valuation(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame(columns=VALUATION_COLUMNS)
    frame = pd.read_csv(csv_path, dtype={"stock_id": str})
    for column in VALUATION_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["financial_quarter"] = frame["financial_quarter"].fillna("").astype(str)
    for column in ["pe_ratio", "pb_ratio", "dividend_yield"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[VALUATION_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "valuation_score": 50.0,
        "pe_ratio": None,
        "pb_ratio": None,
        "dividend_yield": None,
        "valuation_reason": NEUTRAL_REASON,
        "valuation_warning": "",
    }


def _revenue_lookup(revenue_scores: pd.DataFrame | None) -> dict[str, float]:
    if revenue_scores is None or revenue_scores.empty or "revenue_yoy" not in revenue_scores.columns:
        return {}
    frame = revenue_scores.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    values = pd.to_numeric(frame["revenue_yoy"], errors="coerce")
    return {
        stock_id: float(value)
        for stock_id, value in zip(frame["stock_id"], values)
        if not pd.isna(value)
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
