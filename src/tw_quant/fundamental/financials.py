"""Financial statement data loading and observation scoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


FINANCIAL_COLUMNS = [
    "stock_id",
    "stock_name",
    "financial_quarter",
    "eps",
    "eps_yoy",
    "roe",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "debt_ratio",
    "operating_cash_flow",
]

NEUTRAL_REASON = "財報資料不足，採中性分數"


def score_financials_for_symbols(
    symbols: list[str],
    financials_path: str | Path = "data/financials.csv",
) -> pd.DataFrame:
    financials = load_financials(financials_path)
    return pd.DataFrame([score_financials(symbol, financials) for symbol in symbols])


def score_financials(stock_id: str, financials: pd.DataFrame) -> dict[str, object]:
    symbol = str(stock_id).strip()
    if financials.empty:
        return _neutral(symbol)

    frame = financials[financials["stock_id"].astype(str).str.strip() == symbol].copy()
    if frame.empty:
        return _neutral(symbol)

    frame = frame.sort_values("financial_quarter")
    latest = frame.iloc[-1]
    previous = frame.iloc[-2] if len(frame) >= 2 else None
    eps = _to_float(latest.get("eps"))
    eps_yoy = _to_float(latest.get("eps_yoy"))
    roe = _to_float(latest.get("roe"))
    gross_margin = _to_float(latest.get("gross_margin"))
    operating_margin = _to_float(latest.get("operating_margin"))
    debt_ratio = _to_float(latest.get("debt_ratio"))
    operating_cash_flow = _to_float(latest.get("operating_cash_flow"))

    score = 50.0
    reasons: list[str] = []
    warnings: list[str] = []

    if eps_yoy is not None and eps_yoy > 0:
        score += 12
        reasons.append("EPS 成長")
    if roe is not None and roe > 10:
        score += 12
        reasons.append("ROE 大於 10%")
    if previous is not None and gross_margin is not None:
        prev_gross = _to_float(previous.get("gross_margin"))
        if prev_gross is not None and gross_margin > prev_gross:
            score += 8
            reasons.append("毛利率提升")

    if previous is not None and operating_margin is not None:
        prev_operating = _to_float(previous.get("operating_margin"))
        if prev_operating is not None and operating_margin < prev_operating:
            score -= 8
            reasons.append("營益率下降扣分")

    latest_eps_values = pd.to_numeric(frame["eps"], errors="coerce").dropna().tail(2)
    if len(latest_eps_values) == 2 and (latest_eps_values < 0).all():
        score -= 18
        warnings.append("連續虧損")
        reasons.append("連續虧損警告")
    elif eps is not None and eps < 0:
        score -= 10
        warnings.append("EPS 為負")

    if debt_ratio is not None and debt_ratio > 60:
        score -= 10
        warnings.append("負債比偏高")
        reasons.append("負債比過高扣分")
    if operating_cash_flow is not None and operating_cash_flow < 0:
        score -= 8
        warnings.append("營業現金流為負")

    if not reasons and not warnings:
        reasons.append("財報資料中性")

    return {
        "stock_id": symbol,
        "financial_score": _bounded(score),
        "eps": eps,
        "roe": roe,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
        "debt_ratio": debt_ratio,
        "financial_reason": "；".join(reasons),
        "financial_warning": "；".join(warnings),
    }


def load_financials(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame(columns=FINANCIAL_COLUMNS)
    frame = pd.read_csv(csv_path, dtype={"stock_id": str})
    for column in FINANCIAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["financial_quarter"] = frame["financial_quarter"].fillna("").astype(str)
    for column in [
        "eps",
        "eps_yoy",
        "roe",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "debt_ratio",
        "operating_cash_flow",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[FINANCIAL_COLUMNS].copy()


def _neutral(stock_id: str) -> dict[str, object]:
    return {
        "stock_id": str(stock_id).strip(),
        "financial_score": 50.0,
        "eps": None,
        "roe": None,
        "gross_margin": None,
        "operating_margin": None,
        "debt_ratio": None,
        "financial_reason": NEUTRAL_REASON,
        "financial_warning": "",
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
