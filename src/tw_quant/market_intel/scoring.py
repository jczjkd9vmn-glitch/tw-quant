"""Rule-based market intelligence scoring.

The first version intentionally uses transparent rules instead of AI judgment.
Scores are auxiliary only and must not create trades by themselves.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from tw_quant.market_intel.providers.base import MarketContext


POSITIVE_NEWS_KEYWORDS = [
    "營收創高",
    "獲利成長",
    "接單增加",
    "AI",
    "資料中心",
    "法說正向",
    "產品漲價",
    "擴產",
    "股利增加",
    "客戶需求強",
    "毛利率改善",
]

NEGATIVE_NEWS_KEYWORDS = [
    "虧損",
    "下修",
    "砍單",
    "法說保守",
    "調降財測",
    "跌停",
    "檢調",
    "財報不如預期",
    "毛利率下滑",
    "庫存過高",
    "客戶延遲拉貨",
    "匯損",
    "訂單減少",
]


def build_market_context(
    *,
    symbol: str,
    date: str,
    close: object = None,
    volume: object = None,
    volume_change_ratio: object = None,
    pe_ratio: object = None,
    pb_ratio: object = None,
    dividend_yield: object = None,
    revenue_growth_yoy: object = None,
    eps_growth_yoy: object = None,
    roe: object = None,
    debt_ratio: object = None,
    momentum_score_hint: object = None,
    latest_news_titles: Iterable[object] | None = None,
    data_source: str = "mock",
    warning_message: str = "",
) -> MarketContext:
    titles = [str(title) for title in (latest_news_titles or []) if not _is_blank(title)]
    news_score, matched_keywords = score_news_sentiment(titles)
    fundamental_score, fundamental_warning, fundamental_flags = score_fundamental(
        revenue_growth_yoy=revenue_growth_yoy,
        eps_growth_yoy=eps_growth_yoy,
        roe=roe,
        debt_ratio=debt_ratio,
    )
    valuation_score, valuation_warning, valuation_flags = score_valuation(
        pe_ratio=pe_ratio,
        pb_ratio=pb_ratio,
        dividend_yield=dividend_yield,
    )
    momentum_score, momentum_warning, momentum_flags = score_momentum(
        momentum_score_hint=momentum_score_hint,
        volume_change_ratio=volume_change_ratio,
        close=close,
    )

    news_as_0_to_100 = max(min((news_score + 100.0) / 2.0, 100.0), 0.0)
    final_score = round(
        momentum_score * 0.40
        + fundamental_score * 0.25
        + valuation_score * 0.20
        + news_as_0_to_100 * 0.15,
        2,
    )
    warnings = [item for item in [warning_message, fundamental_warning, valuation_warning, momentum_warning] if item]
    confidence_score = max(20.0, 100.0 - len(warnings) * 15.0)
    risk_flags = fundamental_flags + valuation_flags + momentum_flags
    if news_score <= -40:
        risk_flags.append("新聞偏負面")
    if final_score < 45:
        risk_flags.append("綜合分數偏弱")
    risk_score = round(max(0.0, 100.0 - final_score + max(0.0, -news_score) * 0.2), 2)

    return MarketContext(
        symbol=str(symbol),
        date=date,
        close=_to_float(close),
        volume=_to_float(volume),
        volume_change_ratio=_to_float(volume_change_ratio),
        pe_ratio=_to_float(pe_ratio),
        pb_ratio=_to_float(pb_ratio),
        dividend_yield=_to_float(dividend_yield),
        revenue_growth_yoy=_to_float(revenue_growth_yoy),
        eps_growth_yoy=_to_float(eps_growth_yoy),
        latest_news_titles=titles,
        matched_news_keywords=matched_keywords,
        news_sentiment_score=float(news_score),
        fundamental_score=round(fundamental_score, 2),
        valuation_score=round(valuation_score, 2),
        momentum_score=round(momentum_score, 2),
        final_market_score=final_score,
        confidence_score=round(confidence_score, 2),
        risk_score=risk_score,
        risk_flags=risk_flags,
        final_comment=build_final_comment(final_score, fundamental_score, valuation_score, momentum_score, news_score, warnings),
        data_source=data_source,
        warning_message="；".join(warnings),
    )


def score_news_sentiment(titles: Iterable[str]) -> tuple[int, list[str]]:
    text = " ".join(str(title) for title in titles)
    positive = [keyword for keyword in POSITIVE_NEWS_KEYWORDS if keyword in text]
    negative = [keyword for keyword in NEGATIVE_NEWS_KEYWORDS if keyword in text]
    score = min(len(positive) * 20, 100) - min(len(negative) * 25, 100)
    return max(min(score, 100), -100), positive + negative


def score_fundamental(
    *,
    revenue_growth_yoy: object = None,
    eps_growth_yoy: object = None,
    roe: object = None,
    debt_ratio: object = None,
) -> tuple[float, str, list[str]]:
    score = 50.0
    flags: list[str] = []
    available = 0
    revenue = _to_float(revenue_growth_yoy)
    eps = _to_float(eps_growth_yoy)
    roe_value = _to_float(roe)
    debt = _to_float(debt_ratio)
    if revenue is not None:
        available += 1
        score += 12 if revenue > 0 else -12
    if eps is not None:
        available += 1
        score += 12 if eps > 0 else -12
        if eps < 0:
            flags.append("EPS 衰退")
    if roe_value is not None:
        available += 1
        if roe_value > 10:
            score += 10
    if debt is not None:
        available += 1
        if debt > 70:
            score -= 15
            flags.append("負債比偏高")
    warning = "" if available else "基本面資料不足，採中性分數"
    return _clamp_score(score), warning, flags


def score_valuation(
    *,
    pe_ratio: object = None,
    pb_ratio: object = None,
    dividend_yield: object = None,
) -> tuple[float, str, list[str]]:
    # TODO: future sector-adjusted valuation.
    score = 50.0
    flags: list[str] = []
    available = 0
    pe = _to_float(pe_ratio)
    pb = _to_float(pb_ratio)
    yield_value = _to_float(dividend_yield)
    if pe is not None:
        available += 1
        if pe <= 0:
            flags.append("PE 為空或負值")
        elif pe <= 20:
            score += 12
        elif pe > 40:
            score -= 20
            flags.append("PE 偏高")
    if pb is not None:
        available += 1
        if pb > 5:
            score -= 12
            flags.append("PB 偏高")
        elif 0 < pb <= 2:
            score += 6
    if yield_value is not None:
        available += 1
        if yield_value >= 3:
            score += 5
    warning = "" if available else "估值資料不足，採中性分數"
    return _clamp_score(score), warning, flags


def score_momentum(
    *,
    momentum_score_hint: object = None,
    volume_change_ratio: object = None,
    close: object = None,
) -> tuple[float, str, list[str]]:
    score = _to_float(momentum_score_hint)
    if score is None:
        score = 50.0
    flags: list[str] = []
    volume_ratio = _to_float(volume_change_ratio)
    if volume_ratio is not None:
        if volume_ratio >= 1.5:
            score += 8
        elif volume_ratio < 0.5:
            score -= 5
    warning = "" if _to_float(close) is not None else "動能資料不足，採中性分數"
    return _clamp_score(score), warning, flags


def build_final_comment(
    final_score: float,
    fundamental_score: float,
    valuation_score: float,
    momentum_score: float,
    news_score: float,
    warnings: list[str],
) -> str:
    if warnings:
        return "資料不足，僅能依技術面判斷"
    if news_score < -30:
        return "新聞偏負面，暫不列入高優先候選"
    if momentum_score >= 70 and valuation_score < 45:
        return "技術面偏強，但估值偏高，不建議追高"
    if momentum_score >= 70 and fundamental_score >= 60:
        return "基本面與動能同步轉強，可列入優先觀察"
    if final_score >= 60:
        return "動能轉強，基本面普通，適合觀察"
    return "綜合條件普通，維持觀察"


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
