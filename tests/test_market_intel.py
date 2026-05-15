from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.market_intel.providers.mock_provider import MockMarketIntelProvider
from tw_quant.market_intel.report import build_market_intel_report
from tw_quant.market_intel.scoring import build_market_context, score_news_sentiment


def test_market_intel_missing_fundamental_data_does_not_crash() -> None:
    context = build_market_context(symbol="2330", date="2026-05-08")

    assert context.fundamental_score == 50.0
    assert "資料不足" in context.warning_message


def test_market_intel_high_pe_lowers_valuation_score() -> None:
    context = build_market_context(symbol="2330", date="2026-05-08", pe_ratio=55)

    assert context.valuation_score < 50
    assert "PE 偏高" in context.risk_flags


def test_market_intel_positive_news_raises_news_sentiment() -> None:
    score, keywords = score_news_sentiment(["營收創高，AI 資料中心接單增加"])

    assert score > 0
    assert "營收創高" in keywords


def test_market_intel_negative_news_lowers_news_sentiment() -> None:
    score, keywords = score_news_sentiment(["財報不如預期，毛利率下滑且訂單減少"])

    assert score < 0
    assert "財報不如預期" in keywords


def test_market_intel_final_score_uses_weights() -> None:
    context = build_market_context(
        symbol="2330",
        date="2026-05-08",
        pe_ratio=15,
        pb_ratio=1.5,
        dividend_yield=4,
        revenue_growth_yoy=20,
        eps_growth_yoy=12,
        roe=15,
        close=100,
        momentum_score_hint=80,
        latest_news_titles=["營收創高"],
    )

    assert context.final_market_score > 60
    assert context.confidence_score >= 80


def test_market_intel_provider_failure_returns_warning() -> None:
    result = MockMarketIntelProvider().fetch(["2330"], as_of="2026-05-08")

    assert result[0].symbol == "2330"
    assert "中性 mock 資料" in result[0].warning_message


def test_market_intel_filter_default_false(tmp_path: Path) -> None:
    candidates = _candidates()
    result, status = build_market_intel_report(candidates, tmp_path, "2026-05-08", config={})

    assert "final_market_score" in result.columns
    assert status.iloc[0]["source_name"] == "market_intel"
    assert status.iloc[0]["status"] in {"OK", "OK_WITH_WARNING"}


def test_market_intel_cache_is_created_and_reused(tmp_path: Path) -> None:
    candidates = _candidates()
    result, _ = build_market_intel_report(candidates, tmp_path, "2026-05-08", config={})
    cache_path = tmp_path / "cache" / "market_intel_20260508.json"

    assert cache_path.exists()
    cached, status = build_market_intel_report(candidates, tmp_path, "2026-05-08", config={})
    assert len(cached) == len(result)
    assert status.iloc[0]["status"] == "CACHE"


def test_market_intel_confidence_drops_when_data_missing() -> None:
    context = build_market_context(symbol="2330", date="2026-05-08")

    assert context.confidence_score < 100


def _candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 100,
                "momentum_score": 80,
                "pe_ratio": 15,
                "pb_ratio": 2,
                "dividend_yield": 2,
                "revenue_yoy": 22,
                "eps": 8,
                "roe": 16,
                "debt_ratio": 30,
                "event_reason": "營收創高",
            }
        ]
    )
