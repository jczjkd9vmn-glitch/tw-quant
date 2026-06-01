from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.market_intel.providers.mock_provider import MockMarketIntelProvider
from tw_quant.market_intel.report import (
    _cache_has_recomputed_data_gap_warnings,
    _context_from_candidate,
    build_market_intel_report,
)
from tw_quant.market_intel.scoring import build_market_context, score_news_sentiment
from tw_quant.reporting.data_quality import build_data_quality_health


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
        chip_score=60,
        credit_score=60,
        event_risk_score=70,
        liquidity_score=70,
        latest_news_titles=["營收創高"],
        data_source="best_effort",
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
    assert status.iloc[0]["status"] == "OK_WITH_FALLBACK"
    assert cached.iloc[0]["data_freshness_level"] == "CURRENT"


def test_market_intel_non_trading_fallback_is_current_not_stale(tmp_path: Path) -> None:
    candidates = _candidates()

    result, status = build_market_intel_report(
        candidates,
        tmp_path,
        "2026-05-29",
        config={"cache_enabled": False},
        requested_date="2026-05-30",
        fallback_date="2026-05-29",
        fallback_reason="no trading data",
    )

    row = result.iloc[0]
    assert row["fallback_date"] == "2026-05-29"
    assert row["cache_age_days"] == 1
    assert row["data_freshness_level"] == "CURRENT"
    assert bool(row["is_stale_data"]) is False
    assert "非交易日，使用最近交易日資料" in row["system_comment"]
    assert status.iloc[0]["fallback_reason"] == "no trading data"
    assert status.iloc[0]["data_freshness_level"] == "CURRENT"


def test_market_intel_non_trading_fallback_within_threshold_is_not_stale(tmp_path: Path) -> None:
    candidates = _candidates()

    result, status = build_market_intel_report(
        candidates,
        tmp_path,
        "2026-05-29",
        config={"cache_enabled": False},
        requested_date="2026-05-31",
        fallback_date="2026-05-29",
        fallback_reason="no trading data",
    )

    row = result.iloc[0]
    assert row["cache_age_days"] == 2
    assert row["data_freshness_level"] == "CURRENT"
    assert bool(row["is_stale_data"]) is False
    assert "非交易日，使用最近交易日資料" in row["system_comment"]
    assert status.iloc[0]["data_freshness_level"] == "CURRENT"
    assert bool(status.iloc[0]["is_stale"]) is False


def test_market_intel_cache_age_over_threshold_is_stale(tmp_path: Path) -> None:
    candidates = _candidates()

    result, status = build_market_intel_report(
        candidates,
        tmp_path,
        "2026-05-29",
        config={"cache_enabled": False, "stale_days_threshold": 2},
        requested_date="2026-06-01",
        fallback_date="2026-05-29",
        fallback_reason="no trading data",
    )

    row = result.iloc[0]
    assert row["cache_age_days"] == 3
    assert row["data_freshness_level"] == "STALE"
    assert bool(row["is_stale_data"]) is True
    assert "資料來源缺失或快取資料" not in str(row["market_intel_warning"])
    assert "市場資料過期，暫不建立買進候選" in row["system_comment"]
    assert bool(status.iloc[0]["is_stale"]) is True
    assert "市場資料過期，不建議短線進場" in status.iloc[0]["warning"]


def test_market_intel_non_trading_fallback_is_not_data_quality_warning() -> None:
    status = pd.DataFrame(
        [
            {
                "source_name": "market_intel",
                "status": "OK_WITH_FALLBACK",
                "rows": 20,
                "fallback_reason": "no trading data",
                "fallback_action": "cache",
                "data_freshness_level": "CURRENT",
            }
        ]
    )

    health = build_data_quality_health(pd.DataFrame(), status)
    row = health[health["source_name"] == "market_intel"].iloc[0]

    assert row["health_status"] == "OK"
    assert "非交易日，使用最近交易日資料" in row["review_reason"]


def test_market_intel_recomputes_candidate_data_warnings_after_merge() -> None:
    row = _candidates().iloc[0]
    provider_context = build_market_context(
        symbol="2330",
        date="2026-05-08",
        warning_message="市場事件資料不足，採中性處理；基本面資料不足，採中性分數；估值資料不足，採中性分數；價格資料不足，動能採中性分數",
        data_source="mixed",
    )

    context = _context_from_candidate(row, provider_context, "2026-05-08")

    assert "市場事件資料不足，採中性處理" in context.warning_message
    assert "基本面資料不足，採中性分數" not in context.warning_message
    assert "估值資料不足，採中性分數" not in context.warning_message
    assert "價格資料不足，動能採中性分數" not in context.warning_message


def test_market_intel_accepts_cache_when_only_some_rows_have_recomputed_warnings() -> None:
    legacy_cache = pd.DataFrame(
        {
            "market_intel_warning": [
                "基本面資料不足，採中性分數",
                "估值資料不足，採中性分數",
            ]
        }
    )
    current_cache = pd.DataFrame(
        {
            "market_intel_warning": [
                "市場事件資料不足，採中性處理",
                "估值資料不足，採中性分數",
                "",
            ]
        }
    )

    assert _cache_has_recomputed_data_gap_warnings(legacy_cache) is True
    assert _cache_has_recomputed_data_gap_warnings(current_cache) is False


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
