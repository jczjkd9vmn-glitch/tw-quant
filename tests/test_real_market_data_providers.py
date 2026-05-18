from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data_sources.mops_provider import (
    MOPSProvider,
    normalize_financials_openapi,
    normalize_material_events_openapi,
    normalize_monthly_revenue_openapi,
)
from tw_quant.data_sources.twse_provider import normalize_valuation_openapi
from tw_quant.market_intel.providers.real_provider import RealMarketIntelProvider


def test_real_market_intel_uses_material_events_not_mock(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    pd.DataFrame(
        [
            {
                "event_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "title": "接單增加",
                "summary": "營收成長",
                "event_type": "material_event",
                "event_sentiment": "POSITIVE",
                "event_risk_level": "LOW",
            }
        ]
    ).to_csv(data_dir / "material_events.csv", index=False, encoding="utf-8-sig")

    context = RealMarketIntelProvider(data_dir=data_dir).fetch(["2330"], as_of="2026-05-08")[0]

    assert context.data_source != "mock"
    assert context.news_sentiment_score > 0
    assert context.final_market_score != 50.0


def test_real_market_intel_attention_disposition_adds_risk_flags(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "is_attention_stock": True,
                "attention_reason": "短線波動過大",
                "is_disposition_stock": False,
                "disposition_start_date": "",
                "disposition_end_date": "",
                "disposition_reason": "",
            }
        ]
    ).to_csv(data_dir / "attention_disposition.csv", index=False, encoding="utf-8-sig")

    context = RealMarketIntelProvider(data_dir=data_dir).fetch(["2330"], as_of="2026-05-08")[0]

    assert context.data_source != "mock"
    assert "注意股" in context.risk_flags
    assert "注意股" in context.final_comment


def test_real_market_intel_uses_mock_only_without_real_or_fallback_data(tmp_path: Path) -> None:
    context = RealMarketIntelProvider(data_dir=tmp_path / "missing").fetch(["2330"], as_of="2026-05-08")[0]

    assert context.data_source == "mock"
    assert context.confidence_score < 50


def test_monthly_revenue_openapi_latest_available_month_scores_non_neutral() -> None:
    payload = [
        {
            "資料年月": "202604",
            "公司代號": "2330",
            "公司名稱": "台積電",
            "營業收入-當月營收": "100000",
            "營業收入-上月比較增減(%)": "3.5",
            "營業收入-去年同月增減(%)": "25.0",
            "累計營業收入-當月累計營收": "400000",
            "累計營業收入-前期比較增減(%)": "15.0",
        }
    ]

    frame = normalize_monthly_revenue_openapi(payload, requested_month="202605")

    assert frame.iloc[0]["revenue_source_status"] == "OK_WITH_FALLBACK"
    assert frame.iloc[0]["latest_available_month"] == "202604"
    assert frame.iloc[0]["revenue_yoy"] == 25.0


def test_mops_monthly_revenue_result_marks_recent_available_month() -> None:
    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [
                {
                    "資料年月": "202604",
                    "公司代號": "2330",
                    "公司名稱": "台積電",
                    "營業收入-當月營收": "100000",
                    "營業收入-去年同月增減(%)": "25.0",
                }
            ]

    provider = MOPSProvider(requester=lambda *_args, **_kwargs: Response(), cache_enabled=False)

    result = provider.fetch_monthly_revenue("20260515")

    assert result.status == "OK_WITH_FALLBACK"
    assert result.latest_available_period == "202604"
    assert result.data.iloc[0]["revenue_source_status"] == "OK_WITH_FALLBACK"


def test_valuation_openapi_calculates_real_fields() -> None:
    frame = normalize_valuation_openapi(
        [{"Date": "1150515", "Code": "2330", "Name": "台積電", "PEratio": "18.5", "PBratio": "2.1", "DividendYield": "2.5"}]
    )

    assert frame.iloc[0]["stock_id"] == "2330"
    assert frame.iloc[0]["date"] == "2026-05-15"
    assert frame.iloc[0]["pe_ratio"] == 18.5
    assert frame.iloc[0]["valuation_source_status"] == "OK"


def test_financials_openapi_calculates_margin_and_eps() -> None:
    frame = normalize_financials_openapi(
        [
            {
                "公司代號": "2330",
                "公司名稱": "台積電",
                "年度": "115",
                "季別": "1",
                "營業收入": "1000",
                "營業毛利（毛損）": "550",
                "營業利益（損失）": "450",
                "本期淨利（淨損）": "400",
                "基本每股盈餘（元）": "8.0",
            }
        ]
    )

    assert frame.iloc[0]["eps"] == 8.0
    assert frame.iloc[0]["gross_margin"] == 55.0
    assert frame.iloc[0]["financial_source_status"] == "OK"


def test_material_events_openapi_keyword_sentiment() -> None:
    frame = normalize_material_events_openapi(
        [{"發言日期": "115/05/15", "公司代號": "2330", "公司名稱": "台積電", "主旨 ": "接單增加", "說明": "營收成長"}],
        "20260515",
    )

    assert frame.iloc[0]["event_sentiment"] == "POSITIVE"
    assert frame.iloc[0]["event_risk_level"] == "LOW"
