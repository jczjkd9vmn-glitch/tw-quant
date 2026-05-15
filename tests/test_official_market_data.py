from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from scripts.send_daily_notification import build_notification_message
from tw_quant.chips.institutional import score_institutional_for_symbols
from tw_quant.config import load_config
from tw_quant.data_sources.cache import read_cache, write_cache
from tw_quant.data_sources.twse_provider import TWSEProvider
from tw_quant.events.material_events import score_material_events_for_symbols
from tw_quant.fundamental.revenue import score_revenue_for_symbols
from tw_quant.scoring.multi_factor import apply_multi_factor_scores, calculate_final_market_score
from tw_quant.scoring.official_factors import (
    score_attention_disposition_for_symbols,
    score_credit_for_symbols,
    score_liquidity_for_symbols,
    score_sector_strength_for_symbols,
)


def test_institutional_missing_data_does_not_crash(tmp_path: Path) -> None:
    row = score_institutional_for_symbols(["2330"], tmp_path / "missing.csv").iloc[0]

    assert row["institutional_score"] == 50.0
    assert "中性" in row["institutional_reason"]


def test_foreign_and_investment_trust_buying_raise_chip_score(tmp_path: Path) -> None:
    path = tmp_path / "institutional.csv"
    pd.DataFrame(
        [
            _institutional("2026-05-06", 100, 200, 0, volume=1000),
            _institutional("2026-05-07", 120, 210, 0, volume=1000),
            _institutional("2026-05-08", 130, 220, 0, volume=1000),
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_institutional_for_symbols(["2330"], path).iloc[0]

    assert row["institutional_score"] > 50
    assert row["foreign_buy_days"] == 3
    assert row["investment_trust_buy_days"] == 3


def test_institutional_consecutive_selling_lowers_score(tmp_path: Path) -> None:
    path = tmp_path / "institutional.csv"
    pd.DataFrame(
        [
            _institutional("2026-05-06", -100, -200, 0),
            _institutional("2026-05-07", -120, -210, 0),
            _institutional("2026-05-08", -130, -220, 0),
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_institutional_for_symbols(["2330"], path).iloc[0]

    assert row["institutional_score"] < 50
    assert "投信連賣" in row["institutional_warning"]


def test_margin_surge_without_price_gain_adds_risk_flag(tmp_path: Path) -> None:
    path = tmp_path / "margin_short.csv"
    pd.DataFrame([_credit(margin_balance=10_000, margin_change=2_000)]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )
    context = pd.DataFrame([{"stock_id": "2330", "return_5": 0.0, "total_institutional_net_buy": -100}])

    row = score_credit_for_symbols(["2330"], path, context).iloc[0]

    assert row["credit_score"] < 50
    assert "融資暴增但法人賣超" in row["credit_risk_flags"]


def test_lending_sell_surge_adds_risk_flag(tmp_path: Path) -> None:
    path = tmp_path / "margin_short.csv"
    pd.DataFrame([_credit(securities_lending_sell_volume=5_000, securities_lending_balance=10_000)]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )

    row = score_credit_for_symbols(["2330"], path).iloc[0]

    assert row["credit_score"] < 50
    assert "借券賣出暴增" in row["credit_risk_flags"]


def test_attention_stock_adds_risk_flag(tmp_path: Path) -> None:
    path = tmp_path / "attention_disposition.csv"
    pd.DataFrame([_attention(is_attention_stock=True, attention_reason="週轉率過高")]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )

    row = score_attention_disposition_for_symbols(["2330"], path).iloc[0]

    assert bool(row["is_attention_stock"]) is True
    assert "注意股" in row["event_risk_flags"]
    assert bool(row["event_blocked"]) is False


def test_disposition_stock_blocks_by_default(tmp_path: Path) -> None:
    path = tmp_path / "attention_disposition.csv"
    pd.DataFrame([_attention(is_disposition_stock=True, disposition_reason="處置交易")]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )

    row = score_attention_disposition_for_symbols(["2330"], path, {}).iloc[0]

    assert bool(row["event_blocked"]) is True
    assert row["event_risk_level"] == "HIGH"


def test_revenue_positive_yoy_raises_fundamental_score(tmp_path: Path) -> None:
    path = _write_revenue(tmp_path, [1, 2, 3])

    row = score_revenue_for_symbols(["2330"], path).iloc[0]

    assert row["fundamental_score"] > 50


def test_revenue_decline_lowers_fundamental_score(tmp_path: Path) -> None:
    path = _write_revenue(tmp_path, [-1, -2, -25])

    row = score_revenue_for_symbols(["2330"], path).iloc[0]

    assert row["fundamental_score"] < 50
    assert "衰退" in row["revenue_warning"]


def test_negative_event_lowers_event_risk_score(tmp_path: Path) -> None:
    path = tmp_path / "material_events.csv"
    pd.DataFrame(
        [{"event_date": "2026-05-08", "stock_id": "2330", "stock_name": "台積電", "title": "檢調調查", "summary": "涉及訴訟"}]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_material_events_for_symbols(["2330"], path).iloc[0]

    assert row["event_risk_score"] < 35
    assert bool(row["event_blocked"]) is True


def test_low_liquidity_lowers_liquidity_score(tmp_path: Path) -> None:
    path = tmp_path / "liquidity.csv"
    pd.DataFrame([{"trade_date": "2026-05-08", "stock_id": "2330", "avg_volume_20d": 100, "avg_turnover_20d": 10_000_000}]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )

    row = score_liquidity_for_symbols(["2330"], path).iloc[0]

    assert row["liquidity_score"] < 40
    assert "不建議進場" in row["liquidity_warning"]


def test_sector_relative_strength_raises_score(tmp_path: Path) -> None:
    path = tmp_path / "sector_strength.csv"
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "industry": "半導體",
                "stock_return_5d": 0.05,
                "stock_return_20d": 0.20,
                "market_return_5d": 0.01,
                "market_return_20d": 0.05,
                "sector_return_5d": 0.03,
                "sector_return_20d": 0.10,
                "relative_strength_5d": 0.04,
                "relative_strength_20d": 0.15,
                "sector_strength_rank": 10,
            }
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_sector_strength_for_symbols(["2330"], path).iloc[0]

    assert row["sector_strength_score"] > 50


def test_final_market_score_uses_official_factor_weights() -> None:
    row = pd.Series(
        {
            "momentum_score": 80,
            "institutional_score": 70,
            "fundamental_score": 60,
            "valuation_score": 50,
            "sector_strength_score": 60,
            "event_risk_score": 40,
            "liquidity_score": 80,
            "news_sentiment_score": 20,
        }
    )

    assert calculate_final_market_score(row) == 65.0


def test_confidence_drops_when_sources_missing(tmp_path: Path) -> None:
    result = apply_multi_factor_scores(_candidates(), data_dir=tmp_path)

    assert "MISSING" in result.candidates.iloc[0]["data_source_warning"]


def test_html_report_shows_official_fields(tmp_path: Path) -> None:
    _write_html_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "信用健康分數" in html
    assert "事件風險健康分數" in html
    assert "流動性分數" in html
    assert "產業相對強弱分數" in html


def test_discord_summary_shows_official_digest(tmp_path: Path) -> None:
    _write_html_reports(tmp_path)
    summary = pd.read_csv(tmp_path / "daily_summary_20260508.csv").iloc[0].to_dict()

    message = build_notification_message(summary, reports_dir=tmp_path, pages_url="https://example.test")

    assert "今日法人偏多股票前 5 名" in message
    assert "事件風險最高股票前 5 名" in message
    assert "處置股 / 注意股提示" in message
    assert "資料來源失敗警告" in message


def test_twse_provider_failure_does_not_raise(tmp_path: Path) -> None:
    def broken_request(*_args, **_kwargs):
        raise TimeoutError("timeout")

    result = TWSEProvider(requester=broken_request, cache_dir=tmp_path).fetch_institutional("20260508")

    assert result.status == "FAILED"
    assert result.data.empty


def test_cache_read_write_round_trip(tmp_path: Path) -> None:
    frame = pd.DataFrame([{"stock_id": "2330", "value": 1}])
    write_cache(tmp_path, "institutional", "20260508", frame)

    cached, warning = read_cache(tmp_path, "institutional", "20260508", ["stock_id", "value"])

    assert warning == ""
    assert cached is not None
    assert cached.iloc[0]["stock_id"] == "2330"


def test_damaged_cache_returns_warning(tmp_path: Path) -> None:
    cache_file = tmp_path / "institutional_20260508.json"
    cache_file.write_text("{bad json", encoding="utf-8")

    cached, warning = read_cache(tmp_path, "institutional", "20260508", ["stock_id"])

    assert cached is None
    assert "cache damaged" in warning


def test_market_intel_affect_trading_default_false() -> None:
    assert load_config()["market_intel"]["affect_trading"] is False


def _institutional(trade_date: str, foreign: float, trust: float, dealer: float, volume: float = 10_000) -> dict:
    return {
        "trade_date": trade_date,
        "stock_id": "2330",
        "stock_name": "台積電",
        "foreign_net_buy": foreign,
        "investment_trust_net_buy": trust,
        "dealer_net_buy": dealer,
        "total_institutional_net_buy": foreign + trust + dealer,
        "volume": volume,
    }


def _credit(**overrides) -> dict:
    row = {
        "trade_date": "2026-05-08",
        "stock_id": "2330",
        "stock_name": "台積電",
        "margin_balance": 10_000,
        "margin_change": 0,
        "short_balance": 100,
        "short_change": 0,
        "securities_lending_sell_volume": 0,
        "securities_lending_balance": 10_000,
    }
    row.update(overrides)
    return row


def _attention(**overrides) -> dict:
    row = {
        "trade_date": "2026-05-08",
        "stock_id": "2330",
        "stock_name": "台積電",
        "is_attention_stock": False,
        "attention_reason": "",
        "is_disposition_stock": False,
        "disposition_start_date": "",
        "disposition_end_date": "",
        "disposition_reason": "",
    }
    row.update(overrides)
    return row


def _write_revenue(path: Path, yoy_values: list[float]) -> Path:
    rows = []
    for index, yoy in enumerate(yoy_values, start=1):
        rows.append(
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "year_month": f"2026{index:02d}",
                "revenue": 1000 + index,
                "monthly_revenue": 1000 + index,
                "revenue_yoy": yoy,
                "revenue_mom": 1.0,
                "accumulated_revenue": 3000 + index,
                "accumulated_revenue_yoy": 12.0,
            }
        )
    revenue_path = path / "monthly_revenue.csv"
    pd.DataFrame(rows).to_csv(revenue_path, index=False, encoding="utf-8-sig")
    return revenue_path


def _candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 100,
                "total_score": 80,
                "risk_pass": 1,
                "risk_reason": "",
                "reason": "",
            }
        ]
    )


def _write_html_reports(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "candidate_rows": 1,
                "risk_pass_rows": 1,
                "pending_orders": 0,
                "open_positions": 0,
                "closed_positions": 0,
                "status": "OK",
            }
        ]
    ).to_csv(path / "daily_summary_20260508.csv", index=False, encoding="utf-8-sig")
    candidate = pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 100,
                "total_score": 80,
                "institutional_score": 80,
                "credit_score": 70,
                "event_risk_score": 30,
                "liquidity_score": 40,
                "sector_strength_score": 65,
                "final_market_score": 70,
                "confidence_score": 80,
                "risk_flags": "注意股",
                "data_source_warning": "margin_short:MISSING",
                "system_comment": "有風險標籤，需降低優先度",
                "risk_pass": 1,
            }
        ]
    )
    candidate.to_csv(path / "candidates_20260508.csv", index=False, encoding="utf-8-sig")
    candidate.to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")
