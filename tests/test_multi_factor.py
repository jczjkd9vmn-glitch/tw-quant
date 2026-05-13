from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.chips.institutional import score_institutional_for_symbols
from tw_quant.data.database import create_db_engine, init_db, save_candidate_scores
from tw_quant.events.material_events import score_material_events_for_symbols
from tw_quant.fundamental.financials import score_financials_for_symbols
from tw_quant.fundamental.valuation import score_valuation_for_symbols
from tw_quant.reporting.export import export_latest_candidates
from tw_quant.trading.paper import run_paper_trade


def test_missing_multi_factor_data_does_not_fail_and_writes_status(tmp_path: Path) -> None:
    engine = _engine_with_scores()

    result = export_latest_candidates(
        engine,
        output_dir=tmp_path,
        revenue_path=tmp_path / "missing_monthly_revenue.csv",
        valuation_path=tmp_path / "missing_valuation.csv",
        financials_path=tmp_path / "missing_financials.csv",
        events_path=tmp_path / "missing_material_events.csv",
        institutional_path=tmp_path / "missing_institutional.csv",
    )

    assert result.warning == ""
    assert result.data_fetch_status_path is not None
    assert result.data_fetch_status_path.exists()
    assert set(result.data_fetch_status["status"]) == {"MISSING"}
    row = result.candidates.iloc[0]
    assert row["revenue_score"] == 50.0
    assert row["valuation_score"] == 50.0
    assert row["financial_score"] == 50.0
    assert row["event_score"] == 50.0
    assert row["institutional_score"] == 50.0
    assert "multi_factor_score" in result.candidates.columns


def test_revenue_yoy_above_20_adds_score(tmp_path: Path) -> None:
    revenue_path = _write_revenue(tmp_path, [5, 8, 25])
    result = export_latest_candidates(_engine_with_scores(), output_dir=tmp_path, revenue_path=revenue_path)

    row = result.candidates[result.candidates["stock_id"] == "2330"].iloc[0]
    assert row["revenue_score"] > 50
    assert "月營收年增率大於 20%" in row["revenue_reason"]


def test_pe_too_high_penalizes_valuation(tmp_path: Path) -> None:
    path = tmp_path / "valuation.csv"
    pd.DataFrame(
        [{"stock_id": "2330", "stock_name": "台積電", "financial_quarter": "2026Q1", "pe_ratio": 55, "pb_ratio": 2, "dividend_yield": 1}]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_valuation_for_symbols(["2330"], path).iloc[0]

    assert row["valuation_score"] < 50
    assert "PE 過高" in row["valuation_reason"]


def test_roe_above_10_adds_financial_score(tmp_path: Path) -> None:
    path = tmp_path / "financials.csv"
    pd.DataFrame(
        [{"stock_id": "2330", "stock_name": "台積電", "financial_quarter": "2026Q1", "eps": 8, "eps_yoy": 5, "roe": 15}]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_financials_for_symbols(["2330"], path).iloc[0]

    assert row["financial_score"] > 50
    assert "ROE 大於 10%" in row["financial_reason"]


def test_negative_material_event_blocks_entry(tmp_path: Path) -> None:
    path = tmp_path / "material_events.csv"
    pd.DataFrame(
        [{"event_date": "2026-05-08", "stock_id": "2330", "stock_name": "台積電", "title": "公告發生重大資安事件", "summary": "資安事件影響營運"}]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_material_events_for_symbols(["2330"], path).iloc[0]

    assert bool(row["event_blocked"]) is True
    assert row["event_risk_level"] == "HIGH"


def test_investment_trust_consecutive_buy_adds_score(tmp_path: Path) -> None:
    path = tmp_path / "institutional.csv"
    pd.DataFrame(
        [
            _institutional_row("2026-05-06", 10, 20, 1),
            _institutional_row("2026-05-07", 11, 21, 1),
            _institutional_row("2026-05-08", 12, 22, 1),
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")

    row = score_institutional_for_symbols(["2330"], path).iloc[0]

    assert row["institutional_score"] > 50
    assert "投信連買" in row["institutional_reason"]


def test_multi_factor_does_not_change_default_ranking_or_risk_pass(tmp_path: Path) -> None:
    engine = _engine_with_scores()
    events_path = tmp_path / "material_events.csv"
    pd.DataFrame(
        [{"event_date": "2026-05-08", "stock_id": "2330", "stock_name": "台積電", "title": "重大訴訟", "summary": "訴訟風險"}]
    ).to_csv(events_path, index=False, encoding="utf-8-sig")

    result = export_latest_candidates(engine, output_dir=tmp_path, events_path=events_path)

    assert list(result.candidates["stock_id"]) == ["2330", "2317"]
    assert list(result.candidates["total_score"]) == [90.0, 88.0]
    assert list(result.candidates["risk_pass"]) == [1, 1]
    blocked = result.candidates[result.candidates["stock_id"] == "2330"].iloc[0]
    assert bool(blocked["event_blocked"]) is True


def test_high_risk_event_blocks_new_pending_order(tmp_path: Path) -> None:
    engine = _engine_with_scores()
    events_path = tmp_path / "material_events.csv"
    pd.DataFrame(
        [{"event_date": "2026-05-08", "stock_id": "2330", "stock_name": "台積電", "title": "內控缺失", "summary": "重大內控缺失"}]
    ).to_csv(events_path, index=False, encoding="utf-8-sig")
    export_latest_candidates(engine, output_dir=tmp_path, events_path=events_path)

    result = run_paper_trade(reports_dir=tmp_path, capital=1_000_000)

    assert len(result.pending_orders) == 1
    assert "2330" not in set(result.pending_orders["stock_id"].astype(str))
    assert "2317" in set(result.pending_orders["stock_id"].astype(str))


def _engine_with_scores():
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    save_candidate_scores(engine, _scores())
    return engine


def _scores() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _score("2330", "台積電", 90.0),
            _score("2317", "鴻海", 88.0),
        ]
    )


def _score(symbol: str, name: str, total_score: float) -> dict:
    return {
        "trade_date": "2026-05-08",
        "symbol": symbol,
        "name": name,
        "close": 100.0,
        "total_score": total_score,
        "trend_score": 80.0,
        "momentum_score": 80.0,
        "fundamental_score": 50.0,
        "chip_score": 50.0,
        "risk_score": 80.0,
        "buy_reasons": "測試",
        "stop_loss": 92.0,
        "suggested_position_pct": 0.1,
        "is_candidate": 1,
        "risk_pass": 1,
        "risk_reasons": "通過風控",
        "data_quality_status": "OK",
    }


def _write_revenue(path: Path, yoy_values: list[float]) -> Path:
    rows = []
    for index, yoy in enumerate(yoy_values, start=1):
        rows.append(
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "year_month": f"2026{index:02d}",
                "revenue": 1000 + index,
                "revenue_yoy": yoy,
                "revenue_mom": 2.5,
                "accumulated_revenue": 3000 + index,
                "accumulated_revenue_yoy": 12.0,
            }
        )
    revenue_path = path / "monthly_revenue.csv"
    pd.DataFrame(rows).to_csv(revenue_path, index=False, encoding="utf-8-sig")
    return revenue_path


def _institutional_row(trade_date: str, foreign: float, trust: float, dealer: float) -> dict:
    total = foreign + trust + dealer
    return {
        "trade_date": trade_date,
        "stock_id": "2330",
        "stock_name": "台積電",
        "foreign_net_buy": foreign,
        "investment_trust_net_buy": trust,
        "dealer_net_buy": dealer,
        "institutional_total_net_buy": total,
    }
