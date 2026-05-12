from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_candidate_scores
from tw_quant.fundamental.revenue import score_revenue_for_symbols
from tw_quant.reporting.export import export_latest_candidates


def test_revenue_yoy_above_20_adds_score(tmp_path: Path) -> None:
    path = _write_revenue(tmp_path, [5, 8, 25])

    result = score_revenue_for_symbols(["2330"], path).iloc[0]

    assert result["fundamental_score"] > 50
    assert "月營收年增率大於 20%" in result["fundamental_reason"]


def test_three_positive_yoy_months_adds_score(tmp_path: Path) -> None:
    path = _write_revenue(tmp_path, [3, 4, 5])

    result = score_revenue_for_symbols(["2330"], path).iloc[0]

    assert result["fundamental_score"] > 50
    assert "連續 3 個月為正" in result["fundamental_reason"]


def test_negative_yoy_penalizes_score(tmp_path: Path) -> None:
    path = _write_revenue(tmp_path, [-5, -8, -25])

    result = score_revenue_for_symbols(["2330"], path).iloc[0]

    assert result["fundamental_score"] < 50
    assert "月營收年增率低於 -20%" in result["fundamental_reason"]


def test_missing_fundamental_data_is_neutral(tmp_path: Path) -> None:
    result = score_revenue_for_symbols(["2330"], tmp_path / "missing.csv").iloc[0]

    assert result["fundamental_score"] == 50
    assert result["fundamental_reason"] == "基本面資料不足，採中性分數"


def test_export_fundamental_fields_do_not_change_order_or_total_score(tmp_path: Path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    save_candidate_scores(engine, _scores())
    revenue_path = _write_revenue(tmp_path, [25, 26, 27])

    result = export_latest_candidates(engine, output_dir=tmp_path, revenue_path=revenue_path)

    assert list(result.candidates["stock_id"]) == ["2330", "2317"]
    assert list(result.candidates["total_score"]) == [90.0, 88.0]
    assert "fundamental_reason" in result.candidates.columns
    assert "revenue_yoy" in result.candidates.columns


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


def _scores() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _score("2330", "台積電", 90.0, 1),
            _score("2317", "鴻海", 88.0, 1),
        ]
    )


def _score(symbol: str, name: str, total_score: float, is_candidate: int) -> dict:
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
        "is_candidate": is_candidate,
        "risk_pass": 1,
        "risk_reasons": "通過風控",
        "data_quality_status": "OK",
    }
