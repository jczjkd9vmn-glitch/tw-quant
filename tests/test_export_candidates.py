from __future__ import annotations

import pandas as pd

from tw_quant.data.database import create_db_engine, init_db, save_candidate_scores
from tw_quant.reporting.export import EXPORT_COLUMNS, export_latest_candidates


def test_export_latest_candidates_writes_candidate_and_risk_pass_csv(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    save_candidate_scores(engine, _scores())

    result = export_latest_candidates(engine, output_dir=tmp_path)

    assert result.warning == ""
    assert result.candidates_path is not None
    assert result.risk_pass_path is not None
    assert result.candidates_path.exists()
    assert result.risk_pass_path.exists()
    assert len(result.candidates) == 2
    assert len(result.risk_pass_candidates) == 1
    assert list(result.candidates.columns) == EXPORT_COLUMNS
    assert result.candidates.iloc[0]["rank"] == 1
    assert result.candidates.iloc[0]["stock_id"] == "2330"
    assert result.candidates.iloc[0]["reason"] == "收盤價高於 20 日均線"
    assert result.candidates.iloc[0]["risk_reason"] == "通過風控"
    assert result.candidates.iloc[0]["stop_loss_price"] == 92.0

    exported = pd.read_csv(result.candidates_path)
    risk_exported = pd.read_csv(result.risk_pass_path)
    assert list(exported.columns) == EXPORT_COLUMNS
    assert len(exported) == 2
    assert len(risk_exported) == 1
    assert risk_exported.iloc[0]["stock_id"] == 2330


def test_export_latest_candidates_warns_when_no_candidates(tmp_path) -> None:
    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)

    result = export_latest_candidates(engine, output_dir=tmp_path)

    assert result.warning == "no scoring data found"
    assert result.candidates_path is None
    assert result.risk_pass_path is None
    assert result.candidates.empty


def _scores() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "symbol": "2330",
                "name": "台積電",
                "close": 100.0,
                "total_score": 90.0,
                "trend_score": 95.0,
                "momentum_score": 90.0,
                "fundamental_score": 80.0,
                "chip_score": 70.0,
                "risk_score": 85.0,
                "buy_reasons": "收盤價高於 20 日均線",
                "stop_loss": 92.0,
                "suggested_position_pct": 0.1,
                "is_candidate": 1,
                "risk_pass": 1,
                "risk_reasons": "通過風控",
                "data_quality_status": "OK",
            },
            {
                "trade_date": "2026-05-08",
                "symbol": "2317",
                "name": "鴻海",
                "close": 150.0,
                "total_score": 88.0,
                "trend_score": 90.0,
                "momentum_score": 88.0,
                "fundamental_score": 75.0,
                "chip_score": 65.0,
                "risk_score": 80.0,
                "buy_reasons": "成交量高於 20 日均量",
                "stop_loss": 138.0,
                "suggested_position_pct": 0.0,
                "is_candidate": 1,
                "risk_pass": 0,
                "risk_reasons": "流動性不足",
                "data_quality_status": "OK",
            },
            {
                "trade_date": "2026-05-08",
                "symbol": "9999",
                "name": "非候選",
                "close": 50.0,
                "total_score": 40.0,
                "trend_score": 40.0,
                "momentum_score": 40.0,
                "fundamental_score": 50.0,
                "chip_score": 50.0,
                "risk_score": 40.0,
                "buy_reasons": "量化條件不足",
                "stop_loss": 46.0,
                "suggested_position_pct": 0.0,
                "is_candidate": 0,
                "risk_pass": 0,
                "risk_reasons": "未達候選條件",
                "data_quality_status": "OK",
            },
        ]
    )
