from __future__ import annotations

import pandas as pd

from tw_quant.risk.controls import RiskManager


def test_risk_manager_splits_technical_risk_pass_and_tradable_pass_for_high_event_risk() -> None:
    scores = pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "symbol": "2330",
                "name": "台積電",
                "close": 100.0,
                "total_score": 90.0,
                "is_candidate": True,
                "data_quality_status": "OK",
                "liquidity_value": 10_000_000,
                "volatility_20": 0.02,
                "event_risk_level": "HIGH",
                "event_blocked": True,
            }
        ]
    )

    result = RiskManager().apply_candidate_controls(scores)
    row = result.iloc[0]

    assert bool(row["technical_risk_pass"]) is True
    assert bool(row["tradable_pass"]) is False
    assert bool(row["risk_pass"]) is False
    assert "重大事件" in row["risk_reasons"]


def test_risk_manager_blocks_attention_and_disposition_from_tradable_pass() -> None:
    scores = pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "symbol": "1001",
                "name": "注意股",
                "close": 50.0,
                "total_score": 90.0,
                "is_candidate": True,
                "data_quality_status": "OK",
                "liquidity_value": 10_000_000,
                "volatility_20": 0.02,
                "is_attention_stock": True,
            },
            {
                "trade_date": "2026-06-05",
                "symbol": "1002",
                "name": "處置股",
                "close": 50.0,
                "total_score": 90.0,
                "is_candidate": True,
                "data_quality_status": "OK",
                "liquidity_value": 10_000_000,
                "volatility_20": 0.02,
                "is_disposition_stock": True,
            },
        ]
    )

    result = RiskManager().apply_candidate_controls(scores)

    assert result["technical_risk_pass"].tolist() == [True, True]
    assert result["tradable_pass"].tolist() == [False, False]
    assert result["risk_pass"].tolist() == [False, False]
