from __future__ import annotations

import pandas as pd
import pytest

from tw_quant.data.exceptions import DataQualityError
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.trading.simulator import RealBroker, SimulatedBroker


def valid_price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2025-01-02",
                "symbol": "2330",
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "volume": 1_000_000,
            }
        ]
    )


def test_price_data_quality_rejects_invalid_ohlc() -> None:
    frame = valid_price_frame()
    frame.loc[0, "high"] = 90
    with pytest.raises(DataQualityError):
        RiskManager().validate_price_data(frame)


def test_real_broker_is_disabled() -> None:
    with pytest.raises(RuntimeError, match="禁止真實下單"):
        RealBroker().place_order({})


def test_simulated_broker_requires_risk_pass_and_reason() -> None:
    manager = RiskManager(RiskConfig(min_liquidity_value=1_000, max_volatility_20=0.20))
    signal = {
        "trade_date": pd.Timestamp("2025-01-02"),
        "symbol": "2330",
        "close": 100.0,
        "stop_loss": 92.0,
        "suggested_position_pct": 0.05,
        "liquidity_value": 100_000_000,
        "volatility_20": 0.02,
        "data_quality_status": "OK",
        "buy_reasons": "收盤價高於 20 日均線",
    }
    decision = manager.evaluate_candidate(signal)
    order = SimulatedBroker(manager).place_order(signal, quantity=1000, price=100, risk_decision=decision)

    assert decision.allowed
    assert order.status == "SIMULATED"
    assert order.reason
