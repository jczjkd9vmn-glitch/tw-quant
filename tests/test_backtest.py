from __future__ import annotations

from typing import Any

import pandas as pd

from tests.test_scoring import make_prices
from tw_quant.backtest.engine import BacktestConfig, BacktestEngine
from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


def test_backtest_returns_equity_curve_and_metrics() -> None:
    prices = make_prices(days=90)
    manager = RiskManager(
        RiskConfig(min_liquidity_value=1_000, max_volatility_20=0.20, max_position_pct=0.20)
    )
    scorer = StockScorer(
        ScoringConfig(minimum_total_score=65, min_history_days=35, max_candidates=1),
        risk_manager=manager,
    )
    engine = BacktestEngine(
        BacktestConfig(initial_cash=1_000_000, top_n=1, max_holding_days=5),
        scorer=scorer,
        risk_manager=manager,
    )

    result = engine.run(prices)

    assert not result.equity_curve.empty
    assert pd.api.types.is_datetime64_any_dtype(result.equity_curve["trade_date"])
    assert "total_return" in result.metrics
    assert "max_drawdown" in result.metrics
    assert result.metrics["trades"] >= 0


def test_backtest_gap_down_below_stop_exits_at_open_not_stop() -> None:
    prices = pd.DataFrame(
        [
            _price("2026-05-08", 100, 101, 99, 100),
            _price("2026-05-09", 100, 103, 95, 102),
            _price("2026-05-10", 80, 82, 75, 81),
        ]
    )
    manager = RiskManager(RiskConfig(min_liquidity_value=0, max_volatility_20=1.0, max_position_pct=0.50))
    engine = BacktestEngine(
        BacktestConfig(initial_cash=100_000, top_n=1, transaction_cost_bps=0, max_holding_days=10),
        scorer=_SingleSignalScorer("2026-05-08"),
        risk_manager=manager,
    )

    result = engine.run(prices)

    sells = result.trades[result.trades["side"] == "SELL"]
    assert len(sells) == 1
    sell = sells.iloc[0]
    assert sell["trade_date"] == pd.Timestamp("2026-05-10")
    assert sell["price"] == 80
    assert sell["reason"] == "跳空跌破停損"


def _price(trade_date: str, open_price: float, high: float, low: float, close: float) -> dict[str, Any]:
    return {
        "trade_date": trade_date,
        "symbol": "2330",
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1_000_000,
    }


class _SingleSignalScorer:
    def __init__(self, signal_date: str) -> None:
        self.signal_date = pd.Timestamp(signal_date)

    def score(self, history: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        if as_of != self.signal_date:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "symbol": "2330",
                    "close": 100.0,
                    "total_score": 90.0,
                    "is_candidate": True,
                    "risk_pass": True,
                    "stop_loss": 90.0,
                    "suggested_position_pct": 0.50,
                    "buy_reasons": "test signal",
                    "data_quality_status": "OK",
                }
            ]
        )
