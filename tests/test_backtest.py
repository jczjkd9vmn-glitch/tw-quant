from __future__ import annotations

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
