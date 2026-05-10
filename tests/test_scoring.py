from __future__ import annotations

import numpy as np
import pandas as pd

from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


def make_prices(days: int = 80) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=days)
    rows = []
    for i, trade_date in enumerate(dates):
        strong_close = 50 + i * 0.8
        weak_close = 80 - i * 0.2
        rows.append(
            {
                "trade_date": trade_date,
                "symbol": "2330",
                "name": "台積電",
                "open": strong_close * 0.99,
                "high": strong_close * 1.02,
                "low": strong_close * 0.98,
                "close": strong_close,
                "volume": 2_000_000 + i * 1_000,
                "turnover": strong_close * (2_000_000 + i * 1_000),
                "market": "TSE",
                "source": "TEST",
                "pe_ratio": 12,
                "pb_ratio": 1.8,
                "dividend_yield": 3.5,
                "revenue_yoy": 20,
                "foreign_net_buy": 1000,
                "investment_trust_net_buy": 500,
                "dealer_net_buy": 100,
                "margin_balance_change": -10,
            }
        )
        rows.append(
            {
                "trade_date": trade_date,
                "symbol": "9999",
                "name": "弱勢股",
                "open": weak_close * 1.01,
                "high": weak_close * 1.03,
                "low": weak_close * 0.95,
                "close": weak_close,
                "volume": 30_000,
                "turnover": weak_close * 30_000,
                "market": "TSE",
                "source": "TEST",
                "pe_ratio": 45,
                "pb_ratio": 8,
                "dividend_yield": 0,
                "revenue_yoy": -15,
                "foreign_net_buy": -1000,
                "investment_trust_net_buy": -500,
                "dealer_net_buy": -100,
                "margin_balance_change": 50,
            }
        )
    return pd.DataFrame(rows)


def test_scoring_ranks_stronger_stock_higher() -> None:
    risk_manager = RiskManager(
        RiskConfig(min_liquidity_value=1_000, max_volatility_20=0.20, max_position_pct=0.20)
    )
    scorer = StockScorer(
        ScoringConfig(minimum_total_score=65, min_history_days=40, max_candidates=5),
        risk_manager=risk_manager,
    )

    scores = risk_manager.apply_candidate_controls(scorer.score(make_prices()))
    strong = scores[scores["symbol"] == "2330"].iloc[0]
    weak = scores[scores["symbol"] == "9999"].iloc[0]

    assert strong["total_score"] > weak["total_score"]
    assert strong["is_candidate"]
    assert strong["risk_pass"]
    assert strong["stop_loss"] < strong["close"]
    assert "收盤價高於 20 日均線" in strong["buy_reasons"]
    assert not scores["buy_reasons"].isna().any()


def test_scores_are_bounded() -> None:
    scorer = StockScorer(ScoringConfig(min_history_days=40))
    scores = scorer.score(make_prices())
    score_columns = [
        "total_score",
        "trend_score",
        "momentum_score",
        "fundamental_score",
        "chip_score",
        "risk_score",
    ]
    assert np.isfinite(scores[score_columns].to_numpy()).all()
    assert ((scores[score_columns] >= 0) & (scores[score_columns] <= 100)).all().all()
