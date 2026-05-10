"""Simple event-driven backtest engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float = 1_000_000
    top_n: int = 10
    transaction_cost_bps: float = 14.25
    max_holding_days: int = 20

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> "BacktestConfig":
        return cls(
            initial_cash=float(values.get("initial_cash", cls.initial_cash)),
            top_n=int(values.get("top_n", cls.top_n)),
            transaction_cost_bps=float(values.get("transaction_cost_bps", cls.transaction_cost_bps)),
            max_holding_days=int(values.get("max_holding_days", cls.max_holding_days)),
        )


@dataclass(frozen=True)
class BacktestResult:
    equity_curve: pd.DataFrame
    trades: pd.DataFrame
    metrics: dict[str, float]


class BacktestEngine:
    """Generate signals after close and simulate entry on the next open."""

    def __init__(
        self,
        config: BacktestConfig | None = None,
        scorer: StockScorer | None = None,
        risk_manager: RiskManager | None = None,
    ):
        self.config = config or BacktestConfig()
        self.risk_manager = risk_manager or RiskManager(RiskConfig(initial_equity=self.config.initial_cash))
        self.scorer = scorer or StockScorer(
            ScoringConfig(minimum_total_score=70, min_history_days=40, max_candidates=self.config.top_n),
            risk_manager=self.risk_manager,
        )

    def run(self, prices: pd.DataFrame) -> BacktestResult:
        self.risk_manager.validate_price_data(prices)
        frame = prices.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame = frame.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
        dates = list(frame["trade_date"].drop_duplicates().sort_values())
        cash = float(self.config.initial_cash)
        positions: dict[str, dict[str, Any]] = {}
        pending_orders: list[dict[str, Any]] = []
        equity_rows: list[dict[str, Any]] = []
        trade_rows: list[dict[str, Any]] = []

        for current_date in dates:
            day = frame[frame["trade_date"] == current_date].set_index("symbol")
            equity_at_open = _portfolio_value(cash, positions, day, price_column="open")

            for order in pending_orders:
                symbol = order["symbol"]
                if symbol in positions or symbol not in day.index:
                    continue
                open_price = float(day.loc[symbol, "open"])
                if open_price <= 0:
                    continue
                target_value = equity_at_open * float(order["suggested_position_pct"])
                quantity = np.floor(target_value / open_price)
                if quantity <= 0:
                    continue
                cost = quantity * open_price
                fee = _fee(cost, self.config.transaction_cost_bps)
                if cost + fee > cash:
                    continue
                cash -= cost + fee
                positions[symbol] = {
                    "quantity": quantity,
                    "entry_price": open_price,
                    "entry_date": current_date,
                    "stop_loss": float(order["stop_loss"]),
                    "reason": order["buy_reasons"],
                    "fee": fee,
                }
                trade_rows.append(
                    {
                        "trade_date": current_date,
                        "symbol": symbol,
                        "side": "BUY",
                        "price": open_price,
                        "quantity": quantity,
                        "reason": order["buy_reasons"],
                    }
                )
            pending_orders = []

            for symbol, position in list(positions.items()):
                if symbol not in day.index:
                    continue
                row = day.loc[symbol]
                holding_days = int((current_date - position["entry_date"]).days)
                exit_price: float | None = None
                exit_reason: str | None = None
                if float(row["low"]) <= float(position["stop_loss"]):
                    exit_price = float(position["stop_loss"])
                    exit_reason = "觸及停損"
                elif holding_days >= self.config.max_holding_days:
                    exit_price = float(row["close"])
                    exit_reason = "達最大持有天數"

                if exit_price is not None:
                    proceeds = float(position["quantity"]) * exit_price
                    fee = _fee(proceeds, self.config.transaction_cost_bps)
                    cash += proceeds - fee
                    trade_rows.append(
                        {
                            "trade_date": current_date,
                            "symbol": symbol,
                            "side": "SELL",
                            "price": exit_price,
                            "quantity": position["quantity"],
                            "reason": exit_reason,
                        }
                    )
                    del positions[symbol]

            equity = _portfolio_value(cash, positions, day, price_column="close")
            equity_rows.append(
                {
                    "trade_date": current_date,
                    "equity": equity,
                    "cash": cash,
                    "positions": len(positions),
                }
            )

            history = frame[frame["trade_date"] <= current_date]
            scores = self.scorer.score(history, as_of=current_date)
            if not scores.empty:
                scores = self.risk_manager.apply_candidate_controls(scores)
                next_candidates = scores[(scores["is_candidate"]) & (scores["risk_pass"])].head(
                    self.config.top_n
                )
                pending_orders = [
                    row.to_dict()
                    for _, row in next_candidates.iterrows()
                    if row["symbol"] not in positions
                ]

        equity_curve = pd.DataFrame(equity_rows)
        trades = pd.DataFrame(trade_rows)
        metrics = _metrics(equity_curve, trades, self.config.initial_cash)
        return BacktestResult(equity_curve=equity_curve, trades=trades, metrics=metrics)


def _portfolio_value(
    cash: float,
    positions: dict[str, dict[str, Any]],
    day: pd.DataFrame,
    price_column: str,
) -> float:
    value = cash
    for symbol, position in positions.items():
        if symbol not in day.index:
            continue
        value += float(position["quantity"]) * float(day.loc[symbol, price_column])
    return float(value)


def _fee(value: float, bps: float) -> float:
    return float(value) * (float(bps) / 10_000)


def _metrics(equity_curve: pd.DataFrame, trades: pd.DataFrame, initial_cash: float) -> dict[str, float]:
    if equity_curve.empty:
        return {
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "win_rate": 0.0,
            "trades": 0.0,
        }

    equity = equity_curve["equity"].astype(float)
    returns = equity.pct_change().dropna()
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1
    sharpe = 0.0
    if not returns.empty and returns.std() > 0:
        sharpe = float((returns.mean() / returns.std()) * np.sqrt(252))

    win_rate = _win_rate(trades)
    return {
        "total_return": float(equity.iloc[-1] / initial_cash - 1),
        "max_drawdown": float(drawdown.min()),
        "sharpe": sharpe,
        "win_rate": win_rate,
        "trades": float(len(trades)),
    }


def _win_rate(trades: pd.DataFrame) -> float:
    if trades.empty:
        return 0.0
    buys: dict[str, list[float]] = {}
    wins = 0
    closed = 0
    for _, trade in trades.iterrows():
        symbol = str(trade["symbol"])
        if trade["side"] == "BUY":
            buys.setdefault(symbol, []).append(float(trade["price"]))
        elif trade["side"] == "SELL" and buys.get(symbol):
            entry = buys[symbol].pop(0)
            closed += 1
            if float(trade["price"]) > entry:
                wins += 1
    return float(wins / closed) if closed else 0.0
