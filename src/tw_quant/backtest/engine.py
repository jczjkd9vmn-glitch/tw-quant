"""Simple event-driven backtest engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from tw_quant.risk.controls import RiskConfig, RiskManager
from tw_quant.strategy.scoring import ScoringConfig, StockScorer
from tw_quant.trading.costs import TradingCostConfig, calculate_affordable_shares, calculate_entry, calculate_exit


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float = 1_000_000
    top_n: int = 10
    max_holding_days: int = 20
    trading_cost: TradingCostConfig = field(default_factory=TradingCostConfig)
    transaction_cost_bps: float | None = None

    def __post_init__(self) -> None:
        if self.transaction_cost_bps is None:
            return
        # Legacy compatibility only. New configuration should use trading_cost so
        # backtest and paper trading share the same fee, tax, and slippage model.
        legacy_rate = float(self.transaction_cost_bps) / 10_000
        object.__setattr__(self, "trading_cost", TradingCostConfig(commission_rate=legacy_rate))

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> "BacktestConfig":
        data = values or {}
        backtest_data = data.get("backtest", data) if isinstance(data.get("backtest", data), dict) else data
        trading_cost_data = backtest_data.get("trading_cost")
        if trading_cost_data is None and isinstance(data.get("trading_cost"), dict):
            trading_cost_data = data["trading_cost"]
        return cls(
            initial_cash=float(backtest_data.get("initial_cash", cls.initial_cash)),
            top_n=int(backtest_data.get("top_n", cls.top_n)),
            max_holding_days=int(backtest_data.get("max_holding_days", cls.max_holding_days)),
            trading_cost=TradingCostConfig.from_mapping(trading_cost_data),
            transaction_cost_bps=(
                float(backtest_data["transaction_cost_bps"]) if "transaction_cost_bps" in backtest_data else None
            ),
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
                quantity = calculate_affordable_shares(
                    target_value=target_value,
                    available_cash=cash,
                    raw_entry_price=open_price,
                    config=self.config.trading_cost,
                )
                if quantity <= 0:
                    continue
                entry_costs = calculate_entry(open_price, int(quantity), self.config.trading_cost)
                cash_required = entry_costs["position_value"] + entry_costs["entry_commission"]
                if cash_required > cash:
                    continue
                cash -= cash_required
                positions[symbol] = {
                    "quantity": quantity,
                    "entry_price": entry_costs["entry_price"],
                    "entry_price_raw": entry_costs["entry_price_raw"],
                    "entry_date": current_date,
                    "stop_loss": float(order["stop_loss"]),
                    "reason": order["buy_reasons"],
                    "entry_commission": entry_costs["entry_commission"],
                    "entry_slippage": entry_costs["entry_slippage"],
                    "buy_slippage_cost": entry_costs["buy_slippage_cost"],
                }
                trade_rows.append(
                    {
                        "trade_date": current_date,
                        "symbol": symbol,
                        "side": "BUY",
                        "price": entry_costs["entry_price"],
                        "raw_price": entry_costs["entry_price_raw"],
                        "quantity": quantity,
                        "commission": entry_costs["entry_commission"],
                        "tax": 0.0,
                        "slippage": entry_costs["entry_slippage"],
                        "slippage_cost": entry_costs["buy_slippage_cost"],
                        "total_cost": round(entry_costs["entry_commission"] + entry_costs["buy_slippage_cost"], 2),
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
                stop_loss = float(position["stop_loss"])
                open_price = float(row["open"])
                low_price = float(row["low"])
                if open_price <= stop_loss:
                    exit_price = open_price
                    exit_reason = "跳空跌破停損"
                elif low_price <= stop_loss:
                    exit_price = stop_loss
                    exit_reason = "觸及停損"
                elif holding_days >= self.config.max_holding_days:
                    exit_price = float(row["close"])
                    exit_reason = "達最大持有天數"

                if exit_price is not None:
                    exit_costs = calculate_exit(
                        exit_price,
                        float(position["quantity"]),
                        symbol,
                        self.config.trading_cost,
                    )
                    cash += exit_costs["exit_proceeds"] - exit_costs["exit_commission"] - exit_costs["exit_tax"]
                    trade_rows.append(
                        {
                            "trade_date": current_date,
                            "symbol": symbol,
                            "side": "SELL",
                            "price": exit_costs["exit_price"],
                            "raw_price": exit_costs["exit_price_raw"],
                            "quantity": position["quantity"],
                            "commission": exit_costs["exit_commission"],
                            "tax": exit_costs["exit_tax"],
                            "slippage": exit_costs["exit_slippage"],
                            "slippage_cost": exit_costs["sell_slippage_cost"],
                            "total_cost": round(
                                exit_costs["exit_commission"]
                                + exit_costs["exit_tax"]
                                + exit_costs["sell_slippage_cost"],
                                2,
                            ),
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
                next_candidates = scores[(scores["is_candidate"]) & (scores["risk_pass"])].head(self.config.top_n)
                pending_orders = [
                    row.to_dict() for _, row in next_candidates.iterrows() if row["symbol"] not in positions
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
