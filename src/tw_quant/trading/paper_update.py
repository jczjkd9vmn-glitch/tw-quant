"""Daily paper position valuation and exit-strategy handling."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.data.database import load_price_history
from tw_quant.trading.costs import TradingCostConfig, calculate_exit
from tw_quant.trading.paper import POSITION_COLUMNS


UPDATE_COLUMNS = [
    "current_price",
    "market_value",
    "unrealized_pnl",
    "unrealized_pnl_pct",
    "holding_days",
    "stop_loss_hit",
    "exit_date",
    "exit_price",
    "realized_pnl",
    "realized_pnl_pct",
    "exit_reason",
    "original_shares",
    "remaining_shares",
    "partial_exit_1_done",
    "partial_exit_2_done",
    "highest_price_since_entry",
    "highest_pnl_pct_since_entry",
    "trailing_stop_price",
]

TRADE_COLUMNS = POSITION_COLUMNS + UPDATE_COLUMNS

SUMMARY_COLUMNS = [
    "trade_date",
    "total_capital",
    "invested_value",
    "market_value",
    "cash",
    "unrealized_pnl",
    "realized_pnl",
    "total_equity",
    "total_cost",
    "realized_pnl_after_cost",
    "total_equity_after_cost",
    "open_positions",
    "closed_positions",
    "take_profit_count",
    "stop_loss_count",
    "trailing_stop_count",
    "trend_exit_count",
]

TEXT_COLUMNS = [
    "trade_date",
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
    "exit_date",
    "stock_id",
    "stock_name",
    "status",
    "exit_reason",
    "entry_price_source",
    "skipped_reason",
    "warning",
]

NUMERIC_COLUMNS = [
    "entry_price",
    "current_price",
    "market_value",
    "unrealized_pnl",
    "realized_pnl",
    "total_cost",
    "realized_pnl_after_cost",
    "total_equity_after_cost",
    "original_shares",
    "remaining_shares",
    "highest_price_since_entry",
    "highest_pnl_pct_since_entry",
    "trailing_stop_price",
    "shares",
    "position_value",
    "entry_slippage",
    "entry_commission",
    "exit_slippage",
    "exit_commission",
    "exit_tax",
    "realized_pnl_pct",
    "realized_pnl_pct_after_cost",
    "stop_loss_price",
    "suggested_position_pct",
    "holding_days",
]


@dataclass(frozen=True)
class ExitStrategyConfig:
    take_profit_1_pct: float = 0.10
    take_profit_1_sell_pct: float = 0.50
    take_profit_2_pct: float = 0.20
    take_profit_2_sell_pct: float = 1.00
    trailing_stop_activate_pct: float = 0.08
    trailing_stop_drawdown_pct: float = 0.06
    ma_exit_window: int = 20
    max_holding_days: int = 20
    min_profit_for_holding: float = 0.03

    @classmethod
    def from_mapping(cls, mapping: dict | None) -> "ExitStrategyConfig":
        data = mapping or {}
        return cls(**{key: data[key] for key in cls.__dataclass_fields__ if key in data})


@dataclass(frozen=True)
class PaperUpdateResult:
    trade_date: pd.Timestamp | None
    trades_path: Path
    portfolio_path: Path | None
    summary_path: Path | None
    portfolio: pd.DataFrame
    summary: pd.DataFrame
    updated_trades: pd.DataFrame
    warning: str = ""


def update_paper_positions(
    engine: Engine,
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
    capital: float = 1_000_000,
    trading_cost: dict | TradingCostConfig | None = None,
    exit_strategy: dict | ExitStrategyConfig | None = None,
) -> PaperUpdateResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    empty_portfolio = pd.DataFrame(columns=TRADE_COLUMNS)
    empty_summary = pd.DataFrame(columns=SUMMARY_COLUMNS)

    if not trades_path.exists():
        return PaperUpdateResult(
            trade_date=None,
            trades_path=trades_path,
            portfolio_path=None,
            summary_path=None,
            portfolio=empty_portfolio,
            summary=empty_summary,
            updated_trades=empty_portfolio,
            warning="paper_trades.csv not found",
        )

    trades = _load_paper_trades(trades_path)
    if trades[trades["status"] == "OPEN"].empty:
        selected_date = _resolve_price_date(engine, trade_date)
        summary = _build_summary(trades, selected_date, capital)
        portfolio_path, summary_path = _write_reports(report_dir, selected_date, trades, summary)
        return PaperUpdateResult(
            trade_date=selected_date,
            trades_path=trades_path,
            portfolio_path=portfolio_path,
            summary_path=summary_path,
            portfolio=trades,
            summary=summary,
            updated_trades=trades,
            warning="no open paper positions",
        )

    prices = _load_prices_for_date(engine, trade_date)
    if prices.empty:
        label = trade_date or "latest trading day"
        return PaperUpdateResult(
            trade_date=None,
            trades_path=trades_path,
            portfolio_path=None,
            summary_path=None,
            portfolio=empty_portfolio,
            summary=empty_summary,
            updated_trades=trades,
            warning=f"no price data found for {label}",
        )

    selected_date = pd.to_datetime(prices["trade_date"].max())
    close_by_symbol = {str(row["symbol"]).strip(): float(row["close"]) for _, row in prices.iterrows()}
    strategy_config = _resolve_exit_strategy(exit_strategy)
    ma_by_symbol = _load_ma_values(engine, selected_date, strategy_config.ma_exit_window)
    updated = _update_trades(
        trades,
        close_by_symbol,
        selected_date,
        _resolve_trading_cost(trading_cost),
        strategy_config,
        ma_by_symbol,
    )
    summary = _build_summary(updated, selected_date, capital)
    report_dir.mkdir(parents=True, exist_ok=True)
    updated.to_csv(trades_path, index=False, encoding="utf-8-sig")
    portfolio_path, summary_path = _write_reports(report_dir, selected_date, updated, summary)

    return PaperUpdateResult(
        trade_date=selected_date,
        trades_path=trades_path,
        portfolio_path=portfolio_path,
        summary_path=summary_path,
        portfolio=updated,
        summary=summary,
        updated_trades=updated,
    )


def _load_prices_for_date(engine: Engine, trade_date: str | None) -> pd.DataFrame:
    if trade_date:
        return load_price_history(engine, start_date=trade_date, end_date=trade_date)
    history = load_price_history(engine)
    if history.empty:
        return history
    latest_date = pd.to_datetime(history["trade_date"]).max()
    return history[history["trade_date"] == latest_date].copy()


def _resolve_price_date(engine: Engine, trade_date: str | None) -> pd.Timestamp | None:
    prices = _load_prices_for_date(engine, trade_date)
    if prices.empty:
        return pd.to_datetime(trade_date) if trade_date else None
    return pd.to_datetime(prices["trade_date"].max())


def _load_paper_trades(trades_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(trades_path, dtype={"stock_id": str})
    for column in TRADE_COLUMNS:
        if column not in frame.columns:
            frame[column] = "" if column in TEXT_COLUMNS else None

    for column in TEXT_COLUMNS:
        frame[column] = frame[column].fillna("").astype(object)

    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    frame["original_shares"] = frame["original_shares"].fillna(frame["shares"])
    frame["remaining_shares"] = frame["remaining_shares"].fillna(frame["shares"])
    frame["partial_exit_1_done"] = frame["partial_exit_1_done"].fillna(0).astype(int)
    frame["partial_exit_2_done"] = frame["partial_exit_2_done"].fillna(0).astype(int)
    return frame[TRADE_COLUMNS].copy()


def _load_ma_values(engine: Engine, trade_date: pd.Timestamp, window: int) -> dict[str, float]:
    if window <= 1:
        return {}
    history = load_price_history(engine, end_date=trade_date.strftime("%Y-%m-%d"))
    if history.empty or "symbol" not in history.columns or "close" not in history.columns:
        return {}
    history = history.copy()
    history["trade_date"] = pd.to_datetime(history["trade_date"])
    history = history[history["trade_date"] <= trade_date]
    ma_by_symbol: dict[str, float] = {}
    for symbol, group in history.groupby(history["symbol"].astype(str).str.strip()):
        closes = pd.to_numeric(group.sort_values("trade_date")["close"], errors="coerce").dropna()
        if len(closes) >= window:
            ma_by_symbol[str(symbol)] = float(closes.tail(window).mean())
    return ma_by_symbol


def _update_trades(
    trades: pd.DataFrame,
    close_by_symbol: dict[str, float],
    trade_date: pd.Timestamp,
    cost_config: TradingCostConfig,
    exit_strategy: ExitStrategyConfig,
    ma_by_symbol: dict[str, float],
) -> pd.DataFrame:
    frame = trades.copy()
    for index, row in frame.iterrows():
        if row["status"] != "OPEN":
            continue
        stock_id = str(row["stock_id"]).strip()
        if stock_id not in close_by_symbol:
            continue

        current_price = close_by_symbol[stock_id]
        entry_price = _safe_float(row.get("entry_price"))
        original_shares = _safe_float(row.get("original_shares")) or _safe_float(row.get("shares"))
        remaining_shares = _safe_float(row.get("remaining_shares")) or _safe_float(row.get("shares"))
        if entry_price <= 0 or remaining_shares <= 0:
            continue

        stop_loss = _safe_float(row.get("stop_loss_price"))
        market_value = round(remaining_shares * current_price, 2)
        remaining_cost = round(remaining_shares * entry_price, 2)
        unrealized_pnl = round(market_value - remaining_cost, 2)
        unrealized_pnl_pct = round((current_price / entry_price) - 1, 6)
        holding_days = int((trade_date - pd.to_datetime(row["trade_date"])).days)
        highest_price = max(_safe_float(row.get("highest_price_since_entry")), entry_price, current_price)
        highest_pnl_pct = round((highest_price / entry_price) - 1, 6)
        trailing_stop_price = 0.0
        if highest_pnl_pct >= exit_strategy.trailing_stop_activate_pct:
            trailing_stop_price = round(highest_price * (1 - exit_strategy.trailing_stop_drawdown_pct), 4)

        frame.loc[index, "current_price"] = current_price
        frame.loc[index, "market_value"] = market_value
        frame.loc[index, "unrealized_pnl"] = unrealized_pnl
        frame.loc[index, "unrealized_pnl_pct"] = unrealized_pnl_pct
        frame.loc[index, "holding_days"] = holding_days
        frame.loc[index, "stop_loss_hit"] = bool(stop_loss > 0 and current_price <= stop_loss)
        frame.loc[index, "original_shares"] = original_shares
        frame.loc[index, "remaining_shares"] = remaining_shares
        frame.loc[index, "highest_price_since_entry"] = highest_price
        frame.loc[index, "highest_pnl_pct_since_entry"] = highest_pnl_pct
        frame.loc[index, "trailing_stop_price"] = trailing_stop_price

        sell_fraction = 0.0
        exit_reason = ""
        if stop_loss > 0 and current_price <= stop_loss:
            sell_fraction = 1.0
            exit_reason = "STOP_LOSS"
        elif unrealized_pnl_pct >= exit_strategy.take_profit_1_pct and not _safe_bool(row.get("partial_exit_1_done")):
            sell_fraction = exit_strategy.take_profit_1_sell_pct
            exit_reason = "TAKE_PROFIT_1"
        elif unrealized_pnl_pct >= exit_strategy.take_profit_2_pct and not _safe_bool(row.get("partial_exit_2_done")):
            sell_fraction = exit_strategy.take_profit_2_sell_pct
            exit_reason = "TAKE_PROFIT_2"
        elif trailing_stop_price > 0 and current_price <= trailing_stop_price:
            sell_fraction = 1.0
            exit_reason = "TRAILING_STOP"
        elif _below_ma(stock_id, current_price, ma_by_symbol):
            sell_fraction = 1.0
            exit_reason = "MA_EXIT"
        elif holding_days >= exit_strategy.max_holding_days and unrealized_pnl_pct < exit_strategy.min_profit_for_holding:
            sell_fraction = 1.0
            exit_reason = "TIME_EXIT"

        if sell_fraction > 0 and exit_reason:
            frame = _apply_sell(frame, index, trade_date, current_price, stock_id, cost_config, sell_fraction, exit_reason)

    return frame[TRADE_COLUMNS].copy()


def _apply_sell(
    frame: pd.DataFrame,
    index: int,
    trade_date: pd.Timestamp,
    current_price: float,
    stock_id: str,
    cost_config: TradingCostConfig,
    sell_fraction: float,
    exit_reason: str,
) -> pd.DataFrame:
    remaining_shares = _safe_float(frame.loc[index, "remaining_shares"]) or _safe_float(frame.loc[index, "shares"])
    entry_price = _safe_float(frame.loc[index, "entry_price"])
    sell_shares = min(remaining_shares, max(remaining_shares * float(sell_fraction), 0.0))
    if exit_reason != "TAKE_PROFIT_1":
        sell_shares = remaining_shares
    sell_shares = round(sell_shares, 6)
    if sell_shares <= 0:
        return frame

    exit_costs = calculate_exit(current_price, sell_shares, stock_id, cost_config)
    exit_price = exit_costs["exit_price"]
    cost_basis = round(entry_price * sell_shares, 2)
    realized_before_cost = round(exit_costs["exit_proceeds"] - cost_basis, 2)
    exit_slippage_cost = round(exit_costs["exit_slippage"] * sell_shares, 2)
    exit_cost_total = round(exit_slippage_cost + exit_costs["exit_commission"] + exit_costs["exit_tax"], 2)
    realized_after_cost = round(realized_before_cost - exit_costs["exit_commission"] - exit_costs["exit_tax"], 2)
    previous_realized = _safe_float(frame.loc[index].get("realized_pnl_after_cost"))
    previous_cost = _safe_float(frame.loc[index].get("total_cost"))
    new_remaining = round(max(remaining_shares - sell_shares, 0.0), 6)
    realized_total = round(previous_realized + realized_after_cost, 2)
    total_cost = round(previous_cost + exit_cost_total, 2)
    total_cost_basis = round(entry_price * sell_shares + exit_costs["exit_commission"] + exit_costs["exit_tax"], 2)
    realized_pct = round(realized_after_cost / total_cost_basis, 6) if total_cost_basis else 0.0

    frame.loc[index, "exit_date"] = trade_date.strftime("%Y-%m-%d")
    frame.loc[index, "exit_price"] = exit_price
    frame.loc[index, "exit_slippage"] = exit_costs["exit_slippage"]
    frame.loc[index, "exit_commission"] = exit_costs["exit_commission"]
    frame.loc[index, "exit_tax"] = exit_costs["exit_tax"]
    frame.loc[index, "total_cost"] = total_cost
    frame.loc[index, "realized_pnl"] = realized_total
    frame.loc[index, "realized_pnl_pct"] = realized_pct
    frame.loc[index, "realized_pnl_after_cost"] = realized_total
    frame.loc[index, "realized_pnl_pct_after_cost"] = realized_pct
    frame.loc[index, "exit_reason"] = exit_reason
    frame.loc[index, "remaining_shares"] = new_remaining
    if exit_reason == "TAKE_PROFIT_1":
        frame.loc[index, "partial_exit_1_done"] = 1
    if exit_reason == "TAKE_PROFIT_2":
        frame.loc[index, "partial_exit_2_done"] = 1

    if new_remaining <= 0:
        frame.loc[index, "status"] = "CLOSED"
        frame.loc[index, "shares"] = 0
        frame.loc[index, "position_value"] = 0.0
        frame.loc[index, "market_value"] = 0.0
        frame.loc[index, "unrealized_pnl"] = 0.0
        frame.loc[index, "unrealized_pnl_pct"] = 0.0
    else:
        frame.loc[index, "status"] = "OPEN"
        frame.loc[index, "shares"] = new_remaining
        frame.loc[index, "position_value"] = round(entry_price * new_remaining, 2)
        frame.loc[index, "market_value"] = round(current_price * new_remaining, 2)
        frame.loc[index, "unrealized_pnl"] = round((current_price - entry_price) * new_remaining, 2)
        frame.loc[index, "unrealized_pnl_pct"] = round((current_price / entry_price) - 1, 6)

    return frame


def _below_ma(stock_id: str, current_price: float, ma_by_symbol: dict[str, float]) -> bool:
    ma_value = ma_by_symbol.get(str(stock_id).strip())
    return bool(ma_value and current_price < ma_value)


def _build_summary(
    trades: pd.DataFrame,
    trade_date: pd.Timestamp | None,
    capital: float,
) -> pd.DataFrame:
    frame = trades.copy()
    open_frame = frame[frame["status"] == "OPEN"].copy()
    closed_frame = frame[frame["status"] == "CLOSED"].copy()

    invested_value = _sum(open_frame, "position_value")
    market_value = _sum(open_frame, "market_value")
    unrealized_pnl = _sum(open_frame, "unrealized_pnl")
    realized_pnl_after_cost = _sum(frame, "realized_pnl_after_cost")
    realized_pnl = realized_pnl_after_cost
    open_entry_commission = _sum(open_frame, "entry_commission")
    total_cost = _sum(frame, "total_cost")
    cash = round(float(capital) - invested_value - open_entry_commission + realized_pnl_after_cost, 2)
    total_equity = round(cash + market_value, 2)
    date_text = trade_date.strftime("%Y-%m-%d") if trade_date is not None else ""

    today_exits = frame[frame.get("exit_date", "").fillna("").astype(str) == date_text] if date_text and "exit_date" in frame.columns else pd.DataFrame()
    take_profit_count = _exit_count(today_exits, {"TAKE_PROFIT_1", "TAKE_PROFIT_2"})
    stop_loss_count = _exit_count(today_exits, {"STOP_LOSS"})
    trailing_stop_count = _exit_count(today_exits, {"TRAILING_STOP"})
    trend_exit_count = _exit_count(today_exits, {"MA_EXIT", "TIME_EXIT"})

    return pd.DataFrame(
        [
            {
                "trade_date": date_text,
                "total_capital": float(capital),
                "invested_value": invested_value,
                "market_value": market_value,
                "cash": cash,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": realized_pnl,
                "total_equity": total_equity,
                "total_cost": total_cost,
                "realized_pnl_after_cost": realized_pnl_after_cost,
                "total_equity_after_cost": total_equity,
                "open_positions": int(len(open_frame)),
                "closed_positions": int(len(closed_frame)),
                "take_profit_count": take_profit_count,
                "stop_loss_count": stop_loss_count,
                "trailing_stop_count": trailing_stop_count,
                "trend_exit_count": trend_exit_count,
            }
        ],
        columns=SUMMARY_COLUMNS,
    )


def _write_reports(
    report_dir: Path,
    trade_date: pd.Timestamp | None,
    portfolio: pd.DataFrame,
    summary: pd.DataFrame,
) -> tuple[Path | None, Path | None]:
    if trade_date is None:
        return None, None
    report_dir.mkdir(parents=True, exist_ok=True)
    label = trade_date.strftime("%Y%m%d")
    portfolio_path = report_dir / f"paper_portfolio_{label}.csv"
    summary_path = report_dir / f"paper_summary_{label}.csv"
    portfolio.to_csv(portfolio_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return portfolio_path, summary_path


def _sum(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return round(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum(), 2)


def _exit_count(frame: pd.DataFrame, reasons: set[str]) -> int:
    if frame.empty or "exit_reason" not in frame.columns:
        return 0
    return int(frame["exit_reason"].fillna("").astype(str).isin(reasons).sum())


def _resolve_trading_cost(trading_cost: dict | TradingCostConfig | None) -> TradingCostConfig:
    if isinstance(trading_cost, TradingCostConfig):
        return trading_cost
    return TradingCostConfig.from_mapping(trading_cost)


def _resolve_exit_strategy(exit_strategy: dict | ExitStrategyConfig | None) -> ExitStrategyConfig:
    if isinstance(exit_strategy, ExitStrategyConfig):
        return exit_strategy
    return ExitStrategyConfig.from_mapping(exit_strategy)


def _safe_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(parsed):
        return 0.0
    return parsed


def _safe_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return bool(value)


def _has_numeric(frame: pd.DataFrame, column: str) -> bool:
    if frame.empty or column not in frame.columns:
        return False
    return pd.to_numeric(frame[column], errors="coerce").notna().any()
