"""Daily paper position valuation and stop-loss handling."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.data.database import load_price_history
from tw_quant.trading.costs import TradingCostConfig, calculate_exit, total_cost as calculate_total_cost
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
    def from_mapping(cls, data: dict | None) -> "ExitStrategyConfig":
        if not data:
            return cls()
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


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
    close_by_symbol = {
        str(row["symbol"]).strip(): float(row["close"]) for _, row in prices.iterrows()
    }
    updated = _update_trades(
        engine,
        trades,
        close_by_symbol,
        selected_date,
        _resolve_trading_cost(trading_cost),
        _resolve_exit_strategy(exit_strategy),
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
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(object)

    for column in NUMERIC_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame[TRADE_COLUMNS].copy()


def _update_trades(
    engine: Engine,
    trades: pd.DataFrame,
    close_by_symbol: dict[str, float],
    trade_date: pd.Timestamp,
    cost_config: TradingCostConfig,
    exit_strategy: ExitStrategyConfig,
) -> pd.DataFrame:
    frame = trades.copy()
    for index, row in frame.iterrows():
        if row["status"] != "OPEN":
            continue
        stock_id = str(row["stock_id"]).strip()
        if stock_id not in close_by_symbol:
            continue

        current_price = close_by_symbol[stock_id]
        entry_price = float(row["entry_price"])
        shares = float(row["shares"])
        original_shares = _safe_float(row.get("original_shares")) or shares
        remaining_shares = _safe_float(row.get("remaining_shares")) or shares
        partial_exit_1_done = bool(row.get("partial_exit_1_done"))
        partial_exit_2_done = bool(row.get("partial_exit_2_done"))
        position_value = float(row["position_value"])
        entry_slippage = _safe_float(row.get("entry_slippage"))
        entry_commission = _safe_float(row.get("entry_commission"))
        stop_loss = float(row["stop_loss_price"])
        market_value = round(shares * current_price, 2)
        unrealized_pnl = round(market_value - position_value, 2)
        unrealized_pnl_pct = round((current_price / entry_price) - 1, 6) if entry_price else 0.0
        holding_days = int((trade_date - pd.to_datetime(row["trade_date"])).days)
        stop_loss_hit = current_price <= stop_loss
        highest_price = max(_safe_float(row.get("highest_price_since_entry")) or entry_price, current_price)
        highest_pnl_pct = max(_safe_float(row.get("highest_pnl_pct_since_entry")), unrealized_pnl_pct)
        trailing_stop_price = (
            round(highest_price * (1 - exit_strategy.trailing_stop_drawdown_pct), 2)
            if highest_pnl_pct >= exit_strategy.trailing_stop_activate_pct
            else 0.0
        )

        frame.loc[index, "current_price"] = current_price
        frame.loc[index, "market_value"] = market_value
        frame.loc[index, "unrealized_pnl"] = unrealized_pnl
        frame.loc[index, "unrealized_pnl_pct"] = unrealized_pnl_pct
        frame.loc[index, "holding_days"] = holding_days
        frame.loc[index, "stop_loss_hit"] = bool(stop_loss_hit)
        frame.loc[index, "original_shares"] = original_shares
        frame.loc[index, "remaining_shares"] = remaining_shares
        frame.loc[index, "partial_exit_1_done"] = partial_exit_1_done
        frame.loc[index, "partial_exit_2_done"] = partial_exit_2_done
        frame.loc[index, "highest_price_since_entry"] = highest_price
        frame.loc[index, "highest_pnl_pct_since_entry"] = highest_pnl_pct
        frame.loc[index, "trailing_stop_price"] = trailing_stop_price

        ma_exit_hit = _below_ma(engine, stock_id, trade_date, current_price, exit_strategy.ma_exit_window)
        time_exit_hit = holding_days > exit_strategy.max_holding_days and unrealized_pnl_pct < exit_strategy.min_profit_for_holding
        trailing_stop_hit = trailing_stop_price > 0 and current_price <= trailing_stop_price
        if stop_loss_hit:
            _apply_sell(frame, index, trade_date, current_price, remaining_shares, original_shares, position_value, entry_commission, entry_slippage, cost_config, "STOP_LOSS", close_position=True)
        elif (not partial_exit_1_done) and unrealized_pnl_pct >= exit_strategy.take_profit_1_pct:
            sell_shares = max(1.0, round(remaining_shares * exit_strategy.take_profit_1_sell_pct, 0))
            _apply_sell(frame, index, trade_date, current_price, min(sell_shares, remaining_shares), original_shares, position_value, entry_commission, entry_slippage, cost_config, "TAKE_PROFIT_1", close_position=False)
        elif unrealized_pnl_pct >= exit_strategy.take_profit_2_pct and remaining_shares > 0:
            _apply_sell(frame, index, trade_date, current_price, remaining_shares, original_shares, position_value, entry_commission, entry_slippage, cost_config, "TAKE_PROFIT_2", close_position=True)
        elif trailing_stop_hit and remaining_shares > 0:
            _apply_sell(frame, index, trade_date, current_price, remaining_shares, original_shares, position_value, entry_commission, entry_slippage, cost_config, "TRAILING_STOP", close_position=True)
        elif ma_exit_hit and remaining_shares > 0:
            _apply_sell(frame, index, trade_date, current_price, remaining_shares, original_shares, position_value, entry_commission, entry_slippage, cost_config, "MA_EXIT", close_position=True)
        elif time_exit_hit and remaining_shares > 0:
            _apply_sell(frame, index, trade_date, current_price, remaining_shares, original_shares, position_value, entry_commission, entry_slippage, cost_config, "TIME_EXIT", close_position=True)

    return frame[TRADE_COLUMNS].copy()


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
    realized_pnl_after_cost = _sum(closed_frame, "realized_pnl_after_cost")
    if realized_pnl_after_cost == 0.0 and not _has_numeric(closed_frame, "realized_pnl_after_cost"):
        realized_pnl_after_cost = _sum(closed_frame, "realized_pnl")
    realized_pnl = realized_pnl_after_cost
    open_entry_commission = _sum(open_frame, "entry_commission")
    total_cost = _sum(frame, "total_cost")
    cash = round(float(capital) - invested_value - open_entry_commission + realized_pnl_after_cost, 2)
    total_equity = round(cash + market_value, 2)

    date_text = trade_date.strftime("%Y-%m-%d") if trade_date is not None else ""
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


def _resolve_trading_cost(trading_cost: dict | TradingCostConfig | None) -> TradingCostConfig:
    if isinstance(trading_cost, TradingCostConfig):
        return trading_cost
    return TradingCostConfig.from_mapping(trading_cost)


def _resolve_exit_strategy(exit_strategy: dict | ExitStrategyConfig | None) -> ExitStrategyConfig:
    if isinstance(exit_strategy, ExitStrategyConfig):
        return exit_strategy
    return ExitStrategyConfig.from_mapping(exit_strategy if isinstance(exit_strategy, dict) else None)


def _below_ma(engine: Engine, stock_id: str, trade_date: pd.Timestamp, current_price: float, window: int) -> bool:
    history = load_price_history(engine)
    if history.empty:
        return False
    history = history[history["symbol"].astype(str).str.strip() == stock_id]
    history = history[pd.to_datetime(history["trade_date"]) <= trade_date].copy()
    closes = pd.to_numeric(history["close"], errors="coerce").dropna()
    if len(closes) < window:
        return False
    return current_price < float(closes.tail(window).mean())


def _apply_sell(frame: pd.DataFrame, index: int, trade_date: pd.Timestamp, current_price: float, sell_shares: float, original_shares: float, position_value: float, entry_commission: float, entry_slippage: float, cost_config: TradingCostConfig, reason: str, close_position: bool) -> None:
    if sell_shares <= 0:
        return
    row = frame.loc[index]
    stock_id = str(row["stock_id"]).strip()
    remaining_shares = _safe_float(row.get("remaining_shares")) or _safe_float(row.get("shares"))
    exit_costs = calculate_exit(current_price, sell_shares, stock_id, cost_config)
    proportion = sell_shares / original_shares if original_shares else 1.0
    allocated_cost = position_value * proportion
    realized_piece = round(exit_costs["exit_proceeds"] - allocated_cost - (entry_commission * proportion) - exit_costs["exit_commission"] - exit_costs["exit_tax"], 2)
    prev_realized = _safe_float(row.get("realized_pnl_after_cost"))
    frame.loc[index, "realized_pnl"] = round(_safe_float(row.get("realized_pnl")) + realized_piece, 2)
    frame.loc[index, "realized_pnl_after_cost"] = round(prev_realized + realized_piece, 2)
    cost_basis_piece = allocated_cost + (entry_commission * proportion)
    realized_pct_piece = round(realized_piece / cost_basis_piece, 6) if cost_basis_piece else 0.0
    frame.loc[index, "realized_pnl_pct"] = realized_pct_piece
    frame.loc[index, "realized_pnl_pct_after_cost"] = realized_pct_piece
    prev_total_cost = _safe_float(row.get("total_cost"))
    base_entry_cost = 0.0
    if prev_total_cost == 0.0:
        base_entry_cost = calculate_total_cost(
            entry_slippage=entry_slippage,
            entry_commission=entry_commission,
            exit_slippage=0.0,
            exit_commission=0.0,
            exit_tax=0.0,
            shares=original_shares,
        )
    elif prev_total_cost <= entry_commission + 0.01:
        base_entry_cost = round(entry_slippage * original_shares, 2)
    frame.loc[index, "total_cost"] = round(prev_total_cost + base_entry_cost + calculate_total_cost(entry_slippage=0, entry_commission=0, exit_slippage=exit_costs["exit_slippage"], exit_commission=exit_costs["exit_commission"], exit_tax=exit_costs["exit_tax"], shares=sell_shares), 2)
    frame.loc[index, "exit_reason"] = reason
    frame.loc[index, "exit_date"] = trade_date.strftime("%Y-%m-%d")
    frame.loc[index, "exit_price"] = exit_costs["exit_price"]
    frame.loc[index, "exit_slippage"] = exit_costs["exit_slippage"]
    frame.loc[index, "exit_commission"] = exit_costs["exit_commission"]
    frame.loc[index, "exit_tax"] = exit_costs["exit_tax"]
    frame.loc[index, "remaining_shares"] = max(0.0, remaining_shares - sell_shares)
    frame.loc[index, "partial_exit_1_done"] = True if reason == "TAKE_PROFIT_1" else bool(row.get("partial_exit_1_done"))
    frame.loc[index, "partial_exit_2_done"] = True if reason == "TAKE_PROFIT_2" else bool(row.get("partial_exit_2_done"))
    if close_position or frame.loc[index, "remaining_shares"] <= 0:
        frame.loc[index, "status"] = "CLOSED"
        frame.loc[index, "market_value"] = 0.0
        frame.loc[index, "unrealized_pnl"] = 0.0
        frame.loc[index, "unrealized_pnl_pct"] = 0.0


def _safe_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(parsed):
        return 0.0
    return parsed


def _has_numeric(frame: pd.DataFrame, column: str) -> bool:
    if frame.empty or column not in frame.columns:
        return False
    return pd.to_numeric(frame[column], errors="coerce").notna().any()
