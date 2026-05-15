"""Daily paper position valuation and stop-loss handling."""

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
    "exit_price_raw",
    "realized_pnl",
    "realized_pnl_pct",
    "exit_reason",
]

TRADE_COLUMNS = list(dict.fromkeys(POSITION_COLUMNS + UPDATE_COLUMNS))

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
    "take_profit_exits",
    "stop_loss_exits",
    "trailing_stop_exits",
    "trend_exit_exits",
    "time_exit_exits",
    "realized_pnl_after_cost_today",
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
    "entry_price_raw",
    "exit_price",
    "exit_price_raw",
    "slippage_rate",
    "current_price",
    "market_value",
    "unrealized_pnl",
    "realized_pnl",
    "total_cost",
    "realized_pnl_after_cost",
    "realized_pnl_pct_after_cost",
    "last_exit_realized_pnl_after_cost",
    "total_equity_after_cost",
    "shares",
    "original_shares",
    "remaining_shares",
    "position_value",
    "entry_commission",
    "buy_commission",
    "entry_slippage",
    "buy_slippage_cost",
    "exit_slippage",
    "sell_slippage_cost",
    "exit_commission",
    "sell_commission",
    "exit_tax",
    "sell_tax",
    "realized_pnl_pct",
    "unrealized_pnl_pct",
    "holding_days",
    "highest_price_since_entry",
    "highest_pnl_pct_since_entry",
    "trailing_stop_price",
    "stop_loss_price",
]

BOOLEAN_COLUMNS = [
    "partial_exit_1_done",
    "partial_exit_2_done",
    "stop_loss_hit",
]


@dataclass(frozen=True)
class ExitStrategyConfig:
    take_profit_1_pct: float = 0.08
    take_profit_1_sell_pct: float = 0.50
    take_profit_2_pct: float = 0.15
    take_profit_2_sell_pct: float = 0.50
    trailing_stop_activate_pct: float = 0.08
    trailing_stop_drawdown_pct: float = 0.08
    ma_exit_window: int = 20
    max_holding_days: int = 30
    min_profit_for_holding: float = 0.03

    @classmethod
    def from_mapping(cls, mapping: dict | None) -> "ExitStrategyConfig":
        data = mapping or {}
        return cls(
            take_profit_1_pct=float(data.get("take_profit_1_pct", cls.take_profit_1_pct)),
            take_profit_1_sell_pct=float(data.get("take_profit_1_sell_pct", cls.take_profit_1_sell_pct)),
            take_profit_2_pct=float(data.get("take_profit_2_pct", cls.take_profit_2_pct)),
            take_profit_2_sell_pct=float(data.get("take_profit_2_sell_pct", cls.take_profit_2_sell_pct)),
            trailing_stop_activate_pct=float(
                data.get("trailing_stop_activate_pct", cls.trailing_stop_activate_pct)
            ),
            trailing_stop_drawdown_pct=float(
                data.get("trailing_stop_drawdown_pct", cls.trailing_stop_drawdown_pct)
            ),
            ma_exit_window=int(data.get("ma_exit_window", cls.ma_exit_window)),
            max_holding_days=int(data.get("max_holding_days", cls.max_holding_days)),
            min_profit_for_holding=float(data.get("min_profit_for_holding", cls.min_profit_for_holding)),
        )


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
    history = load_price_history(engine, end_date=selected_date.strftime("%Y-%m-%d"))
    updated = _update_trades(
        trades,
        close_by_symbol,
        history,
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

    for column in BOOLEAN_COLUMNS:
        if column not in frame.columns:
            frame[column] = False
        frame[column] = frame[column].apply(_bool_value).astype(object)

    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    frame["original_shares"] = frame["original_shares"].fillna(frame["shares"])
    frame["remaining_shares"] = frame["remaining_shares"].fillna(frame["shares"])
    sold_shares = (frame["original_shares"].fillna(frame["shares"]) - frame["remaining_shares"].fillna(0.0)).clip(
        lower=0.0
    )
    frame["entry_price_raw"] = frame["entry_price_raw"].fillna(frame["entry_price"] - frame["entry_slippage"].fillna(0.0))
    frame["exit_price_raw"] = frame["exit_price_raw"].fillna(frame["exit_price"] + frame["exit_slippage"].fillna(0.0))
    frame["slippage_rate"] = frame["slippage_rate"].fillna(0.0)
    frame["buy_commission"] = frame["buy_commission"].fillna(frame["entry_commission"].fillna(0.0))
    frame["entry_commission"] = frame["entry_commission"].fillna(frame["buy_commission"].fillna(0.0))
    frame["buy_slippage_cost"] = frame["buy_slippage_cost"].fillna(
        (frame["entry_slippage"].fillna(0.0) * frame["original_shares"].fillna(0.0)).round(2)
    )
    frame["sell_commission"] = frame["sell_commission"].fillna(frame["exit_commission"].fillna(0.0))
    frame["sell_tax"] = frame["sell_tax"].fillna(frame["exit_tax"].fillna(0.0))
    frame["sell_slippage_cost"] = frame["sell_slippage_cost"].fillna(
        (frame["exit_slippage"].fillna(0.0) * sold_shares).round(2)
    )
    frame["highest_price_since_entry"] = frame["highest_price_since_entry"].fillna(frame["entry_price"])
    frame["highest_pnl_pct_since_entry"] = frame["highest_pnl_pct_since_entry"].fillna(0.0)
    frame["realized_pnl_after_cost"] = frame["realized_pnl_after_cost"].fillna(0.0)
    frame["last_exit_realized_pnl_after_cost"] = frame["last_exit_realized_pnl_after_cost"].fillna(0.0)
    component_total = (
        frame["buy_slippage_cost"].fillna(0.0)
        + frame["buy_commission"].fillna(0.0)
        + frame["sell_slippage_cost"].fillna(0.0)
        + frame["sell_commission"].fillna(0.0)
        + frame["sell_tax"].fillna(0.0)
    ).round(2)
    frame["total_cost"] = pd.concat([frame["total_cost"].fillna(0.0), component_total], axis=1).max(axis=1)
    return frame[TRADE_COLUMNS].copy()


def _update_trades(
    trades: pd.DataFrame,
    close_by_symbol: dict[str, float],
    history: pd.DataFrame,
    trade_date: pd.Timestamp,
    cost_config: TradingCostConfig,
    exit_config: ExitStrategyConfig,
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
        original_shares = _safe_float(row.get("original_shares")) or _safe_float(row.get("shares"))
        remaining_shares = _safe_float(row.get("remaining_shares")) or _safe_float(row.get("shares"))
        entry_commission = _safe_float(row.get("entry_commission"))
        stop_loss = float(row["stop_loss_price"])
        if remaining_shares <= 0:
            frame.loc[index, "status"] = "CLOSED"
            continue

        highest_price = max(_safe_float(row.get("highest_price_since_entry")), current_price, entry_price)
        highest_pnl_pct = round((highest_price / entry_price) - 1, 6) if entry_price else 0.0
        trailing_stop_price = (
            round(highest_price * (1 - exit_config.trailing_stop_drawdown_pct), 4)
            if highest_pnl_pct >= exit_config.trailing_stop_activate_pct
            else _safe_float(row.get("trailing_stop_price"))
        )
        position_value = round(remaining_shares * entry_price, 2)
        market_value = round(remaining_shares * current_price, 2)
        unrealized_pnl = round(market_value - position_value, 2)
        unrealized_pnl_pct = round((current_price / entry_price) - 1, 6) if entry_price else 0.0
        holding_days = _trading_days_held(history, stock_id, row.get("trade_date"), trade_date)
        stop_loss_hit = current_price <= stop_loss

        frame.loc[index, "original_shares"] = original_shares
        frame.loc[index, "remaining_shares"] = remaining_shares
        frame.loc[index, "position_value"] = position_value
        frame.loc[index, "current_price"] = current_price
        frame.loc[index, "market_value"] = market_value
        frame.loc[index, "unrealized_pnl"] = unrealized_pnl
        frame.loc[index, "unrealized_pnl_pct"] = unrealized_pnl_pct
        frame.loc[index, "holding_days"] = holding_days
        frame.loc[index, "stop_loss_hit"] = bool(stop_loss_hit)
        frame.loc[index, "highest_price_since_entry"] = highest_price
        frame.loc[index, "highest_pnl_pct_since_entry"] = highest_pnl_pct
        frame.loc[index, "trailing_stop_price"] = trailing_stop_price if trailing_stop_price > 0 else None
        frame.loc[index, "last_exit_realized_pnl_after_cost"] = 0.0

        exit_reason, sell_shares = _select_exit_action(
            row=frame.loc[index],
            stock_id=stock_id,
            history=history,
            current_price=current_price,
            entry_price=entry_price,
            remaining_shares=remaining_shares,
            unrealized_pnl_pct=unrealized_pnl_pct,
            holding_days=holding_days,
            trailing_stop_price=trailing_stop_price,
            exit_config=exit_config,
        )
        if exit_reason and sell_shares > 0:
            frame = _apply_exit(
                frame=frame,
                index=index,
                stock_id=stock_id,
                trade_date=trade_date,
                current_price=current_price,
                entry_price=entry_price,
                original_shares=original_shares,
                sell_shares=min(sell_shares, remaining_shares),
                exit_reason=exit_reason,
                cost_config=cost_config,
            )

    return frame[TRADE_COLUMNS].copy()


def _select_exit_action(
    *,
    row: pd.Series,
    stock_id: str,
    history: pd.DataFrame,
    current_price: float,
    entry_price: float,
    remaining_shares: float,
    unrealized_pnl_pct: float,
    holding_days: int,
    trailing_stop_price: float,
    exit_config: ExitStrategyConfig,
) -> tuple[str, float]:
    if current_price <= float(row["stop_loss_price"]):
        return "stop_loss", remaining_shares
    if (
        unrealized_pnl_pct >= exit_config.take_profit_1_pct
        and not _bool_value(row.get("partial_exit_1_done"))
    ):
        return "take_profit_1", max(1.0, remaining_shares * exit_config.take_profit_1_sell_pct)
    if (
        unrealized_pnl_pct >= exit_config.take_profit_2_pct
        and not _bool_value(row.get("partial_exit_2_done"))
    ):
        return "take_profit_2", max(1.0, remaining_shares * exit_config.take_profit_2_sell_pct)
    if trailing_stop_price > 0 and current_price <= trailing_stop_price:
        return "trailing_stop", remaining_shares
    ma_value = _ma_value(history, stock_id, exit_config.ma_exit_window)
    if ma_value is not None and current_price < ma_value:
        return "ma20_break", remaining_shares
    if holding_days > exit_config.max_holding_days and unrealized_pnl_pct < exit_config.min_profit_for_holding:
        return "max_holding_days", remaining_shares
    return "", 0.0


def _apply_exit(
    *,
    frame: pd.DataFrame,
    index: int,
    stock_id: str,
    trade_date: pd.Timestamp,
    current_price: float,
    entry_price: float,
    original_shares: float,
    sell_shares: float,
    exit_reason: str,
    cost_config: TradingCostConfig,
) -> pd.DataFrame:
    sell_shares = float(int(sell_shares))
    if sell_shares <= 0:
        return frame
    row = frame.loc[index]
    remaining_before = _safe_float(row.get("remaining_shares")) or _safe_float(row.get("shares"))
    sell_shares = min(sell_shares, remaining_before)
    exit_costs = calculate_exit(current_price, sell_shares, stock_id, cost_config)
    exit_price = exit_costs["exit_price"]
    entry_commission = _safe_float(row.get("buy_commission")) or _safe_float(row.get("entry_commission"))
    entry_slippage = _safe_float(row.get("entry_slippage"))
    buy_slippage_cost = _safe_float(row.get("buy_slippage_cost")) or round(entry_slippage * original_shares, 2)
    previous_realized = _safe_float(row.get("realized_pnl_after_cost"))
    previous_total_cost = _safe_float(row.get("total_cost"))
    previous_sell_slippage_cost = _safe_float(row.get("sell_slippage_cost"))
    previous_sell_commission = _safe_float(row.get("sell_commission"))
    previous_sell_tax = _safe_float(row.get("sell_tax"))
    allocated_entry_commission = round(entry_commission * (sell_shares / original_shares), 2) if original_shares else 0.0
    realized_delta = round(
        exit_costs["exit_proceeds"]
        - (entry_price * sell_shares)
        - allocated_entry_commission
        - exit_costs["sell_commission"]
        - exit_costs["sell_tax"],
        2,
    )
    remaining_after = round(remaining_before - sell_shares, 6)
    cumulative_realized = round(previous_realized + realized_delta, 2)
    entry_slippage_cost = (
        buy_slippage_cost
        if previous_total_cost <= entry_commission + 0.0001 and buy_slippage_cost > 0
        else 0.0
    )
    incremental_cost = round(
        entry_slippage_cost
        + exit_costs["sell_slippage_cost"]
        + exit_costs["sell_commission"]
        + exit_costs["sell_tax"],
        2,
    )
    total_cost_value = round(previous_total_cost + incremental_cost, 2)
    sell_slippage_cost = round(previous_sell_slippage_cost + exit_costs["sell_slippage_cost"], 2)
    sell_commission = round(previous_sell_commission + exit_costs["sell_commission"], 2)
    sell_tax = round(previous_sell_tax + exit_costs["sell_tax"], 2)
    original_basis = round(entry_price * original_shares + entry_commission, 2)
    realized_pct = round(cumulative_realized / original_basis, 6) if original_basis else 0.0
    new_position_value = round(remaining_after * entry_price, 2)
    new_market_value = round(remaining_after * current_price, 2)

    frame.loc[index, "remaining_shares"] = remaining_after
    frame.loc[index, "position_value"] = new_position_value
    frame.loc[index, "market_value"] = new_market_value
    frame.loc[index, "unrealized_pnl"] = round(new_market_value - new_position_value, 2)
    frame.loc[index, "unrealized_pnl_pct"] = round((current_price / entry_price) - 1, 6) if entry_price else 0.0
    frame.loc[index, "exit_date"] = trade_date.strftime("%Y-%m-%d")
    frame.loc[index, "exit_price"] = exit_price
    frame.loc[index, "exit_price_raw"] = exit_costs["exit_price_raw"]
    frame.loc[index, "slippage_rate"] = exit_costs["slippage_rate"]
    frame.loc[index, "exit_slippage"] = exit_costs["exit_slippage"]
    frame.loc[index, "sell_slippage_cost"] = sell_slippage_cost
    frame.loc[index, "exit_commission"] = exit_costs["sell_commission"]
    frame.loc[index, "sell_commission"] = sell_commission
    frame.loc[index, "exit_tax"] = exit_costs["sell_tax"]
    frame.loc[index, "sell_tax"] = sell_tax
    frame.loc[index, "total_cost"] = total_cost_value
    frame.loc[index, "realized_pnl"] = cumulative_realized
    frame.loc[index, "realized_pnl_pct"] = realized_pct
    frame.loc[index, "realized_pnl_after_cost"] = cumulative_realized
    frame.loc[index, "realized_pnl_pct_after_cost"] = realized_pct
    frame.loc[index, "last_exit_realized_pnl_after_cost"] = realized_delta
    frame.loc[index, "exit_reason"] = exit_reason
    if exit_reason in {"take_profit_1", "TAKE_PROFIT_1"}:
        frame.loc[index, "partial_exit_1_done"] = True
    if exit_reason in {"take_profit_2", "TAKE_PROFIT_2"}:
        frame.loc[index, "partial_exit_2_done"] = True
    if remaining_after <= 0:
        frame.loc[index, "status"] = "CLOSED"
        frame.loc[index, "remaining_shares"] = 0.0
        frame.loc[index, "position_value"] = 0.0
        frame.loc[index, "market_value"] = 0.0
        frame.loc[index, "unrealized_pnl"] = 0.0
        frame.loc[index, "unrealized_pnl_pct"] = 0.0
    return frame


def _ma_value(history: pd.DataFrame, stock_id: str, window: int) -> float | None:
    if history.empty or window <= 0:
        return None
    frame = history[history["symbol"].astype(str).str.strip() == stock_id].sort_values("trade_date")
    if len(frame) < window:
        return None
    return float(pd.to_numeric(frame["close"], errors="coerce").tail(window).mean())


def _trading_days_held(
    history: pd.DataFrame,
    stock_id: str,
    entry_date: object,
    trade_date: pd.Timestamp,
) -> int:
    entry = pd.to_datetime(entry_date, errors="coerce")
    if pd.isna(entry) or pd.isna(trade_date):
        return 0
    if not history.empty and "symbol" in history.columns and "trade_date" in history.columns:
        frame = history[history["symbol"].astype(str).str.strip() == stock_id].copy()
        if not frame.empty:
            dates = pd.to_datetime(frame["trade_date"], errors="coerce").dropna().dt.normalize().drop_duplicates()
            return int(((dates > entry.normalize()) & (dates <= trade_date.normalize())).sum())

    # TODO: If price history is unavailable, this fallback uses calendar days instead of trading days.
    return max(int((trade_date.normalize() - entry.normalize()).days), 0)


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
    realized_pnl_after_cost += _sum(open_frame, "realized_pnl_after_cost")
    if realized_pnl_after_cost == 0.0 and not _has_numeric(frame, "realized_pnl_after_cost"):
        realized_pnl_after_cost = _sum(frame, "realized_pnl")
    realized_pnl = realized_pnl_after_cost
    open_entry_commission = _remaining_entry_commission(open_frame)
    total_cost = _sum(frame, "total_cost")
    cash = round(float(capital) - invested_value - open_entry_commission + realized_pnl_after_cost, 2)
    total_equity = round(cash + market_value, 2)
    today = trade_date.strftime("%Y-%m-%d") if trade_date is not None else ""
    today_exits = frame[frame["exit_date"].fillna("").astype(str) == today].copy() if today else pd.DataFrame()

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
                "take_profit_exits": _count_reasons(
                    today_exits, {"take_profit_1", "take_profit_2", "TAKE_PROFIT_1", "TAKE_PROFIT_2"}
                ),
                "stop_loss_exits": _count_reasons(today_exits, {"stop_loss", "STOP_LOSS"}),
                "trailing_stop_exits": _count_reasons(today_exits, {"trailing_stop", "TRAILING_STOP"}),
                "trend_exit_exits": _count_reasons(today_exits, {"ma20_break", "MA_EXIT"}),
                "time_exit_exits": _count_reasons(today_exits, {"max_holding_days", "TIME_EXIT"}),
                "realized_pnl_after_cost_today": _sum(today_exits, "last_exit_realized_pnl_after_cost"),
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


def _remaining_entry_commission(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    total = 0.0
    for _, row in frame.iterrows():
        original = _safe_float(row.get("original_shares")) or _safe_float(row.get("shares"))
        remaining = _safe_float(row.get("remaining_shares")) or _safe_float(row.get("shares"))
        commission = _safe_float(row.get("buy_commission")) or _safe_float(row.get("entry_commission"))
        total += commission * (remaining / original) if original else commission
    return round(total, 2)


def _count_reasons(frame: pd.DataFrame, reasons: set[str]) -> int:
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
    if exit_strategy is None:
        return ExitStrategyConfig(
            take_profit_1_pct=999.0,
            take_profit_2_pct=999.0,
            trailing_stop_activate_pct=999.0,
            ma_exit_window=0,
            max_holding_days=1_000_000,
        )
    return ExitStrategyConfig.from_mapping(exit_strategy)


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是"}
    try:
        if pd.isna(value):
            return False
    except TypeError:
        pass
    return bool(value)


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
