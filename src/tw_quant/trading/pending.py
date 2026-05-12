"""Pending order execution for next-trading-day paper entries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.data.database import load_price_history
from tw_quant.trading.costs import TradingCostConfig, calculate_entry
from tw_quant.trading.paper import PENDING_ORDER_COLUMNS, POSITION_COLUMNS


EXECUTED_STATUS = "EXECUTED"
PENDING_STATUS = "PENDING"
SKIPPED_EXISTING_STATUS = "SKIPPED_EXISTING_POSITION"


@dataclass(frozen=True)
class PendingExecutionResult:
    pending_orders_path: Path | None
    trades_path: Path
    pending_orders: pd.DataFrame
    executed_orders: pd.DataFrame
    skipped_orders: pd.DataFrame
    updated_trades: pd.DataFrame
    warnings: list[str]


def execute_pending_orders(
    engine: Engine,
    reports_dir: str | Path = "reports",
    capital: float = 1_000_000,
    trading_cost: dict | TradingCostConfig | None = None,
) -> PendingExecutionResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    pending_files = sorted(report_dir.glob("pending_orders_*.csv"))
    trades = _load_trades(trades_path)
    cost_config = _resolve_trading_cost(trading_cost)
    available_cash = _available_cash(trades, capital)
    open_ids = set(trades[trades["status"] == "OPEN"]["stock_id"]) if not trades.empty else set()
    executed_rows: list[dict] = []
    skipped_rows: list[dict] = []
    warnings: list[str] = []
    all_pending_frames: list[pd.DataFrame] = []

    for pending_path in pending_files:
        orders = _load_pending_orders(pending_path)
        if orders.empty:
            all_pending_frames.append(orders)
            continue

        for index, row in orders.iterrows():
            if str(row.get("status", "")) != PENDING_STATUS:
                continue

            stock_id = str(row["stock_id"]).strip()
            if stock_id in open_ids:
                orders = _mark_skipped(orders, index, "已有未平倉持倉，略過重複進場")
                skipped_rows.append(orders.loc[index].to_dict())
                continue

            entry = _find_entry_price(engine, row)
            if entry is None:
                warning = "尚無下一個有效交易日資料，等待下次執行"
                orders.loc[index, "warning"] = warning
                warnings.append(f"{stock_id}: {warning}")
                continue

            entry_date, raw_entry_price, entry_source, warning = entry
            suggested_pct = _safe_float(row.get("suggested_position_pct")) or 0.0
            target_value = float(capital) * suggested_pct
            shares = _calculate_affordable_shares(
                target_value=target_value,
                available_cash=available_cash,
                raw_entry_price=raw_entry_price,
                cost_config=cost_config,
            )
            if shares <= 0:
                orders = _mark_skipped(orders, index, "建議部位不足以建立整股持倉")
                skipped_rows.append(orders.loc[index].to_dict())
                continue

            entry_costs = calculate_entry(raw_entry_price, shares, cost_config)
            entry_price = entry_costs["entry_price"]
            entry_slippage = entry_costs["entry_slippage"]
            position_value = entry_costs["position_value"]
            entry_commission = entry_costs["entry_commission"]
            available_cash = round(available_cash - position_value - entry_commission, 2)
            orders.loc[index, "status"] = EXECUTED_STATUS
            orders.loc[index, "actual_entry_date"] = entry_date.strftime("%Y-%m-%d")
            orders.loc[index, "entry_price"] = entry_price
            orders.loc[index, "entry_price_source"] = entry_source
            orders.loc[index, "shares"] = shares
            orders.loc[index, "position_value"] = position_value
            if warning:
                orders.loc[index, "warning"] = warning
                warnings.append(f"{stock_id}: {warning}")

            trade = _build_trade_row(
                order=orders.loc[index],
                entry_date=entry_date,
                raw_entry_price=raw_entry_price,
                entry_price=entry_price,
                entry_source=entry_source,
                shares=shares,
                position_value=position_value,
                slippage_rate=entry_costs["slippage_rate"],
                entry_slippage=entry_slippage,
                buy_slippage_cost=entry_costs["buy_slippage_cost"],
                entry_commission=entry_commission,
            )
            trades = _append_trade(trades, trade)
            open_ids.add(stock_id)
            executed_rows.append(orders.loc[index].to_dict())

        orders.to_csv(pending_path, index=False, encoding="utf-8-sig")
        all_pending_frames.append(orders)

    if not trades.empty:
        report_dir.mkdir(parents=True, exist_ok=True)
        trades.to_csv(trades_path, index=False, encoding="utf-8-sig")

    pending_orders = (
        pd.concat(all_pending_frames, ignore_index=True)
        if all_pending_frames
        else pd.DataFrame(columns=PENDING_ORDER_COLUMNS)
    )
    return PendingExecutionResult(
        pending_orders_path=pending_files[-1] if pending_files else None,
        trades_path=trades_path,
        pending_orders=pending_orders,
        executed_orders=pd.DataFrame(executed_rows, columns=PENDING_ORDER_COLUMNS),
        skipped_orders=pd.DataFrame(skipped_rows, columns=PENDING_ORDER_COLUMNS),
        updated_trades=trades,
        warnings=warnings,
    )


def _find_entry_price(
    engine: Engine,
    order: pd.Series,
) -> tuple[pd.Timestamp, float, str, str] | None:
    signal_date = pd.to_datetime(order["signal_date"])
    symbol = str(order["stock_id"]).strip()
    history = load_price_history(engine, start_date=signal_date.strftime("%Y-%m-%d"))
    if history.empty:
        return None
    history = history[
        (pd.to_datetime(history["trade_date"]) > signal_date) & (history["symbol"].astype(str).str.strip() == symbol)
    ].copy()
    if history.empty:
        return None
    history = history.sort_values("trade_date")
    row = history.iloc[0]
    entry_date = pd.to_datetime(row["trade_date"])
    open_price = _safe_float(row.get("open"))
    if open_price and open_price > 0:
        return entry_date, open_price, "OPEN", ""
    close_price = _safe_float(row.get("close"))
    if close_price and close_price > 0:
        return entry_date, close_price, "CLOSE_FALLBACK", "開盤價缺失或無效，改用收盤價成交"
    return None


def _build_trade_row(
    order: pd.Series,
    entry_date: pd.Timestamp,
    raw_entry_price: float,
    entry_price: float,
    entry_source: str,
    shares: int,
    position_value: float,
    slippage_rate: float,
    entry_slippage: float,
    buy_slippage_cost: float,
    entry_commission: float,
) -> dict:
    total_cost = round(float(buy_slippage_cost) + float(entry_commission), 2)
    return {
        "signal_date": str(order.get("signal_date", "")),
        "planned_entry_date": str(order.get("planned_entry_date", "")),
        "actual_entry_date": entry_date.strftime("%Y-%m-%d"),
        "entry_price_source": entry_source,
        "trade_date": entry_date.strftime("%Y-%m-%d"),
        "stock_id": str(order["stock_id"]).strip(),
        "stock_name": str(order["stock_name"]),
        "entry_price": entry_price,
        "entry_price_raw": round(float(raw_entry_price), 4),
        "slippage_rate": slippage_rate,
        "shares": shares,
        "original_shares": shares,
        "remaining_shares": shares,
        "position_value": position_value,
        "entry_slippage": entry_slippage,
        "buy_slippage_cost": buy_slippage_cost,
        "entry_commission": entry_commission,
        "buy_commission": entry_commission,
        "exit_price": "",
        "exit_price_raw": "",
        "exit_slippage": "",
        "sell_slippage_cost": "",
        "exit_commission": "",
        "sell_commission": "",
        "exit_tax": "",
        "sell_tax": "",
        "total_cost": total_cost,
        "realized_pnl_after_cost": "",
        "realized_pnl_pct_after_cost": "",
        "last_exit_realized_pnl_after_cost": "",
        "partial_exit_1_done": False,
        "partial_exit_2_done": False,
        "highest_price_since_entry": entry_price,
        "highest_pnl_pct_since_entry": 0.0,
        "trailing_stop_price": "",
        "stop_loss_price": float(order["stop_loss_price"]),
        "suggested_position_pct": float(order["suggested_position_pct"]),
        "status": "OPEN",
    }


def _mark_skipped(frame: pd.DataFrame, index: int, reason: str) -> pd.DataFrame:
    frame.loc[index, "status"] = SKIPPED_EXISTING_STATUS
    frame.loc[index, "skipped_reason"] = reason
    return frame


def _load_pending_orders(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype={"stock_id": str})
    for column in PENDING_ORDER_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(object)
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame[PENDING_ORDER_COLUMNS].copy()


def _load_trades(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=POSITION_COLUMNS)
    frame = pd.read_csv(path, dtype={"stock_id": str})
    for column in POSITION_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame.copy()


def _append_trade(existing: pd.DataFrame, trade: dict) -> pd.DataFrame:
    columns = list(existing.columns)
    for column in POSITION_COLUMNS:
        if column not in columns:
            columns.append(column)
    new_frame = pd.DataFrame([trade])
    for column in columns:
        if column not in new_frame.columns:
            new_frame[column] = ""
    if existing.empty:
        return new_frame[columns].copy()
    return pd.concat([existing, new_frame], ignore_index=True)[columns]


def _resolve_trading_cost(trading_cost: dict | TradingCostConfig | None) -> TradingCostConfig:
    if isinstance(trading_cost, TradingCostConfig):
        return trading_cost
    return TradingCostConfig.from_mapping(trading_cost)


def _calculate_affordable_shares(
    *,
    target_value: float,
    available_cash: float,
    raw_entry_price: float,
    cost_config: TradingCostConfig,
) -> int:
    if target_value <= 0 or available_cash <= 0 or raw_entry_price <= 0:
        return 0
    adjusted_price = calculate_entry(raw_entry_price, 1, cost_config)["entry_price"]
    shares = int(min(target_value, available_cash) // adjusted_price) if adjusted_price > 0 else 0
    while shares > 0:
        entry_costs = calculate_entry(raw_entry_price, shares, cost_config)
        required_cash = entry_costs["position_value"] + entry_costs["entry_commission"]
        if entry_costs["position_value"] <= target_value + 0.0001 and required_cash <= available_cash + 0.0001:
            return shares
        shares -= 1
    return 0


def _available_cash(trades: pd.DataFrame, capital: float) -> float:
    if trades.empty:
        return round(float(capital), 2)
    open_frame = trades[trades["status"] == "OPEN"].copy()
    closed_frame = trades[trades["status"] == "CLOSED"].copy()
    open_cash_used = _sum(open_frame, "position_value") + _sum(open_frame, "entry_commission")
    realized_after_cost = _sum(closed_frame, "realized_pnl_after_cost")
    if realized_after_cost == 0.0 and not _has_numeric(closed_frame, "realized_pnl_after_cost"):
        realized_after_cost = _sum(closed_frame, "realized_pnl")
    return round(float(capital) - open_cash_used + realized_after_cost, 2)


def _sum(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return round(pd.to_numeric(frame[column], errors="coerce").fillna(0.0).sum(), 2)


def _has_numeric(frame: pd.DataFrame, column: str) -> bool:
    if frame.empty or column not in frame.columns:
        return False
    return pd.to_numeric(frame[column], errors="coerce").notna().any()


def _safe_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed
