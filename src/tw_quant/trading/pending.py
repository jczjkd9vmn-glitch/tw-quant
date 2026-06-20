"""Pending order execution for next-trading-day paper entries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from tw_quant.config import load_config
from tw_quant.data.database import load_price_history
from tw_quant.market_regime import MarketRegimeResult, evaluate_market_regime
from tw_quant.trading.costs import TradingCostConfig, calculate_affordable_shares, calculate_entry
from tw_quant.trading.guardrails import build_guardrail_context, evaluate_candidate_entry
from tw_quant.trading.paper import (
    PENDING_ORDER_COLUMNS,
    POSITION_COLUMNS,
    REJECTED_ORDER_COLUMNS,
    _load_rejected_orders,
    _merge_rejected_orders,
    _rejected_report_row,
)


EXECUTED_STATUS = "EXECUTED"
PENDING_STATUS = "PENDING"
SKIPPED_EXISTING_STATUS = "SKIPPED_EXISTING_POSITION"
EXPIRED_STATUS = "EXPIRED"
CANCELLED_BY_GUARDRAIL_STATUS = "CANCELLED_BY_GUARDRAIL"
CANCELLED_BY_MARKET_REGIME_STATUS = "CANCELLED_BY_MARKET_REGIME"
CANCELLED_BY_MAX_POSITION_STATUS = "CANCELLED_BY_MAX_POSITION"
CANCELLED_BY_LOW_GRADE_STATUS = "CANCELLED_BY_LOW_GRADE"
CANCELLED_BY_EVENT_RISK_STATUS = "CANCELLED_BY_EVENT_RISK"
ERROR_STATUS = "ERROR"

CANCELLED_STATUSES = {
    EXPIRED_STATUS,
    CANCELLED_BY_GUARDRAIL_STATUS,
    CANCELLED_BY_MARKET_REGIME_STATUS,
    CANCELLED_BY_MAX_POSITION_STATUS,
    CANCELLED_BY_LOW_GRADE_STATUS,
    CANCELLED_BY_EVENT_RISK_STATUS,
    SKIPPED_EXISTING_STATUS,
    ERROR_STATUS,
}


@dataclass(frozen=True)
class PendingExecutionResult:
    pending_orders_path: Path | None
    trades_path: Path
    pending_orders: pd.DataFrame
    executed_orders: pd.DataFrame
    skipped_orders: pd.DataFrame
    rejected_orders: pd.DataFrame
    updated_trades: pd.DataFrame
    warnings: list[str]


def execute_pending_orders(
    engine: Engine,
    reports_dir: str | Path = "reports",
    capital: float = 1_000_000,
    trading_cost: dict | TradingCostConfig | None = None,
    config: dict | None = None,
    config_path: str | Path = "config.yaml",
    market_regime: MarketRegimeResult | None = None,
) -> PendingExecutionResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    pending_files = sorted(report_dir.glob("pending_orders_*.csv"))
    trades = _load_trades(trades_path)
    active_config = config
    guardrails = None
    if active_config is not None:
        regime = market_regime or _safe_market_regime(engine, active_config, report_dir)
        guardrails = build_guardrail_context(
            reports_dir=report_dir,
            capital=capital,
            config=active_config,
            market_regime=regime,
        )
    cost_config = _resolve_trading_cost(trading_cost)
    available_cash = _available_cash(trades, capital)
    open_ids = set(trades[trades["status"] == "OPEN"]["stock_id"]) if not trades.empty else set()
    executed_rows: list[dict] = []
    skipped_rows: list[dict] = []
    rejected_rows: list[dict] = []
    warnings: list[str] = []
    all_pending_frames: list[pd.DataFrame] = []
    price_history = _load_pending_price_history(engine, pending_files)

    for pending_path in pending_files:
        orders = _load_pending_orders(pending_path)
        if orders.empty:
            all_pending_frames.append(orders)
            continue

        for index, row in orders.iterrows():
            if str(row.get("status", "")) != PENDING_STATUS:
                continue

            stock_id = str(row["stock_id"]).strip()
            order_timing = _order_timing(price_history, row)
            attempted_date = order_timing["attempted_execution_date"]
            age_days = order_timing["order_age_trading_days"]
            expiry_status = _expiry_status(row, active_config, order_timing)
            if expiry_status is not None:
                status, reason = expiry_status
                orders = _mark_cancelled(
                    orders,
                    index,
                    status,
                    reason,
                    attempted_date=attempted_date,
                    order_age_trading_days=age_days,
                    guardrails=guardrails,
                )
                rejected_rows.append(
                    _execution_rejection_row(
                        orders.loc[index],
                        report_date=attempted_date,
                        stage="execution",
                        final_status=status,
                        reason=reason,
                        source_report=pending_path.name,
                    )
                )
                continue

            if stock_id in open_ids:
                reason = "已有未平倉持倉，略過重複進場"
                orders = _mark_cancelled(
                    orders,
                    index,
                    SKIPPED_EXISTING_STATUS,
                    reason,
                    attempted_date=attempted_date,
                    order_age_trading_days=age_days,
                    guardrails=guardrails,
                )
                skipped_rows.append(orders.loc[index].to_dict())
                rejected_rows.append(
                    _execution_rejection_row(
                        orders.loc[index],
                        report_date=attempted_date,
                        stage="execution",
                        final_status=SKIPPED_EXISTING_STATUS,
                        reason=reason,
                        source_report=pending_path.name,
                    )
                )
                continue

            if guardrails is not None:
                if guardrails.open_positions + len(executed_rows) >= guardrails.max_open_positions:
                    reason = f"目前持倉已達上限 {guardrails.max_open_positions} 檔"
                    status = CANCELLED_BY_MAX_POSITION_STATUS
                    orders = _mark_cancelled(
                        orders,
                        index,
                        status,
                        reason,
                        attempted_date=attempted_date,
                        order_age_trading_days=age_days,
                        guardrails=guardrails,
                    )
                    rejected_rows.append(
                        _execution_rejection_row(
                            orders.loc[index],
                            report_date=attempted_date,
                            stage="execution",
                            final_status=status,
                            reason=reason,
                            source_report=pending_path.name,
                        )
                    )
                    continue

                decision = evaluate_candidate_entry(
                    row,
                    guardrails,
                    created_today=len(executed_rows),
                    duplicate_reason="",
                )
                if not decision.allowed:
                    status = _status_for_rejection(decision.reason)
                    orders = _mark_cancelled(
                        orders,
                        index,
                        status,
                        decision.reason,
                        attempted_date=attempted_date,
                        order_age_trading_days=age_days,
                        guardrails=guardrails,
                    )
                    rejected_rows.append(
                        _execution_rejection_row(
                            orders.loc[index],
                            report_date=attempted_date,
                            stage="execution",
                            final_status=status,
                            reason=decision.reason,
                            source_report=pending_path.name,
                        )
                    )
                    continue

            if attempted_date:
                orders.loc[index, "attempted_execution_date"] = attempted_date.strftime("%Y-%m-%d")
            if age_days != "":
                orders.loc[index, "order_age_trading_days"] = age_days

            entry = _find_entry_price(price_history, row, attempted_date)
            if entry is None:
                warning = "尚無下一個有效交易日資料，等待下次執行"
                orders.loc[index, "warning"] = warning
                warnings.append(f"{stock_id}: {warning}")
                continue

            entry_date, raw_entry_price, entry_source, warning = entry
            suggested_pct = _safe_float(row.get("suggested_position_pct")) or 0.0
            target_value = float(capital) * suggested_pct
            shares = calculate_affordable_shares(
                target_value=target_value,
                available_cash=available_cash,
                raw_entry_price=raw_entry_price,
                config=cost_config,
            )
            if shares <= 0:
                reason = "建議部位不足以建立整股持倉"
                orders = _mark_cancelled(
                    orders,
                    index,
                    ERROR_STATUS,
                    reason,
                    attempted_date=attempted_date,
                    order_age_trading_days=age_days,
                    guardrails=guardrails,
                )
                skipped_rows.append(orders.loc[index].to_dict())
                rejected_rows.append(
                    _execution_rejection_row(
                        orders.loc[index],
                        report_date=attempted_date,
                        stage="execution",
                        final_status=ERROR_STATUS,
                        reason=reason,
                        source_report=pending_path.name,
                    )
                )
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

        orders.to_csv(pending_path, index=False, encoding="utf-8")
        all_pending_frames.append(orders)

    rejected_frame = _write_execution_rejections(report_dir, rejected_rows)
    if not trades.empty:
        report_dir.mkdir(parents=True, exist_ok=True)
        trades.to_csv(trades_path, index=False, encoding="utf-8")

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
        rejected_orders=rejected_frame,
        updated_trades=trades,
        warnings=warnings,
    )


def _find_entry_price(
    price_history: pd.DataFrame,
    order: pd.Series,
    execution_date: pd.Timestamp | None,
) -> tuple[pd.Timestamp, float, str, str] | None:
    if execution_date is None or pd.isna(execution_date):
        return None
    signal_date = pd.to_datetime(order["signal_date"])
    symbol = str(order["stock_id"]).strip()
    if price_history.empty:
        return None
    history = price_history[
        (pd.to_datetime(price_history["trade_date"]).dt.normalize() == execution_date.normalize())
        & (pd.to_datetime(price_history["trade_date"]) > signal_date)
        & (price_history["symbol"].astype(str).str.strip() == symbol)
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


def _mark_cancelled(
    frame: pd.DataFrame,
    index: int,
    status: str,
    reason: str,
    attempted_date: pd.Timestamp | None,
    order_age_trading_days: int | str,
    guardrails,
) -> pd.DataFrame:
    original_status = str(frame.loc[index].get("status", ""))
    frame.loc[index, "status"] = status
    frame.loc[index, "skipped_reason"] = reason
    frame.loc[index, "warning"] = reason
    frame.loc[index, "rejection_stage"] = "execution"
    frame.loc[index, "rejection_reason"] = reason
    frame.loc[index, "original_order_status"] = original_status
    frame.loc[index, "final_order_status"] = status
    if attempted_date is not None and not pd.isna(attempted_date):
        frame.loc[index, "attempted_execution_date"] = attempted_date.strftime("%Y-%m-%d")
    frame.loc[index, "order_age_trading_days"] = order_age_trading_days
    if status == EXPIRED_STATUS:
        frame.loc[index, "expired_at"] = (
            attempted_date.strftime("%Y-%m-%d") if attempted_date is not None and not pd.isna(attempted_date) else ""
        )
        frame.loc[index, "expiry_reason"] = reason
    if guardrails is not None:
        frame.loc[index, "market_regime_score"] = guardrails.market_regime_score
        frame.loc[index, "guardrail_status"] = guardrails.guardrail_status
        frame.loc[index, "new_entries_allowed"] = guardrails.new_entries_allowed
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


def _safe_market_regime(engine: Engine, config: dict, report_dir: Path) -> MarketRegimeResult:
    try:
        return evaluate_market_regime(engine=engine, config=config, reports_dir=report_dir)
    except Exception as exc:
        return MarketRegimeResult(
            trade_date=None,
            market_regime_score=50.0,
            source="FAILED",
            warning=f"{type(exc).__name__}: {exc}",
        )


def _load_pending_price_history(engine: Engine, pending_files: list[Path]) -> pd.DataFrame:
    base_dates: list[pd.Timestamp] = []
    symbols: set[str] = set()
    for pending_path in pending_files:
        orders = _load_pending_orders(pending_path)
        if orders.empty:
            continue
        pending_orders = orders[orders["status"].fillna("").astype(str) == PENDING_STATUS]
        for _, row in pending_orders.iterrows():
            base_date = _order_base_date(row)
            if base_date is not None:
                base_dates.append(base_date)
            symbol = str(row.get("stock_id", "")).strip()
            if symbol:
                symbols.add(symbol)
    if not base_dates:
        return pd.DataFrame()
    history = load_price_history(engine, start_date=min(base_dates).strftime("%Y-%m-%d"))
    if history.empty:
        return history
    if symbols and "symbol" in history.columns:
        history = history[history["symbol"].astype(str).str.strip().isin(symbols)].copy()
    return history


def _order_timing(price_history: pd.DataFrame, order: pd.Series) -> dict[str, object]:
    base_date = _order_base_date(order)
    if base_date is None:
        return {"attempted_execution_date": None, "order_age_trading_days": ""}
    symbol = str(order.get("stock_id", "")).strip()
    if price_history.empty:
        return {"attempted_execution_date": None, "order_age_trading_days": 0}
    history = price_history[pd.to_datetime(price_history["trade_date"], errors="coerce") >= base_date].copy()
    if symbol and "symbol" in history.columns:
        history = history[history["symbol"].astype(str).str.strip() == symbol].copy()
    dates = pd.to_datetime(history["trade_date"], errors="coerce").dropna().drop_duplicates().sort_values()
    dates = dates[dates > base_date]
    if dates.empty:
        return {"attempted_execution_date": None, "order_age_trading_days": 0}
    # Late-runner policy: use the current/latest available symbol date as the
    # attempted execution date, and expire by elapsed tradable days to that date.
    attempted = pd.to_datetime(dates.iloc[-1])
    return {"attempted_execution_date": attempted, "order_age_trading_days": int(len(dates))}


def _order_base_date(order: pd.Series) -> pd.Timestamp | None:
    for column in ["signal_date", "planned_entry_date", "created_at"]:
        value = str(order.get(column, "") or "").strip()
        if not value or value == "NEXT_AVAILABLE_TRADING_DAY":
            continue
        parsed = pd.to_datetime(value, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    return None


def _expiry_status(
    order: pd.Series,
    config: dict | None,
    timing: dict[str, object],
) -> tuple[str, str] | None:
    if config is None:
        return None
    pending_config = config.get("pending_order", {}) if isinstance(config, dict) else {}
    expire_after = int(pending_config.get("expire_after_trading_days", 1))
    age = timing.get("order_age_trading_days", "")
    if age == "":
        return None
    try:
        age_int = int(age)
    except (TypeError, ValueError):
        return None
    if age_int > expire_after:
        return (
            EXPIRED_STATUS,
            f"pending order 已超過有效期限 {expire_after} 個交易日，取消執行",
        )
    return None


def _status_for_rejection(reason: str) -> str:
    text = str(reason)
    if "market_regime_score" in text or "市場環境" in text:
        return CANCELLED_BY_MARKET_REGIME_STATUS
    if "持倉" in text and "上限" in text:
        return CANCELLED_BY_MAX_POSITION_STATUS
    if "候選分級" in text or "低於新增紙上交易門檻" in text:
        return CANCELLED_BY_LOW_GRADE_STATUS
    if "高風險事件" in text or "處置" in text or "event" in text.lower():
        return CANCELLED_BY_EVENT_RISK_STATUS
    return CANCELLED_BY_GUARDRAIL_STATUS


def _execution_rejection_row(
    order: pd.Series,
    report_date: pd.Timestamp | None,
    stage: str,
    final_status: str,
    reason: str,
    source_report: str,
) -> dict:
    effective_date = report_date
    if effective_date is None or pd.isna(effective_date):
        parsed = pd.to_datetime(order.get("signal_date", ""), errors="coerce")
        effective_date = parsed if not pd.isna(parsed) else pd.Timestamp.today()
    return _rejected_report_row(
        order,
        report_date=effective_date,
        trade_date=effective_date,
        stage=stage,
        final_status=final_status,
        reason=reason,
        source_report=source_report,
    )


def _write_execution_rejections(report_dir: Path, rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=REJECTED_ORDER_COLUMNS)
    frame = pd.DataFrame(rows)
    output_frames = []
    for report_date, group in frame.groupby("report_date", dropna=False):
        parsed = pd.to_datetime(report_date, errors="coerce")
        label_date = parsed if not pd.isna(parsed) else pd.Timestamp.today()
        path = report_dir / f"rejected_paper_orders_{label_date.strftime('%Y%m%d')}.csv"
        merged = _merge_rejected_orders(_load_rejected_orders(path), group)
        merged.to_csv(path, index=False, encoding="utf-8")
        output_frames.append(merged)
    return (
        pd.concat(output_frames, ignore_index=True) if output_frames else pd.DataFrame(columns=REJECTED_ORDER_COLUMNS)
    )


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
