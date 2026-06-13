"""Paper trading position creation from risk-passed candidate reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd
from sqlalchemy import Engine

from tw_quant.config import load_config
from tw_quant.market_regime import MarketRegimeResult, evaluate_market_regime
from tw_quant.trading.guardrails import build_guardrail_context, evaluate_candidate_entry

POSITION_COLUMNS = [
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
    "entry_price_source",
    "trade_date",
    "stock_id",
    "stock_name",
    "entry_price",
    "entry_price_raw",
    "slippage_rate",
    "shares",
    "original_shares",
    "remaining_shares",
    "position_value",
    "entry_slippage",
    "buy_slippage_cost",
    "entry_commission",
    "buy_commission",
    "exit_price",
    "exit_price_raw",
    "exit_slippage",
    "sell_slippage_cost",
    "exit_commission",
    "sell_commission",
    "exit_tax",
    "sell_tax",
    "total_cost",
    "realized_pnl_after_cost",
    "realized_pnl_pct_after_cost",
    "last_exit_realized_pnl_after_cost",
    "partial_exit_1_done",
    "partial_exit_2_done",
    "highest_price_since_entry",
    "highest_pnl_pct_since_entry",
    "trailing_stop_price",
    "stop_loss_price",
    "suggested_position_pct",
    "status",
]

PENDING_ENTRY_MARKER = "NEXT_AVAILABLE_TRADING_DAY"

PENDING_ORDER_COLUMNS = [
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
    "created_at",
    "expires_after_trading_days",
    "expired_at",
    "expiry_reason",
    "stock_id",
    "stock_name",
    "signal_close",
    "total_score",
    "stop_loss_price",
    "suggested_position_pct",
    "status",
    "entry_price",
    "entry_price_source",
    "shares",
    "position_value",
    "skipped_reason",
    "warning",
    "reason",
    "risk_reason",
    "candidate_grade",
    "grade_reason",
    "grade_risk_flags",
    "requires_manual_review",
    "review_level",
    "review_reason",
    "data_quality_flags",
    "investment_risk_flags",
    "multi_factor_score",
    "multi_factor_reason",
    "final_market_score",
    "confidence_score",
    "risk_flags",
    "system_comment",
    "institutional_score",
    "credit_score",
    "event_risk_score",
    "liquidity_score",
    "sector_strength_score",
    "event_risk_level",
    "event_reason",
    "event_blocked",
    "market_regime_score",
    "guardrail_status",
    "new_entries_allowed",
    "rejection_stage",
    "rejection_reason",
    "original_order_status",
    "final_order_status",
    "source_report",
    "attempted_execution_date",
    "order_age_trading_days",
]

REJECTED_ORDER_COLUMNS = [
    "report_date",
    "trade_date",
    "stock_id",
    "stock_name",
    "source_report",
    "rejection_stage",
    "rejected_status",
    "rejection_reason",
    "rejected_reason",
    "original_order_status",
    "final_order_status",
    "signal_date",
    "planned_entry_date",
    "attempted_execution_date",
    "order_age_trading_days",
    "candidate_grade",
    "decision",
    "total_score",
    "multi_factor_score",
    "final_market_score",
    "confidence_score",
    "liquidity_score",
    "sector_strength_score",
    "event_risk_level",
    "market_regime_score",
    "guardrail_status",
    "new_entries_allowed",
    "expires_after_trading_days",
    "expired_at",
    "expiry_reason",
    "status",
    "skipped_reason",
    "warning",
    "reason",
    "risk_reason",
    "event_blocked",
]


@dataclass(frozen=True)
class PaperTradeResult:
    trade_date: pd.Timestamp | None
    source_report: Path | None
    positions_path: Path | None
    pending_orders_path: Path | None
    rejected_orders_path: Path | None
    trades_path: Path
    positions: pd.DataFrame
    new_positions: pd.DataFrame
    pending_orders: pd.DataFrame
    rejected_orders: pd.DataFrame
    skipped_existing: list[str]
    warning: str = ""
    guardrail_status: str = ""
    pause_new_entries_reason: str = ""
    market_regime_score: float = 0.0
    new_entries_allowed: bool = True


def run_paper_trade(
    reports_dir: str | Path = "reports",
    capital: float = 1_000_000,
    config: dict | None = None,
    config_path: str | Path = "config.yaml",
    engine: Engine | None = None,
    market_regime: MarketRegimeResult | None = None,
) -> PaperTradeResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    active_config = config or load_config(config_path)
    source_report = find_latest_risk_pass_report(report_dir)
    if source_report is None:
        return PaperTradeResult(
            trade_date=None,
            source_report=None,
            positions_path=None,
            pending_orders_path=None,
            rejected_orders_path=None,
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
            pending_orders=pd.DataFrame(columns=PENDING_ORDER_COLUMNS),
            rejected_orders=pd.DataFrame(columns=REJECTED_ORDER_COLUMNS),
            skipped_existing=[],
            warning="no risk_pass_candidates report found",
        )

    candidates = pd.read_csv(source_report, dtype={"stock_id": str})
    if candidates.empty:
        trade_date = _date_from_report_path(source_report)
        return PaperTradeResult(
            trade_date=pd.to_datetime(trade_date) if trade_date else None,
            source_report=source_report,
            positions_path=None,
            pending_orders_path=None,
            rejected_orders_path=None,
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
            pending_orders=pd.DataFrame(columns=PENDING_ORDER_COLUMNS),
            rejected_orders=pd.DataFrame(columns=REJECTED_ORDER_COLUMNS),
            skipped_existing=[],
            warning=f"risk pass report is empty: {source_report}",
        )

    report_dir.mkdir(parents=True, exist_ok=True)
    existing_trades = _load_trades(trades_path)
    open_positions = _open_positions(existing_trades)
    trade_date = pd.to_datetime(candidates["trade_date"].iloc[0])
    pending_path = report_dir / f"pending_orders_{trade_date.strftime('%Y%m%d')}.csv"
    rejected_path = report_dir / f"rejected_paper_orders_{trade_date.strftime('%Y%m%d')}.csv"
    pending_config = active_config.get("pending_order", {}) if isinstance(active_config, dict) else {}
    existing_pending = _load_pending_orders(pending_path)
    existing_order_ids = set(existing_pending["stock_id"]) if not existing_pending.empty else set()
    open_position_ids = set(open_positions["stock_id"]) if not open_positions.empty else set()
    regime_result = market_regime or _safe_market_regime(
        engine=engine,
        config=active_config,
        trade_date=trade_date,
        reports_dir=report_dir,
    )
    guardrails = build_guardrail_context(
        reports_dir=report_dir,
        capital=capital,
        config=active_config,
        market_regime=regime_result,
    )

    pending_rows: list[dict] = []
    rejected_rows: list[dict] = []
    for _, row in candidates.iterrows():
        stock_id = str(row["stock_id"]).strip()
        duplicate_reason = ""
        if stock_id in existing_order_ids:
            duplicate_reason = "已有待執行 pending order，略過重複建立"
        elif stock_id in open_position_ids:
            duplicate_reason = "已有未平倉持倉，略過重複建立"
        decision = evaluate_candidate_entry(
            row,
            guardrails,
            created_today=len(pending_rows),
            duplicate_reason=duplicate_reason,
        )
        if not decision.allowed:
            rejected_rows.append(
                _build_rejected_order(
                    row,
                    decision.reason,
                    guardrails,
                    source_report=source_report.name,
                    report_date=trade_date,
                    final_status="REJECTED_GUARDRAIL",
                    pending_config=pending_config,
                )
            )
            continue
        pending_rows.append(_build_pending_order(row, guardrails, pending_config=pending_config))

    new_pending = pd.DataFrame(pending_rows, columns=PENDING_ORDER_COLUMNS)
    all_pending = _merge_pending_orders(existing_pending, new_pending)
    all_pending.to_csv(pending_path, index=False, encoding="utf-8")
    rejected = _merge_rejected_orders(_load_rejected_orders(rejected_path), pd.DataFrame(rejected_rows))
    rejected.to_csv(rejected_path, index=False, encoding="utf-8")

    return PaperTradeResult(
        trade_date=trade_date,
        source_report=source_report,
        positions_path=None,
        pending_orders_path=pending_path,
        rejected_orders_path=rejected_path,
        trades_path=trades_path,
        positions=open_positions,
        new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
        pending_orders=all_pending,
        rejected_orders=rejected,
        skipped_existing=[],
        guardrail_status=guardrails.guardrail_status,
        pause_new_entries_reason=guardrails.pause_reason,
        market_regime_score=guardrails.market_regime_score,
        new_entries_allowed=guardrails.new_entries_allowed,
    )


def find_latest_risk_pass_report(reports_dir: str | Path) -> Path | None:
    report_dir = Path(reports_dir)
    candidates = []
    for path in report_dir.glob("risk_pass_candidates_*.csv"):
        report_date = _date_from_report_path(path)
        if report_date is not None:
            candidates.append((report_date, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _build_pending_order(row: pd.Series, guardrails=None, pending_config: dict | None = None) -> dict:
    expires_after = int((pending_config or {}).get("expire_after_trading_days", 1))
    return {
        "signal_date": str(row["trade_date"]),
        "planned_entry_date": PENDING_ENTRY_MARKER,
        "actual_entry_date": "",
        "created_at": str(row["trade_date"]),
        "expires_after_trading_days": expires_after,
        "expired_at": "",
        "expiry_reason": "",
        "stock_id": str(row["stock_id"]).strip(),
        "stock_name": str(row["stock_name"]),
        "signal_close": float(row["close"]),
        "total_score": float(row.get("total_score", 0)),
        "stop_loss_price": float(row["stop_loss_price"]),
        "suggested_position_pct": float(row["suggested_position_pct"]),
        "status": "PENDING",
        "entry_price": "",
        "entry_price_source": "",
        "shares": "",
        "position_value": "",
        "skipped_reason": "",
        "warning": "",
        "reason": str(row.get("reason", "")),
        "risk_reason": str(row.get("risk_reason", "")),
        "candidate_grade": str(row.get("candidate_grade", "")),
        "grade_reason": str(row.get("grade_reason", "")),
        "grade_risk_flags": str(row.get("grade_risk_flags", "")),
        "requires_manual_review": row.get("requires_manual_review", ""),
        "review_level": str(row.get("review_level", "")),
        "review_reason": str(row.get("review_reason", "")),
        "data_quality_flags": str(row.get("data_quality_flags", "")),
        "investment_risk_flags": str(row.get("investment_risk_flags", "")),
        "multi_factor_score": row.get("multi_factor_score", ""),
        "multi_factor_reason": str(row.get("multi_factor_reason", "")),
        "final_market_score": row.get("final_market_score", ""),
        "confidence_score": row.get("confidence_score", ""),
        "risk_flags": str(row.get("risk_flags", "")),
        "system_comment": str(row.get("system_comment", "")),
        "institutional_score": row.get("institutional_score", ""),
        "credit_score": row.get("credit_score", ""),
        "event_risk_score": row.get("event_risk_score", ""),
        "liquidity_score": row.get("liquidity_score", ""),
        "sector_strength_score": row.get("sector_strength_score", ""),
        "event_risk_level": str(row.get("event_risk_level", "")),
        "event_reason": str(row.get("event_reason", "")),
        "event_blocked": row.get("event_blocked", ""),
        "market_regime_score": getattr(guardrails, "market_regime_score", ""),
        "guardrail_status": getattr(guardrails, "guardrail_status", ""),
        "new_entries_allowed": getattr(guardrails, "new_entries_allowed", ""),
        "rejection_stage": "",
        "rejection_reason": "",
        "original_order_status": "",
        "final_order_status": "PENDING",
        "source_report": "",
        "attempted_execution_date": "",
        "order_age_trading_days": "",
    }


def _build_rejected_order(
    row: pd.Series,
    reason: str,
    guardrails,
    source_report: str,
    report_date: pd.Timestamp,
    final_status: str,
    pending_config: dict | None = None,
) -> dict:
    order = _build_pending_order(row, guardrails, pending_config=pending_config)
    order["status"] = final_status
    order["skipped_reason"] = reason
    order["warning"] = reason
    order["rejection_stage"] = "signal_creation"
    order["rejection_reason"] = reason
    order["original_order_status"] = "NEW_SIGNAL"
    order["final_order_status"] = final_status
    order["source_report"] = source_report
    return _rejected_report_row(
        order,
        report_date=report_date,
        trade_date=report_date,
        stage="signal_creation",
        final_status=final_status,
        reason=reason,
        source_report=source_report,
    )


def _rejected_report_row(
    order: dict | pd.Series,
    report_date: pd.Timestamp,
    trade_date: pd.Timestamp,
    stage: str,
    final_status: str,
    reason: str,
    source_report: str = "",
) -> dict:
    getter = order.get if isinstance(order, dict) else order.get
    return {
        "report_date": pd.to_datetime(report_date).strftime("%Y-%m-%d"),
        "trade_date": pd.to_datetime(trade_date).strftime("%Y-%m-%d"),
        "stock_id": str(getter("stock_id", "")).strip(),
        "stock_name": str(getter("stock_name", "")),
        "source_report": source_report or str(getter("source_report", "")),
        "rejection_stage": stage,
        "rejected_status": final_status,
        "rejection_reason": reason,
        "rejected_reason": reason,
        "original_order_status": str(getter("original_order_status", getter("status", ""))),
        "final_order_status": final_status,
        "signal_date": str(getter("signal_date", "")),
        "planned_entry_date": str(getter("planned_entry_date", "")),
        "attempted_execution_date": str(getter("attempted_execution_date", "")),
        "order_age_trading_days": getter("order_age_trading_days", ""),
        "candidate_grade": str(getter("candidate_grade", "")),
        "decision": str(getter("decision", "")),
        "total_score": getter("total_score", ""),
        "multi_factor_score": getter("multi_factor_score", ""),
        "final_market_score": getter("final_market_score", ""),
        "confidence_score": getter("confidence_score", ""),
        "liquidity_score": getter("liquidity_score", ""),
        "sector_strength_score": getter("sector_strength_score", ""),
        "event_risk_level": str(getter("event_risk_level", "")),
        "market_regime_score": getter("market_regime_score", ""),
        "guardrail_status": str(getter("guardrail_status", "")),
        "new_entries_allowed": getter("new_entries_allowed", ""),
        "expires_after_trading_days": getter("expires_after_trading_days", ""),
        "expired_at": str(getter("expired_at", "")),
        "expiry_reason": str(getter("expiry_reason", "")),
        "status": final_status,
        "skipped_reason": reason,
        "warning": reason,
        "reason": str(getter("reason", "")),
        "risk_reason": str(getter("risk_reason", "")),
        "event_blocked": getter("event_blocked", ""),
    }


def _safe_market_regime(
    engine: Engine | None,
    config: dict,
    trade_date: pd.Timestamp,
    reports_dir: Path,
) -> MarketRegimeResult:
    if engine is None:
        return MarketRegimeResult(
            trade_date=trade_date,
            market_regime_score=100.0,
            source="not_evaluated",
            warning="market regime not evaluated in standalone paper_trade call",
        )
    try:
        return evaluate_market_regime(
            engine=engine,
            config=config,
            trade_date=trade_date,
            reports_dir=reports_dir,
        )
    except Exception as exc:
        return MarketRegimeResult(
            trade_date=trade_date,
            market_regime_score=50.0,
            source="FAILED",
            warning=f"{type(exc).__name__}: {exc}",
        )


def _load_trades(trades_path: Path) -> pd.DataFrame:
    if not trades_path.exists():
        return pd.DataFrame(columns=POSITION_COLUMNS)
    frame = pd.read_csv(trades_path, dtype={"stock_id": str})
    for column in POSITION_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame.copy()


def _load_pending_orders(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=PENDING_ORDER_COLUMNS)
    frame = pd.read_csv(path, dtype={"stock_id": str})
    for column in PENDING_ORDER_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(object)
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame[PENDING_ORDER_COLUMNS].copy()


def _load_rejected_orders(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=REJECTED_ORDER_COLUMNS)
    frame = pd.read_csv(path, dtype={"stock_id": str})
    for column in REJECTED_ORDER_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(object)
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    return frame[REJECTED_ORDER_COLUMNS].copy()


def _merge_rejected_orders(existing: pd.DataFrame, new_orders: pd.DataFrame) -> pd.DataFrame:
    if new_orders.empty:
        merged = existing.copy()
    else:
        for column in REJECTED_ORDER_COLUMNS:
            if column not in new_orders.columns:
                new_orders[column] = ""
        merged = pd.concat([existing, new_orders[REJECTED_ORDER_COLUMNS]], ignore_index=True)
    if merged.empty:
        return pd.DataFrame(columns=REJECTED_ORDER_COLUMNS)
    for column in REJECTED_ORDER_COLUMNS:
        if column not in merged.columns:
            merged[column] = ""
    key_columns = [
        "stock_id",
        "signal_date",
        "rejection_stage",
        "final_order_status",
        "attempted_execution_date",
    ]
    return merged[REJECTED_ORDER_COLUMNS].drop_duplicates(subset=key_columns, keep="last").reset_index(drop=True)


def _merge_pending_orders(existing: pd.DataFrame, new_orders: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new_orders.copy()
    if new_orders.empty:
        return existing.copy()
    combined = pd.concat([existing, new_orders], ignore_index=True)
    combined["stock_id"] = combined["stock_id"].astype(str).str.strip()
    return combined.drop_duplicates(subset=["signal_date", "stock_id"], keep="first")[
        PENDING_ORDER_COLUMNS
    ].reset_index(drop=True)


def _append_trades(existing: pd.DataFrame, new_positions: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new_positions.copy()
    if new_positions.empty:
        return existing.copy()
    columns = list(existing.columns)
    for column in new_positions.columns:
        if column not in columns:
            columns.append(column)
    return pd.concat([existing, new_positions], ignore_index=True)[columns]


def _open_positions(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=POSITION_COLUMNS)
    frame = trades.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    frame = frame[frame["status"] == "OPEN"].copy()
    frame = frame.drop_duplicates(subset=["stock_id"], keep="first")
    return frame[POSITION_COLUMNS].reset_index(drop=True)


def _date_from_report_path(path: Path) -> str | None:
    match = re.search(r"risk_pass_candidates_(\d{8})\.csv$", path.name)
    return match.group(1) if match else None


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}
