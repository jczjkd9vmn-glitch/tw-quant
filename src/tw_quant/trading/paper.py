"""Paper trading position creation from risk-passed candidate reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


POSITION_COLUMNS = [
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
    "entry_price_source",
    "trade_date",
    "stock_id",
    "stock_name",
    "entry_price",
    "shares",
    "position_value",
    "entry_slippage",
    "entry_commission",
    "exit_slippage",
    "exit_commission",
    "exit_tax",
    "total_cost",
    "realized_pnl_after_cost",
    "realized_pnl_pct_after_cost",
    "stop_loss_price",
    "suggested_position_pct",
    "status",
]

PENDING_ENTRY_MARKER = "NEXT_AVAILABLE_TRADING_DAY"

PENDING_ORDER_COLUMNS = [
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
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
]


@dataclass(frozen=True)
class PaperTradeResult:
    trade_date: pd.Timestamp | None
    source_report: Path | None
    positions_path: Path | None
    pending_orders_path: Path | None
    trades_path: Path
    positions: pd.DataFrame
    new_positions: pd.DataFrame
    pending_orders: pd.DataFrame
    skipped_existing: list[str]
    warning: str = ""


def run_paper_trade(
    reports_dir: str | Path = "reports",
    capital: float = 1_000_000,
) -> PaperTradeResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    source_report = find_latest_risk_pass_report(report_dir)
    if source_report is None:
        return PaperTradeResult(
            trade_date=None,
            source_report=None,
            positions_path=None,
            pending_orders_path=None,
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
            pending_orders=pd.DataFrame(columns=PENDING_ORDER_COLUMNS),
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
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
            pending_orders=pd.DataFrame(columns=PENDING_ORDER_COLUMNS),
            skipped_existing=[],
            warning=f"risk pass report is empty: {source_report}",
        )

    report_dir.mkdir(parents=True, exist_ok=True)
    existing_trades = _load_trades(trades_path)
    open_positions = _open_positions(existing_trades)
    trade_date = pd.to_datetime(candidates["trade_date"].iloc[0])
    pending_path = report_dir / f"pending_orders_{trade_date.strftime('%Y%m%d')}.csv"
    existing_pending = _load_pending_orders(pending_path)
    existing_order_ids = set(existing_pending["stock_id"]) if not existing_pending.empty else set()

    pending_rows: list[dict] = []
    for _, row in candidates.iterrows():
        stock_id = str(row["stock_id"]).strip()
        if stock_id in existing_order_ids:
            continue
        pending_rows.append(_build_pending_order(row))

    new_pending = pd.DataFrame(pending_rows, columns=PENDING_ORDER_COLUMNS)
    all_pending = _merge_pending_orders(existing_pending, new_pending)
    all_pending.to_csv(pending_path, index=False, encoding="utf-8-sig")

    return PaperTradeResult(
        trade_date=trade_date,
        source_report=source_report,
        positions_path=None,
        pending_orders_path=pending_path,
        trades_path=trades_path,
        positions=open_positions,
        new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
        pending_orders=all_pending,
        skipped_existing=[],
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


def _build_pending_order(row: pd.Series) -> dict:
    return {
        "signal_date": str(row["trade_date"]),
        "planned_entry_date": PENDING_ENTRY_MARKER,
        "actual_entry_date": "",
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
    }


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
