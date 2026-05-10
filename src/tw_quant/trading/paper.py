"""Paper trading position creation from risk-passed candidate reports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


POSITION_COLUMNS = [
    "trade_date",
    "stock_id",
    "stock_name",
    "entry_price",
    "shares",
    "position_value",
    "stop_loss_price",
    "suggested_position_pct",
    "status",
]


@dataclass(frozen=True)
class PaperTradeResult:
    trade_date: pd.Timestamp | None
    source_report: Path | None
    positions_path: Path | None
    trades_path: Path
    positions: pd.DataFrame
    new_positions: pd.DataFrame
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
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
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
            trades_path=trades_path,
            positions=pd.DataFrame(columns=POSITION_COLUMNS),
            new_positions=pd.DataFrame(columns=POSITION_COLUMNS),
            skipped_existing=[],
            warning=f"risk pass report is empty: {source_report}",
        )

    report_dir.mkdir(parents=True, exist_ok=True)
    existing_trades = _load_trades(trades_path)
    existing_open_ids = set(existing_trades[existing_trades["status"] == "OPEN"]["stock_id"])

    new_rows: list[dict] = []
    skipped_existing: list[str] = []
    for _, row in candidates.iterrows():
        stock_id = str(row["stock_id"]).strip()
        if stock_id in existing_open_ids:
            skipped_existing.append(stock_id)
            continue

        position = _build_position(row, capital)
        if position["shares"] <= 0:
            continue
        new_rows.append(position)
        existing_open_ids.add(stock_id)

    new_positions = pd.DataFrame(new_rows, columns=POSITION_COLUMNS)
    all_trades = _append_trades(existing_trades, new_positions)
    if not all_trades.empty:
        all_trades.to_csv(trades_path, index=False, encoding="utf-8-sig")

    open_positions = _open_positions(all_trades)
    trade_date = pd.to_datetime(candidates["trade_date"].iloc[0])
    positions_path = report_dir / f"paper_positions_{trade_date.strftime('%Y%m%d')}.csv"
    open_positions.to_csv(positions_path, index=False, encoding="utf-8-sig")

    return PaperTradeResult(
        trade_date=trade_date,
        source_report=source_report,
        positions_path=positions_path,
        trades_path=trades_path,
        positions=open_positions,
        new_positions=new_positions,
        skipped_existing=skipped_existing,
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


def _build_position(row: pd.Series, capital: float) -> dict:
    entry_price = float(row["close"])
    suggested_pct = float(row["suggested_position_pct"])
    target_value = float(capital) * suggested_pct
    shares = int(target_value // entry_price) if entry_price > 0 else 0
    position_value = round(shares * entry_price, 2)
    return {
        "trade_date": str(row["trade_date"]),
        "stock_id": str(row["stock_id"]).strip(),
        "stock_name": str(row["stock_name"]),
        "entry_price": entry_price,
        "shares": shares,
        "position_value": position_value,
        "stop_loss_price": float(row["stop_loss_price"]),
        "suggested_position_pct": suggested_pct,
        "status": "OPEN",
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
