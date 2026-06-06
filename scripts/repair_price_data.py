from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3
import sys
from typing import Iterable
from uuid import uuid4

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tw_quant.risk.controls import detect_price_jumps


PRICE_COLUMNS = [
    "id",
    "trade_date",
    "symbol",
    "name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "market",
    "source",
    "fetched_at",
]


QUARANTINE_COLUMNS = [
    "original_daily_price_id",
    "trade_date",
    "symbol",
    "name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "market",
    "source",
    "fetched_at",
    "quarantine_reason",
    "quarantined_at",
    "repair_run_id",
]


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repair or quarantine contaminated daily_prices rows.")
    parser.add_argument("--db", default=str(ROOT / "data" / "tw_quant.sqlite"), help="SQLite database path.")
    parser.add_argument("--mode", choices=["dry-run", "apply"], default="dry-run")
    parser.add_argument("--backup-dir", default=str(ROOT / "data" / "backups"))
    parser.add_argument("--max-abs-daily-return", type=float, default=0.50)
    args = parser.parse_args(list(argv) if argv is not None else None)

    result = repair_price_data(
        db_path=Path(args.db),
        mode=args.mode,
        backup_dir=Path(args.backup_dir),
        max_abs_daily_return=args.max_abs_daily_return,
    )
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


def repair_price_data(
    db_path: Path,
    *,
    mode: str = "dry-run",
    backup_dir: Path | None = None,
    max_abs_daily_return: float = 0.50,
) -> dict[str, object]:
    if mode not in {"dry-run", "apply"}:
        raise ValueError("mode must be dry-run or apply")
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    backup_path = backup_sqlite(db_path, backup_dir or db_path.parent / "backups")
    with sqlite3.connect(db_path) as conn:
        prices = _load_daily_prices(conn)
        issues = analyze_price_issues(prices, max_abs_daily_return=max_abs_daily_return)
        contaminated_ids = _contaminated_ids(issues)
        if mode == "apply" and contaminated_ids:
            _apply_quarantine(conn, prices, issues, contaminated_ids)
    return {
        "mode": mode,
        "backup_path": str(backup_path),
        "scanned_rows": len(prices),
        "contaminated_rows": len(contaminated_ids),
        "weekend_rows": len(issues.get("weekend", pd.DataFrame())),
        "duplicate_rows": len(issues.get("duplicate_trade_date_symbol", pd.DataFrame())),
        "non_positive_ohlc_rows": len(issues.get("non_positive_ohlc", pd.DataFrame())),
        "invalid_high_low_rows": len(issues.get("invalid_high_low", pd.DataFrame())),
        "negative_volume_rows": len(issues.get("negative_volume", pd.DataFrame())),
        "price_jump_rows": len(issues.get("price_jump", pd.DataFrame())),
        "applied": mode == "apply",
    }


def backup_sqlite(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    backup_path = backup_dir / f"{db_path.stem}_{timestamp}.sqlite.bak"
    shutil.copy2(db_path, backup_path)
    return backup_path


def analyze_price_issues(prices: pd.DataFrame, *, max_abs_daily_return: float = 0.50) -> dict[str, pd.DataFrame]:
    if prices.empty:
        return {}
    frame = prices.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    ohlc_columns = [column for column in ["open", "high", "low", "close"] if column in frame.columns]
    issues: dict[str, pd.DataFrame] = {
        "weekend": frame[frame["trade_date"].dt.weekday >= 5].copy(),
        "duplicate_trade_date_symbol": frame[frame.duplicated(["trade_date", "symbol"], keep=False)].copy(),
        "non_positive_ohlc": frame[(frame[ohlc_columns] <= 0).any(axis=1)].copy() if ohlc_columns else pd.DataFrame(),
        "negative_volume": frame[frame["volume"] < 0].copy() if "volume" in frame.columns else pd.DataFrame(),
    }
    if {"open", "high", "low", "close"}.issubset(frame.columns):
        issues["invalid_high_low"] = frame[
            (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
            | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        ].copy()
    else:
        issues["invalid_high_low"] = pd.DataFrame()

    jumps = detect_price_jumps(
        frame[["trade_date", "symbol", "close"]].dropna(subset=["trade_date", "symbol", "close"]),
        max_abs_daily_return=max_abs_daily_return,
    )
    if not jumps.empty:
        jump_keys = set(zip(jumps["symbol"].astype(str), pd.to_datetime(jumps["trade_date"]).dt.strftime("%Y-%m-%d")))
        frame["_jump_key"] = list(zip(frame["symbol"].astype(str), frame["trade_date"].dt.strftime("%Y-%m-%d")))
        issues["price_jump"] = frame[frame["_jump_key"].isin(jump_keys)].drop(columns=["_jump_key"]).copy()
    else:
        issues["price_jump"] = pd.DataFrame()
    return issues


def _load_daily_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    table = pd.read_sql_query("select name from sqlite_master where type='table' and name='daily_prices'", conn)
    if table.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    return pd.read_sql_query("select * from daily_prices", conn)


def _contaminated_ids(issues: dict[str, pd.DataFrame]) -> set[int]:
    ids: set[int] = set()
    for frame in issues.values():
        if frame.empty or "id" not in frame.columns:
            continue
        ids.update(int(value) for value in pd.to_numeric(frame["id"], errors="coerce").dropna().tolist())
    return ids


def _apply_quarantine(
    conn: sqlite3.Connection,
    prices: pd.DataFrame,
    issues: dict[str, pd.DataFrame],
    contaminated_ids: set[int],
) -> None:
    _ensure_quarantine_table(conn)
    reasons = _reason_by_id(issues)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    run_id = str(uuid4())
    contaminated = prices[pd.to_numeric(prices["id"], errors="coerce").isin(contaminated_ids)].copy()
    records = []
    for _, row in contaminated.iterrows():
        original_id = int(row["id"])
        records.append(
            {
                "original_daily_price_id": original_id,
                "trade_date": str(row.get("trade_date", "")),
                "symbol": str(row.get("symbol", "")),
                "name": str(row.get("name", "")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "turnover": row.get("turnover"),
                "market": str(row.get("market", "")),
                "source": str(row.get("source", "")),
                "fetched_at": str(row.get("fetched_at", "")),
                "quarantine_reason": ";".join(reasons.get(original_id, ["unknown_price_data_issue"])),
                "quarantined_at": now,
                "repair_run_id": run_id,
            }
        )
    placeholders = ",".join("?" for _ in QUARANTINE_COLUMNS)
    sql = f"insert or ignore into daily_prices_quarantine ({','.join(QUARANTINE_COLUMNS)}) values ({placeholders})"
    conn.executemany(sql, [[record.get(column) for column in QUARANTINE_COLUMNS] for record in records])
    conn.executemany("delete from daily_prices where id = ?", [(value,) for value in sorted(contaminated_ids)])
    conn.commit()


def _reason_by_id(issues: dict[str, pd.DataFrame]) -> dict[int, list[str]]:
    output: dict[int, list[str]] = {}
    for reason, frame in issues.items():
        if frame.empty or "id" not in frame.columns:
            continue
        for value in pd.to_numeric(frame["id"], errors="coerce").dropna().tolist():
            output.setdefault(int(value), []).append(reason)
    return output


def _ensure_quarantine_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists daily_prices_quarantine (
            id integer primary key autoincrement,
            original_daily_price_id integer not null unique,
            trade_date text not null,
            symbol text not null,
            name text,
            open real,
            high real,
            low real,
            close real,
            volume real,
            turnover real,
            market text,
            source text,
            fetched_at text,
            quarantine_reason text not null,
            quarantined_at text not null,
            repair_run_id text not null
        )
        """
    )


if __name__ == "__main__":
    raise SystemExit(main())
