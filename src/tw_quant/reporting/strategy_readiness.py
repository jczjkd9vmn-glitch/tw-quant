"""Strategy sample maturity checks for alpha conclusions.

The readiness checks are report-only. They do not change scoring, orders, or
paper trading state; they only decide whether an alpha conclusion has enough
strategy-side history to be shown as judgeable.
"""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


STRATEGY_ALPHA_WINDOWS = [1, 5, 20, 60, 120, 252]
MIN_VALID_TRADE_COUNT_FOR_ALPHA = 20


def strategy_readiness_snapshot(
    reports_dir: str | Path,
    selected_date: str | pd.Timestamp | None = None,
    *,
    equity_frame: pd.DataFrame | None = None,
    trades_frame: pd.DataFrame | None = None,
    portfolio_frame: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Return strategy-side sample maturity flags for common alpha windows."""

    report_dir = Path(reports_dir)
    target_date = _parse_date(selected_date)
    equity = _normalize_equity_frame(
        equity_frame if equity_frame is not None else _read_equity_reports(report_dir),
        target_date,
    )
    trades = _normalize_trades(
        trades_frame if trades_frame is not None else _read_csv(report_dir / "paper_trades.csv"),
        target_date,
    )
    portfolio = _normalize_portfolio(
        portfolio_frame if portfolio_frame is not None else _read_latest_report(report_dir, "paper_portfolio_*.csv"),
        target_date,
    )

    strategy_history_days = int(equity["trade_date"].nunique()) if not equity.empty else 0
    valid_trade_count = int(len(trades))
    holding_record_count = int(len(portfolio))
    open_position_count = _open_position_count(portfolio)
    flags = {
        f"can_judge_strategy_alpha_{days}d": bool(
            strategy_history_days > days and valid_trade_count >= MIN_VALID_TRADE_COUNT_FOR_ALPHA
        )
        for days in STRATEGY_ALPHA_WINDOWS
    }
    return {
        "strategy_history_days": strategy_history_days,
        "valid_trade_count": valid_trade_count,
        "holding_record_count": holding_record_count,
        "open_position_count": open_position_count,
        "min_valid_trade_count_for_alpha": MIN_VALID_TRADE_COUNT_FOR_ALPHA,
        "can_judge_strategy_alpha": any(flags.values()),
        **flags,
    }


def strategy_can_judge_window(readiness: dict[str, object], window: str) -> bool:
    """Return whether the strategy sample is mature enough for a window."""

    if window == "total":
        return bool(readiness.get("can_judge_strategy_alpha", False))
    if window == "1d":
        return bool(readiness.get("can_judge_strategy_alpha_1d", False))
    return bool(readiness.get(f"can_judge_strategy_alpha_{window}", False))


def strategy_insufficient_reason(readiness: dict[str, object], window: str) -> str:
    """Human-readable reason when strategy sample maturity is insufficient."""

    days = _window_days(window)
    history_days = int(readiness.get("strategy_history_days", 0) or 0)
    valid_trades = int(readiness.get("valid_trade_count", 0) or 0)
    min_trades = int(readiness.get("min_valid_trade_count_for_alpha", MIN_VALID_TRADE_COUNT_FOR_ALPHA) or 0)
    reasons = []
    if days and history_days <= days:
        reasons.append(f"strategy_history_days={history_days} 不足以計算 {window} alpha")
    if valid_trades < min_trades:
        reasons.append(f"valid_trade_count={valid_trades} 低於成熟度門檻 {min_trades}")
    return "；".join(reasons) or "策略樣本不足"


def _read_equity_reports(report_dir: Path) -> pd.DataFrame:
    pnl = _read_all_reports(report_dir, "pnl_chart_data_*.csv")
    if not pnl.empty:
        return pnl
    return _read_all_reports(report_dir, "paper_summary_*.csv")


def _normalize_equity_frame(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame(columns=["trade_date", "equity"])
    output = frame.copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"], errors="coerce")
    output = output.dropna(subset=["trade_date"])
    if selected_date is not None:
        output = output[output["trade_date"] <= selected_date]
    equity_column = _first_existing_column(output, ["equity", "total_equity_after_cost", "total_equity"])
    if equity_column is None:
        return pd.DataFrame(columns=["trade_date", "equity"])
    output["equity"] = pd.to_numeric(output[equity_column], errors="coerce")
    output = output.dropna(subset=["equity"]).sort_values("trade_date")
    output = output.drop_duplicates("trade_date", keep="last")
    return output[["trade_date", "equity"]].reset_index(drop=True)


def _normalize_trades(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "stock_id" not in frame.columns:
        return pd.DataFrame(columns=["stock_id"])
    output = frame.copy()
    output["stock_id"] = output["stock_id"].astype(str).str.strip()
    output = output[output["stock_id"] != ""].copy()
    if selected_date is not None:
        output["_effective_date"] = output.apply(_effective_trade_date, axis=1)
        output = output[(output["_effective_date"].isna()) | (output["_effective_date"] <= selected_date)].copy()
    valid = pd.Series(False, index=output.index)
    for column in ["entry_price", "entry_price_raw", "shares", "original_shares", "remaining_shares"]:
        if column in output.columns:
            valid = valid | (pd.to_numeric(output[column], errors="coerce").fillna(0) > 0)
    if "status" in output.columns:
        valid = valid | (output["status"].fillna("").astype(str).str.strip() != "")
    return output[valid].copy()


def _normalize_portfolio(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "stock_id" not in frame.columns:
        return pd.DataFrame(columns=["stock_id"])
    output = frame.copy()
    output["stock_id"] = output["stock_id"].astype(str).str.strip()
    output = output[output["stock_id"] != ""].copy()
    if selected_date is not None and "trade_date" in output.columns:
        dates = pd.to_datetime(output["trade_date"], errors="coerce")
        output = output[(dates.isna()) | (dates <= selected_date)].copy()
    return output


def _open_position_count(portfolio: pd.DataFrame) -> int:
    if portfolio.empty:
        return 0
    if "status" not in portfolio.columns:
        return int(len(portfolio))
    statuses = portfolio["status"].fillna("").astype(str).str.upper().str.strip()
    if statuses.empty:
        return 0
    return int((~statuses.isin({"CLOSED", "EXITED", "SOLD"})).sum())


def _effective_trade_date(row: pd.Series) -> pd.Timestamp | None:
    for column in ["trade_date", "actual_entry_date", "planned_entry_date", "signal_date", "exit_date"]:
        if column not in row.index:
            continue
        parsed = _parse_date(row.get(column))
        if parsed is not None:
            return parsed
    return None


def _read_all_reports(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = [_read_csv(path) for path in _sorted_report_files(report_dir, pattern)]
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _read_latest_report(report_dir: Path, pattern: str) -> pd.DataFrame:
    files = _sorted_report_files(report_dir, pattern)
    return _read_csv(files[-1]) if files else pd.DataFrame()


def _sorted_report_files(report_dir: Path, pattern: str) -> list[Path]:
    return sorted(
        report_dir.glob(pattern),
        key=lambda path: (_date_from_path(path) or pd.Timestamp.min, path.stat().st_mtime),
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    return _parse_date(match.group(1))


def _parse_date(value: object) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _window_days(window: str) -> int | None:
    if window == "total":
        return None
    match = re.match(r"^(\d+)d$", str(window))
    return int(match.group(1)) if match else None


def _first_existing_column(frame: pd.DataFrame, columns: list[str]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None
