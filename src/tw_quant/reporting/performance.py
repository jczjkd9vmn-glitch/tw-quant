"""Paper trading performance report loading and metrics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SUMMARY_COLUMNS = [
    "trade_date",
    "total_equity",
    "unrealized_pnl",
    "realized_pnl",
    "open_positions",
    "closed_positions",
]


@dataclass(frozen=True)
class PaperPerformance:
    summary: pd.DataFrame
    open_positions: pd.DataFrame
    closed_trades: pd.DataFrame
    metrics: dict[str, float]
    warning: str = ""


def load_paper_performance(
    reports_dir: str | Path = "reports",
    capital: float | None = None,
) -> PaperPerformance:
    report_dir = Path(reports_dir)
    trades = _load_trades(report_dir / "paper_trades.csv")
    summary = _load_summary_curve(report_dir)

    if trades.empty and summary.empty:
        return PaperPerformance(
            summary=pd.DataFrame(columns=SUMMARY_COLUMNS),
            open_positions=trades,
            closed_trades=trades,
            metrics=_empty_metrics(capital),
            warning="paper trading reports not found",
        )

    open_positions = trades[trades.get("status", pd.Series(dtype=str)) == "OPEN"].copy()
    closed_trades = trades[trades.get("status", pd.Series(dtype=str)) == "CLOSED"].copy()
    metrics = _calculate_metrics(summary, closed_trades, capital)
    warning = ""
    if summary.empty:
        warning = "paper summary reports not found"

    return PaperPerformance(
        summary=summary,
        open_positions=open_positions,
        closed_trades=closed_trades,
        metrics=metrics,
        warning=warning,
    )


def _load_trades(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, dtype={"stock_id": str})
    if "status" not in frame.columns:
        frame["status"] = ""
    frame["stock_id"] = frame.get("stock_id", pd.Series(dtype=str)).astype(str).str.strip()
    frame["status"] = frame["status"].fillna("").astype(str)
    return frame


def _load_summary_curve(report_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for pattern, priority in [("daily_summary_*.csv", 1), ("paper_summary_*.csv", 2)]:
        for path in report_dir.glob(pattern):
            frame = pd.read_csv(path)
            if "trade_date" not in frame.columns or "total_equity" not in frame.columns:
                continue
            frame = _normalize_summary(frame)
            frame["_priority"] = priority
            frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"])
    combined = combined.sort_values(["trade_date", "_priority"])
    combined = combined.drop_duplicates(subset=["trade_date"], keep="last")
    combined = combined.sort_values("trade_date").reset_index(drop=True)
    return combined[SUMMARY_COLUMNS].copy()


def _normalize_summary(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in SUMMARY_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = 0
    for column in ["total_equity", "unrealized_pnl", "realized_pnl"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0.0)
    for column in ["open_positions", "closed_positions"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)
    return normalized


def _calculate_metrics(
    summary: pd.DataFrame,
    closed_trades: pd.DataFrame,
    capital: float | None,
) -> dict[str, float]:
    metrics = _empty_metrics(capital)
    if not summary.empty:
        equity = pd.to_numeric(summary["total_equity"], errors="coerce").fillna(0.0)
        start_capital = float(capital) if capital else float(equity.iloc[0])
        metrics["total_return_pct"] = (float(equity.iloc[-1]) / start_capital - 1) if start_capital else 0.0
        running_max = equity.cummax()
        drawdown = (equity / running_max.replace(0, pd.NA)) - 1
        metrics["max_drawdown"] = float(drawdown.fillna(0.0).min())
        metrics["open_positions"] = float(summary["open_positions"].iloc[-1])
        metrics["closed_positions"] = float(summary["closed_positions"].iloc[-1])

    if not closed_trades.empty and "realized_pnl" in closed_trades.columns:
        realized = pd.to_numeric(closed_trades["realized_pnl"], errors="coerce").fillna(0.0)
        metrics["win_rate"] = float((realized > 0).sum() / len(realized)) if len(realized) else 0.0
    return metrics


def _empty_metrics(capital: float | None) -> dict[str, float]:
    return {
        "open_positions": 0.0,
        "closed_positions": 0.0,
        "win_rate": 0.0,
        "total_return_pct": 0.0,
        "max_drawdown": 0.0,
        "capital": float(capital) if capital else 0.0,
    }
