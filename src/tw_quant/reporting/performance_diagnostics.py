"""Report-only performance risk diagnostics for paper trading.

This module intentionally uses only pandas and the standard library. QuantStats
can be considered later, but the current implementation keeps the daily workflow
dependency-light and fallback-safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from pathlib import Path
import re

import pandas as pd


PERFORMANCE_DIAGNOSTICS_COLUMNS = [
    "trade_date",
    "source",
    "observation_start",
    "observation_end",
    "observation_count",
    "daily_return_count",
    "cumulative_return",
    "annualized_return",
    "volatility",
    "sharpe_like_ratio",
    "sortino_like_ratio",
    "max_drawdown",
    "win_rate_by_day",
    "best_day",
    "best_day_return",
    "worst_day",
    "worst_day_return",
    "profit_factor",
    "benchmark_return",
    "benchmark_window",
    "alpha",
    "benchmark_source",
    "benchmark_warning",
    "status",
    "data_quality_warning",
    "notes",
]


@dataclass(frozen=True)
class PerformanceDiagnosticsResult:
    trade_date: pd.Timestamp | None
    frame: pd.DataFrame
    output_path: Path
    status: str = "OK"
    warning: str = ""


def generate_performance_diagnostics(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
) -> PerformanceDiagnosticsResult:
    """Generate performance tearsheet-style risk metrics from local reports."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    date_label = (selected_date or pd.Timestamp.today()).strftime("%Y%m%d")

    equity_frame, source = _daily_equity_series(report_dir, selected_date)
    benchmark = _benchmark_return(report_dir, selected_date, max(len(equity_frame) - 1, 0))
    row = _performance_row(equity_frame, source, selected_date, benchmark)
    frame = pd.DataFrame([row], columns=PERFORMANCE_DIAGNOSTICS_COLUMNS)
    output_path = report_dir / f"performance_diagnostics_{date_label}.csv"
    frame.to_csv(output_path, index=False, encoding="utf-8-sig")

    status = str(row.get("status") or "OK")
    warning = str(row.get("data_quality_warning") or "")
    return PerformanceDiagnosticsResult(
        trade_date=selected_date,
        frame=frame,
        output_path=output_path,
        status=status,
        warning=warning,
    )


def _daily_equity_series(report_dir: Path, selected_date: pd.Timestamp | None) -> tuple[pd.DataFrame, str]:
    pnl_data = _read_all_reports(report_dir, "pnl_chart_data_*.csv")
    frame = _normalize_equity_frame(pnl_data, selected_date)
    if not frame.empty:
        return frame, "pnl_chart_data"

    summaries = _read_all_reports(report_dir, "paper_summary_*.csv")
    frame = _normalize_equity_frame(summaries, selected_date)
    if not frame.empty:
        return frame, "paper_summary"

    return pd.DataFrame(columns=["trade_date", "equity", "total_return_pct"]), "missing"


def _normalize_equity_frame(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame(columns=["trade_date", "equity", "total_return_pct"])

    output = frame.copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"], errors="coerce")
    output = output.dropna(subset=["trade_date"])
    if selected_date is not None:
        output = output[output["trade_date"] <= selected_date]
    if output.empty:
        return pd.DataFrame(columns=["trade_date", "equity", "total_return_pct"])

    equity_column = _first_existing_column(output, ["total_equity_after_cost", "total_equity"])
    if equity_column is None:
        return pd.DataFrame(columns=["trade_date", "equity", "total_return_pct"])
    output["equity"] = pd.to_numeric(output[equity_column], errors="coerce")

    if "total_return_pct" in output.columns:
        output["total_return_pct"] = pd.to_numeric(output["total_return_pct"], errors="coerce")
    elif "total_capital" in output.columns:
        capital = pd.to_numeric(output["total_capital"], errors="coerce")
        output["total_return_pct"] = output["equity"] / capital - 1.0
    else:
        output["total_return_pct"] = pd.NA

    output = output.dropna(subset=["equity"]).sort_values("trade_date")
    output = output.drop_duplicates("trade_date", keep="last")
    return output[["trade_date", "equity", "total_return_pct"]].reset_index(drop=True)


def _performance_row(
    equity_frame: pd.DataFrame,
    source: str,
    selected_date: pd.Timestamp | None,
    benchmark: dict[str, object],
) -> dict[str, object]:
    if equity_frame.empty:
        return _empty_row(selected_date, "missing", "DATA_INSUFFICIENT: no pnl_chart_data or paper_summary equity series")

    frame = equity_frame.copy()
    returns = frame["equity"].pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
    cumulative_return = _latest_total_return(frame)
    series_return = _series_cumulative_return(frame)
    annualized_return = _annualized_return(series_return, len(returns))
    volatility = _volatility(returns)
    sharpe = _sharpe_like(returns)
    sortino = _sortino_like(returns)
    drawdown = _max_drawdown(frame["equity"])
    win_rate = _win_rate(returns)
    best_day, best_day_return = _extreme_day(frame, returns, "max")
    worst_day, worst_day_return = _extreme_day(frame, returns, "min")
    profit_factor = _profit_factor(returns)
    benchmark_return = _num(benchmark.get("return"))
    alpha = _sub_or_none(cumulative_return, benchmark_return)

    warnings: list[str] = []
    if len(returns) < 5:
        warnings.append("DATA_INSUFFICIENT: daily_return_count < 5")
    if benchmark.get("warning"):
        warnings.append(str(benchmark["warning"]))
    if benchmark_return is None:
        warnings.append("DATA_INSUFFICIENT: benchmark_return unavailable")

    status = "DATA_INSUFFICIENT" if any("DATA_INSUFFICIENT" in item for item in warnings) else "OK"
    if status == "OK" and warnings:
        status = "OK_WITH_WARNINGS"

    return {
        "trade_date": _date_text(selected_date or frame["trade_date"].max()),
        "source": source,
        "observation_start": _date_text(frame["trade_date"].min()),
        "observation_end": _date_text(frame["trade_date"].max()),
        "observation_count": int(len(frame)),
        "daily_return_count": int(len(returns)),
        "cumulative_return": _round(cumulative_return),
        "annualized_return": _round(annualized_return),
        "volatility": _round(volatility),
        "sharpe_like_ratio": _round(sharpe),
        "sortino_like_ratio": _round(sortino),
        "max_drawdown": _round(drawdown),
        "win_rate_by_day": _round(win_rate),
        "best_day": best_day,
        "best_day_return": _round(best_day_return),
        "worst_day": worst_day,
        "worst_day_return": _round(worst_day_return),
        "profit_factor": _round(profit_factor),
        "benchmark_return": _round(benchmark_return),
        "benchmark_window": benchmark.get("window", ""),
        "alpha": _round(alpha),
        "benchmark_source": benchmark.get("source", "benchmark 資料不足"),
        "benchmark_warning": benchmark.get("warning", ""),
        "status": status,
        "data_quality_warning": "; ".join(dict.fromkeys(warnings)),
        "notes": "報表診斷用途；不修改交易策略、出場規則或訂單。",
    }


def _empty_row(selected_date: pd.Timestamp | None, source: str, warning: str) -> dict[str, object]:
    row = {column: "" for column in PERFORMANCE_DIAGNOSTICS_COLUMNS}
    row.update(
        {
            "trade_date": _date_text(selected_date) if selected_date is not None else "",
            "source": source,
            "observation_count": 0,
            "daily_return_count": 0,
            "status": "DATA_INSUFFICIENT",
            "data_quality_warning": warning,
            "notes": "報表診斷用途；不修改交易策略、出場規則或訂單。",
        }
    )
    return row


def _benchmark_return(report_dir: Path, selected_date: pd.Timestamp | None, return_days: int) -> dict[str, object]:
    market_regime = _read_latest(report_dir, "market_regime_*.csv", selected_date)
    sector_strength = _read_sector_strength(report_dir)
    window = "20d" if return_days >= 20 else ("5d" if return_days >= 5 else "1d")

    regime_row = market_regime.iloc[0].to_dict() if not market_regime.empty else {}
    if str(regime_row.get("source", "")).strip().lower() == "index":
        value = _benchmark_value_for_window(regime_row, "market_return", window)
        if value is not None:
            return {"return": value, "window": window, "source": "加權指數", "warning": ""}

    if not sector_strength.empty and "stock_id" in sector_strength.columns:
        frame = sector_strength.copy()
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
        etf_0050 = frame[frame["stock_id"] == "0050"]
        if not etf_0050.empty:
            value = _benchmark_value_for_window(etf_0050.iloc[0].to_dict(), "stock_return", window)
            if value is not None:
                return {
                    "return": value,
                    "window": window,
                    "source": "0050 fallback",
                    "warning": "未使用正式加權指數資料；benchmark fallback 使用 0050。",
                }
        value_frame = frame.dropna(subset=["market_return_5d", "market_return_20d"], how="all") if {
            "market_return_5d",
            "market_return_20d",
        }.issubset(frame.columns) else pd.DataFrame()
        if not value_frame.empty:
            value = _benchmark_value_for_window(value_frame.iloc[0].to_dict(), "market_return", window)
            if value is not None:
                return {
                    "return": value,
                    "window": window,
                    "source": "全市場等權 fallback",
                    "warning": "未使用正式加權指數資料；benchmark fallback 使用全市場等權報酬。",
                }

    return {
        "return": None,
        "window": window,
        "source": "benchmark 資料不足",
        "warning": "缺少正式加權指數、0050 與全市場等權資料，無法計算績效 alpha。",
    }


def _benchmark_value_for_window(row: dict[str, object], prefix: str, window: str) -> float | None:
    candidates = [window]
    if window == "1d":
        candidates.extend(["5d", "20d"])
    elif window == "5d":
        candidates.append("20d")
    for candidate in candidates:
        value = _normalized_return(row.get(f"{prefix}_{candidate}"))
        if value is not None:
            return value
    return None


def _latest_total_return(frame: pd.DataFrame) -> float | None:
    if "total_return_pct" in frame.columns:
        values = pd.to_numeric(frame["total_return_pct"], errors="coerce").dropna()
        if not values.empty:
            return float(values.iloc[-1])
    return _series_cumulative_return(frame)


def _series_cumulative_return(frame: pd.DataFrame) -> float | None:
    if frame.empty or len(frame) < 2:
        return None
    first = _num(frame.iloc[0].get("equity"))
    last = _num(frame.iloc[-1].get("equity"))
    if first is None or last is None or abs(first) < 0.000001:
        return None
    return last / first - 1.0


def _annualized_return(cumulative_return: float | None, return_count: int) -> float | None:
    if cumulative_return is None or return_count <= 0 or 1 + cumulative_return <= 0:
        return None
    return (1 + cumulative_return) ** (252 / return_count) - 1


def _volatility(returns: pd.Series) -> float | None:
    if returns.empty:
        return None
    std = float(returns.std(ddof=0))
    return std * sqrt(252)


def _sharpe_like(returns: pd.Series) -> float | None:
    if returns.empty:
        return None
    std = float(returns.std(ddof=0))
    if abs(std) < 0.000001:
        return None
    return float(returns.mean()) / std * sqrt(252)


def _sortino_like(returns: pd.Series) -> float | None:
    if returns.empty:
        return None
    downside = returns[returns < 0]
    if downside.empty:
        return None
    std = float(downside.std(ddof=0))
    if abs(std) < 0.000001:
        return None
    return float(returns.mean()) / std * sqrt(252)


def _max_drawdown(equity: pd.Series) -> float | None:
    values = pd.to_numeric(equity, errors="coerce").dropna()
    if values.empty:
        return None
    drawdown = values / values.cummax() - 1.0
    return float(drawdown.min())


def _win_rate(returns: pd.Series) -> float | None:
    if returns.empty:
        return None
    return float((returns > 0).mean())


def _extreme_day(frame: pd.DataFrame, returns: pd.Series, mode: str) -> tuple[str, float | None]:
    if returns.empty:
        return "", None
    index = returns.idxmax() if mode == "max" else returns.idxmin()
    date_value = frame.loc[index, "trade_date"] if index in frame.index else None
    return _date_text(date_value), float(returns.loc[index])


def _profit_factor(returns: pd.Series) -> float | None:
    if returns.empty:
        return None
    gains = float(returns[returns > 0].sum())
    losses = abs(float(returns[returns < 0].sum()))
    if losses < 0.000001:
        return None
    return gains / losses


def _read_all_reports(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = [_read_csv(path) for path in _sorted_report_files(report_dir, pattern)]
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _read_latest(report_dir: Path, pattern: str, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    files = _sorted_report_files(report_dir, pattern)
    if selected_date is not None:
        target = selected_date.strftime("%Y%m%d")
        for path in files:
            if target in path.name:
                return _read_csv(path)
    return _read_csv(files[-1]) if files else pd.DataFrame()


def _read_sector_strength(report_dir: Path) -> pd.DataFrame:
    for path in [report_dir / "sector_strength.csv", report_dir.parent / "data" / "sector_strength.csv"]:
        frame = _read_csv(path)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _sorted_report_files(report_dir: Path, pattern: str) -> list[Path]:
    return sorted(
        report_dir.glob(pattern),
        key=lambda path: (_date_from_path(path) or pd.Timestamp.min, path.stat().st_mtime),
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _resolve_trade_date(report_dir: Path, trade_date: str | None) -> pd.Timestamp | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    for pattern in ["pnl_chart_data_*.csv", "paper_summary_*.csv", "daily_summary_*.csv"]:
        files = _sorted_report_files(report_dir, pattern)
        if files:
            parsed = _date_from_path(files[-1])
            if parsed is not None:
                return parsed
    return None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _first_existing_column(frame: pd.DataFrame, columns: list[str]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _normalized_return(value: object) -> float | None:
    number = _num(value)
    if number is None:
        return None
    if abs(number) > 0.5 and abs(number) <= 100:
        return number / 100.0
    if abs(number) > 100:
        return None
    return number


def _sub_or_none(left: object, right: object) -> float | None:
    left_number = _num(left)
    right_number = _num(right)
    if left_number is None or right_number is None:
        return None
    return left_number - right_number


def _num(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _round(value: object, digits: int = 6) -> float | None:
    number = _num(value)
    if number is None:
        return None
    return round(number, digits)


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")
