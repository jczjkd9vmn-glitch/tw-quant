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

from tw_quant.reporting.benchmark import benchmark_return_for_window
from tw_quant.reporting.risk_adjusted_alpha import RISK_ADJUSTED_ALPHA_COLUMNS, risk_adjusted_alpha_snapshot
from tw_quant.reporting.strategy_readiness import (
    STRATEGY_ALPHA_WINDOWS,
    strategy_can_judge_window,
    strategy_insufficient_reason,
    strategy_readiness_snapshot,
)


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
    "benchmark_is_official",
    "fallback_reason",
    "can_judge_alpha",
    "can_judge_alpha_5d",
    "can_judge_alpha_20d",
    "can_judge_alpha_60d",
    "can_judge_alpha_120d",
    "can_judge_alpha_252d",
    "benchmark_history_days",
    "strategy_history_days",
    "valid_trade_count",
    "holding_record_count",
    "can_judge_strategy_alpha",
    "can_judge_strategy_alpha_5d",
    "can_judge_strategy_alpha_20d",
    "can_judge_strategy_alpha_60d",
    "can_judge_strategy_alpha_120d",
    "can_judge_strategy_alpha_252d",
    "conclusion_status",
    *[column for column in RISK_ADJUSTED_ALPHA_COLUMNS if column != "conclusion_status"],
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
    benchmark = benchmark_return_for_window(report_dir, selected_date, max(len(equity_frame) - 1, 0))
    readiness = strategy_readiness_snapshot(report_dir, selected_date, equity_frame=equity_frame)
    risk_alpha = risk_adjusted_alpha_snapshot(report_dir, selected_date, equity_frame=equity_frame, readiness=readiness)
    row = _performance_row(equity_frame, source, selected_date, benchmark, readiness, risk_alpha)
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
    readiness: dict[str, object],
    risk_alpha: dict[str, object],
) -> dict[str, object]:
    if equity_frame.empty:
        return _empty_row(
            selected_date,
            "missing",
            "DATA_INSUFFICIENT: no pnl_chart_data or paper_summary equity series",
            readiness,
            risk_alpha,
        )

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
    benchmark_window = str(benchmark.get("window", "") or "")
    benchmark_can_judge = bool(benchmark.get("can_judge_alpha", False))
    strategy_can_judge = strategy_can_judge_window(readiness, benchmark_window)
    can_judge_alpha = bool(benchmark_can_judge and strategy_can_judge)
    alpha = _sub_or_none(cumulative_return, benchmark_return) if can_judge_alpha else None

    warnings: list[str] = []
    hard_insufficient = False
    if len(returns) < 5:
        warnings.append("DATA_INSUFFICIENT: daily_return_count < 5")
        hard_insufficient = True
    if benchmark.get("warning"):
        warnings.append(str(benchmark["warning"]))
    if not benchmark_can_judge:
        warnings.append("NO_OFFICIAL_BENCHMARK: can_judge_alpha=false")
        hard_insufficient = True
    if benchmark_can_judge and not strategy_can_judge:
        warnings.append(f"NOT_ENOUGH_STRATEGY_HISTORY: {strategy_insufficient_reason(readiness, benchmark_window)}")
        hard_insufficient = True
    if benchmark_return is None:
        warnings.append("DATA_INSUFFICIENT: benchmark_return unavailable")
        hard_insufficient = True

    conclusion_status = str(risk_alpha.get("conclusion_status") or _conclusion_status(benchmark_can_judge, strategy_can_judge, benchmark_return))
    status = conclusion_status if hard_insufficient else "OK"
    if status == "OK" and warnings:
        status = "OK_WITH_WARNINGS"
    risk_reason = str(risk_alpha.get("conclusion_reason", "") or "").strip()
    if risk_reason and risk_reason not in warnings:
        warnings.append(risk_reason)
    if conclusion_status in {"DATA_INSUFFICIENT", "OBSERVATION_ONLY"}:
        hard_insufficient = conclusion_status == "DATA_INSUFFICIENT"
        status = conclusion_status if hard_insufficient else "OK_WITH_WARNINGS"
    elif conclusion_status in {"UNDERPERFORMING", "OUTPERFORMING_SHORT_TERM", "OUTPERFORMING_CONFIRMED"} and status == "OK":
        status = "OK_WITH_WARNINGS" if warnings else "OK"

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
        "benchmark_window": benchmark_window,
        "alpha": _round(alpha),
        "benchmark_source": benchmark.get("source", "benchmark 資料不足"),
        "benchmark_is_official": bool(benchmark.get("benchmark_is_official", False)),
        "fallback_reason": benchmark.get("fallback_reason", ""),
        "can_judge_alpha": can_judge_alpha,
        "can_judge_alpha_5d": bool(benchmark.get("can_judge_alpha_5d", False)),
        "can_judge_alpha_20d": bool(benchmark.get("can_judge_alpha_20d", False)),
        "can_judge_alpha_60d": bool(benchmark.get("can_judge_alpha_60d", False)),
        "can_judge_alpha_120d": bool(benchmark.get("can_judge_alpha_120d", False)),
        "can_judge_alpha_252d": bool(benchmark.get("can_judge_alpha_252d", False)),
        "benchmark_history_days": int(benchmark.get("benchmark_history_days", 0) or 0),
        **_readiness_columns(readiness, benchmark_window),
        **_risk_alpha_columns(risk_alpha),
        "conclusion_status": conclusion_status,
        "benchmark_warning": benchmark.get("warning", ""),
        "status": status,
        "data_quality_warning": "; ".join(dict.fromkeys(warnings)),
        "notes": "報表診斷用途；不修改交易策略、出場規則或訂單。",
    }


def _empty_row(
    selected_date: pd.Timestamp | None,
    source: str,
    warning: str,
    readiness: dict[str, object] | None = None,
    risk_alpha: dict[str, object] | None = None,
) -> dict[str, object]:
    row = {column: "" for column in PERFORMANCE_DIAGNOSTICS_COLUMNS}
    readiness = readiness or {}
    risk_alpha = risk_alpha or {}
    row.update(
        {
            "trade_date": _date_text(selected_date) if selected_date is not None else "",
            "source": source,
            "observation_count": 0,
            "daily_return_count": 0,
            **_readiness_columns(readiness, ""),
            **_risk_alpha_columns(risk_alpha),
            "conclusion_status": "DATA_INSUFFICIENT",
            "status": "DATA_INSUFFICIENT",
            "data_quality_warning": warning,
            "notes": "報表診斷用途；不修改交易策略、出場規則或訂單。",
        }
    )
    return row


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


def _sub_or_none(left: object, right: object) -> float | None:
    left_number = _num(left)
    right_number = _num(right)
    if left_number is None or right_number is None:
        return None
    return left_number - right_number


def _readiness_columns(readiness: dict[str, object], selected_window: str) -> dict[str, object]:
    output = {
        "strategy_history_days": int(readiness.get("strategy_history_days", 0) or 0),
        "valid_trade_count": int(readiness.get("valid_trade_count", 0) or 0),
        "holding_record_count": int(readiness.get("holding_record_count", 0) or 0),
        "can_judge_strategy_alpha": bool(strategy_can_judge_window(readiness, selected_window))
        if selected_window
        else bool(readiness.get("can_judge_strategy_alpha", False)),
    }
    for days in STRATEGY_ALPHA_WINDOWS:
        if days == 1:
            continue
        key = f"can_judge_strategy_alpha_{days}d"
        output[key] = bool(readiness.get(key, False))
    return output


def _risk_alpha_columns(risk_alpha: dict[str, object]) -> dict[str, object]:
    return {column: risk_alpha.get(column) for column in RISK_ADJUSTED_ALPHA_COLUMNS}


def _conclusion_status(benchmark_can_judge: bool, strategy_can_judge: bool, benchmark_return: object) -> str:
    if not benchmark_can_judge or _num(benchmark_return) is None:
        return "DATA_INSUFFICIENT"
    if not strategy_can_judge:
        return "NOT_ENOUGH_STRATEGY_HISTORY"
    return "OK"


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
