"""Risk-adjusted alpha diagnostics for paper trading reports.

This module only produces reporting metrics. It does not change the trading
strategy, exits, orders, broker integration, or paper-trading state.
"""

from __future__ import annotations

from math import sqrt
from pathlib import Path

import pandas as pd

from tw_quant.reporting.benchmark import select_benchmark_snapshot, select_official_benchmark_history
from tw_quant.reporting.strategy_readiness import strategy_can_judge_window, strategy_readiness_snapshot


RISK_ALPHA_WINDOWS = [5, 20, 60, 120, 252]
RISK_ADJUSTED_ALPHA_COLUMNS = [
    "primary_alpha_window",
    "strategy_return_5d",
    "benchmark_return_5d",
    "excess_return_5d",
    "strategy_return_20d",
    "benchmark_return_20d",
    "excess_return_20d",
    "strategy_return_60d",
    "benchmark_return_60d",
    "excess_return_60d",
    "strategy_return_120d",
    "benchmark_return_120d",
    "excess_return_120d",
    "strategy_return_252d",
    "benchmark_return_252d",
    "excess_return_252d",
    "excess_return",
    "strategy_max_drawdown",
    "benchmark_max_drawdown",
    "drawdown_ratio",
    "strategy_volatility",
    "benchmark_volatility",
    "volatility_ratio",
    "risk_adjusted_alpha_status",
    "conclusion_status",
    "conclusion_reason",
]


def risk_adjusted_alpha_snapshot(
    reports_dir: str | Path,
    selected_date: str | pd.Timestamp | None = None,
    *,
    equity_frame: pd.DataFrame | None = None,
    readiness: dict[str, object] | None = None,
    benchmark_snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    """Calculate risk-adjusted alpha and conclusion fields."""

    report_dir = Path(reports_dir)
    target = _parse_date(selected_date)
    strategy = _normalize_strategy_equity(
        equity_frame if equity_frame is not None else _read_strategy_equity(report_dir),
        target,
    )
    readiness = readiness or strategy_readiness_snapshot(report_dir, target, equity_frame=strategy)
    benchmark_snapshot = benchmark_snapshot or select_benchmark_snapshot(report_dir, target)
    benchmark_history = select_official_benchmark_history(report_dir, target)
    benchmark = _normalize_benchmark_frame(benchmark_history.get("frame", pd.DataFrame()), target)

    metrics: dict[str, object] = {column: None for column in RISK_ADJUSTED_ALPHA_COLUMNS}
    metrics.update(
        {
            "primary_alpha_window": "",
            "risk_adjusted_alpha_status": "DATA_INSUFFICIENT",
            "conclusion_status": "DATA_INSUFFICIENT",
            "conclusion_reason": "",
        }
    )

    benchmark_returns = (
        benchmark_snapshot.get("returns", {}) if isinstance(benchmark_snapshot.get("returns"), dict) else {}
    )
    for days in RISK_ALPHA_WINDOWS:
        window = f"{days}d"
        strategy_return = _period_return(strategy["equity"] if not strategy.empty else pd.Series(dtype=float), days)
        benchmark_return = (
            _num(benchmark_returns.get(window)) if _benchmark_can_judge(benchmark_snapshot, window) else None
        )
        can_judge = bool(
            strategy_can_judge_window(readiness, window) and _benchmark_can_judge(benchmark_snapshot, window)
        )
        excess_return = _sub_or_none(strategy_return, benchmark_return) if can_judge else None
        metrics[f"strategy_return_{window}"] = _round(strategy_return)
        metrics[f"benchmark_return_{window}"] = _round(benchmark_return)
        metrics[f"excess_return_{window}"] = _round(excess_return)

    primary_days = _primary_window(metrics, readiness, benchmark_snapshot)
    if primary_days is None:
        reason = _insufficient_reason(strategy, benchmark, readiness, benchmark_snapshot)
        metrics["conclusion_reason"] = reason
        return metrics

    primary_window = f"{primary_days}d"
    metrics["primary_alpha_window"] = primary_window
    metrics["excess_return"] = metrics.get(f"excess_return_{primary_window}")
    strategy_tail = _tail_window(strategy, primary_days)
    benchmark_tail = _tail_window(benchmark, primary_days)
    strategy_dd = _max_drawdown(strategy_tail["equity"] if not strategy_tail.empty else pd.Series(dtype=float))
    benchmark_dd = _max_drawdown(benchmark_tail["close"] if not benchmark_tail.empty else pd.Series(dtype=float))
    strategy_vol = _volatility(strategy_tail["equity"] if not strategy_tail.empty else pd.Series(dtype=float))
    benchmark_vol = _volatility(benchmark_tail["close"] if not benchmark_tail.empty else pd.Series(dtype=float))
    drawdown_ratio = _risk_ratio(strategy_dd, benchmark_dd, risk_type="drawdown")
    volatility_ratio = _risk_ratio(strategy_vol, benchmark_vol, risk_type="volatility")
    metrics.update(
        {
            "strategy_max_drawdown": _round(strategy_dd),
            "benchmark_max_drawdown": _round(benchmark_dd),
            "drawdown_ratio": _round(drawdown_ratio),
            "strategy_volatility": _round(strategy_vol),
            "benchmark_volatility": _round(benchmark_vol),
            "volatility_ratio": _round(volatility_ratio),
        }
    )
    risk_status, conclusion_status, reason = _status_from_metrics(
        primary_days=primary_days,
        excess_return=_num(metrics.get("excess_return")),
        strategy_max_drawdown=strategy_dd,
        drawdown_ratio=drawdown_ratio,
        strategy_volatility=strategy_vol,
        volatility_ratio=volatility_ratio,
        readiness=readiness,
    )
    metrics["risk_adjusted_alpha_status"] = risk_status
    metrics["conclusion_status"] = conclusion_status
    metrics["conclusion_reason"] = reason
    return metrics


def _status_from_metrics(
    *,
    primary_days: int,
    excess_return: float | None,
    strategy_max_drawdown: float | None,
    drawdown_ratio: float | None,
    strategy_volatility: float | None,
    volatility_ratio: float | None,
    readiness: dict[str, object],
) -> tuple[str, str, str]:
    if excess_return is None:
        return "DATA_INSUFFICIENT", "DATA_INSUFFICIENT", "benchmark 或策略報酬資料不足，無法計算 excess_return。"
    if not strategy_can_judge_window(readiness, f"{primary_days}d"):
        return "DATA_INSUFFICIENT", "DATA_INSUFFICIENT", "策略樣本不足，不能判斷 alpha。"
    if excess_return <= 0:
        return "UNDERPERFORMING", "UNDERPERFORMING", "excess_return 未高於 0，不能宣稱打敗大盤。"

    risk_issues = []
    if drawdown_ratio is None:
        if strategy_max_drawdown is not None and abs(strategy_max_drawdown) > 0.12:
            risk_issues.append(f"策略最大回撤 {strategy_max_drawdown:.2%} 高於 12% 絕對風險門檻")
    elif drawdown_ratio > 1.2:
        risk_issues.append(f"策略回撤為 benchmark 的 {drawdown_ratio:.2f} 倍，高於 1.20 門檻")
    if volatility_ratio is None:
        if strategy_volatility is not None and strategy_volatility > 0.35:
            risk_issues.append(f"策略年化波動 {strategy_volatility:.2%} 高於 35% 絕對風險門檻")
    elif volatility_ratio > 1.2:
        risk_issues.append(f"策略波動為 benchmark 的 {volatility_ratio:.2f} 倍，高於 1.20 門檻")
    if risk_issues:
        return "RISK_TOO_HIGH", "OBSERVATION_ONLY", "；".join(risk_issues)

    valid_trade_count = int(readiness.get("valid_trade_count", 0) or 0)
    if primary_days >= 60 and valid_trade_count >= 50:
        return (
            "OUTPERFORMING_CONFIRMED",
            "OUTPERFORMING_CONFIRMED",
            (f"{primary_days} 日樣本足夠，excess_return 為正，且回撤 / 波動未明顯劣於 benchmark。"),
        )
    return (
        "OUTPERFORMING_SHORT_TERM",
        "OUTPERFORMING_SHORT_TERM",
        (f"excess_return 為正，但目前僅有 {primary_days} 日或交易筆數偏短，仍屬短期觀察。"),
    )


def _primary_window(
    metrics: dict[str, object],
    readiness: dict[str, object],
    benchmark_snapshot: dict[str, object],
) -> int | None:
    for days in [252, 120, 60, 20, 5]:
        window = f"{days}d"
        if not strategy_can_judge_window(readiness, window):
            continue
        if not _benchmark_can_judge(benchmark_snapshot, window):
            continue
        if _num(metrics.get(f"strategy_return_{window}")) is None:
            continue
        if _num(metrics.get(f"benchmark_return_{window}")) is None:
            continue
        return days
    return None


def _insufficient_reason(
    strategy: pd.DataFrame,
    benchmark: pd.DataFrame,
    readiness: dict[str, object],
    benchmark_snapshot: dict[str, object],
) -> str:
    parts = []
    if strategy.empty:
        parts.append("缺少策略投組績效序列")
    if benchmark.empty or not bool(benchmark_snapshot.get("benchmark_is_official", False)):
        parts.append("缺少正式 benchmark 歷史序列")
    if int(readiness.get("valid_trade_count", 0) or 0) < 20:
        parts.append(f"valid_trade_count={int(readiness.get('valid_trade_count', 0) or 0)} 低於成熟度門檻 20")
    if int(readiness.get("strategy_history_days", 0) or 0) <= 5:
        parts.append(f"strategy_history_days={int(readiness.get('strategy_history_days', 0) or 0)} 不足")
    return "；".join(parts) or "策略或 benchmark 樣本不足。"


def _read_strategy_equity(report_dir: Path) -> pd.DataFrame:
    pnl = _read_all_reports(report_dir, "pnl_chart_data_*.csv")
    if not pnl.empty:
        return pnl
    return _read_all_reports(report_dir, "paper_summary_*.csv")


def _normalize_strategy_equity(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
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
    output = output.dropna(subset=["equity"]).sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    return output[["trade_date", "equity"]].reset_index(drop=True)


def _normalize_benchmark_frame(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "trade_date" not in frame.columns or "close" not in frame.columns:
        return pd.DataFrame(columns=["trade_date", "close"])
    output = frame.copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"], errors="coerce")
    output["close"] = pd.to_numeric(output["close"], errors="coerce")
    output = output.dropna(subset=["trade_date", "close"])
    if selected_date is not None:
        output = output[output["trade_date"] <= selected_date]
    output = output.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    return output[["trade_date", "close"]].reset_index(drop=True)


def _tail_window(frame: pd.DataFrame, days: int) -> pd.DataFrame:
    if frame.empty or len(frame) <= days:
        return pd.DataFrame(columns=frame.columns)
    return frame.tail(days + 1).reset_index(drop=True)


def _period_return(values: pd.Series, days: int) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna().reset_index(drop=True)
    if len(clean) <= days:
        return None
    base = float(clean.iloc[-days - 1])
    if abs(base) < 0.000001:
        return None
    return float(clean.iloc[-1] / base - 1.0)


def _max_drawdown(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    drawdown = clean / clean.cummax() - 1.0
    return float(drawdown.min())


def _volatility(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    returns = clean.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
    if returns.empty:
        return None
    return float(returns.std(ddof=0) * sqrt(252))


def _risk_ratio(strategy_value: float | None, benchmark_value: float | None, *, risk_type: str) -> float | None:
    strategy_number = _num(strategy_value)
    benchmark_number = _num(benchmark_value)
    if strategy_number is None or benchmark_number is None:
        return None
    if risk_type == "drawdown":
        strategy_risk = abs(strategy_number)
        benchmark_risk = abs(benchmark_number)
    else:
        strategy_risk = strategy_number
        benchmark_risk = benchmark_number
    if benchmark_risk < 0.000001:
        return None
    return strategy_risk / benchmark_risk


def _benchmark_can_judge(benchmark: dict[str, object], window: str) -> bool:
    return bool(benchmark.get(f"can_judge_alpha_{window}", False))


def _read_all_reports(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = []
    for path in sorted(report_dir.glob(pattern)):
        frame = _read_csv(path)
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _sub_or_none(left: object, right: object) -> float | None:
    left_number = _num(left)
    right_number = _num(right)
    if left_number is None or right_number is None:
        return None
    return left_number - right_number


def _round(value: object) -> float | None:
    number = _num(value)
    if number is None:
        return None
    return round(number, 6)


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


def _parse_date(value: object) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _first_existing_column(frame: pd.DataFrame, columns: list[str]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None
