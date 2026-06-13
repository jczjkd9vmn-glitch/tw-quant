"""Strategy validation reports for candidate scoring variants.

This module compares advisory model filters against existing candidate and
paper-trade report data. It does not mutate paper trades or create orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


VALIDATION_COLUMNS = [
    "validation_date",
    "trade_date",
    "model_name",
    "description",
    "candidate_count",
    "selected_count",
    "simulated_trades",
    "win_rate",
    "avg_return_pct",
    "median_return_pct",
    "total_return_pct",
    "max_drawdown_pct",
    "avg_holding_days",
    "stop_loss_count",
    "take_profit_count",
    "trailing_stop_count",
    "ma_exit_count",
    "max_holding_exit_count",
    "profit_factor",
    "expectancy",
    "consecutive_loss_count",
    "notes",
]


@dataclass(frozen=True)
class StrategyValidationResult:
    trade_date: pd.Timestamp | None
    validation: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_strategy_validation(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
    min_trades_required: int = 10,
) -> StrategyValidationResult:
    report_dir = Path(reports_dir)
    candidates_path = _latest_file(report_dir, "candidates_*.csv", trade_date)
    if candidates_path is None:
        return StrategyValidationResult(None, pd.DataFrame(columns=VALIDATION_COLUMNS), None, "no candidates report found")
    candidates = _read_csv(candidates_path)
    paper_trades = _read_csv(report_dir / "paper_trades.csv")
    selected_date = _resolve_trade_date(candidates, candidates_path)
    if candidates.empty:
        validation = _empty_validation(selected_date, "candidate report is empty")
        output_path = _write_validation(report_dir, selected_date, validation)
        return StrategyValidationResult(selected_date, validation, output_path, "candidate report is empty")

    model_specs = [
        ("baseline_total_score", "原始 total_score / risk_pass baseline", _baseline_total_score),
        ("multi_factor_rank", "以 multi_factor_score 作為排序參考", _multi_factor_rank),
        ("confidence_filter", "排除 confidence_score < 60", _confidence_filter),
        ("liquidity_filter", "排除 liquidity_score < 50 或 slippage_risk_score < 50", _liquidity_filter),
        ("sector_strength_filter", "排除 sector_strength_score < 45", _sector_strength_filter),
        ("event_risk_filter", "排除 HIGH event、處置股或重大負面事件", _event_risk_filter),
        ("combined_model", "綜合多因子、市場情報、資料可信度與風險旗標", _combined_model),
    ]
    rows = []
    for model_name, description, selector in model_specs:
        selected, notes = selector(candidates)
        rows.append(
            _validation_row(
                selected_date,
                model_name,
                description,
                candidates,
                selected,
                paper_trades,
                min_trades_required,
                notes,
            )
        )
    validation = pd.DataFrame(rows, columns=VALIDATION_COLUMNS)
    output_path = _write_validation(report_dir, selected_date, validation)
    return StrategyValidationResult(selected_date, validation, output_path)


def _baseline_total_score(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    sort_cols = [column for column in ["total_score", "risk_score"] if column in frame.columns]
    return _sort_desc(frame, sort_cols), ""


def _multi_factor_rank(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    if "multi_factor_score" not in frame.columns:
        return _baseline_total_score(candidates)[0], "missing multi_factor_score; used baseline"
    return _sort_desc(frame, ["multi_factor_score", "total_score"]), ""


def _confidence_filter(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    if "confidence_score" not in frame.columns:
        return frame, "missing confidence_score; skipped filter"
    return frame[pd.to_numeric(frame["confidence_score"], errors="coerce").fillna(50) >= 60].copy(), ""


def _liquidity_filter(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    notes = []
    if "liquidity_score" in frame.columns:
        frame = frame[pd.to_numeric(frame["liquidity_score"], errors="coerce").fillna(50) >= 50].copy()
    else:
        notes.append("missing liquidity_score")
    if "slippage_risk_score" in frame.columns:
        frame = frame[pd.to_numeric(frame["slippage_risk_score"], errors="coerce").fillna(50) >= 50].copy()
    else:
        notes.append("missing slippage_risk_score")
    return frame, "; ".join(notes)


def _sector_strength_filter(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    if "sector_strength_score" not in frame.columns:
        return frame, "missing sector_strength_score; skipped filter"
    return frame[pd.to_numeric(frame["sector_strength_score"], errors="coerce").fillna(50) >= 45].copy(), ""


def _event_risk_filter(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _risk_pass(candidates)
    if "event_risk_level" in frame.columns:
        frame = frame[frame["event_risk_level"].fillna("").astype(str).str.upper() != "HIGH"].copy()
    if "is_disposition_stock" in frame.columns:
        frame = frame[~frame["is_disposition_stock"].apply(_to_bool)].copy()
    if "event_blocked" in frame.columns:
        frame = frame[~frame["event_blocked"].apply(_to_bool)].copy()
    return frame, ""


def _combined_model(candidates: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = _event_risk_filter(candidates)[0]
    for column, threshold in [
        ("confidence_score", 60),
        ("liquidity_score", 50),
        ("sector_strength_score", 45),
    ]:
        if column in frame.columns:
            frame = frame[pd.to_numeric(frame[column], errors="coerce").fillna(50) >= threshold].copy()
    score_columns = [column for column in ["multi_factor_score", "final_market_score", "total_score"] if column in frame.columns]
    return _sort_desc(frame, score_columns), ""


def _validation_row(
    trade_date: pd.Timestamp,
    model_name: str,
    description: str,
    candidates: pd.DataFrame,
    selected: pd.DataFrame,
    paper_trades: pd.DataFrame,
    min_trades_required: int,
    notes: str,
) -> dict:
    returns = _matched_returns(selected, paper_trades)
    simulated = len(returns)
    note_parts = [notes] if notes else []
    if simulated < min_trades_required:
        note_parts.append(f"歷史交易樣本不足：{simulated}/{min_trades_required}")
    metrics = _return_metrics(returns)
    exits = _exit_counts(selected, paper_trades)
    return {
        "validation_date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "model_name": model_name,
        "description": description,
        "candidate_count": int(len(candidates)),
        "selected_count": int(len(selected)),
        "simulated_trades": int(simulated),
        **metrics,
        **exits,
        "notes": "；".join(part for part in note_parts if part),
    }


def _matched_returns(selected: pd.DataFrame, paper_trades: pd.DataFrame) -> pd.DataFrame:
    if selected.empty or paper_trades.empty or "stock_id" not in paper_trades.columns:
        return pd.DataFrame(columns=["stock_id", "return_pct", "holding_days", "exit_reason"])
    symbols = set(selected["stock_id"].astype(str).str.strip())
    frame = paper_trades[paper_trades["stock_id"].astype(str).str.strip().isin(symbols)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["stock_id", "return_pct", "holding_days", "exit_reason"])
    realized = pd.to_numeric(_series(frame, "realized_pnl_pct_after_cost"), errors="coerce")
    unrealized = pd.to_numeric(_series(frame, "unrealized_pnl_pct"), errors="coerce")
    frame["return_pct"] = realized.where(realized.notna(), unrealized).fillna(0.0)
    frame["holding_days"] = pd.to_numeric(_series(frame, "holding_days"), errors="coerce").fillna(0)
    frame["exit_reason"] = _series(frame, "exit_reason", "").fillna("").astype(str)
    return frame[["stock_id", "return_pct", "holding_days", "exit_reason"]]


def _return_metrics(returns: pd.DataFrame) -> dict:
    if returns.empty:
        return {
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_holding_days": 0.0,
            "profit_factor": 0.0,
            "expectancy": 0.0,
            "consecutive_loss_count": 0,
        }
    values = pd.to_numeric(returns["return_pct"], errors="coerce").fillna(0.0)
    wins = values[values > 0]
    losses = values[values < 0]
    cumulative = values.cumsum()
    running_peak = cumulative.cummax()
    drawdown = cumulative - running_peak
    gross_profit = float(wins.sum())
    gross_loss = abs(float(losses.sum()))
    return {
        "win_rate": round(float((values > 0).mean()), 4),
        "avg_return_pct": round(float(values.mean()), 6),
        "median_return_pct": round(float(values.median()), 6),
        "total_return_pct": round(float(values.sum()), 6),
        "max_drawdown_pct": round(float(drawdown.min()), 6),
        "avg_holding_days": round(float(pd.to_numeric(returns["holding_days"], errors="coerce").fillna(0).mean()), 2),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else round(gross_profit, 4),
        "expectancy": round(float(values.mean()), 6),
        "consecutive_loss_count": _max_consecutive_losses(values),
    }


def _exit_counts(selected: pd.DataFrame, paper_trades: pd.DataFrame) -> dict:
    returns = _matched_returns(selected, paper_trades)
    reasons = returns["exit_reason"].fillna("").astype(str).str.lower() if not returns.empty else pd.Series(dtype=str)
    return {
        "stop_loss_count": int(reasons.isin(["stop_loss", "stop_loss_hit"]).sum()),
        "take_profit_count": int(reasons.isin(["take_profit_1", "take_profit_2"]).sum()),
        "trailing_stop_count": int(reasons.isin(["trailing_stop"]).sum()),
        "ma_exit_count": int(reasons.isin(["ma20_break", "ma_exit"]).sum()),
        "max_holding_exit_count": int(reasons.isin(["max_holding_days", "time_exit"]).sum()),
    }


def _empty_validation(trade_date: pd.Timestamp, note: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "validation_date": pd.Timestamp.today().strftime("%Y-%m-%d"),
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "model_name": "baseline_total_score",
                "description": "原始 total_score / risk_pass baseline",
                "candidate_count": 0,
                "selected_count": 0,
                "simulated_trades": 0,
                "win_rate": 0.0,
                "avg_return_pct": 0.0,
                "median_return_pct": 0.0,
                "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "avg_holding_days": 0.0,
                "stop_loss_count": 0,
                "take_profit_count": 0,
                "trailing_stop_count": 0,
                "ma_exit_count": 0,
                "max_holding_exit_count": 0,
                "profit_factor": 0.0,
                "expectancy": 0.0,
                "consecutive_loss_count": 0,
                "notes": note,
            }
        ],
        columns=VALIDATION_COLUMNS,
    )


def _write_validation(report_dir: Path, trade_date: pd.Timestamp, validation: pd.DataFrame) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"strategy_validation_{trade_date.strftime('%Y%m%d')}.csv"
    validation.to_csv(path, index=False, encoding="utf-8")
    return path


def _risk_pass(candidates: pd.DataFrame) -> pd.DataFrame:
    if "risk_pass" not in candidates.columns:
        return candidates.copy()
    return candidates[candidates["risk_pass"].apply(_to_bool)].copy()


def _sort_desc(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    valid = [column for column in columns if column in frame.columns]
    if not valid:
        return frame.copy()
    output = frame.copy()
    for column in valid:
        output[column] = pd.to_numeric(output[column], errors="coerce").fillna(0)
    return output.sort_values(valid, ascending=[False] * len(valid)).copy()


def _max_consecutive_losses(values: pd.Series) -> int:
    current = longest = 0
    for value in values:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return int(longest)


def _latest_file(report_dir: Path, pattern: str, trade_date: str | None = None) -> Path | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return target
    files = sorted(report_dir.glob(pattern), key=lambda path: (_date_from_path(path) or pd.Timestamp.min))
    return files[-1] if files else None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _resolve_trade_date(candidates: pd.DataFrame, path: Path) -> pd.Timestamp:
    if not candidates.empty and "trade_date" in candidates.columns:
        parsed = pd.to_datetime(candidates["trade_date"].iloc[0], errors="coerce")
        if not pd.isna(parsed):
            return parsed
    return _date_from_path(path) or pd.Timestamp.today()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _series(frame: pd.DataFrame, column: str, default: object = None) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}
