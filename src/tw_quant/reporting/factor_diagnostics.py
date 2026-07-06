"""Strategy factor diagnostics and benchmark attribution reports.

The diagnostics in this module are report-only. They read existing candidate,
decision, market, and paper-trade CSV outputs, then write attribution CSVs
without mutating trading strategy, orders, or reference data.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

import pandas as pd

from tw_quant.reporting.benchmark import select_benchmark_snapshot
from tw_quant.reporting.risk_adjusted_alpha import RISK_ADJUSTED_ALPHA_COLUMNS, risk_adjusted_alpha_snapshot
from tw_quant.reporting.strategy_readiness import (
    STRATEGY_ALPHA_WINDOWS,
    strategy_can_judge_window,
    strategy_insufficient_reason,
    strategy_readiness_snapshot,
)


FACTOR_ATTRIBUTION_COLUMNS = [
    "factor_name",
    "bucket",
    "stock_count",
    "trade_count",
    "avg_forward_return_1d",
    "avg_forward_return_5d",
    "avg_forward_return_20d",
    "win_rate_1d",
    "win_rate_5d",
    "win_rate_20d",
    "avg_return_pct",
    "median_return_pct",
    "total_realized_pnl_after_cost",
    "avg_realized_pnl_after_cost",
    "max_loss_pct",
    "max_gain_pct",
    "benchmark_return_1d",
    "benchmark_return_5d",
    "benchmark_return_20d",
    "alpha_1d",
    "alpha_5d",
    "alpha_20d",
    "conclusion",
    "data_quality_warning",
    "notes",
]

FACTOR_ATTRIBUTION_SUMMARY_COLUMNS = [
    "report_date",
    "factor_name",
    "best_bucket",
    "best_alpha_20d",
    "best_avg_return_pct",
    "worst_bucket",
    "worst_alpha_20d",
    "worst_avg_return_pct",
    "total_stock_count",
    "total_trade_count",
    "sample_status",
    "conclusion",
    "data_quality_warning",
    "notes",
]

BENCHMARK_DIAGNOSTICS_COLUMNS = [
    "trade_date",
    "system_cumulative_return",
    "benchmark_cumulative_return",
    "alpha",
    "benchmark_return_1d",
    "benchmark_return_5d",
    "benchmark_return_20d",
    "benchmark_return_60d",
    "benchmark_return_120d",
    "benchmark_return_252d",
    "alpha_1d",
    "alpha_5d",
    "alpha_20d",
    "alpha_60d",
    "alpha_120d",
    "alpha_252d",
    "win_rate_vs_benchmark",
    "max_drawdown",
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
    *[
        column
        for column in RISK_ADJUSTED_ALPHA_COLUMNS
        if column
        not in {
            "conclusion_status",
            "benchmark_return_5d",
            "benchmark_return_20d",
            "benchmark_return_60d",
            "benchmark_return_120d",
            "benchmark_return_252d",
        }
    ],
    "benchmark_warning",
    "data_quality_warning",
    "notes",
]

GUARDRAIL_IMPACT_COLUMNS = [
    "rejected_reason",
    "rejected_count",
    "avg_forward_return_1d",
    "avg_forward_return_5d",
    "avg_forward_return_20d",
    "missed_winner_count",
    "avoided_loser_count",
    "estimated_alpha_impact",
    "notes",
]

NUMERIC_FACTORS = [
    "total_score",
    "multi_factor_score",
    "final_market_score",
    "confidence_score",
    "liquidity_score",
    "sector_strength_score",
    "fundamental_score",
    "valuation_score",
    "financial_score",
    "institutional_score",
    "event_risk_score",
    "market_regime_score",
    "holding_days",
]

CATEGORICAL_FACTORS = [
    "candidate_grade",
    "risk_pass",
    "decision_action",
    "event_risk_level",
    "是否注意股 / 處置股",
    "entry_price_source",
]

FORWARD_RETURN_COLUMNS = {
    "1d": "forward_return_1d",
    "5d": "forward_return_5d",
    "20d": "forward_return_20d",
}


@dataclass(frozen=True)
class FactorDiagnosticsResult:
    trade_date: pd.Timestamp | None
    factor_attribution: pd.DataFrame
    factor_summary: pd.DataFrame
    benchmark_diagnostics: pd.DataFrame
    guardrail_impact: pd.DataFrame
    output_paths: dict[str, Path]
    warning: str = ""


def generate_factor_diagnostics(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
) -> FactorDiagnosticsResult:
    """Generate report-only diagnostics for factor attribution and alpha."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    date_label = (selected_date or pd.Timestamp.today()).strftime("%Y%m%d")

    candidates = _read_latest(report_dir, "candidates_*.csv", trade_date)
    risk_pass = _read_latest(report_dir, "risk_pass_candidates_*.csv", trade_date)
    decisions = _read_latest(report_dir, "trading_decisions_*.csv", trade_date)
    market_intel = _read_latest(report_dir, "market_intel_*.csv", trade_date)
    market_regime = _read_latest(report_dir, "market_regime_*.csv", trade_date)
    paper_summary = _read_latest(report_dir, "paper_summary_*.csv", trade_date)
    recent_summaries = _read_recent_summaries(report_dir)
    trades = _read_csv(report_dir / "paper_trades.csv")
    portfolio = _read_latest(report_dir, "paper_portfolio_*.csv", trade_date)
    rejected = _read_latest(report_dir, "rejected_paper_orders_*.csv", trade_date)

    analysis_frame = _build_analysis_frame(candidates, risk_pass, decisions, market_intel, trades, market_regime)
    trade_returns = _trade_returns(trades)
    benchmark = select_benchmark_snapshot(report_dir, selected_date)
    readiness = strategy_readiness_snapshot(
        report_dir,
        selected_date,
        trades_frame=trades,
        portfolio_frame=portfolio,
    )
    risk_alpha = risk_adjusted_alpha_snapshot(
        report_dir,
        selected_date,
        readiness=readiness,
        benchmark_snapshot=benchmark,
    )

    warnings: list[str] = []
    if trades.empty:
        warnings.append("no paper_trades.csv data; diagnostics are DATA_INSUFFICIENT")
    if analysis_frame.empty:
        warnings.append("no candidate or decision rows available; factor attribution is DATA_INSUFFICIENT")

    factor_attribution = _factor_attribution(analysis_frame, trade_returns, benchmark)
    factor_summary = _factor_summary(factor_attribution, selected_date)
    benchmark_diagnostics = _benchmark_diagnostics(
        selected_date,
        paper_summary,
        recent_summaries,
        benchmark,
        readiness,
        risk_alpha,
    )
    guardrail_impact = _guardrail_impact(rejected, benchmark)

    output_paths = {
        "factor_attribution": report_dir / f"factor_attribution_{date_label}.csv",
        "factor_summary": report_dir / f"factor_attribution_summary_{date_label}.csv",
        "benchmark_diagnostics": report_dir / f"benchmark_diagnostics_{date_label}.csv",
        "guardrail_impact": report_dir / f"guardrail_impact_{date_label}.csv",
    }
    factor_attribution.to_csv(output_paths["factor_attribution"], index=False, encoding="utf-8")
    factor_summary.to_csv(output_paths["factor_summary"], index=False, encoding="utf-8")
    benchmark_diagnostics.to_csv(output_paths["benchmark_diagnostics"], index=False, encoding="utf-8")
    guardrail_impact.to_csv(output_paths["guardrail_impact"], index=False, encoding="utf-8")

    return FactorDiagnosticsResult(
        trade_date=selected_date,
        factor_attribution=factor_attribution,
        factor_summary=factor_summary,
        benchmark_diagnostics=benchmark_diagnostics,
        guardrail_impact=guardrail_impact,
        output_paths=output_paths,
        warning="; ".join(warnings),
    )


def _build_analysis_frame(
    candidates: pd.DataFrame,
    risk_pass: pd.DataFrame,
    decisions: pd.DataFrame,
    market_intel: pd.DataFrame,
    trades: pd.DataFrame,
    market_regime: pd.DataFrame,
) -> pd.DataFrame:
    frames = [candidates, risk_pass, decisions, market_intel, trades]
    context: dict[str, dict[str, object]] = {}
    for frame in frames:
        if frame.empty or "stock_id" not in frame.columns:
            continue
        normalized = frame.copy()
        normalized["stock_id"] = normalized["stock_id"].astype(str).str.strip()
        for _, row in normalized.iterrows():
            stock_id = _text(row.get("stock_id"))
            if not stock_id:
                continue
            values = row.to_dict()
            if frame is risk_pass:
                values["risk_pass"] = True
            if frame is decisions:
                values["decision_action"] = _first_text(values.get("action"), values.get("decision"))
            context.setdefault(stock_id, {}).update(values)

    rows = list(context.values())
    if not rows:
        return pd.DataFrame()
    output = pd.DataFrame(rows)
    if "stock_id" in output.columns:
        output["stock_id"] = output["stock_id"].astype(str).str.strip()
    if "market_regime_score" not in output.columns:
        score = _first_number_from_frame(market_regime, "market_regime_score")
        output["market_regime_score"] = score
    if "decision_action" not in output.columns:
        output["decision_action"] = output.get("decision", "")
    output["是否注意股 / 處置股"] = output.apply(_attention_disposition_bucket, axis=1)
    return output.drop_duplicates("stock_id", keep="last") if "stock_id" in output.columns else output


def _factor_attribution(
    analysis_frame: pd.DataFrame,
    trade_returns: pd.DataFrame,
    benchmark: dict[str, object],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    factors: list[tuple[str, str, str]] = [(name, name, "numeric") for name in NUMERIC_FACTORS]
    factors.extend((name, name, "categorical") for name in CATEGORICAL_FACTORS)
    for factor_name, column, factor_type in factors:
        if analysis_frame.empty or column not in analysis_frame.columns:
            rows.append(_missing_factor_row(factor_name, benchmark, "missing factor column"))
            continue
        frame = analysis_frame.copy()
        frame["_bucket"] = _bucket_factor(frame[column], factor_type)
        for bucket, bucket_frame in frame.groupby("_bucket", dropna=False):
            stock_ids = _stock_id_set(bucket_frame)
            bucket_trades = _trades_for_stock_ids(trade_returns, stock_ids)
            rows.append(
                _factor_row(
                    factor_name=factor_name,
                    bucket=_text(bucket) or "MISSING",
                    stock_count=int(len(bucket_frame)),
                    trade_frame=bucket_trades,
                    benchmark=benchmark,
                    notes="",
                )
            )
    return pd.DataFrame(rows, columns=FACTOR_ATTRIBUTION_COLUMNS)


def _factor_row(
    *,
    factor_name: str,
    bucket: str,
    stock_count: int,
    trade_frame: pd.DataFrame,
    benchmark: dict[str, object],
    notes: str,
) -> dict[str, object]:
    trade_count = int(len(trade_frame))
    sample_status, conclusion = _sample_status(trade_count)
    warning = "DATA_INSUFFICIENT" if trade_count < 20 else ""
    if trade_count < 20:
        notes = _join_notes(notes, "trade_count < 20，不可作為正式調參依據")
    elif trade_count < 50:
        notes = _join_notes(notes, "trade_count < 50，僅供觀察")

    return_values = pd.to_numeric(_series(trade_frame, "return_pct"), errors="coerce").dropna()
    pnl_values = pd.to_numeric(_series(trade_frame, "realized_pnl_after_cost"), errors="coerce").dropna()
    benchmark_returns = benchmark.get("returns", {}) if isinstance(benchmark.get("returns"), dict) else {}
    row = {
        "factor_name": factor_name,
        "bucket": bucket,
        "stock_count": stock_count,
        "trade_count": trade_count,
        "avg_forward_return_1d": _avg_forward(trade_frame, "1d"),
        "avg_forward_return_5d": _avg_forward(trade_frame, "5d"),
        "avg_forward_return_20d": _avg_forward(trade_frame, "20d"),
        "win_rate_1d": _win_rate(trade_frame, "1d"),
        "win_rate_5d": _win_rate(trade_frame, "5d"),
        "win_rate_20d": _win_rate(trade_frame, "20d"),
        "avg_return_pct": _mean_or_none(return_values),
        "median_return_pct": _median_or_none(return_values),
        "total_realized_pnl_after_cost": _sum_or_none(pnl_values),
        "avg_realized_pnl_after_cost": _mean_or_none(pnl_values),
        "max_loss_pct": _min_or_none(return_values),
        "max_gain_pct": _max_or_none(return_values),
        "benchmark_return_1d": benchmark_returns.get("1d"),
        "benchmark_return_5d": benchmark_returns.get("5d"),
        "benchmark_return_20d": benchmark_returns.get("20d"),
        "conclusion": conclusion,
        "data_quality_warning": warning,
        "notes": _join_notes(notes, sample_status),
    }
    row["alpha_1d"] = _sub_or_none(row["avg_forward_return_1d"], row["benchmark_return_1d"])
    row["alpha_5d"] = _sub_or_none(row["avg_forward_return_5d"], row["benchmark_return_5d"])
    row["alpha_20d"] = _sub_or_none(row["avg_forward_return_20d"], row["benchmark_return_20d"])
    return row


def _missing_factor_row(factor_name: str, benchmark: dict[str, object], notes: str) -> dict[str, object]:
    row = _factor_row(
        factor_name=factor_name,
        bucket="MISSING",
        stock_count=0,
        trade_frame=pd.DataFrame(),
        benchmark=benchmark,
        notes=notes,
    )
    row["data_quality_warning"] = "DATA_INSUFFICIENT"
    row["conclusion"] = "DATA_INSUFFICIENT"
    return row


def _factor_summary(attribution: pd.DataFrame, trade_date: pd.Timestamp | None) -> pd.DataFrame:
    if attribution.empty:
        return pd.DataFrame(columns=FACTOR_ATTRIBUTION_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    report_date = (trade_date or pd.Timestamp.today()).strftime("%Y-%m-%d")
    for factor_name, frame in attribution.groupby("factor_name", dropna=False):
        sortable = frame.copy()
        sortable["_metric"] = pd.to_numeric(sortable.get("alpha_20d"), errors="coerce")
        if sortable["_metric"].dropna().empty:
            sortable["_metric"] = pd.to_numeric(sortable.get("avg_return_pct"), errors="coerce")
        non_missing = sortable.dropna(subset=["_metric"])
        if non_missing.empty:
            best = worst = sortable.iloc[0]
        else:
            best = non_missing.sort_values("_metric", ascending=False).iloc[0]
            worst = non_missing.sort_values("_metric", ascending=True).iloc[0]
        total_trade_count = int(pd.to_numeric(frame.get("trade_count"), errors="coerce").fillna(0).sum())
        sample_status, conclusion = _sample_status(total_trade_count)
        warning_values = frame.get("data_quality_warning", pd.Series(dtype=str)).fillna("").astype(str)
        warning = "DATA_INSUFFICIENT" if (warning_values == "DATA_INSUFFICIENT").any() else ""
        rows.append(
            {
                "report_date": report_date,
                "factor_name": factor_name,
                "best_bucket": best.get("bucket", ""),
                "best_alpha_20d": best.get("alpha_20d"),
                "best_avg_return_pct": best.get("avg_return_pct"),
                "worst_bucket": worst.get("bucket", ""),
                "worst_alpha_20d": worst.get("alpha_20d"),
                "worst_avg_return_pct": worst.get("avg_return_pct"),
                "total_stock_count": int(pd.to_numeric(frame.get("stock_count"), errors="coerce").fillna(0).sum()),
                "total_trade_count": total_trade_count,
                "sample_status": sample_status,
                "conclusion": conclusion,
                "data_quality_warning": warning,
                "notes": "forward_return 欄位不足時，以既有 paper_trades 報酬做觀察，不作為正式調參依據",
            }
        )
    return pd.DataFrame(rows, columns=FACTOR_ATTRIBUTION_SUMMARY_COLUMNS)


def _benchmark_diagnostics(
    trade_date: pd.Timestamp | None,
    paper_summary: pd.DataFrame,
    recent_summaries: pd.DataFrame,
    benchmark: dict[str, object],
    readiness: dict[str, object],
    risk_alpha: dict[str, object],
) -> pd.DataFrame:
    summary = paper_summary.iloc[0].to_dict() if not paper_summary.empty else {}
    system_returns = _system_return_snapshot(summary, recent_summaries)
    benchmark_returns = benchmark.get("returns", {}) if isinstance(benchmark.get("returns"), dict) else {}
    benchmark_can_judge = bool(benchmark.get("can_judge_alpha", False))
    can_judge_alpha = bool(benchmark_can_judge and readiness.get("can_judge_strategy_alpha", False))
    alpha_1d = _window_alpha(system_returns, benchmark_returns, benchmark, readiness, "1d")
    alpha_5d = _risk_excess_or_window_alpha(risk_alpha, system_returns, benchmark_returns, benchmark, readiness, "5d")
    alpha_20d = _risk_excess_or_window_alpha(risk_alpha, system_returns, benchmark_returns, benchmark, readiness, "20d")
    alpha_60d = _risk_excess_or_window_alpha(risk_alpha, system_returns, benchmark_returns, benchmark, readiness, "60d")
    alpha_120d = _risk_excess_or_window_alpha(
        risk_alpha, system_returns, benchmark_returns, benchmark, readiness, "120d"
    )
    alpha_252d = _risk_excess_or_window_alpha(
        risk_alpha, system_returns, benchmark_returns, benchmark, readiness, "252d"
    )
    primary_alpha = _num(risk_alpha.get("excess_return"))
    if primary_alpha is None:
        primary_window = str(risk_alpha.get("primary_alpha_window") or "")
        primary_alpha = _num(risk_alpha.get(f"excess_return_{primary_window}")) if primary_window else None
    available_alpha = [
        value for value in [alpha_1d, alpha_5d, alpha_20d, alpha_60d, alpha_120d, alpha_252d] if value is not None
    ]
    warning_parts = []
    if benchmark.get("warning"):
        warning_parts.append(str(benchmark.get("warning")))
    if not benchmark_can_judge:
        warning_parts.append("NO_OFFICIAL_BENCHMARK: can_judge_alpha=false")
    for window in ["20d", "60d", "120d", "252d"]:
        if bool(benchmark.get("benchmark_is_official", False)) and not _can_judge_window(benchmark, window):
            warning_parts.append(f"DATA_INSUFFICIENT: {window} alpha")
        if _can_judge_window(benchmark, window) and not strategy_can_judge_window(readiness, window):
            warning_parts.append(f"NOT_ENOUGH_STRATEGY_HISTORY: {strategy_insufficient_reason(readiness, window)}")
    if not available_alpha and primary_alpha is None:
        warning_parts.append("DATA_INSUFFICIENT: benchmark 或系統報酬資料不足")
    conclusion_status = str(
        risk_alpha.get("conclusion_status")
        or _conclusion_status(benchmark_can_judge, readiness, available_alpha, primary_alpha)
    )
    risk_reason = str(risk_alpha.get("conclusion_reason", "") or "").strip()
    if risk_reason:
        warning_parts.append(risk_reason)
    row = {
        "trade_date": (trade_date or pd.Timestamp.today()).strftime("%Y-%m-%d"),
        "system_cumulative_return": system_returns.get("total"),
        "benchmark_cumulative_return": benchmark_returns.get("total"),
        "alpha": primary_alpha,
        "benchmark_return_1d": benchmark_returns.get("1d")
        if _can_judge_window(benchmark, "1d") and strategy_can_judge_window(readiness, "1d")
        else None,
        "benchmark_return_5d": _risk_benchmark_or_snapshot_return(
            risk_alpha, benchmark_returns, benchmark, readiness, "5d"
        ),
        "benchmark_return_20d": _risk_benchmark_or_snapshot_return(
            risk_alpha, benchmark_returns, benchmark, readiness, "20d"
        ),
        "benchmark_return_60d": _risk_benchmark_or_snapshot_return(
            risk_alpha, benchmark_returns, benchmark, readiness, "60d"
        ),
        "benchmark_return_120d": _risk_benchmark_or_snapshot_return(
            risk_alpha, benchmark_returns, benchmark, readiness, "120d"
        ),
        "benchmark_return_252d": _risk_benchmark_or_snapshot_return(
            risk_alpha, benchmark_returns, benchmark, readiness, "252d"
        ),
        "alpha_1d": alpha_1d,
        "alpha_5d": alpha_5d,
        "alpha_20d": alpha_20d,
        "alpha_60d": alpha_60d,
        "alpha_120d": alpha_120d,
        "alpha_252d": alpha_252d,
        "win_rate_vs_benchmark": round(sum(value >= 0 for value in available_alpha) / len(available_alpha), 4)
        if available_alpha
        else None,
        "max_drawdown": _max_drawdown(recent_summaries),
        "benchmark_source": benchmark.get("source_label", "benchmark 資料不足"),
        "benchmark_is_official": bool(benchmark.get("benchmark_is_official", False)),
        "fallback_reason": benchmark.get("fallback_reason", ""),
        "can_judge_alpha": can_judge_alpha,
        "can_judge_alpha_5d": bool(benchmark.get("can_judge_alpha_5d", False)),
        "can_judge_alpha_20d": bool(benchmark.get("can_judge_alpha_20d", False)),
        "can_judge_alpha_60d": bool(benchmark.get("can_judge_alpha_60d", False)),
        "can_judge_alpha_120d": bool(benchmark.get("can_judge_alpha_120d", False)),
        "can_judge_alpha_252d": bool(benchmark.get("can_judge_alpha_252d", False)),
        "benchmark_history_days": int(benchmark.get("benchmark_history_days", 0) or 0),
        **_readiness_columns(readiness),
        **_risk_alpha_columns(risk_alpha),
        "conclusion_status": conclusion_status,
        "benchmark_warning": benchmark.get("warning", ""),
        "data_quality_warning": "DATA_INSUFFICIENT" if warning_parts else "",
        "notes": "；".join(warning_parts),
    }
    return pd.DataFrame([row], columns=BENCHMARK_DIAGNOSTICS_COLUMNS)


def _guardrail_impact(rejected: pd.DataFrame, benchmark: dict[str, object]) -> pd.DataFrame:
    if rejected.empty:
        return pd.DataFrame(
            [
                {
                    "rejected_reason": "NO_REJECTED_TRADES",
                    "rejected_count": 0,
                    "avg_forward_return_1d": None,
                    "avg_forward_return_5d": None,
                    "avg_forward_return_20d": None,
                    "missed_winner_count": 0,
                    "avoided_loser_count": 0,
                    "estimated_alpha_impact": None,
                    "notes": "沒有 guardrail rejected orders，無法評估影響",
                }
            ],
            columns=GUARDRAIL_IMPACT_COLUMNS,
        )
    frame = rejected.copy()
    reason_col = _first_existing_column(
        frame, ["rejected_reason", "rejection_reason", "skipped_reason", "warning", "reason"]
    )
    if reason_col is None:
        frame["_reason"] = "UNKNOWN"
    else:
        frame["_reason"] = frame[reason_col].fillna("").astype(str).replace("", "UNKNOWN")
    benchmark_returns = benchmark.get("returns", {}) if isinstance(benchmark.get("returns"), dict) else {}
    rows = []
    for reason, group in frame.groupby("_reason", dropna=False):
        avg_1d = _avg_forward(group, "1d")
        avg_5d = _avg_forward(group, "5d")
        avg_20d = _avg_forward(group, "20d")
        forward_20 = _forward_series(group, "20d")
        missed_winners = int((forward_20 > (benchmark_returns.get("20d") or 0.0)).sum()) if not forward_20.empty else 0
        avoided_losers = int((forward_20 < 0).sum()) if not forward_20.empty else 0
        estimated_alpha = _sub_or_none(avg_20d, benchmark_returns.get("20d"))
        notes = ""
        if forward_20.empty:
            notes = "DATA_INSUFFICIENT: 缺少 forward_return 欄位，暫不能判斷 guardrail 是否過度保守"
        rows.append(
            {
                "rejected_reason": _text(reason) or "UNKNOWN",
                "rejected_count": int(len(group)),
                "avg_forward_return_1d": avg_1d,
                "avg_forward_return_5d": avg_5d,
                "avg_forward_return_20d": avg_20d,
                "missed_winner_count": missed_winners,
                "avoided_loser_count": avoided_losers,
                "estimated_alpha_impact": estimated_alpha,
                "notes": notes,
            }
        )
    return pd.DataFrame(rows, columns=GUARDRAIL_IMPACT_COLUMNS)


def _system_return_snapshot(summary: dict[str, object], recent_summaries: pd.DataFrame) -> dict[str, float | None]:
    total_equity = _first_number(summary, ["total_equity_after_cost", "total_equity"])
    total_capital = _first_number(summary, ["total_capital"])
    total = total_equity / total_capital - 1.0 if total_equity is not None and total_capital else None
    return {
        "1d": _equity_return_over_recent_window(recent_summaries, 1),
        "5d": _equity_return_over_recent_window(recent_summaries, 5),
        "20d": _equity_return_over_recent_window(recent_summaries, 20),
        "60d": _equity_return_over_recent_window(recent_summaries, 60),
        "120d": _equity_return_over_recent_window(recent_summaries, 120),
        "252d": _equity_return_over_recent_window(recent_summaries, 252),
        "total": total,
    }


def _trade_returns(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "stock_id" not in trades.columns:
        return pd.DataFrame(columns=["stock_id", "return_pct", "realized_pnl_after_cost"])
    frame = trades.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    realized = pd.to_numeric(_series(frame, "realized_pnl_pct_after_cost"), errors="coerce")
    fallback_realized = pd.to_numeric(_series(frame, "realized_pnl_pct"), errors="coerce")
    unrealized = pd.to_numeric(_series(frame, "unrealized_pnl_pct"), errors="coerce")
    frame["return_pct"] = realized.where(realized.notna(), fallback_realized).where(
        lambda series: series.notna(),
        unrealized,
    )
    frame["realized_pnl_after_cost"] = pd.to_numeric(_series(frame, "realized_pnl_after_cost"), errors="coerce")
    keep = [
        "stock_id",
        "return_pct",
        "realized_pnl_after_cost",
        "entry_price_source",
        "holding_days",
        *FORWARD_RETURN_COLUMNS.values(),
    ]
    for column in keep:
        if column not in frame.columns:
            frame[column] = None
    return frame[keep].dropna(subset=["stock_id"])


def _bucket_factor(series: pd.Series, factor_type: str) -> pd.Series:
    if factor_type == "numeric":
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.dropna().empty:
            return pd.Series(["MISSING"] * len(series), index=series.index)
        unique_count = numeric.dropna().nunique()
        if unique_count >= 3:
            try:
                return (
                    pd.qcut(numeric, q=3, labels=["LOW", "MID", "HIGH"], duplicates="drop")
                    .astype(str)
                    .fillna("MISSING")
                )
            except ValueError:
                pass
        return numeric.apply(lambda value: "MISSING" if pd.isna(value) else _numeric_bucket(float(value)))
    return series.apply(lambda value: _text(value) or "MISSING")


def _numeric_bucket(value: float) -> str:
    if value >= 70:
        return "HIGH"
    if value >= 50:
        return "MID"
    return "LOW"


def _attention_disposition_bucket(row: pd.Series) -> str:
    if _truthy(row.get("is_disposition_stock")):
        return "處置股"
    if _truthy(row.get("is_attention_stock")):
        return "注意股"
    return "正常"


def _sample_status(trade_count: int) -> tuple[str, str]:
    if trade_count < 20:
        return "DATA_INSUFFICIENT", "DATA_INSUFFICIENT"
    if trade_count < 50:
        return "OBSERVATION_ONLY", "僅供觀察"
    return "PRELIMINARY_REFERENCE", "可列為初步參考"


def _trades_for_stock_ids(trade_returns: pd.DataFrame, stock_ids: set[str]) -> pd.DataFrame:
    if trade_returns.empty or not stock_ids or "stock_id" not in trade_returns.columns:
        return pd.DataFrame()
    return trade_returns[trade_returns["stock_id"].astype(str).str.strip().isin(stock_ids)].copy()


def _stock_id_set(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "stock_id" not in frame.columns:
        return set()
    return set(frame["stock_id"].astype(str).str.strip().dropna())


def _avg_forward(frame: pd.DataFrame, window: str) -> float | None:
    values = _forward_series(frame, window)
    return _mean_or_none(values)


def _win_rate(frame: pd.DataFrame, window: str) -> float | None:
    values = _forward_series(frame, window)
    if values.empty:
        return None
    return round(float((values > 0).mean()), 4)


def _forward_series(frame: pd.DataFrame, window: str) -> pd.Series:
    column = FORWARD_RETURN_COLUMNS[window]
    if frame.empty or column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _equity_return_over_recent_window(recent_summaries: pd.DataFrame, window: int) -> float | None:
    if recent_summaries.empty:
        return None
    equity_column = (
        "total_equity_after_cost" if "total_equity_after_cost" in recent_summaries.columns else "total_equity"
    )
    if equity_column not in recent_summaries.columns:
        return None
    frame = recent_summaries.copy()
    if "trade_date" in frame.columns:
        frame["_sort_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame = frame.sort_values("_sort_date", ascending=False)
    values = pd.to_numeric(frame[equity_column], errors="coerce").dropna().tolist()
    if len(values) <= window:
        return None
    latest, baseline = float(values[0]), float(values[window])
    if abs(baseline) < 0.000001:
        return None
    return latest / baseline - 1.0


def _max_drawdown(recent_summaries: pd.DataFrame) -> float | None:
    if recent_summaries.empty:
        return None
    equity_column = (
        "total_equity_after_cost" if "total_equity_after_cost" in recent_summaries.columns else "total_equity"
    )
    if equity_column not in recent_summaries.columns:
        return None
    frame = recent_summaries.copy()
    if "trade_date" in frame.columns:
        frame["_sort_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame = frame.sort_values("_sort_date", ascending=True)
    values = pd.to_numeric(frame[equity_column], errors="coerce").dropna()
    if values.empty:
        return None
    running_peak = values.cummax()
    drawdown = values / running_peak - 1.0
    return round(float(drawdown.min()), 6)


def _read_recent_summaries(report_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(
        report_dir.glob("daily_summary_*.csv"), key=lambda item: _date_from_path(item) or pd.Timestamp.min, reverse=True
    ):
        frame = _read_csv(path)
        if frame.empty:
            continue
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_trade_date(report_dir: Path, trade_date: str | None) -> pd.Timestamp | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    for pattern in ["daily_summary_*.csv", "paper_summary_*.csv", "candidates_*.csv"]:
        latest = _latest_file(report_dir, pattern)
        if latest is not None:
            parsed = _date_from_path(latest)
            if parsed is not None:
                return parsed
    trades = _read_csv(report_dir / "paper_trades.csv")
    if not trades.empty and "trade_date" in trades.columns:
        values = pd.to_datetime(trades["trade_date"], errors="coerce").dropna()
        if not values.empty:
            return values.max()
    return None


def _read_latest(report_dir: Path, pattern: str, trade_date: str | None) -> pd.DataFrame:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return _read_csv(target)
    latest = _latest_file(report_dir, pattern)
    return _read_csv(latest) if latest is not None else pd.DataFrame()


def _latest_file(report_dir: Path, pattern: str) -> Path | None:
    files = sorted(
        report_dir.glob(pattern),
        key=lambda path: (_date_from_path(path) or pd.Timestamp.min, path.stat().st_mtime),
        reverse=True,
    )
    return files[0] if files else None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _series(frame: pd.DataFrame, column: str, default: object = None) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=object)
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _first_existing_column(frame: pd.DataFrame, columns: Iterable[str]) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _first_number_from_frame(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    return _num(frame.iloc[0].get(column))


def _first_number(row: dict[str, object], columns: list[str]) -> float | None:
    for column in columns:
        value = _num(row.get(column))
        if value is not None:
            return value
    return None


def _mean_or_none(values: pd.Series) -> float | None:
    values = pd.to_numeric(values, errors="coerce").dropna()
    return round(float(values.mean()), 6) if not values.empty else None


def _median_or_none(values: pd.Series) -> float | None:
    values = pd.to_numeric(values, errors="coerce").dropna()
    return round(float(values.median()), 6) if not values.empty else None


def _sum_or_none(values: pd.Series) -> float | None:
    values = pd.to_numeric(values, errors="coerce").dropna()
    return round(float(values.sum()), 2) if not values.empty else None


def _min_or_none(values: pd.Series) -> float | None:
    values = pd.to_numeric(values, errors="coerce").dropna()
    return round(float(values.min()), 6) if not values.empty else None


def _max_or_none(values: pd.Series) -> float | None:
    values = pd.to_numeric(values, errors="coerce").dropna()
    return round(float(values.max()), 6) if not values.empty else None


def _sub_or_none(left: object, right: object) -> float | None:
    left_number = _num(left)
    right_number = _num(right)
    if left_number is None or right_number is None:
        return None
    return round(left_number - right_number, 6)


def _window_alpha(
    system_returns: dict[str, float | None],
    benchmark_returns: dict[str, object],
    benchmark: dict[str, object],
    readiness: dict[str, object],
    window: str,
) -> float | None:
    if not _can_judge_window(benchmark, window) or not strategy_can_judge_window(readiness, window):
        return None
    return _sub_or_none(system_returns.get(window), benchmark_returns.get(window))


def _risk_excess_or_window_alpha(
    risk_alpha: dict[str, object],
    system_returns: dict[str, float | None],
    benchmark_returns: dict[str, object],
    benchmark: dict[str, object],
    readiness: dict[str, object],
    window: str,
) -> float | None:
    primary_value = _num(risk_alpha.get(f"excess_return_{window}"))
    if primary_value is not None:
        return primary_value
    return _window_alpha(system_returns, benchmark_returns, benchmark, readiness, window)


def _risk_benchmark_or_snapshot_return(
    risk_alpha: dict[str, object],
    benchmark_returns: dict[str, object],
    benchmark: dict[str, object],
    readiness: dict[str, object],
    window: str,
) -> float | None:
    if not (_can_judge_window(benchmark, window) and strategy_can_judge_window(readiness, window)):
        return None
    primary_value = _num(risk_alpha.get(f"benchmark_return_{window}"))
    if primary_value is not None:
        return primary_value
    return _num(benchmark_returns.get(window))


def _can_judge_window(benchmark: dict[str, object], window: str) -> bool:
    if window == "1d":
        return bool(benchmark.get("benchmark_is_official", False) and benchmark.get("can_judge_alpha", False))
    return bool(benchmark.get(f"can_judge_alpha_{window}", False))


def _readiness_columns(readiness: dict[str, object]) -> dict[str, object]:
    output = {
        "strategy_history_days": int(readiness.get("strategy_history_days", 0) or 0),
        "valid_trade_count": int(readiness.get("valid_trade_count", 0) or 0),
        "holding_record_count": int(readiness.get("holding_record_count", 0) or 0),
        "can_judge_strategy_alpha": bool(readiness.get("can_judge_strategy_alpha", False)),
    }
    for days in STRATEGY_ALPHA_WINDOWS:
        if days == 1:
            continue
        key = f"can_judge_strategy_alpha_{days}d"
        output[key] = bool(readiness.get(key, False))
    return output


def _risk_alpha_columns(risk_alpha: dict[str, object]) -> dict[str, object]:
    return {
        column: risk_alpha.get(column)
        for column in RISK_ADJUSTED_ALPHA_COLUMNS
        if not re.fullmatch(r"benchmark_return_\d+d", column)
    }


def _conclusion_status(
    benchmark_can_judge: bool,
    readiness: dict[str, object],
    available_alpha: list[float],
    cumulative_alpha: float | None,
) -> str:
    if not benchmark_can_judge:
        return "DATA_INSUFFICIENT"
    if not bool(readiness.get("can_judge_strategy_alpha", False)):
        return "NOT_ENOUGH_STRATEGY_HISTORY"
    if not available_alpha and cumulative_alpha is None:
        return "DATA_INSUFFICIENT"
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


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _first_text(*values: object) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _join_notes(*parts: str) -> str:
    return "；".join(part for part in parts if part)
