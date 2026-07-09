"""Observation-only market regime threshold diagnostics.

This report estimates how different market_regime_score thresholds would have
changed paper-trading exposure from existing local report CSVs. It does not
change config, guardrails, selection, exits, or orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd


THRESHOLDS = (45, 50, 55, 60, 65, 70)
CURRENT_THRESHOLD = 60
MIN_OBSERVATIONS = 4
MIN_5D_LABEL_COVERAGE = 0.70
MIN_20D_LABEL_COVERAGE = 0.60
MIN_VALIDATION_ELIGIBLE_SAMPLES = 30
MIN_VALIDATION_BLOCKED_SAMPLES = 10

MARKET_REGIME_THRESHOLD_OPTIMIZATION_COLUMNS = [
    "trade_date",
    "threshold",
    "current_threshold",
    "current_market_regime_score",
    "walk_forward_split",
    "train_start",
    "train_end",
    "validation_start",
    "validation_end",
    "train_observation_count",
    "validation_observation_count",
    "eligible_candidate_count",
    "blocked_candidate_count",
    "would_allow_new_entries",
    "estimated_exposure_pct",
    "cash_drag_proxy",
    "forward_return_5d_mean",
    "forward_return_20d_mean",
    "positive_forward_5d_rate",
    "positive_forward_20d_rate",
    "label_5d_coverage",
    "label_20d_coverage",
    "validation_eligible_sample_count",
    "validation_blocked_sample_count",
    "readiness_status",
    "readiness_reason",
    "can_recommend_threshold_change",
    "can_recommend_dynamic_exposure",
    "estimated_strategy_return_proxy",
    "estimated_benchmark_return",
    "estimated_excess_return",
    "estimated_max_drawdown_proxy",
    "train_estimated_excess_return",
    "validation_estimated_excess_return",
    "dynamic_exposure_pct",
    "dynamic_cash_drag_proxy",
    "dynamic_estimated_excess_return",
    "dynamic_estimated_max_drawdown_proxy",
    "risk_status",
    "recommendation",
    "is_observation_only",
    "data_sufficiency_status",
    "notes",
]


@dataclass(frozen=True)
class MarketRegimeThresholdOptimizationResult:
    trade_date: pd.Timestamp | None
    frame: pd.DataFrame
    output_path: Path
    status: str = "OK"
    warning: str = ""


@dataclass(frozen=True)
class LabelCoverage:
    sample_count: int
    label_5d_count: int
    label_20d_count: int
    label_5d_coverage: float
    label_20d_coverage: float


@dataclass(frozen=True)
class SampleReadiness:
    label_5d_coverage: float
    label_20d_coverage: float
    validation_eligible_sample_count: int
    validation_blocked_sample_count: int
    readiness_status: str
    readiness_reason: str
    can_recommend_threshold_change: bool
    can_recommend_dynamic_exposure: bool


def generate_market_regime_threshold_optimization(
    reports_dir: str | Path = "reports",
    trade_date: str | None = None,
    *,
    thresholds: tuple[int, ...] = THRESHOLDS,
    current_threshold: int = CURRENT_THRESHOLD,
) -> MarketRegimeThresholdOptimizationResult:
    """Generate an observation-only threshold optimizer report."""

    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    date_label = (selected_date or pd.Timestamp.today()).strftime("%Y%m%d")

    observations = _observation_frame(report_dir, selected_date)
    label_coverage = _candidate_forward_label_coverage(report_dir, selected_date)
    if label_coverage.sample_count <= 0:
        label_coverage = _label_coverage(observations)
    current_score = _current_market_regime_score(observations)
    train, validation, split_note = _walk_forward_split(observations)

    current_train = _threshold_metrics(train, current_threshold) if not train.empty else _empty_metrics()
    dynamic_validation = (
        _threshold_metrics(validation, None, exposure_func=_dynamic_exposure)
        if not validation.empty
        else _empty_metrics()
    )
    dynamic_train = (
        _threshold_metrics(train, None, exposure_func=_dynamic_exposure) if not train.empty else _empty_metrics()
    )

    rows = [
        _threshold_row(
            threshold=threshold,
            current_threshold=current_threshold,
            selected_date=selected_date,
            current_score=current_score,
            train=train,
            validation=validation,
            current_train=current_train,
            dynamic_validation=dynamic_validation,
            label_coverage=label_coverage,
            split_note=split_note,
        )
        for threshold in thresholds
    ]
    rows.append(
        _dynamic_row(
            current_threshold=current_threshold,
            selected_date=selected_date,
            current_score=current_score,
            train=train,
            validation=validation,
            current_train=current_train,
            dynamic_train=dynamic_train,
            dynamic_validation=dynamic_validation,
            label_coverage=label_coverage,
            split_note=split_note,
        )
    )

    frame = pd.DataFrame(rows, columns=MARKET_REGIME_THRESHOLD_OPTIMIZATION_COLUMNS)
    output_path = report_dir / f"market_regime_threshold_optimization_{date_label}.csv"
    frame.to_csv(output_path, index=False, encoding="utf-8")

    data_warnings = [
        str(value)
        for value in frame.get("data_sufficiency_status", pd.Series(dtype=str)).dropna().unique()
        if str(value) == "DATA_INSUFFICIENT"
    ]
    readiness_warnings = [
        str(value)
        for value in frame.get("readiness_status", pd.Series(dtype=str)).dropna().unique()
        if str(value).startswith("DATA_INSUFFICIENT")
    ]
    status = (
        "DATA_INSUFFICIENT"
        if data_warnings and frame["data_sufficiency_status"].astype(str).eq("DATA_INSUFFICIENT").all()
        else "OK"
    )
    if (data_warnings or readiness_warnings) and status == "OK":
        status = "OK_WITH_WARNINGS"
    return MarketRegimeThresholdOptimizationResult(
        trade_date=selected_date,
        frame=frame,
        output_path=output_path,
        status=status,
        warning="; ".join(dict.fromkeys(data_warnings + readiness_warnings)),
    )


def _threshold_row(
    *,
    threshold: int,
    current_threshold: int,
    selected_date: pd.Timestamp | None,
    current_score: float | None,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    current_train: dict[str, object],
    dynamic_validation: dict[str, object],
    label_coverage: LabelCoverage,
    split_note: str,
) -> dict[str, object]:
    train_metrics = _threshold_metrics(train, threshold)
    validation_metrics = _threshold_metrics(validation, threshold)
    sufficient = _is_sufficient(train_metrics) and _is_sufficient(validation_metrics)
    readiness = _sample_readiness(
        label_coverage,
        validation_metrics,
        evaluate_threshold_change=True,
        evaluate_dynamic_exposure=False,
    )
    recommendation = _gated_recommendation(readiness)
    if sufficient and readiness.can_recommend_threshold_change:
        recommendation = _recommendation(threshold, current_threshold, train_metrics, current_train, sufficient)
    return _row(
        selected_date=selected_date,
        threshold=threshold,
        current_threshold=current_threshold,
        current_score=current_score,
        train=train,
        validation=validation,
        validation_metrics=validation_metrics,
        train_metrics=train_metrics,
        dynamic_validation=dynamic_validation,
        would_allow=_score_allows(current_score, threshold),
        recommendation=recommendation,
        sufficient=sufficient,
        readiness=readiness,
        split_note=split_note,
    )


def _dynamic_row(
    *,
    current_threshold: int,
    selected_date: pd.Timestamp | None,
    current_score: float | None,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    current_train: dict[str, object],
    dynamic_train: dict[str, object],
    dynamic_validation: dict[str, object],
    label_coverage: LabelCoverage,
    split_note: str,
) -> dict[str, object]:
    sufficient = _is_sufficient(dynamic_train) and _is_sufficient(dynamic_validation)
    readiness = _sample_readiness(
        label_coverage,
        dynamic_validation,
        evaluate_threshold_change=False,
        evaluate_dynamic_exposure=True,
    )
    recommendation = _gated_recommendation(readiness)
    if sufficient and readiness.can_recommend_dynamic_exposure:
        dynamic_excess = _num(dynamic_train.get("estimated_excess_return"))
        current_excess = _num(current_train.get("estimated_excess_return"))
        recommendation = (
            "CONSIDER_DYNAMIC_EXPOSURE"
            if dynamic_excess is not None and current_excess is not None and dynamic_excess > current_excess + 0.005
            else "KEEP_CURRENT"
        )
    return _row(
        selected_date=selected_date,
        threshold="DYNAMIC_EXPOSURE",
        current_threshold=current_threshold,
        current_score=current_score,
        train=train,
        validation=validation,
        validation_metrics=dynamic_validation,
        readiness=readiness,
        train_metrics=dynamic_train,
        dynamic_validation=dynamic_validation,
        would_allow=(_dynamic_exposure(current_score) or 0) > 0,
        recommendation=recommendation,
        sufficient=sufficient,
        split_note=split_note,
        notes="score<45:0%; 45-55:20%; 55-65:40%; 65-75:60%; >=75:80%。此列只比較 dynamic exposure proxy。",
    )


def _row(
    *,
    selected_date: pd.Timestamp | None,
    threshold: object,
    current_threshold: int,
    current_score: float | None,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    validation_metrics: dict[str, object],
    train_metrics: dict[str, object],
    dynamic_validation: dict[str, object],
    would_allow: bool,
    recommendation: str,
    sufficient: bool,
    readiness: SampleReadiness,
    split_note: str,
    notes: str = "",
) -> dict[str, object]:
    data_status = _data_sufficiency_status(sufficient, readiness)
    risk_status = "OBSERVATION_ONLY" if sufficient else "DATA_INSUFFICIENT"
    drawdown = _num(validation_metrics.get("estimated_max_drawdown_proxy"))
    if sufficient and drawdown is not None and drawdown < -0.1:
        risk_status = "HIGH_RISK_OBSERVATION"
    show_5d = readiness.label_5d_coverage >= MIN_5D_LABEL_COVERAGE
    show_20d = readiness.label_20d_coverage >= MIN_20D_LABEL_COVERAGE
    return {
        "trade_date": _date_text(selected_date),
        "threshold": threshold,
        "current_threshold": current_threshold,
        "current_market_regime_score": current_score,
        "walk_forward_split": split_note,
        "train_start": _frame_date(train, first=True),
        "train_end": _frame_date(train, first=False),
        "validation_start": _frame_date(validation, first=True),
        "validation_end": _frame_date(validation, first=False),
        "train_observation_count": len(train),
        "validation_observation_count": len(validation),
        "eligible_candidate_count": validation_metrics.get("eligible_candidate_count", 0),
        "blocked_candidate_count": validation_metrics.get("blocked_candidate_count", 0),
        "would_allow_new_entries": bool(would_allow),
        "estimated_exposure_pct": validation_metrics.get("estimated_exposure_pct"),
        "cash_drag_proxy": validation_metrics.get("cash_drag_proxy"),
        "forward_return_5d_mean": validation_metrics.get("forward_return_5d_mean") if show_5d else None,
        "forward_return_20d_mean": validation_metrics.get("forward_return_20d_mean") if show_20d else None,
        "positive_forward_5d_rate": validation_metrics.get("positive_forward_5d_rate") if show_5d else None,
        "positive_forward_20d_rate": validation_metrics.get("positive_forward_20d_rate") if show_20d else None,
        "label_5d_coverage": readiness.label_5d_coverage,
        "label_20d_coverage": readiness.label_20d_coverage,
        "validation_eligible_sample_count": readiness.validation_eligible_sample_count,
        "validation_blocked_sample_count": readiness.validation_blocked_sample_count,
        "readiness_status": readiness.readiness_status,
        "readiness_reason": readiness.readiness_reason,
        "can_recommend_threshold_change": readiness.can_recommend_threshold_change,
        "can_recommend_dynamic_exposure": readiness.can_recommend_dynamic_exposure,
        "estimated_strategy_return_proxy": validation_metrics.get("estimated_strategy_return_proxy"),
        "estimated_benchmark_return": validation_metrics.get("estimated_benchmark_return"),
        "estimated_excess_return": validation_metrics.get("estimated_excess_return"),
        "estimated_max_drawdown_proxy": validation_metrics.get("estimated_max_drawdown_proxy"),
        "train_estimated_excess_return": train_metrics.get("estimated_excess_return"),
        "validation_estimated_excess_return": validation_metrics.get("estimated_excess_return"),
        "dynamic_exposure_pct": dynamic_validation.get("estimated_exposure_pct"),
        "dynamic_cash_drag_proxy": dynamic_validation.get("cash_drag_proxy"),
        "dynamic_estimated_excess_return": dynamic_validation.get("estimated_excess_return"),
        "dynamic_estimated_max_drawdown_proxy": dynamic_validation.get("estimated_max_drawdown_proxy"),
        "risk_status": risk_status,
        "recommendation": recommendation,
        "is_observation_only": True,
        "data_sufficiency_status": data_status,
        "notes": notes or "OBSERVATION_ONLY: threshold optimizer proxy，不修改正式策略或 config.yaml。",
    }


def _recommendation(
    threshold: int,
    current_threshold: int,
    train_metrics: dict[str, object],
    current_train: dict[str, object],
    sufficient: bool,
) -> str:
    if not sufficient:
        return "DATA_INSUFFICIENT"
    if threshold == current_threshold:
        return "KEEP_CURRENT"
    train_excess = _num(train_metrics.get("estimated_excess_return"))
    current_excess = _num(current_train.get("estimated_excess_return"))
    train_drawdown = _num(train_metrics.get("estimated_max_drawdown_proxy")) or 0.0
    current_drawdown = _num(current_train.get("estimated_max_drawdown_proxy")) or 0.0
    if (
        threshold < current_threshold
        and train_excess is not None
        and current_excess is not None
        and train_excess > current_excess + 0.005
        and train_drawdown >= current_drawdown - 0.02
    ):
        return "CONSIDER_LOWERING"
    return "KEEP_CURRENT"


def _threshold_metrics(
    frame: pd.DataFrame,
    threshold: int | None,
    exposure_func: Callable[[float | None], float] | None = None,
) -> dict[str, object]:
    if frame.empty or "market_regime_score" not in frame.columns:
        return _empty_metrics()

    data = frame.copy()
    scores = pd.to_numeric(data["market_regime_score"], errors="coerce")
    if exposure_func is None:
        exposure = scores.apply(
            lambda value: 1.0 if pd.notna(value) and threshold is not None and value >= threshold else 0.0
        )
    else:
        exposure = scores.apply(lambda value: exposure_func(float(value)) if pd.notna(value) else 0.0)
    candidate_count = pd.to_numeric(
        data.get("candidate_count", pd.Series(0, index=data.index)), errors="coerce"
    ).fillna(0)
    eligible_mask = exposure > 0
    eligible_count = int(candidate_count[eligible_mask].sum())
    blocked_count = int(candidate_count[~eligible_mask].sum())
    benchmark_20d = pd.to_numeric(data.get("benchmark_return_20d", pd.Series(dtype=float)), errors="coerce")
    benchmark_5d = pd.to_numeric(data.get("benchmark_return_5d", pd.Series(dtype=float)), errors="coerce")

    forward_5d = pd.to_numeric(data.get("forward_return_5d_mean", pd.Series(dtype=float)), errors="coerce")
    forward_20d = pd.to_numeric(data.get("forward_return_20d_mean", pd.Series(dtype=float)), errors="coerce")
    positive_5d = pd.to_numeric(data.get("positive_forward_5d_rate", pd.Series(dtype=float)), errors="coerce")
    positive_20d = pd.to_numeric(data.get("positive_forward_20d_rate", pd.Series(dtype=float)), errors="coerce")

    strategy_basis = forward_20d.where(forward_20d.notna(), forward_5d)
    benchmark_basis = benchmark_20d.where(forward_20d.notna(), benchmark_5d)
    strategy_frame = pd.DataFrame(
        {
            "strategy": exposure * strategy_basis,
            "benchmark": benchmark_basis,
        }
    ).dropna(subset=["strategy"])
    strategy_series = strategy_frame["strategy"]
    strategy_return = _mean_or_none(strategy_series)
    benchmark_return = _mean_or_none(strategy_frame["benchmark"].dropna())
    excess = _sub_or_none(strategy_return, benchmark_return)
    drawdown = _max_drawdown_proxy(strategy_series)
    cash_drag = _cash_drag_proxy(exposure, benchmark_20d)

    return {
        "observation_count": len(data),
        "eligible_candidate_count": eligible_count,
        "blocked_candidate_count": blocked_count,
        "estimated_exposure_pct": _mean_or_none(exposure.dropna()),
        "cash_drag_proxy": cash_drag,
        "forward_return_5d_mean": _weighted_mean(forward_5d[eligible_mask], candidate_count[eligible_mask]),
        "forward_return_20d_mean": _weighted_mean(forward_20d[eligible_mask], candidate_count[eligible_mask]),
        "positive_forward_5d_rate": _weighted_mean(positive_5d[eligible_mask], candidate_count[eligible_mask]),
        "positive_forward_20d_rate": _weighted_mean(positive_20d[eligible_mask], candidate_count[eligible_mask]),
        "estimated_strategy_return_proxy": strategy_return,
        "estimated_benchmark_return": benchmark_return
        if benchmark_return is not None
        else _mean_or_none(benchmark_5d.dropna()),
        "estimated_excess_return": excess,
        "estimated_max_drawdown_proxy": drawdown,
        "forward_observation_count": int(strategy_basis[eligible_mask].dropna().count()),
    }


def _is_sufficient(metrics: dict[str, object]) -> bool:
    return int(metrics.get("observation_count") or 0) > 0 and int(metrics.get("forward_observation_count") or 0) > 0


def _empty_metrics() -> dict[str, object]:
    return {
        "observation_count": 0,
        "eligible_candidate_count": 0,
        "blocked_candidate_count": 0,
        "estimated_exposure_pct": None,
        "cash_drag_proxy": None,
        "forward_return_5d_mean": None,
        "forward_return_20d_mean": None,
        "positive_forward_5d_rate": None,
        "positive_forward_20d_rate": None,
        "estimated_strategy_return_proxy": None,
        "estimated_benchmark_return": None,
        "estimated_excess_return": None,
        "estimated_max_drawdown_proxy": None,
        "forward_observation_count": 0,
    }


def _observation_frame(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    summaries = _daily_summary_observations(report_dir, selected_date)
    regimes = _market_regime_observations(report_dir, selected_date)
    candidates = _candidate_observations(report_dir, selected_date)
    candidate_forward = _candidate_forward_label_observations(report_dir, selected_date)
    rejected = _rejected_observations(report_dir, selected_date)
    paper = _paper_summary_observations(report_dir, selected_date)
    benchmark = _benchmark_observations(report_dir, selected_date)

    frames = [summaries, regimes, candidates, candidate_forward, rejected, paper, benchmark]
    dates = sorted(
        {
            date
            for frame in frames
            if not frame.empty and "trade_date" in frame.columns
            for date in frame["trade_date"].dropna().tolist()
        }
    )
    if not dates:
        return pd.DataFrame()
    output = pd.DataFrame({"trade_date": dates})
    for frame in frames:
        if frame.empty:
            continue
        output = output.merge(frame, on="trade_date", how="left")
    output["market_regime_score"] = _coalesce_numeric(
        output, ["summary_market_regime_score", "regime_market_regime_score"]
    )
    output["candidate_count"] = _coalesce_numeric(
        output,
        ["candidate_forward_candidate_count", "candidate_count", "summary_candidate_rows"],
    ).fillna(0)
    output["label_sample_count"] = _coalesce_numeric(
        output,
        ["candidate_forward_candidate_count", "candidate_count"],
    ).fillna(0)
    output["label_5d_count"] = _coalesce_numeric(
        output,
        ["candidate_forward_5d_label_count", "candidate_5d_label_count"],
    ).fillna(0)
    output["label_20d_count"] = _coalesce_numeric(
        output,
        ["candidate_forward_20d_label_count", "candidate_20d_label_count"],
    ).fillna(0)
    output["forward_return_5d_mean"] = _coalesce_numeric(
        output,
        ["candidate_forward_return_5d_mean", "forward_return_5d_mean"],
    )
    output["forward_return_20d_mean"] = _coalesce_numeric(
        output,
        ["candidate_forward_return_20d_mean", "forward_return_20d_mean"],
    )
    output["positive_forward_5d_rate"] = _coalesce_numeric(
        output,
        ["candidate_forward_positive_5d_rate", "positive_forward_5d_rate"],
    )
    output["positive_forward_20d_rate"] = _coalesce_numeric(
        output,
        ["candidate_forward_positive_20d_rate", "positive_forward_20d_rate"],
    )
    output["benchmark_return_5d"] = _coalesce_numeric(
        output,
        ["candidate_forward_benchmark_return_5d", "benchmark_return_5d", "regime_market_return_5d"],
    )
    output["benchmark_return_20d"] = _coalesce_numeric(
        output,
        ["candidate_forward_benchmark_return_20d", "benchmark_return_20d", "regime_market_return_20d"],
    )
    output["cash_ratio"] = _coalesce_numeric(output, ["cash_ratio"]).fillna(0)
    output = output.dropna(subset=["market_regime_score"]).sort_values("trade_date")
    return output.reset_index(drop=True)


def _daily_summary_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "daily_summary_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame()
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    output = pd.DataFrame(
        {
            "trade_date": frame["trade_date"],
            "summary_market_regime_score": pd.to_numeric(frame.get("market_regime_score"), errors="coerce"),
            "summary_candidate_rows": pd.to_numeric(frame.get("candidate_rows"), errors="coerce"),
        }
    )
    return _last_by_date(output)


def _market_regime_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "market_regime_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame()
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    output = pd.DataFrame(
        {
            "trade_date": frame["trade_date"],
            "regime_market_regime_score": pd.to_numeric(frame.get("market_regime_score"), errors="coerce"),
            "regime_market_return_5d": pd.to_numeric(frame.get("market_return_5d"), errors="coerce"),
            "regime_market_return_20d": pd.to_numeric(frame.get("market_return_20d"), errors="coerce"),
        }
    )
    return _last_by_date(output)


def _candidate_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "candidates_*.csv")
    if frame.empty:
        return pd.DataFrame()
    frame = _with_trade_date(frame)
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("trade_date", dropna=True)
    output = grouped.size().rename("candidate_count").reset_index()
    for window in ["5d", "20d"]:
        column = f"forward_return_{window}"
        if column not in frame.columns:
            continue
        returns = pd.to_numeric(frame[column], errors="coerce")
        forward = frame.assign(_forward_return=returns)
        output = output.merge(
            forward.assign(_has_label=returns.notna())
            .groupby("trade_date")["_has_label"]
            .sum()
            .rename(f"candidate_{window}_label_count")
            .reset_index(),
            on="trade_date",
            how="left",
        )
        output = output.merge(
            forward.groupby("trade_date")["_forward_return"]
            .mean()
            .rename(f"forward_return_{window}_mean")
            .reset_index(),
            on="trade_date",
            how="left",
        )
        output = output.merge(
            forward.assign(_positive=returns > 0)
            .groupby("trade_date")["_positive"]
            .mean()
            .rename(f"positive_forward_{window}_rate")
            .reset_index(),
            on="trade_date",
            how="left",
        )
    return output


def _candidate_forward_label_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "candidate_forward_returns_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame()
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("trade_date", dropna=True)
    output = grouped.size().rename("candidate_forward_candidate_count").reset_index()
    for window in ["5d", "20d"]:
        return_column = f"forward_return_{window}"
        if return_column in frame.columns:
            returns = pd.to_numeric(frame[return_column], errors="coerce")
            forward = frame.assign(_forward_return=returns)
            output = output.merge(
                forward.assign(_has_label=returns.notna())
                .groupby("trade_date")["_has_label"]
                .sum()
                .rename(f"candidate_forward_{window}_label_count")
                .reset_index(),
                on="trade_date",
                how="left",
            )
            output = output.merge(
                forward.groupby("trade_date")["_forward_return"]
                .mean()
                .rename(f"candidate_forward_return_{window}_mean")
                .reset_index(),
                on="trade_date",
                how="left",
            )
            output = output.merge(
                forward.assign(_positive=returns > 0)
                .loc[returns.notna()]
                .groupby("trade_date")["_positive"]
                .mean()
                .rename(f"candidate_forward_positive_{window}_rate")
                .reset_index(),
                on="trade_date",
                how="left",
            )
        benchmark_column = f"benchmark_return_{window}"
        if benchmark_column in frame.columns:
            benchmark = frame.assign(_benchmark_return=pd.to_numeric(frame[benchmark_column], errors="coerce"))
            output = output.merge(
                benchmark.groupby("trade_date")["_benchmark_return"]
                .mean()
                .rename(f"candidate_forward_benchmark_return_{window}")
                .reset_index(),
                on="trade_date",
                how="left",
            )
    return output


def _rejected_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "rejected_paper_orders_*.csv")
    if frame.empty:
        return pd.DataFrame()
    frame = _with_trade_date(frame)
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    reason_text = _joined_text(frame, ["rejection_reason", "rejected_reason", "skipped_reason", "warning"])
    market_regime_rejected = reason_text.str.contains("market_regime|市場環境|新增持倉門檻", case=False, na=False)
    return (
        frame.assign(_market_regime_rejected=market_regime_rejected)
        .groupby("trade_date")["_market_regime_rejected"]
        .sum()
        .rename("historical_market_regime_rejected_count")
        .reset_index()
    )


def _paper_summary_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "paper_summary_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame()
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    cash = pd.to_numeric(frame.get("cash"), errors="coerce")
    equity = pd.to_numeric(frame.get("total_equity_after_cost"), errors="coerce")
    equity = equity.where(equity.notna(), pd.to_numeric(frame.get("total_equity"), errors="coerce"))
    output = pd.DataFrame({"trade_date": frame["trade_date"], "cash_ratio": cash / equity})
    return _last_by_date(output)


def _benchmark_observations(report_dir: Path, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    frame = _read_all_reports(report_dir, "benchmark_diagnostics_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return pd.DataFrame()
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return pd.DataFrame()
    output = pd.DataFrame(
        {
            "trade_date": frame["trade_date"],
            "benchmark_return_5d": pd.to_numeric(frame.get("benchmark_return_5d"), errors="coerce"),
            "benchmark_return_20d": pd.to_numeric(frame.get("benchmark_return_20d"), errors="coerce"),
        }
    )
    return _last_by_date(output)


def _walk_forward_split(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    data = _matured_forward_observations(frame)
    if data.empty or len(data) < MIN_OBSERVATIONS:
        return pd.DataFrame(), pd.DataFrame(), "DATA_INSUFFICIENT: observation_count < 4"
    split_index = int(len(data) * 0.7)
    split_index = max(1, min(split_index, len(data) - 1))
    train = data.iloc[:split_index].copy()
    validation = data.iloc[split_index:].copy()
    return train, validation, "train_first_70pct_validation_last_30pct"


def _matured_forward_observations(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    data = frame.sort_values("trade_date").reset_index(drop=True)
    forward_columns = [
        column for column in ["forward_return_20d_mean", "forward_return_5d_mean"] if column in data.columns
    ]
    if not forward_columns:
        return pd.DataFrame()
    has_forward_label = pd.Series(False, index=data.index)
    for column in forward_columns:
        has_forward_label = has_forward_label | pd.to_numeric(data[column], errors="coerce").notna()
    return data[has_forward_label].reset_index(drop=True)


def _label_coverage(frame: pd.DataFrame) -> LabelCoverage:
    if frame.empty:
        return LabelCoverage(0, 0, 0, 0.0, 0.0)
    candidate_forward_sample_count = int(
        pd.to_numeric(frame.get("candidate_forward_candidate_count", pd.Series(dtype=float)), errors="coerce")
        .fillna(0)
        .sum()
    )
    if candidate_forward_sample_count > 0:
        sample_count = candidate_forward_sample_count
        label_5d_count = int(
            pd.to_numeric(frame.get("candidate_forward_5d_label_count", pd.Series(dtype=float)), errors="coerce")
            .fillna(0)
            .sum()
        )
        label_20d_count = int(
            pd.to_numeric(frame.get("candidate_forward_20d_label_count", pd.Series(dtype=float)), errors="coerce")
            .fillna(0)
            .sum()
        )
    else:
        sample_count = int(
            pd.to_numeric(frame.get("label_sample_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
        )
        label_5d_count = int(
            pd.to_numeric(frame.get("label_5d_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
        )
        label_20d_count = int(
            pd.to_numeric(frame.get("label_20d_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
        )
    if sample_count <= 0:
        return LabelCoverage(0, label_5d_count, label_20d_count, 0.0, 0.0)
    return LabelCoverage(
        sample_count=sample_count,
        label_5d_count=label_5d_count,
        label_20d_count=label_20d_count,
        label_5d_coverage=float(label_5d_count / sample_count),
        label_20d_coverage=float(label_20d_count / sample_count),
    )


def _candidate_forward_label_coverage(report_dir: Path, selected_date: pd.Timestamp | None) -> LabelCoverage:
    frame = _read_all_reports(report_dir, "candidate_forward_returns_*.csv")
    if frame.empty or "trade_date" not in frame.columns:
        return LabelCoverage(0, 0, 0, 0.0, 0.0)
    frame = _normalize_dates(frame, selected_date)
    if frame.empty:
        return LabelCoverage(0, 0, 0, 0.0, 0.0)
    sample_count = len(frame)
    label_5d_count = _label_count(frame, "forward_return_5d")
    label_20d_count = _label_count(frame, "forward_return_20d")
    return LabelCoverage(
        sample_count=sample_count,
        label_5d_count=label_5d_count,
        label_20d_count=label_20d_count,
        label_5d_coverage=float(label_5d_count / sample_count) if sample_count else 0.0,
        label_20d_coverage=float(label_20d_count / sample_count) if sample_count else 0.0,
    )


def _label_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame.columns:
        return 0
    return int(pd.to_numeric(frame[column], errors="coerce").notna().sum())


def _sample_readiness(
    label_coverage: LabelCoverage,
    validation_metrics: dict[str, object],
    *,
    evaluate_threshold_change: bool,
    evaluate_dynamic_exposure: bool,
) -> SampleReadiness:
    eligible_samples = int(validation_metrics.get("eligible_candidate_count") or 0)
    blocked_samples = int(validation_metrics.get("blocked_candidate_count") or 0)
    ready_5d = label_coverage.label_5d_coverage >= MIN_5D_LABEL_COVERAGE
    ready_20d = label_coverage.label_20d_coverage >= MIN_20D_LABEL_COVERAGE
    ready_validation = (
        eligible_samples >= MIN_VALIDATION_ELIGIBLE_SAMPLES and blocked_samples >= MIN_VALIDATION_BLOCKED_SAMPLES
    )

    status = "READY_FOR_20D_OBSERVATION"
    reason = (
        f"5d label coverage {label_coverage.label_5d_coverage:.2%} >= {MIN_5D_LABEL_COVERAGE:.0%}; "
        f"20d label coverage {label_coverage.label_20d_coverage:.2%} >= {MIN_20D_LABEL_COVERAGE:.0%}; "
        f"validation eligible/block samples {eligible_samples}/{blocked_samples} "
        f">= {MIN_VALIDATION_ELIGIBLE_SAMPLES}/{MIN_VALIDATION_BLOCKED_SAMPLES}."
    )
    if label_coverage.sample_count <= 0:
        status = "DATA_INSUFFICIENT_VALIDATION"
        reason = "No forward labels are available; threshold optimizer recommendations must remain data insufficient."
    elif not ready_5d:
        status = "DATA_INSUFFICIENT_VALIDATION"
        reason = (
            f"5d label coverage {label_coverage.label_5d_coverage:.2%} < {MIN_5D_LABEL_COVERAGE:.0%}; "
            "5d observation sample is insufficient; recommendation must remain DATA_INSUFFICIENT."
        )
    elif not ready_20d:
        status = "DATA_INSUFFICIENT_20D"
        reason = (
            f"5d label coverage {label_coverage.label_5d_coverage:.2%} is ready for 5d observation; "
            f"20d label coverage {label_coverage.label_20d_coverage:.2%} < {MIN_20D_LABEL_COVERAGE:.0%}. "
            "20d sample is insufficient; observation-only; not a basis for lowering the formal threshold."
        )
    elif not ready_validation:
        status = "DATA_INSUFFICIENT_VALIDATION"
        reason = (
            f"validation eligible/block samples {eligible_samples}/{blocked_samples} "
            f"< {MIN_VALIDATION_ELIGIBLE_SAMPLES}/{MIN_VALIDATION_BLOCKED_SAMPLES}; "
            "validation sample is insufficient; do not output formal threshold change or dynamic exposure recommendations."
        )

    can_recommend = bool(ready_5d and ready_20d and ready_validation and label_coverage.sample_count > 0)
    return SampleReadiness(
        label_5d_coverage=label_coverage.label_5d_coverage,
        label_20d_coverage=label_coverage.label_20d_coverage,
        validation_eligible_sample_count=eligible_samples,
        validation_blocked_sample_count=blocked_samples,
        readiness_status=status,
        readiness_reason=reason,
        can_recommend_threshold_change=can_recommend and evaluate_threshold_change,
        can_recommend_dynamic_exposure=can_recommend and evaluate_dynamic_exposure,
    )


def _gated_recommendation(readiness: SampleReadiness) -> str:
    if readiness.readiness_status == "DATA_INSUFFICIENT_20D":
        return "OBSERVATION_ONLY"
    return "DATA_INSUFFICIENT"


def _data_sufficiency_status(sufficient: bool, readiness: SampleReadiness) -> str:
    if not sufficient:
        return "DATA_INSUFFICIENT"
    if readiness.readiness_status == "DATA_INSUFFICIENT_VALIDATION":
        return "DATA_INSUFFICIENT"
    return "OBSERVATION_ONLY"


def _dynamic_exposure(score: float | None) -> float:
    if score is None or pd.isna(score):
        return 0.0
    if score < 45:
        return 0.0
    if score < 55:
        return 0.2
    if score < 65:
        return 0.4
    if score < 75:
        return 0.6
    return 0.8


def _score_allows(score: float | None, threshold: float) -> bool:
    return bool(score is not None and not pd.isna(score) and score >= threshold)


def _current_market_regime_score(frame: pd.DataFrame) -> float | None:
    if frame.empty or "market_regime_score" not in frame.columns:
        return None
    value = pd.to_numeric(frame["market_regime_score"], errors="coerce").dropna()
    if value.empty:
        return None
    return float(value.iloc[-1])


def _cash_drag_proxy(exposure: pd.Series, benchmark_return: pd.Series) -> float | None:
    aligned = pd.DataFrame({"exposure": exposure, "benchmark": benchmark_return}).dropna()
    if aligned.empty:
        return None
    positive_benchmark = aligned["benchmark"].clip(lower=0)
    return float(((1.0 - aligned["exposure"]) * positive_benchmark).mean())


def _max_drawdown_proxy(returns: pd.Series) -> float | None:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return None
    equity = (1.0 + values).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    return float(drawdown.min())


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    data = pd.DataFrame(
        {"value": pd.to_numeric(values, errors="coerce"), "weight": pd.to_numeric(weights, errors="coerce")}
    ).dropna()
    if data.empty:
        return None
    total_weight = float(data["weight"].sum())
    if total_weight <= 0:
        return float(data["value"].mean())
    return float((data["value"] * data["weight"]).sum() / total_weight)


def _mean_or_none(values: pd.Series) -> float | None:
    data = pd.to_numeric(values, errors="coerce").dropna()
    if data.empty:
        return None
    return float(data.mean())


def _sub_or_none(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left - right)


def _num(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _coalesce_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series(pd.NA, index=frame.index, dtype="Float64")
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        result = result.where(result.notna(), values)
    return result


def _joined_text(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    parts = []
    for column in columns:
        if column in frame.columns:
            data = frame[column]
            if isinstance(data, pd.DataFrame):
                data = data.iloc[:, 0]
            parts.append(data.fillna("").astype(str))
    if not parts:
        return pd.Series("", index=frame.index)
    result = parts[0]
    for part in parts[1:]:
        result = result + " " + part
    return result


def _read_all_reports(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = []
    for path in sorted(report_dir.glob(pattern)):
        try:
            frame = pd.read_csv(path)
        except (OSError, pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        frame = frame.copy()
        frame["_source_file"] = path.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _with_trade_date(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "trade_date" in output.columns:
        return output
    output["trade_date"] = output.get("_source_file", "").astype(str).str.extract(r"(\d{8})", expand=False)
    return output


def _normalize_dates(frame: pd.DataFrame, selected_date: pd.Timestamp | None) -> pd.DataFrame:
    output = frame.copy()
    output["trade_date"] = pd.to_datetime(output["trade_date"], errors="coerce")
    output = output.dropna(subset=["trade_date"])
    if selected_date is not None:
        output = output[output["trade_date"] <= selected_date]
    return output.sort_values(["trade_date", "_source_file"] if "_source_file" in output.columns else ["trade_date"])


def _last_by_date(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)


def _resolve_trade_date(report_dir: Path, trade_date: str | None) -> pd.Timestamp | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        return None if pd.isna(parsed) else parsed.normalize()
    for pattern in ["daily_summary_*.csv", "market_regime_*.csv", "candidates_*.csv"]:
        files = sorted(report_dir.glob(pattern))
        if not files:
            continue
        try:
            frame = pd.read_csv(files[-1], nrows=1)
        except (OSError, pd.errors.EmptyDataError, UnicodeDecodeError):
            continue
        if "trade_date" in frame.columns and not frame.empty:
            parsed = pd.to_datetime(frame.iloc[0].get("trade_date"), errors="coerce")
            if pd.notna(parsed):
                return parsed.normalize()
        parsed = pd.to_datetime(files[-1].stem[-8:], format="%Y%m%d", errors="coerce")
        if pd.notna(parsed):
            return parsed.normalize()
    return None


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _frame_date(frame: pd.DataFrame, *, first: bool) -> str:
    if frame.empty or "trade_date" not in frame.columns:
        return ""
    value = frame["trade_date"].iloc[0 if first else -1]
    return _date_text(value)
